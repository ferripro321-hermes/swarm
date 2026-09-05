"""Job orchestrator: link -> FileSpecs -> parallel ChunkWorkers -> verification.

One asyncio engine thread owns everything. Flask talks to it via
run_coroutine_threadsafe. Jobs/files/proxies persist in the Store so a crash
resumes where it left off (chunks_state).
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

from swarm.providers.mega import (
    ParsedLink,
    chunk_table,
    fold_key,
    prepare_key,
)
from Crypto.Cipher import AES
from swarm.providers.mega_api import FileSpec, MegaClient
from swarm.proxies.pool import ProxyPool
from swarm.store import Store
from swarm.engine.downloader import ChunkWorker, DownloadResult


class Engine:
    def __init__(self, settings, store: Store, pool: ProxyPool):
        self.settings = settings
        self.store = store
        self.pool = pool
        self.mega = MegaClient(timeout_s=settings.engine.url_timeout_s)
        self._workers: dict[int, list[ChunkWorker]] = {}
        self._tasks: set[asyncio.Task] = set()
        self._stopped = False
        self._refresh_task: asyncio.Task | None = None
        self._nord_retry_at: dict[str, float] = {}   # url -> earliest re-bench time (throttle backoff)
        self._sched: dict[int, asyncio.Task] = {}    # job_id -> active schedule task
        self._dry_passes: int = 0                    # consecutive bench passes with 0 ready

    # ── job creation ───────────────────────────────────────────────────
    async def create_job(self, link: str, dest: str | None = None) -> int:
        parsed = ParsedLink(*_decompose(link))
        job_dest = Path(dest) if dest else Path(self.settings.downloads.dest) / _job_slug(parsed)
        job_id = self.store.create_job(link=link, dest=str(job_dest))
        self.store.add_event("job", f"job {job_id} created for {parsed.kind} link", job_id=job_id)

        if parsed.kind == "file":
            spec = await self.mega.file_info(parsed)
            self._register_file(job_id, spec, job_dest)
        else:
            specs = await self.mega.folder_tree(parsed)
            if not specs:
                raise ValueError("folder has no downloadable files (or keys undecryptable)")
            specs.sort(key=lambda s: -s.size)   # biggest first
            for spec in specs:
                self._register_file(job_id, spec, job_dest)

        self.set_job_status_threadsafe(job_id, "queued")
        return job_id

    def _register_file(self, job_id: int, spec: FileSpec, job_dest: Path) -> int:
        return self.store.add_file(
            job_id, name=spec.name, size=spec.size,
            handle=spec.handle, key=spec.key.hex(),
            relpath=spec.relpath or spec.name,
            share_handle=spec.share_handle,
            expected_mac=spec.expected_mac.hex() if spec.expected_mac else "",
        )

    def set_job_status_threadsafe(self, job_id: int, status: str) -> None:
        self.store.set_job_status(job_id, status)

    # ── running ────────────────────────────────────────────────────────
    async def start_job(self, job_id: int) -> None:
        job = self.store.get_job(job_id)
        if job is None:
            raise ValueError(f"job {job_id} not found")
        if job["status"] in ("running", "done"):
            return
        self.store.set_job_status(job_id, "running")
        self.store.add_event("job", f"job {job_id} started", job_id=job_id)
        # schedule-and-return: the API must not block until the job drains
        prev = self._sched.get(job_id)
        if prev is not None and not prev.done():
            return          # a pass is already active — never double-schedule
        t = asyncio.create_task(self._schedule_job(job_id))
        self._sched[job_id] = t
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    async def _schedule_job(self, job_id: int) -> None:
        """One pass over pending files, bounded by max_parallel_files.

        After the pass, any failed files are re-queued and another pass runs
        (throttle cooldowns recover in 90 s) until the job drains.
        """
        try:
            await self._schedule_job_inner(job_id)
        finally:
            self._sched.pop(job_id, None)

    async def _schedule_job_inner(self, job_id: int) -> None:
        """One pass over pending files, bounded by max_parallel_files.

        After the pass, any failed files are re-queued and another pass runs
        (throttle cooldowns recover in 90 s) until the job drains.
        """
        job = self.store.get_job(job_id)
        if job is None or self._stopped:
            return
        pending = [f for f in job["files"] if f["status"] in ("pending", "downloading", "failed")]
        sem = asyncio.Semaphore(self.settings.engine.max_parallel_files)

        async def run_one(f: dict) -> None:
            async with sem:
                await self._run_file(job_id, f)

        if pending:
            await asyncio.gather(*(run_one(f) for f in pending))
        job = self.store.get_job(job_id)
        if job is None or self._stopped:
            return
        retry = [f for f in job["files"] if f["status"] in ("failed", "pending")]
        if retry:
            for f in retry:
                if f["status"] == "failed":
                    self.store.set_file_status(f["id"], "pending")
            self.store.set_job_status(job_id, "running")
            self.store.add_event("job", f"job {job_id}: re-queued {len(retry)} files for another pass")
            await asyncio.sleep(90)   # let throttle cooldowns expire
            if not self._stopped:
                await self._schedule_job_inner(job_id)

    async def _run_file(self, job_id: int, file_row: dict) -> None:
        file_id = file_row["id"]
        job = self.store.get_job(job_id)
        if job is None:
            return
        self.store.set_file_status(file_id, "downloading")
        spec = FileSpec(
            handle=file_row["handle"],
            key=bytes.fromhex(file_row["key"]),
            size=file_row["size"],
            name=file_row["name"],
            url=None,
            relpath=file_row["relpath"],
            share_handle=file_row["share_handle"] or None,
            expected_mac=(bytes.fromhex(file_row["expected_mac"])
                          if file_row.get("expected_mac") else None),
        )
        worker = ChunkWorker(
            spec, self.pool,
            cdn_get=_http_cdn_get,
            dest=job["dest"],
            chunks_state=file_row["chunks_state"],
            chunk_timeout_s=self.settings.engine.chunk_timeout_s,
            mega_client=self.mega,
        )
        self._workers.setdefault(job_id, []).append(worker)
        try:
            result = await worker.run()
            if result == DownloadResult.COMPLETE:
                self.store.set_file_status(file_id, "verifying")
                ok, err = self._verify_file(spec, Path(job["dest"]) / Path(spec.relpath).name)
                self.store.set_file_status(file_id, "done" if ok else "corrupt", err)
                self.store.add_event("file", f"{spec.name}: {'MAC OK' if ok else 'MAC FAIL ' + str(err)}",
                                     job_id=job_id, file_id=file_id)
            elif result == DownloadResult.PENDING:
                self.store.set_file_status(file_id, "failed", "no proxies available")
                self.store.add_event("file", f"{spec.name}: paused (no proxies)", job_id=job_id, file_id=file_id)
            else:
                self.store.set_file_status(file_id, "failed", result.value)
        except Exception as e:
            self.store.set_file_status(file_id, "failed", str(e)[:200])
            self.store.add_event("error", f"{spec.name}: {e}", job_id=job_id, file_id=file_id)
        finally:
            self._workers.get(job_id, []).remove(worker)
            # job finished? mark it
            job = self.store.get_job(job_id)
            if job and all(f["status"] in ("done", "corrupt", "cancelled") for f in job["files"]):
                self.store.set_job_status(job_id, "done" if all(f["status"] == "done" for f in job["files"]) else "failed")
                self.store.add_event("job", f"job {job_id} finished", job_id=job_id)

    def _verify_file(self, spec: FileSpec, path: Path) -> tuple[bool, str | None]:
        """Integrity check: size + streaming MAC over plaintext vs k[24:32].

        The file on disk is already CTR-decrypted, so this is MAC-only
        (megajs MAC class, byte-verified 2026-09-05). If the key blob carries
        no expected MAC (single-file links unfold to zero MAC), size is all
        we can check.
        """
        if not path.exists() or path.stat().st_size != spec.size:
            return False, "size mismatch"
        _, nonce, expected = prepare_key(spec.key)
        if not any(expected):
            return True, None
        aes_key = fold_key(spec.key)
        seed = nonce + nonce          # every segment reseeds to nonce||nonce

        # megajs MAC schedule: snapshot the running chain every posNext bytes,
        # where increments grow by 128K up to 1M (NOT the download chunking —
        # byte-exact validated 2026-09-05 via scripts/debug_mac.py).
        # The chain IS a CBC encryption (c_i = E(p_i ⊕ c_{i-1}), c_0 = seed),
        # so each segment is one CBC call and its last block is the snapshot.
        snaps: list[bytes] = []
        pos = 0
        pos_next, increment = 131072, 131072
        with open(path, "rb") as f:
            while pos < spec.size:
                seg = min(pos_next - pos, spec.size - pos)
                data = f.read(seg)
                if len(data) != seg:
                    return False, "short read during MAC"
                if len(data) % 16:
                    data += b"\x00" * (16 - len(data) % 16)
                snaps.append(AES.new(aes_key, AES.MODE_CBC, seed).encrypt(data)[-16:])
                pos += seg
                if pos == pos_next and pos < spec.size:
                    if increment < 1048576:
                        increment += 131072
                    pos_next += increment
        if pos == pos_next:       # EOF exactly on a boundary: megajs's post-loop
            snaps.append(bytes(seed))  # append captures the reseeded (fresh) mac
        # condense (megajs): XOR-fold snapshots w/ one ECB encrypt per fold
        ecb_fold = AES.new(aes_key, AES.MODE_ECB)
        accb = bytearray(16)
        for m in snaps:
            for j in range(16):
                accb[j] ^= m[j]
            accb = bytearray(ecb_fold.encrypt(bytes(accb)))
        w = [int.from_bytes(bytes(accb[i:i + 4]), "big") for i in (0, 4, 8, 12)]
        computed = (w[0] ^ w[1]).to_bytes(4, "big") + (w[2] ^ w[3]).to_bytes(4, "big")
        if computed != expected:
            return False, f"MAC mismatch: {computed.hex()} != {expected.hex()}"
        return True, None

    # ── control ────────────────────────────────────────────────────────
    async def resume_stalled_jobs(self) -> None:
        """Boot-time recovery: jobs left 'running' by a crash/restart get
        their scheduler restarted (chunk state on disk resumes for free)."""
        for job in self.store.list_jobs(limit=100):
            if job["status"] == "running":
                self.store.set_job_status(job["id"], "queued")
                self.store.add_event("job", f"job {job['id']}: resuming after restart")
                try:
                    await self.start_job(job["id"])
                except Exception as e:
                    self.store.add_event("error", f"resume job {job['id']}: {e}")

    async def pause_job(self, job_id: int) -> None:
        for w in self._workers.get(job_id, []):
            w.cancel()
        self.store.set_job_status(job_id, "paused")

    async def cancel_job(self, job_id: int) -> None:
        for w in self._workers.get(job_id, []):
            w.cancel()
        self.store.set_job_status(job_id, "cancelled")
        job = self.store.get_job(job_id)
        for f in job["files"]:
            if f["status"] not in ("done",):
                self.store.set_file_status(f["id"], "cancelled")

    async def refresh_proxies_forever(self) -> None:
        """Background: fetch sources + bench new proxies, forever."""
        from swarm.proxies.sources import fetch_all_sources
        from swarm.proxies.bench import bench_proxy

        cfg = self.settings.proxy
        nord = None
        nord_cfg = getattr(self.settings, "nord", None)
        if nord_cfg and nord_cfg.enabled and cfg.mode == "nord":
            from swarm.proxies.nord import NordProvider
            nord = NordProvider(nord_cfg.user, nord_cfg.password,
                                countries=nord_cfg.countries,
                                port89=nord_cfg.port89,
                                scan_concurrency=nord_cfg.scan_concurrency)
            self.store.add_event("proxy", f"nord provider enabled (mode=nord, max_leases={nord_cfg.max_leases})")

        while not self._stopped:
            try:
                # Public lists only in public mode — nord mode leases curated
                # Nord endpoints exclusively (either/or, no mixing).
                if cfg.mode == "public":
                    entries = await fetch_all_sources(cfg.sources)
                else:
                    entries = []
                    # Nord endpoints: alive-verified by their own probes, so a
                    # row marked dead (rate-limit blip) gets re-queued here
                    if nord is not None:
                        try:
                            nord_urls = await nord.endpoints()
                            now = time.time()
                            for u in nord_urls:
                                row = self.store.get_proxy(u)
                                if row is None:
                                    self.store.upsert_proxy(
                                        u, protocol=("socks5" if u.startswith("socks5") else "https"),
                                        source="nord")
                                # re-queue dead rows for re-verification, but
                                # respect throttle backoff (auth rate-limit):
                                # hammering auth re-triggers the 407 flood
                                due = self._nord_retry_at.get(u, 0.0)
                                if (row is None or (row["state"] == "dead" and now >= due)) \
                                        and u not in self.pool._leased:
                                    self.store.set_proxy_state(u, "new")
                            self.store.add_event("proxy", f"nord endpoints alive: {len(nord_urls)}")
                        except Exception as e:
                            self.store.add_event("error", f"nord provider: {e}")
                fresh = [e for e in entries if self.store.get_proxy(e.url) is None]
                for e in entries:   # remember all (even old) for dedup
                    if e.url not in {p["url"] for p in self.store.list_proxies(limit=10) if False}:
                        self.store.upsert_proxy(e.url, protocol=e.protocol, source=e.source)
                self.store.add_event("proxy", f"sources fetched: {len(entries)} entries, {len(fresh)} new")
                # bench: unseen URLs + anything still sitting in 'new' (e.g. a
                # previous pass was interrupted, or states were reset by hand)
                bench_urls = [e.url for e in fresh]
                bench_urls += [p["url"] for p in self.store.get_proxies_by_state("new", limit=5000)]
                bench_urls = list(dict.fromkeys(bench_urls))   # dedupe, keep order

                def _is_nord(u: str) -> bool:
                    return "nordvpn.com:" in u or "nordhold.net:" in u

                nord_bench = [u for u in bench_urls if _is_nord(u)]
                public_bench = [u for u in bench_urls if not _is_nord(u)]

                async def bench_many(urls: list[str], conc: int) -> None:
                    sem = asyncio.Semaphore(conc)

                    async def one(url: str):
                        async with sem:
                            r = await bench_proxy(
                                url,
                                connect_timeout_s=cfg.bench.connect_timeout_s,
                                mega_timeout_s=cfg.bench.mega_probe_timeout_s,
                                speed_cap_mb=cfg.bench.speed_cap_mb,
                                speed_timeout_s=cfg.bench.speed_timeout_s,
                                min_throughput_kbps=cfg.bench.min_throughput_kbps,
                                speed_url=cfg.bench.speed_url,
                            )
                            throttled = getattr(r, "stage_failed", None) == "throttle"
                            self.pool.add_result(url, r.ok, grade_from(r),
                                                 r.latency_ms, r.throughput_kbps,
                                                 throttled=throttled)
                            if throttled:
                                self._nord_retry_at[url] = (
                                    time.time() + self.pool.throttle_skip_minutes(url) * 60)

                    await asyncio.gather(*(one(u) for u in urls))

                # public haystack: wide flood. Nord curated set: TRICKLE — at
                # most a few endpoints per pass, spaced ~1.5 s apart. Nord
                # rate-limits CONNECT-auth per source IP; probing the whole
                # set in one pass re-triggers the 407 window and scores the
                # healthy endpoints as dead/throttled.
                await bench_many(public_bench, 150)
                for u in nord_bench:
                    await bench_many([u], 1)
                    await asyncio.sleep(1.5)
                self.store.add_event("proxy", f"bench pass done; pool stats: {self.pool.stats()}")
            except Exception as e:
                self.store.add_event("error", f"refresh loop: {e}")
            # cadence: nord pool dry → rescan, but back off progressively —
            # hammering Nord's auth endpoint while rate-limited only extends
            # the throttle window (90 s → 3 → 6 → … cap 15 min). Healthy nord
            # → 5 min; public → refresh_min.
            if cfg.mode == "nord":
                stats = self.pool.stats()
                if stats.get("ready", 0) > 0:
                    self._dry_passes = 0
                    wait = 300.0
                else:
                    self._dry_passes += 1
                    wait = min(90.0 * (2 ** (self._dry_passes - 1)), 900.0)
            else:
                wait = cfg.refresh_min * 60
            await asyncio.sleep(wait)

    async def bench_new_now(self) -> None:
        """One-shot bench pass over 'new' proxies (manual trigger)."""
        from swarm.proxies.bench import bench_proxy
        from swarm.proxies.nord import is_nord_url
        cfg = self.settings.proxy
        pending = self.store.get_proxies_by_state("new", limit=5000)
        if cfg.mode == "nord":
            pending = [p for p in pending if is_nord_url(p["url"])]
        else:
            pending = [p for p in pending if not is_nord_url(p["url"])]

        nord_bench = [p["url"] for p in pending if is_nord_url(p["url"])]
        public_bench = [p["url"] for p in pending if not is_nord_url(p["url"])]
        # see refresh_proxies_forever: nord auth rate-limits per source IP —
        # manual passes trickle nord endpoints too
        for urls, conc in ((public_bench, 150), (nord_bench, 1)):
            sem = asyncio.Semaphore(conc)

            async def one(url: str):
                async with sem:
                    r = await bench_proxy(
                        url,
                        connect_timeout_s=cfg.bench.connect_timeout_s,
                        mega_timeout_s=cfg.bench.mega_probe_timeout_s,
                        speed_cap_mb=cfg.bench.speed_cap_mb,
                        speed_timeout_s=cfg.bench.speed_timeout_s,
                        min_throughput_kbps=cfg.bench.min_throughput_kbps,
                        speed_url=cfg.bench.speed_url,
                    )
                    throttled = getattr(r, "stage_failed", None) == "throttle"
                    self.pool.add_result(url, r.ok, grade_from(r), r.latency_ms,
                                         r.throughput_kbps, throttled=throttled)
                    if throttled:
                        self._nord_retry_at[url] = (
                            time.time() + self.pool.throttle_skip_minutes(url) * 60)

            await asyncio.gather(*(one(u) for u in urls))
        self.store.add_event("proxy", f"manual bench pass: {self.pool.stats()}")

    async def enqueue_source(self, url: str) -> None:
        """Add an extra proxy source and fetch+bench it now."""
        self.settings.proxy.sources.append(url)
        from swarm.proxies.sources import fetch_source, parse_proxy_lines
        text = await fetch_source(url)
        entries = parse_proxy_lines(text, source=url)
        for e in entries:
            self.store.upsert_proxy(e.url, protocol=e.protocol, source=e.source)
        self.store.add_event("proxy", f"source {url}: {len(entries)} proxies imported")
        await self.bench_new_now()

    async def shutdown(self) -> None:
        self._stopped = True
        for tasks in self._tasks:
            tasks.cancel()
        from swarm.proxies.tls_connect import close_cached_sessions
        try:
            await close_cached_sessions()
        except Exception:
            pass
        await self.mega.close()


def grade_from(result) -> float:
    """Map a BenchResult to a pool score (mirrors bench.grade but reads result fields)."""
    from swarm.proxies.bench import grade
    return grade(result.latency_ms, result.throughput_kbps)


async def _http_cdn_get(url, headers=None, proxy=None):
    """Real CDN GET through a leased proxy — all dialects (socks5/http/https-TLS).

    Uses the per-proxy session cache: one warm TLS-CONNECT tunnel per proxy
    (Nord rate-limits per-connection auth → per-chunk fresh tunnels 407).
    The session stays open across chunks; the response is released on context
    exit and the keepalive connection returns to the cached session's pool.
    """
    from swarm.proxies.tls_connect import cached_session

    session, per_req_proxy = await cached_session(proxy, timeout_s=60)
    if per_req_proxy:
        resp = await session.get(url, headers=headers, proxy=per_req_proxy)
    else:
        resp = await session.get(url, headers=headers)

    class _Resp:
        def __init__(self, resp):
            self._resp = resp
            self.status = resp.status
            self.content = resp.content

        async def read(self, n=-1):
            return await self._resp.read()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            await self._resp.release()   # keepalive conn returns to the cached session

    return _Resp(resp)


def _decompose(link: str) -> tuple[str, str, bytes]:
    """Return (kind, handle, key) from a link — note: NOT a ParsedLink."""
    from swarm.providers.mega import parse_link
    p = parse_link(link)
    return p.kind, p.handle, p.key_bytes


def _job_slug(parsed: ParsedLink) -> str:
    return f"{parsed.kind}-{parsed.handle}"
