"""Chunk worker: downloads + decrypts one file through leased proxies.

The worker owns no network session of its own — `cdn_get` (and the MegaClient
inside it) are injected, making every path testable. On quota errors (509 /
-4) the current proxy is released as `quota`, a new one is leased, and the
download continues at the next unfinished chunk. Chunks are independent, so
resume == skipping finished chunks.
"""

from __future__ import annotations

import asyncio
import enum
import time
from pathlib import Path

from swarm.providers.mega import (
    ParsedLink,
    chunk_table,
    ctr_crypt,
    prepare_key,
)
from swarm.providers.mega_api import FileSpec


class DownloadResult(enum.Enum):
    COMPLETE = "complete"
    PENDING = "pending"      # no proxy available now; resumable
    FAILED = "failed"        # hard, non-quota failure
    CANCELLED = "cancelled"


class Lease:
    """Opaque handle returned by the pool; the pool fills `proxy`."""

    def __init__(self, proxy: str | None):
        self.proxy = proxy

    def report(self, outcome: str) -> None:  # pragma: no cover - pool fills
        raise NotImplementedError


class ChunkWorker:
    def __init__(
        self,
        spec: FileSpec,
        pool,
        cdn_get,                      # async (url, headers, proxy) -> resp(200: read())
        dest: str | Path,
        chunks_state: str = "",
        on_progress=None,             # callable(bytes_done_delta)
        on_chunk_done=None,           # callable(chunks_state_str) — persist resume state
        chunk_timeout_s: float = 30.0,
        max_lease_wait_s: float = 120.0,
        mega_client=None,             # for URL re-fetch after rotation (phase 3)
    ):
        self.spec = spec
        self.pool = pool
        self.cdn_get = cdn_get
        self.dest = Path(dest)
        self.dest.mkdir(parents=True, exist_ok=True)
        self.table = chunk_table(spec.size)
        self.chunks_state = list(chunks_state) if chunks_state else ["0"] * len(self.table)
        if len(self.chunks_state) < len(self.table):
            self.chunks_state += ["0"] * (len(self.table) - len(self.chunks_state))
        self.on_progress = on_progress
        self.on_chunk_done = on_chunk_done
        self.chunk_timeout_s = chunk_timeout_s
        self.max_lease_wait_s = max_lease_wait_s
        self.mega_client = mega_client
        self.path = self.dest / Path(spec.relpath).name
        self._cancel = asyncio.Event()

    def cancel(self):
        self._cancel.set()

    def _bytes_done(self) -> int:
        return sum(ln for bit, (_, ln) in zip(self.chunks_state, self.table) if bit == "1")

    def _write_chunk(self, offset: int, data: bytes) -> None:
        with open(self.path, "r+b" if self.path.exists() else "wb") as f:
            f.seek(offset)
            f.write(data)

    async def _lease_with_wait(self, exclude: set[str] | None = None):
        deadline = time.monotonic() + self.max_lease_wait_s
        while time.monotonic() < deadline:
            if self._cancel.is_set():
                return None
            lease = await self.pool.lease(exclude=exclude)
            if lease is not None:
                return lease
            await asyncio.sleep(0.25)
        return None

    async def _fetch_chunk(self, url: str, offset: int, length: int, proxy: str) -> bytes:
        headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
        resp = await self.cdn_get(url, headers=headers, proxy=proxy)
        if resp.status == 509:
            raise QuotaSignal()
        # MEGA CDN answers Range requests with 206 Partial Content (200 only
        # for full-body GETs) — both are success; anything else is an error.
        if resp.status not in (200, 206):
            raise IOError(f"CDN HTTP {resp.status}")
        data = await asyncio.wait_for(resp.read(), timeout=self.chunk_timeout_s)
        if len(data) != length:
            raise IOError(f"short read: {len(data)} != {length}")
        return data

    async def _ensure_url(self, lease) -> str:
        """Return a CDN URL, re-fetching through the current proxy if needed.

        Folder files need the share handle as API context (&n=<share>):
        body {"a":"g","g":1,"ssl":2,"n":<file handle>}, URL &n=<share>.
        Single public file links use the p: form instead.
        """
        if self.spec.url is not None:
            return self.spec.url
        if self.mega_client is None:
            raise IOError("no url and no mega client to fetch one")
        if self.spec.share_handle:
            url, size = await self.mega_client.file_url(
                self.spec.handle, self.spec.share_handle, proxy=lease.proxy)
        else:
            spec = await self.mega_client.file_info(
                ParsedLink("file", self.spec.handle, self.spec.key), proxy=lease.proxy)
            url = spec.url
        self.spec.url = url
        return self.spec.url  # type: ignore[return-value]

    async def run(self) -> DownloadResult:
        aes_key, nonce, mac_key = prepare_key(self.spec.key)
        current: Lease | None = None
        url: str | None = None

        try:
            while any(b == "0" for b in self.chunks_state):
                if self._cancel.is_set():
                    return DownloadResult.CANCELLED

                if current is None:
                    current = await self._lease_with_wait()
                    if current is None:
                        return DownloadResult.PENDING
                    self.pool.release_placeholder(current) if hasattr(self.pool, "release_placeholder") else None
                    url = None  # force URL fetch through the new proxy

                if url is None:
                    try:
                        url = await self._ensure_url(current)
                    except QuotaSignal:
                        self.pool.release(current, "quota")
                        current = None
                        continue
                    except Exception as e:
                        outcome = "throttle" if is_throttle_error(e) else "fail"
                        if outcome == "throttle":
                            from swarm.proxies.tls_connect import drop_cached_session
                            await drop_cached_session(current.proxy)
                        self.pool.release(current, outcome)
                        current = None
                        continue

                # next unfinished chunk
                idx = self.chunks_state.index("0")
                offset, length = self.table[idx]

                try:
                    data = await self._fetch_chunk(url, offset, length, current.proxy)
                    plain = ctr_crypt(data, aes_key, nonce, offset)
                    self._write_chunk(offset, plain)
                    self.chunks_state[idx] = "1"
                    if self.on_progress:
                        self.on_progress(length)
                    if self.on_chunk_done:
                        # persist chunk state so a crash/restart resumes free
                        self.on_chunk_done("".join(self.chunks_state))
                except QuotaSignal:
                    self.pool.release(current, "quota")
                    current = None
                    url = None
                    continue
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    # Nord rate-limits proxy AUTH per source IP (407/502/503):
                    # that's a temporary throttle, not a dead proxy — drop the
                    # (possibly poisoned) tunnel and give the exit a short
                    # cooldown instead of burning fail strikes.
                    if is_throttle_error(e):
                        from swarm.proxies.tls_connect import drop_cached_session
                        await drop_cached_session(current.proxy)
                        self.pool.release(current, "throttle")
                    else:
                        self.pool.release(current, "fail")
                    current = None
                    url = None
                    continue

            # finished: optional MAC verification happens in the orchestrator
            return DownloadResult.COMPLETE
        finally:
            if current is not None:
                self.pool.release(current, "ok")


class QuotaSignal(Exception):
    """Raised by the fetch layer on HTTP 509 from the CDN."""


class ThrottledError(Exception):
    """Proxy auth/connect throttled (Nord 407/502/503, 429) — not a dead proxy.

    The proxy stays graded; it just needs a short cooldown before more auth
    attempts. The worker releases the lease with outcome "throttle".
    """

    @classmethod
    def from_exc(cls, e: Exception) -> "ThrottledError":
        status = getattr(e, "status", None)
        return cls(f"proxy throttled (HTTP {status or 'err'})")


def is_throttle_error(e: Exception) -> bool:
    """True when an exception looks like proxy throttling, not a hard fail.

    Timeouts count: Nord's throttle window manifests as stalls/timeouts during
    the CONNECT/auth handshake (tunnels hang), so killing the endpoint for a
    timeout would let the throttle window destroy the pool. Truly dead
    endpoints fail with DNS/refused errors, which stay 'fail'.
    """
    import asyncio as _aio
    if isinstance(e, _aio.TimeoutError):
        return True
    status = getattr(e, "status", None)
    if status in (407, 502, 503, 429):
        return True
    text = str(e)[:200].lower()
    return ("407" in text and "authentication" in text) or "429" in text or "502" in text
