"""Proxy benchmark pipeline.

Stages (each eliminates garbage cheaply before spending bandwidth):
  1. TCP/TLS reachability through the proxy (fast timeout)
  2. MEGA API reachability — a GET to g.api.mega.co.nz through the proxy proves
     it can actually reach MEGA (most public proxies pass generic checks but die here)
  3. Throughput: stream MEGA-owned bytes from the static MEGA edge asset
     (mega.nz/secureboot.js — stable name, no key, no transfer quota).

Why not a gfsN.storage.mega.nz host for stage 3? MEGA assigns per-download CDN
hosts dynamically via the API (and old names rot to NXDOMAIN); the static edge
serves real MEGA infrastructure bytes without burning any quota-metered call.
The actual download path still resolves its CDN URL fresh from the API per file.

Grading produces score 0-100; pool decisions consume it.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import aiohttp

from swarm.proxies.tls_connect import proxied_session
from swarm.engine.downloader import is_throttle_error

MEGA_API_PROBE = "https://g.api.mega.co.nz/cs?id=0"   # returns [-4]-ish JSON or error; status 200 is enough
SPEED_URL = "https://mega.nz/secureboot.js"            # static MEGA edge asset (~194 KB), no key/quota
SPEED_ASSET_APPROX_BYTES = 190 * 1024                  # asset size hint, for deciding single vs multi fill


@dataclass
class BenchResult:
    url: str
    ok: bool
    stage_failed: str | None = None   # connect | mega | speed
    latency_ms: float | None = None
    throughput_kbps: float | None = None
    error: str | None = None


async def _stage_connect(session_get, timeout_s: float) -> float | None:
    """Cheap GET to the MEGA API root through the (connector-bound) proxy; returns latency_ms or raises."""
    start = time.monotonic()
    async with session_get(
        "https://g.api.mega.co.nz/",
        timeout=aiohttp.ClientTimeout(total=timeout_s),
    ) as resp:
        await resp.read()
    return (time.monotonic() - start) * 1000.0


async def _stage_speed(session_get, cap_bytes: int, timeout_s: float,
                       speed_url: str = SPEED_URL) -> float:
    """Stream up to cap_bytes of MEGA edge bytes; return KB/s.

    The asset is ~194 KB, so caps larger than the asset are filled with
    successive cache-busted requests. Timing covers the whole stage (including
    per-request handshakes), i.e. real achievable throughput through the proxy.
    A cap not reached within timeout_s is fine — we grade what arrived; only
    less than one chunk (64 KB) total counts as unusable.
    """
    start = time.monotonic()
    total = 0
    buster = int(time.time())
    deadline = start + timeout_s
    while total < cap_bytes:
        remaining_s = deadline - time.monotonic()
        if remaining_s <= 0:
            break
        url = f"{speed_url}?_swarm_bench={buster}"
        buster += 1
        try:
            async with session_get(
                url, timeout=aiohttp.ClientTimeout(total=remaining_s),
            ) as resp:
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    take = min(len(chunk), cap_bytes - total)
                    total += take
                    if total >= cap_bytes:
                        break
        except (asyncio.TimeoutError, TimeoutError):
            break   # ran out of time — grade what arrived
        if total >= cap_bytes:
            break
    elapsed = time.monotonic() - start
    if elapsed <= 0 or total < 64 * 1024:      # less than one chunk -> unusable
        raise IOError(f"insufficient data ({total}B in {elapsed:.1f}s)")
    return (total / 1024.0) / elapsed          # KB/s


def grade(latency_ms: float | None, throughput_kbps: float | None,
          min_throughput_kbps: float = 250.0) -> float:
    """Score 0-100. 60% throughput (vs 5 MB/s reference), 40% latency (vs 3000ms).

    Strict on purpose: no throughput measurement -> 0 (an unmeasured proxy is
    not a proven one). Stage-3 failures therefore kill, which is correct given
    the speed target is plain MEGA edge bytes.
    """
    if throughput_kbps is None or throughput_kbps < min_throughput_kbps:
        return 0.0
    speed_norm = min(1.0, throughput_kbps / 5000.0)
    lat_norm = 1.0 if latency_ms is None else max(0.0, 1.0 - latency_ms / 3000.0)
    return round(60.0 * speed_norm + 40.0 * lat_norm, 1)


async def bench_proxy(url: str, connect_timeout_s: float = 5.0,
                      mega_timeout_s: float = 8.0, speed_cap_mb: float = 3.0,
                      speed_timeout_s: float = 10.0,
                      min_throughput_kbps: float = 250.0,
                      speed_url: str = SPEED_URL) -> BenchResult:
    try:
        session, per_req_proxy = proxied_session(url, timeout_s=(
            connect_timeout_s + mega_timeout_s + speed_timeout_s + 5))
    except Exception as e:
        return BenchResult(url, ok=False, stage_failed="connect", error=str(e)[:120])

    try:
        # Proxying is bound by the session's connector (+ per-request proxy for
        # http/https CONNECT dialects). Never proxy="socks5://..." per request —
        # aiohttp speaks HTTP at SOCKS ports.
        async with session:
            get = lambda u, **kw: session.get(u, proxy=per_req_proxy, **kw)  # noqa: E731

            # stage 1+2 combined: a GET to the MEGA API through the proxy
            try:
                latency = await _stage_connect(get, connect_timeout_s + mega_timeout_s)
            except Exception as e:
                # 407/502/503 = Nord auth throttling (per source IP), not a dead
                # endpoint — surfaced so the pool keeps the row's previous grade
                stage = "throttle" if is_throttle_error(e) else "mega"
                return BenchResult(url, ok=False, stage_failed=stage, error=str(e)[:120])

            # stage 3: throughput through the proxy against MEGA edge bytes
            try:
                kbps = await _stage_speed(get, int(speed_cap_mb * 1024 * 1024),
                                          speed_timeout_s, speed_url)
            except Exception:
                kbps = None

            score = grade(latency, kbps, min_throughput_kbps)
            if score <= 0:
                return BenchResult(url, ok=False, stage_failed="speed",
                                   latency_ms=latency, throughput_kbps=kbps,
                                   error="too slow")
            return BenchResult(url, ok=True, latency_ms=latency,
                               throughput_kbps=kbps)
    except Exception as e:
        return BenchResult(url, ok=False, stage_failed="connect", error=str(e)[:120])


async def bench_batch(urls: list[str], concurrency: int = 150, **kwargs) -> list[BenchResult]:
    """Benchmark many proxies with bounded concurrency."""
    sem = asyncio.Semaphore(concurrency)

    async def one(u: str) -> BenchResult:
        async with sem:
            return await bench_proxy(u, **kwargs)

    return list(await asyncio.gather(*(one(u) for u in urls)))
