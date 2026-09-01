"""Proxy benchmark pipeline.

Stages (each eliminates garbage cheaply before spending bandwidth):
  1. TCP/TLS reachability through the proxy (fast timeout)
  2. MEGA API reachability — proves the proxy can actually reach g.api.mega.co.nz
     (most public proxies pass generic checks but die here)
  3. Throughput: stream a bounded payload from a MEGA CDN host

Grading produces score 0-100; pool decisions consume it.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import aiohttp
from aiohttp_socks import ProxyConnector

MEGA_API_PROBE = "https://g.api.mega.co.nz/cs?id=0"   # returns [-4]-ish JSON or error; status 200 is enough
SPEED_URL = "https://gfs301.storage.mega.nz/"          # CDN edge; Range-limited GET


@dataclass
class BenchResult:
    url: str
    ok: bool
    stage_failed: str | None = None   # connect | mega | speed
    latency_ms: float | None = None
    throughput_kbps: float | None = None
    error: str | None = None


async def _stage_connect(session_get, proxy: str, timeout_s: float) -> float | None:
    """Cheap HEAD-ish request; returns latency_ms or raises."""
    start = time.monotonic()
    async with session_get(
        "https://g.api.mega.co.nz/", proxy=proxy,
        timeout=aiohttp.ClientTimeout(total=timeout_s),
    ) as resp:
        await resp.read()
    return (time.monotonic() - start) * 1000.0


async def _stage_speed(session_get, proxy: str, cap_bytes: int, timeout_s: float) -> float:
    """Stream up to cap_bytes; return kbps (bytes/sec / 1024 * 1000... actual KB/s)."""
    start = time.monotonic()
    total = 0
    timeout = aiohttp.ClientTimeout(total=timeout_s)
    async with session_get(SPEED_URL, proxy=proxy, timeout=timeout,
                           headers={"Range": f"bytes=0-{cap_bytes - 1}"}) as resp:
        async for chunk in resp.content.iter_chunked(64 * 1024):
            total += len(chunk)
            if total >= cap_bytes:
                break
    elapsed = time.monotonic() - start
    if elapsed <= 0 or total < 64 * 1024:      # less than one chunk -> unusable
        raise IOError(f"insufficient data ({total}B)")
    return (total / 1024.0) / elapsed          # KB/s


def grade(latency_ms: float | None, throughput_kbps: float | None,
          min_throughput_kbps: float = 250.0) -> float:
    """Score 0-100. 60% throughput (vs 5 MB/s reference), 40% latency (vs 3000ms)."""
    if throughput_kbps is None or throughput_kbps < min_throughput_kbps:
        return 0.0
    speed_norm = min(1.0, throughput_kbps / 5000.0)
    lat_norm = 1.0 if latency_ms is None else max(0.0, 1.0 - latency_ms / 3000.0)
    return round(60.0 * speed_norm + 40.0 * lat_norm, 1)


async def bench_proxy(url: str, connect_timeout_s: float = 5.0,
                      mega_timeout_s: float = 8.0, speed_cap_mb: float = 3.0,
                      speed_timeout_s: float = 10.0,
                      min_throughput_kbps: float = 250.0) -> BenchResult:
    try:
        connector = ProxyConnector.from_url(url)
    except Exception as e:
        return BenchResult(url, ok=False, stage_failed="connect", error=str(e)[:120])

    timeout = aiohttp.ClientTimeout(total=connect_timeout_s + mega_timeout_s + speed_timeout_s + 5)
    try:
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            get = session.get

            # stage 1+2 combined: a GET to the MEGA API through the proxy
            try:
                latency = await _stage_connect(get, url, connect_timeout_s + mega_timeout_s)
            except Exception as e:
                return BenchResult(url, ok=False, stage_failed="mega", error=str(e)[:120])

            # stage 3: throughput (best-effort; failure downgrades but proxies with
            # good latency still get graded by latency alone if speed endpoint fails)
            kbps: float | None = None
            try:
                kbps = await _stage_speed(get, url, int(speed_cap_mb * 1024 * 1024), speed_timeout_s)
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
