#!/usr/bin/env python3
"""QA: real-quota integration test.

Usage:
    .venv/bin/python scripts/qa_quota.py <mega-folder-link> [--max-requests N]

What it proves (against the REAL network):
  1. Folder parse + tree walk works against MEGA
  2. Proxies get fetched, benched, and used
  3. When a proxy burns (509/-4), rotation happens and the file continues
  4. Files finish MAC-verified (no chunk duplicates / corruption)

This is a manual QA tool — it downloads REAL bytes. Point it at a folder
you actually want, or Ctrl+C once you've seen enough rotations in the log.
"""

import argparse
import asyncio
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from swarm.config import load_settings
from swarm.store import Store
from swarm.proxies.pool import ProxyPool
from swarm.proxies.sources import fetch_all_sources
from swarm.proxies.bench import bench_proxy, grade
from swarm.providers.mega_api import MegaClient
from swarm.engine.downloader import ChunkWorker, DownloadResult
from swarm.engine.jobs import _http_cdn_get, grade_from


async def main(link: str, max_requests: int) -> None:
    settings = load_settings("config.yaml")
    store = Store("data/qa_swarm.db")
    pool = ProxyPool(store, ban_ttl_s=settings.proxy.ban_ttl_h * 3600,
                     fail_ban_after=settings.proxy.fail_ban_after)
    pool.prime_from_store()
    mega = MegaClient(timeout_s=20)

    # 1) Parse + inspect the link
    from swarm.providers.mega import parse_link
    parsed = parse_link(link)
    print(f"[1] parsed: {parsed.kind} {parsed.handle}")

    if parsed.kind == "file":
        spec = await mega.file_info(parsed)
        specs = [spec]
    else:
        specs = await mega.folder_tree(parsed)
        if not specs:
            print("!! folder yielded no files (keys undecryptable?)")
            return
    total_mb = sum(s.size for s in specs) / 1e6
    print(f"    {len(specs)} files, {total_mb:.1f} MB total")
    for s in specs[:5]:
        print(f"    - {s.name} ({s.size/1e6:.1f} MB)")

    # 2) Ensure some proxies exist
    stats = pool.stats()
    print(f"[2] pool: {stats}")
    if stats["ready"] == 0:
        print("    fetching sources + benching (this takes a few minutes)…")
        entries = await fetch_all_sources(settings.proxy.sources)
        for e in entries:
            store.upsert_proxy(e.url, protocol=e.protocol, source=e.source)
        urls = [e.url for e in entries][:400]  # cap for QA
        sem = asyncio.Semaphore(100)

        async def one(url):
            async with sem:
                r = await bench_proxy(
                    url,
                    connect_timeout_s=settings.proxy.bench.connect_timeout_s,
                    mega_timeout_s=settings.proxy.bench.mega_probe_timeout_s,
                    speed_cap_mb=0.5,
                    speed_timeout_s=8,
                    min_throughput_kbps=settings.proxy.bench.min_throughput_kbps,
                )
                pool.add_result(url, r.ok, grade_from(r), r.latency_ms, r.throughput_kbps)

        t0 = time.monotonic()
        await asyncio.gather(*(one(u) for u in urls))
        print(f"    benched {len(urls)} in {time.monotonic()-t0:.0f}s → {pool.stats()}")

    # 3) Download the first (or biggest) file with the worker — watch rotations
    spec = specs[0]
    dest = Path("data/qa_downloads")
    req_count = {"n": 0}

    orig_get = _http_cdn_get

    async def counting_get(url, headers=None, proxy=None):
        req_count["n"] += 1
        if req_count["n"] > max_requests:
            raise KeyboardInterrupt
        return await orig_get(url, headers, proxy)

    progress = {"bytes": 0, "last": 0.0}

    def on_progress(delta):
        progress["bytes"] += delta
        now = time.monotonic()
        if now - progress["last"] > 3:
            progress["last"] = now
            print(f"    ↓ {progress['bytes']/1e6:.1f} MB | pool {pool.stats()}")

    worker = ChunkWorker(spec, pool, cdn_get=counting_get, dest=dest,
                         chunk_timeout_s=settings.engine.chunk_timeout_s,
                         mega_client=mega, on_progress=on_progress)

    print(f"[3] downloading '{spec.name}' ({spec.size/1e6:.1f} MB) — Ctrl+C to stop")
    t0 = time.monotonic()
    try:
        result = await worker.run()
    except (KeyboardInterrupt, asyncio.CancelledError):
        worker.cancel()
        result = DownloadResult.CANCELLED
        print("\n    interrupted")

    print(f"[4] result: {result.value} | {req_count['n']} requests | "
          f"{progress['bytes']/1e6:.1f} MB in {time.monotonic()-t0:.0f}s")
    print(f"    events: {[e for e in store.get_events(limit=10)]}")
    print(f"    final pool: {pool.stats()}")

    rotations = sum(1 for e in store.get_events(limit=200) if e["kind"] == "quota")
    print(f"[5] quota rotations observed: {rotations}")
    await mega.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("link")
    ap.add_argument("--max-requests", type=int, default=100)
    args = ap.parse_args()
    try:
        asyncio.run(main(args.link, args.max_requests))
    except KeyboardInterrupt:
        print("\nstopped")
