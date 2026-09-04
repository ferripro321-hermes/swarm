"""Check DNS rotation on nordhold socks endpoints + throughput through one.

Usage: NORD_USER=... NORD_PASS=... .venv/bin/python scripts/nordhold_rotation.py
"""

from __future__ import annotations

import asyncio
import os
import socket
import time

import aiohttp
from aiohttp_socks import ProxyConnector

from swarm.proxies.bench import bench_proxy

USER = os.environ.get("NORD_USER", "")
PASS = os.environ.get("NORD_PASS", "")
HOSTS = ["amsterdam.nl.socks.nordhold.net", "nl.socks.nordhold.net",
         "se.socks.nordhold.net", "stockholm.se.socks.nordhold.net"]

print("== DNS rotation (6 samples, 2s apart) ==")
seen: dict[str, set[str]] = {}
for i in range(6):
    for h in HOSTS:
        try:
            ip = socket.gethostbyname(h)
            seen.setdefault(h, set()).add(ip)
        except OSError:
            pass
    time.sleep(2)
for h, ips in seen.items():
    tag = "ROTATES ✅" if len(ips) > 1 else "static"
    print(f"  {h:42s} {len(ips)} distinct IPs  {sorted(ips)[:4]} {tag}")


async def main() -> None:
    print("\n== throughput through nl endpoint (Swarm bench, 3MB cap) ==")
    url = f"socks5://{USER}:{PASS}@nl.socks.nordhold.net:1080"
    t0 = time.monotonic()
    r = await bench_proxy(url, speed_cap_mb=3.0, speed_timeout_s=25.0)
    dt = time.monotonic() - t0
    print(f"  ok={r.ok} stage={r.stage_failed} latency={r.latency_ms and round(r.latency_ms)}ms "
          f"kbps={r.throughput_kbps and round(r.throughput_kbps)} ({dt:.0f}s) err={r.error}")
    print("\n== same, amsterdam endpoint ==")
    url2 = f"socks5://{USER}:{PASS}@amsterdam.nl.socks.nordhold.net:1080"
    r2 = await bench_proxy(url2, speed_cap_mb=3.0, speed_timeout_s=25.0)
    print(f"  ok={r2.ok} stage={r2.stage_failed} latency={r2.latency_ms and round(r2.latency_ms)}ms "
          f"kbps={r2.throughput_kbps and round(r2.throughput_kbps)} err={r2.error}")


asyncio.run(main())
