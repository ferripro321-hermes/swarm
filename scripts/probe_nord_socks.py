"""Probe NordVPN SOCKS5 proxy endpoints: resolve, authenticate, report exit IP.

Usage: NORD_USER=... NORD_PASS=... .venv/bin/python scripts/probe_nord_socks.py [name,name2,...]
Default probes candidate name formats spread over ES/FR/DE/BE/NL.
"""

from __future__ import annotations

import asyncio
import os
import socket
import sys

import aiohttp
from aiohttp_socks import ProxyConnector

USER = os.environ.get("NORD_USER", "")
PASS = os.environ.get("NORD_PASS", "")

DEFAULT_CANDIDATES = [
    # old short format
    "es29", "nl120", "de100", "fr48",
    # current 3-4 digit + country format (samples)
    "es10", "es25", "es36", "es78", "es120",
    "de666", "de701", "de712",
    "nl636", "nl715",
    "fr539", "fr581",
    "be100", "ch128",
]


def resolves(name: str) -> bool:
    try:
        socket.gethostbyname(f"{name}.nordvpn.com")
        return True
    except OSError:
        return False


async def exit_ip(name: str, timeout_s: float = 15.0) -> tuple[str, str, str | None]:
    """Return (name, verdict, exit_ip_or_error)."""
    url = f"socks5://{USER}:{PASS}@{name}.nordvpn.com:1080"
    try:
        connector = ProxyConnector.from_url(url)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get("https://api.ipify.org?format=json",
                                   timeout=aiohttp.ClientTimeout(total=timeout_s)) as resp:
                ip = (await resp.json())["ip"]
                return name, "OK", ip
    except Exception as e:
        return name, "FAIL", str(e)[:90]


async def main() -> None:
    raw = sys.argv[1].split(",") if len(sys.argv) > 1 else DEFAULT_CANDIDATES
    alive = [n for n in raw if resolves(n)]
    dead_dns = [n for n in raw if not resolves(n)]
    print(f"resolving ({len(alive)}/{len(raw)}): {alive or 'none'}")
    print(f"NXDOMAIN: {dead_dns or 'none'}\n")
    if not USER or not PASS:
        print("no creds in NORD_USER/NORD_PASS — DNS check only")
        return

    async with aiohttp.ClientSession() as s:
        async with s.get("https://api.ipify.org?format=json") as r:
            print(f"own IP: {(await r.json())['ip']}\n")

    results = await asyncio.gather(*(exit_ip(n) for n in alive))
    for name, verdict, info in results:
        mark = "✅" if verdict == "OK" else "❌"
        print(f"{mark} {name:8s} {verdict:4s} {info}")


if __name__ == "__main__":
    asyncio.run(main())
