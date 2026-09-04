"""Discover which socks.nordhold.net endpoints exist for a country/city list,
then SOCKS-authenticate through the live ones and report exit IPs.

Usage: NORD_USER=... NORD_PASS=... .venv/bin/python scripts/discover_nordhold.py [suffix]
Default suffix list covers ES/FR/DE/BE/NL/CH + cities.
"""

from __future__ import annotations

import asyncio
import os
import socket

import aiohttp
from aiohttp_socks import ProxyConnector

USER = os.environ.get("NORD_USER", "")
PASS = os.environ.get("NORD_PASS", "")

COUNTRIES = ["es", "fr", "de", "be", "nl", "se", "ch", "at", "it", "pt", "pl", "cz", "lu", "dk", "no", "ie", "uk", "us"]
CITIES_ES = ["madrid", "barcelona", "valencia", "seville", "malaga", "bilbao"]
CITIES_FR = ["paris", "marseille", "lyon", "strasbourg", "lille", "toulouse"]
CITIES_DE = ["frankfurt", "berlin", "dusseldorf", "munich", "nuremberg", "stuttgart", "leipzig", "cologne", "hamburg", "darmstadt"]
CITIES_BE = ["brussels", "antwerp", "ghent"]
CITIES_NL = ["amsterdam", "rotterdam", "the-hague", "utrecht", "maastricht", "eindhoven"]
CITIES_OTHER = ["zurich", "geneva", "vienna", "milan", "rome", "lisbon", "warsaw", "prague", "dublin", "london", "stockholm", "copenhagen", "oslo", "luxembourg"]

CANDIDATES = [f"{c}.socks.nordhold.net" for c in COUNTRIES]
for cities in (CITIES_ES, CITIES_FR, CITIES_DE, CITIES_BE, CITIES_NL):
    cc = {"CITIES_ES": "es", "CITIES_FR": "fr", "CITIES_DE": "de",
          "CITIES_BE": "be", "CITIES_NL": "nl"}[cities.__name__] if False else None
for cc, cl in (("es", CITIES_ES), ("fr", CITIES_FR), ("de", CITIES_DE),
               ("be", CITIES_BE), ("nl", CITIES_NL), ("misc", CITIES_OTHER)):
    fixed = cc if cc != "misc" else None
    for city in cl:
        ccx = {"zurich": "ch", "geneva": "ch", "vienna": "at", "milan": "it",
               "rome": "it", "lisbon": "pt", "warsaw": "pl", "prague": "cz",
               "dublin": "ie", "london": "uk", "stockholm": "se",
               "copenhagen": "dk", "oslo": "no", "luxembourg": "lu"}.get(city) if fixed is None else fixed
        if ccx:
            CANDIDATES.append(f"{city}.{ccx}.socks.nordhold.net")


def resolve(host: str) -> list[str]:
    try:
        return socket.gethostbyname_ex(host)[2]
    except OSError:
        return []


async def exit_ip(host: str, timeout_s: float = 20.0) -> tuple[str, str, str | None]:
    url = f"socks5://{USER}:{PASS}@{host}:1080"
    try:
        connector = ProxyConnector.from_url(url)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get("https://api.ipify.org?format=json",
                                   timeout=aiohttp.ClientTimeout(total=timeout_s)) as resp:
                return host, "OK", (await resp.json())["ip"]
    except Exception as e:
        return host, "FAIL", f"{type(e).__name__}: {str(e)[:80]}"


async def main() -> None:
    print(f"candidate hosts: {len(CANDIDATES)}")
    live: dict[str, list[str]] = {}
    for host in CANDIDATES:
        ips = resolve(host)
        if ips:
            live[host] = ips
    print(f"resolve OK: {len(live)}\n")
    for host, ips in sorted(live.items()):
        print(f"  {host:42s} {','.join(ips[:3])}{'…' if len(ips) > 3 else ''} ({len(ips)} IPs)")

    if not USER:
        print("\n(no creds — DNS-only discovery)")
        return
    print("\nauthenticating through each…")
    results = await asyncio.gather(*(exit_ip(h) for h in sorted(live)))
    for host, verdict, info in results:
        mark = "✅" if verdict == "OK" else "❌"
        print(f"{mark} {host:42s} {verdict:4s} {info}")


if __name__ == "__main__":
    asyncio.run(main())
