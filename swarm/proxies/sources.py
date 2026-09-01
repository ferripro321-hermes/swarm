"""Proxy source fetchers and line parsers.

Supports:
- monosans/all.txt style:  proto://host:port  (or bare host:port -> http)
- MegaBasterd style:       *host:port (socks), user:pass@host:port
- arbitrary user lists
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import aiohttp

_PROXY_LINE = re.compile(
    r"^(?:(?P<proto>https?|socks[45])://)?"
    r"(?:(?P<user>[^:@/\s]+):(?P<pass>[^@/\s]+)@)?"
    r"(?P<host>[A-Za-z0-9._-]+):(?P<port>\d{1,5})$"
)


@dataclass
class ProxyEntry:
    url: str            # normalized: proto://[user:pass@]host:port
    protocol: str       # http | socks5 | socks4
    source: str = ""


def _normalize(proto: str, user: str | None, password: str | None,
               host: str, port: str) -> str:
    auth = f"{user}:{password}@" if user else ""
    return f"{proto}://{auth}{host}:{port}"


def parse_proxy_lines(text: str, source: str = "") -> list[ProxyEntry]:
    entries: dict[str, ProxyEntry] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        # legacy socks marker
        if line.startswith("*"):
            line = "socks5://" + line[1:]
        m = _PROXY_LINE.match(line)
        if not m:
            continue
        proto = (m.group("proto") or "http").lower()
        if proto == "https":
            proto = "http"  # public lists label CONNECT proxies as https; aiohttp uses http:// for them
        url = _normalize(proto, m.group("user"), m.group("pass"),
                         m.group("host"), m.group("port"))
        if url not in entries:
            entries[url] = ProxyEntry(url=url, protocol=proto, source=source)
    return list(entries.values())


async def fetch_source(url: str, timeout_s: float = 20.0) -> str:
    """Fetch a raw proxy list over HTTP(S)."""
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=timeout_s)
    ) as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                raise IOError(f"source {url} -> HTTP {resp.status}")
            return await resp.text()


async def fetch_all_sources(sources: list[str], timeout_s: float = 20.0) -> list[ProxyEntry]:
    """Fetch every source concurrently; failures are logged, not raised."""
    import asyncio

    async def one(url: str) -> list[ProxyEntry]:
        try:
            text = await fetch_source(url, timeout_s)
            return parse_proxy_lines(text, source=url)
        except Exception:
            return []

    results = await asyncio.gather(*(one(u) for u in sources))
    merged: dict[str, ProxyEntry] = {}
    for entries in results:
        for e in entries:
            merged.setdefault(e.url, e)
    return list(merged.values())
