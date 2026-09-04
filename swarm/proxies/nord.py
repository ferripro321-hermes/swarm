"""NordVPN proxy endpoint provider.

Two usable proxy surfaces for Nord service credentials (manual-config
user/pass, not the account email):

  1. SOCKS5        *.socks.nordhold.net:1080 — country + city hosts, static
                   small pool (es/fr/de socks hosts were retired by Nord;
                   nl/se/us remain).
  2. TLS CONNECT   port 89 on <server>.nordvpn.com for the subset of servers
                   whose API services include "proxy" (undocumented; ~5% of
                   legacy-standard servers). Routed by tls_connect.py as
                   https://user:pass@host:89.

Yields lease URLs ready for the pool; alive-ness is verified per refresh pass
by the engine's bench, and freshly-verified endpoints are re-queued even if a
previous pass marked them dead.
"""

from __future__ import annotations

import asyncio
import base64
import json
import socket
import ssl
import urllib.request

NORD_API = "https://api.nordvpn.com/v1/servers?limit=0&groups%5B%5D=legacy-standard"


class NordProvider:
    def __init__(self, user: str, password: str, *,
                 countries: list[str] | None = None,
                 nordhold_hosts: list[str] | None = None,
                 port89: bool = True,
                 scan_concurrency: int = 120,
                 probe_timeout_s: float = 12.0):
        self.user = user
        self.password = password
        self.countries = [c.upper() for c in (countries or ["ES", "FR", "DE", "BE", "NL", "SE"])]
        self.nordhold_hosts = nordhold_hosts or [
            "nl.socks.nordhold.net",
            "amsterdam.nl.socks.nordhold.net",
            "se.socks.nordhold.net",
            "stockholm.se.socks.nordhold.net",
        ]
        self.port89 = port89
        self.scan_concurrency = scan_concurrency
        self.probe_timeout_s = probe_timeout_s

    # ── helpers ────────────────────────────────────────────────────────
    @property
    def _token(self) -> str:
        return base64.b64encode(f"{self.user}:{self.password}".encode()).decode()

    def _socks_url(self, host: str) -> str:
        return f"socks5://{self.user}:{self.password}@{host}:1080"

    def _tls_url(self, host: str) -> str:
        return f"https://{self.user}:{self.password}@{host}:89"

    # ── nordhold SOCKS5 ───────────────────────────────────────────────
    async def nordhold_endpoints(self) -> list[str]:
        """SOCKS5 hosts that resolve — Nord's officially supported proxy list."""
        hosts = self.nordhold_hosts

        def resolves(host: str) -> bool:
            try:
                socket.gethostbyname(host)
                return True
            except OSError:
                return False

        loop = asyncio.get_running_loop()
        alive = await asyncio.gather(*(loop.run_in_executor(None, resolves, h) for h in hosts))
        return [self._socks_url(h) for h, ok in zip(hosts, alive) if ok]

    # ── port-89 TLS CONNECT ───────────────────────────────────────────
    async def _fetch_servers(self) -> list[dict]:
        """Proxy-enabled servers in target countries from Nord's public API."""

        def _fetch() -> list[dict]:
            req = urllib.request.Request(NORD_API, headers={"User-Agent": "Mozilla/5.0"})
            servers = json.load(urllib.request.urlopen(req, timeout=60))
            out = []
            for s in servers:
                if not s.get("locations") or s.get("status") not in (None, "online"):
                    continue
                cc = s["locations"][0]["country"]["code"]
                if cc not in self.countries:
                    continue
                if "proxy" not in [x.get("identifier") for x in s.get("services", [])]:
                    continue
                out.append({"hostname": s["hostname"], "ip": s["station"], "cc": cc})
            return out

        return await asyncio.get_running_loop().run_in_executor(None, _fetch)

    async def scan_port89(self) -> list[str]:
        """Probe port 89 on proxy-enabled servers; return alive TLS-CONNECT URLs."""
        servers = await self._fetch_servers()
        if not servers:
            return []
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        sem = asyncio.Semaphore(self.scan_concurrency)

        async def alive(s: dict) -> str | None:
            async with sem:
                try:
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection(s["ip"], 89, ssl=ctx,
                                                server_hostname=s["hostname"]),
                        timeout=self.probe_timeout_s)
                    writer.write((f"CONNECT api.ipify.org:80 HTTP/1.1\r\n"
                                  f"Host: api.ipify.org:80\r\n"
                                  f"Proxy-Authorization: Basic {self._token}\r\n\r\n").encode())
                    await writer.drain()
                    data = await asyncio.wait_for(reader.read(2048), timeout=self.probe_timeout_s)
                    writer.close()
                    try:
                        await writer.wait_closed()
                    except Exception:
                        pass
                    head = data.split(b"\r\n\r\n")[0].decode(errors="replace")
                    return self._tls_url(s["hostname"]) if " 200" in head else None
                except Exception:
                    return None

        results = await asyncio.gather(*(alive(s) for s in servers))
        return [u for u in results if u]

    # ── merged ────────────────────────────────────────────────────────
    async def endpoints(self) -> list[str]:
        """All verified Nord endpoints (socks5 + tls-connect), deduped."""
        tasks = [self.nordhold_endpoints()]
        if self.port89:
            tasks.append(self.scan_port89())
        groups = await asyncio.gather(*tasks)
        seen: dict[str, str] = {}
        for group in groups:
            for url in group:
                seen.setdefault(url, url)
        return list(seen)
