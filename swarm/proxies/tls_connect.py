"""Proxy routing: one factory that turns a lease URL into the right session.

Three proxy dialects in the pool:
  socks5://...          -> aiohttp_socks ProxyConnector (tunnel at socket level)
  https://user:pass@... -> TLS-wrapped HTTP CONNECT (NordVPN port-89 style):
                           TLS to the PROXY with a permissive context (their
                           cert is self-signed), then CONNECT, then strict,
                           verified TLS to the DESTINATION (req.ssl is untouched
                           for the tunnel leg — aiohttp applies req.ssl via
                           start_tls after the CONNECT).
  http://...            -> plain HTTP CONNECT, native aiohttp.

Note: never pass proxy="socks5://..." to aiohttp requests — aiohttp speaks
HTTP at SOCKS ports. All SOCKS routing must go through ProxyConnector sessions.
"""

from __future__ import annotations

import ssl

import aiohttp

# Trust anchor-free context for the TLS leg TO the proxy (self-signed Nord
# proxies). Destination TLS stays strict — see module docstring.
def _permissive_ssl_context() -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.options |= ssl.OP_NO_COMPRESSION
    return ctx


class TLSProxyConnector(aiohttp.TCPConnector):
    """TCPConnector that relaxes TLS only on the leg to an https:// proxy.

    aiohttp builds the proxy request with ssl=req.ssl (connector.py:
    _update_proxy_auth_header_and_build_proxy_req). We swap in the permissive
    context after the parent builds it; the destination tunnel leg keeps the
    request's own (strict) ssl.
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs.setdefault("ssl", _permissive_ssl_context())
        super().__init__(*args, **kwargs)

    def _update_proxy_auth_header_and_build_proxy_req(self, req):  # type: ignore[override]
        proxy_req = super()._update_proxy_auth_header_and_build_proxy_req(req)
        try:
            proxy_req._ssl = _permissive_ssl_context()   # proxy leg only
        except AttributeError:                            # pragma: no cover
            pass
        return proxy_req


def is_socks(proxy_url: str | None) -> bool:
    return bool(proxy_url) and proxy_url.startswith(("socks5://", "socks4://"))


def is_tls_connect(proxy_url: str | None) -> bool:
    return bool(proxy_url) and proxy_url.startswith("https://")


def proxied_session(proxy_url: str | None,
                    timeout_s: float = 30.0) -> tuple[aiohttp.ClientSession, str | None]:
    """Build (session, per_request_proxy) for a lease.

    socks5  -> dedicated ProxyConnector session, no per-request proxy
    https   -> TLSProxyConnector session, per-request proxy=<url>
    http    -> plain session, per-request proxy=<url>
    None    -> plain session, no proxy
    Caller owns the session (async with / .close()).
    """
    timeout = aiohttp.ClientTimeout(total=timeout_s)
    if proxy_url and is_socks(proxy_url):
        from aiohttp_socks import ProxyConnector
        return aiohttp.ClientSession(
            connector=ProxyConnector.from_url(proxy_url), timeout=timeout), None
    if is_tls_connect(proxy_url):
        return aiohttp.ClientSession(
            connector=TLSProxyConnector(), timeout=timeout), proxy_url
    return aiohttp.ClientSession(timeout=timeout), proxy_url


# ── session cache: one tunnel per proxy, reused across chunks ──────────
# Repeatedly building a fresh TLS-CONNECT session per request hammers Nord's
# proxy auth endpoint — it rate-limits auth per source IP and answers 407 to
# even valid endpoints. Keep ONE warm session per proxy URL; keepalives make
# subsequent chunk GETs auth-free through the established tunnel.
import asyncio as _asyncio

_session_cache: dict[str, tuple[aiohttp.ClientSession, str | None]] = {}
_session_lock = _asyncio.Lock()
_SESSION_IDLE_TTL_S = 600.0
_last_used: dict[str, float] = {}


async def cached_session(proxy_url: str | None,
                         timeout_s: float = 30.0) -> tuple[aiohttp.ClientSession, str | None]:
    """Return a (possibly cached) session for this proxy.

    The cache is keyed by proxy URL; a closed session is transparently
    rebuilt. Periodic reaping drops sessions idle past _SESSION_IDLE_TTL_S.
    """
    import time
    if proxy_url is None:
        return proxied_session(None, timeout_s)
    async with _session_lock:
        entry = _session_cache.get(proxy_url)
        if entry is not None and entry[0].closed:
            entry = None
        if entry is None:
            entry = proxied_session(proxy_url, timeout_s)
            _session_cache[proxy_url] = entry
        _last_used[proxy_url] = time.monotonic()
        now = time.monotonic()
        stale = [u for u, t in _last_used.items()
                 if now - t > _SESSION_IDLE_TTL_S and u != proxy_url]
        for u in stale:
            sess, _ = _session_cache.pop(u, (None, None))
            if sess is not None and not sess.closed:
                try:
                    await sess.close()
                except Exception:
                    pass
            _last_used.pop(u, None)
        return entry


async def close_cached_sessions() -> None:
    async with _session_lock:
        for sess, _ in _session_cache.values():
            if not sess.closed:
                try:
                    await sess.close()
                except Exception:
                    pass
        _session_cache.clear()
        _last_used.clear()


async def drop_cached_session(proxy_url: str | None) -> None:
    """Discard one proxy's cached session (poisoned tunnel after a 407/502)."""
    if proxy_url is None:
        return
    async with _session_lock:
        sess, _ = _session_cache.pop(proxy_url, (None, None))
        _last_used.pop(proxy_url, None)
        if sess is not None and not sess.closed:
            try:
                await sess.close()
            except Exception:
                pass
