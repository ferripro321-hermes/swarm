"""Async MEGA API client: file info + folder tree over the cs endpoint.

A single `MegaClient` instance owns one aiohttp session per event loop.
`proxy` is injected per-call by the downloader (each worker uses its lease).
"""

from __future__ import annotations

import asyncio
import json
import secrets
from dataclasses import dataclass

import aiohttp

from swarm.providers.mega import (
    MEGA_API_URL,
    MegaError,
    ParsedLink,
    QuotaExceeded,
    base64url_decode,
    base64url_encode,
    decrypt_attr,
    prepare_key,
)


@dataclass
class FileSpec:
    handle: str
    key: bytes            # 32-byte file key
    size: int
    name: str
    url: str | None = None  # filled by file_info; folder nodes resolve lazily
    relpath: str = ""


class MegaClient:
    def __init__(self, session=None, timeout_s: float = 20.0):
        self._session = session          # injected (tests) or lazy aiohttp
        self._timeout_s = timeout_s
        self._seq = secrets.randbelow(2 ** 31)
        self._own_session = None

    async def _http(self):
        if self._session is not None:
            return self._session
        if self._own_session is None or self._own_session.closed:
            self._own_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self._timeout_s)
            )
        return self._own_session

    async def close(self):
        if self._own_session is not None and not self._own_session.closed:
            await self._own_session.close()

    async def api(self, payload: list | dict, handle: str | None = None,
                  proxy: str | None = None) -> list | dict:
        """POST one cs command batch; map MEGA numeric errors to exceptions.

        Routing per proxy dialect (see swarm.proxies.tls_connect):
          None           -> own session, direct
          socks5://...   -> temporary ProxyConnector session per call
          https://...    -> own session + TLSProxyConnector + per-request proxy
          http://...     -> own session + per-request proxy
        """
        self._seq += 1
        url = f"{MEGA_API_URL}?id={self._seq}"
        if handle:
            url += f"&n={handle}"
        body = json.dumps(payload if isinstance(payload, list) else [payload])
        if proxy and proxy.startswith(("socks5://", "socks4://")):
            from swarm.proxies.tls_connect import proxied_session
            session, _ = proxied_session(proxy, timeout_s=self._timeout_s)
            try:
                return await self._api_via(session, url, body)
            finally:
                await session.close()
        if proxy:
            session = await self._with_tls_connector(proxy)
            try:
                return await self._api_via(session, url, body, proxy=proxy)
            finally:
                await session.close()
        http = await self._http()
        return await self._api_via(http, url, body)

    async def _with_tls_connector(self, proxy: str):
        from swarm.proxies.tls_connect import TLSProxyConnector
        return aiohttp.ClientSession(
            connector=TLSProxyConnector(),
            timeout=aiohttp.ClientTimeout(total=self._timeout_s))

    async def _api_via(self, http, url: str, body: str, proxy: str | None = None) -> list | dict:
        async with http.post(url, data=body, proxy=proxy,
                             timeout=aiohttp.ClientTimeout(total=self._timeout_s)) as resp:
            if resp.status == 509:
                raise QuotaExceeded()
            text = await resp.text()
        data = json.loads(text)
        if isinstance(data, (int, float)):
            self._raise(int(data))
        if not isinstance(data, list) or not data:
            raise MegaError(-1, f"unexpected API response: {str(data)[:100]}")
        first = data[0]
        if not isinstance(first, (dict, list)):
            self._raise(int(first))  # numeric error code inside the batch
        if isinstance(first, dict):
            return first
        return first

    @staticmethod
    def _raise(code: int):
        if code in (-4, -11):
            raise QuotaExceeded()
        raise MegaError(code)

    # ── file links ─────────────────────────────────────────────────────
    async def file_info(self, link: ParsedLink, proxy: str | None = None) -> FileSpec:
        resp = await self.api({"a": "g", "p": link.handle, "ssl": 2},
                              handle=link.handle, proxy=proxy)
        if isinstance(resp, int):
            self._raise(resp)
        aes_key, _, _ = prepare_key(link.key_bytes)
        attr = decrypt_attr(base64url_decode(resp["at"]), aes_key)
        return FileSpec(
            handle=link.handle,
            key=link.key_bytes,
            size=int(resp["s"]),
            name=attr.get("n", link.handle),
            url=resp.get("g"),
            relpath=attr.get("n", link.handle),
        )

    # ── folder links ───────────────────────────────────────────────────
    async def folder_tree(self, link: ParsedLink, proxy: str | None = None) -> list[FileSpec]:
        """Walk a public folder and return every file node with names/paths.

        Node decryption: the share 'ok' entry carries the folder master key
        encrypted with the link key; each node's 'k' field is
        '<handle>:<encrypted_node_key>' encrypted with that share key.
        """
        resp = await self.api({"a": "f", "c": 1, "r": 1}, handle=link.handle, proxy=proxy)
        if isinstance(resp, int):
            self._raise(resp)

        nodes = {n["h"]: n for n in resp.get("f", []) if isinstance(n, dict)}

        # Resolve the share key: 'ok' nodes hold AES-ECB(link_key) encrypted 16B key
        share_keys: dict[str, bytes] = {}
        for ok in resp.get("ok", []):
            try:
                dec = self._ecb_decrypt(base64url_decode(ok["k"]), link.key_bytes)
                share_keys[ok["h"]] = dec[:16]
            except Exception:
                continue

        out: list[FileSpec] = []
        for node in resp.get("f", []):
            if node.get("t") != 0:      # 0=file, 1=folder, 2=root
                continue
            handle = node["h"]
            key = self._node_key(node, share_keys)
            if key is None:
                continue
            aes_key, _, _ = prepare_key(key)
            try:
                attr = decrypt_attr(base64url_decode(node["a"]), aes_key)
            except Exception:
                continue
            name = attr.get("n")
            if not name:
                continue
            relpath = self._relpath(node, nodes, name)
            out.append(FileSpec(
                handle=handle,
                key=key,
                size=int(node.get("s", 0)),
                name=name,
                relpath=relpath,
            ))
        return out

    @staticmethod
    def _ecb_decrypt(data: bytes, key16: bytes) -> bytes:
        from Crypto.Cipher import AES
        return AES.new(key16, AES.MODE_ECB).decrypt(data)

    def _node_key(self, node: dict, share_keys: dict[str, bytes]) -> bytes | None:
        """Decrypt a node's 'k' entry. Returns the 32-byte file key or None."""
        raw = node.get("k", "")
        if not raw:
            return None
        # '<handle>:<b64key>[/<handle>:<b64key>...]'
        for part in raw.split("/"):
            if ":" not in part:
                continue
            kh, kb64 = part.split(":", 1)
            if kh != node["h"]:
                continue
            enc = base64url_decode(kb64)
            for sk in share_keys.values():
                try:
                    dec = self._ecb_decrypt(enc, sk)
                except Exception:
                    continue
                if len(dec) == 32 and any(dec):
                    return dec
            # Some public links embed nodes already decryptable with the share
            # key of the root; if none matched, skip (cannot decrypt).
        return None

    @staticmethod
    def _relpath(node: dict, nodes: dict[str, dict], name: str) -> str:
        parts = [name]
        parent = node.get("p")
        seen = set()
        while parent and parent not in seen:
            seen.add(parent)
            pn = nodes.get(parent)
            if pn is None or pn.get("t") == 2:   # root of the share
                break
            # parent folder names need their own key; try best-effort plain
            # (folder nodes store names encrypted with folder keys — when we
            # cannot decrypt them we keep the file name only).
            parts.append(pn.get("_name", ""))
            parent = pn.get("p")
        parts = [p for p in parts if p]
        return "/".join(reversed(parts)) if parts else name
