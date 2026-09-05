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
    key: bytes            # 32-byte file key (compkey)
    size: int
    name: str
    url: str | None = None  # filled by file_info; folder nodes resolve lazily
    relpath: str = ""
    share_handle: str | None = None   # folder share handle (folder links only)
    expected_mac: bytes | None = None  # file MAC = compkey[24:32]


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
        key = link.key_bytes
        # single-file links carry a 16-byte folded content key; store as
        # key||zeros so prepare_key() folds back to the same 16 bytes and
        # derives zero nonce + zero (absent) MAC
        if len(key) == 16:
            key = key + b"\x00" * 16
        content_key, nonce, mac8 = prepare_key(key)
        attr_key = key[:16]   # attrs decrypt with k[:16] (pre-fold), CBC zero-IV
        attr = decrypt_attr(base64url_decode(resp["at"]), attr_key)
        return FileSpec(
            handle=link.handle,
            key=key,
            size=int(resp["s"]),
            name=attr.get("n", link.handle),
            url=resp.get("g"),
            relpath=attr.get("n", link.handle),
            expected_mac=mac8,
        )

    # ── folder links ───────────────────────────────────────────────────
    async def folder_tree(self, link: ParsedLink, proxy: str | None = None) -> list[FileSpec]:
        """Walk a public folder and return every file node with names/paths.

        Verified against live shares (megajs/go-mega semantics):
          - share key (if 'ok' entries exist): AES-ECB(link_key, ok.k)[:16]
          - else the root folder node's own k IS share-encrypted with the link key
          - each file node's k blob: AES-ECB-decrypt with the share key OR the
            raw link key (shares differ) -> 32-byte compkey
          - content/CTR key = compkey[:16] XOR compkey[16:32] (the fold)
          - attrs decrypt CBC zero-IV with the FOLDED key
          - expected file MAC = compkey[24:32]
        """
        resp = await self.api({"a": "f", "c": 1, "r": 1}, handle=link.handle, proxy=proxy)
        if isinstance(resp, int):
            self._raise(resp)
        if not isinstance(resp, dict):
            raise ValueError(f"unexpected folder response type: {type(resp).__name__}")

        nodes = {n["h"]: n for n in resp.get("f", []) if isinstance(n, dict)}

        # Candidate key-decryptors, in order: 'ok' share keys, root-node key,
        # raw link key (some shares encrypt file k-entries directly with it).
        cands: list[bytes] = []
        for ok in resp.get("ok", []):
            try:
                cands.append(self._ecb_decrypt(base64url_decode(ok["k"]), link.key_bytes)[:16])
            except Exception:
                continue
        for n in resp.get("f", []):
            if n.get("t") == 1 and n.get("k"):
                try:
                    cands.append(self._ecb_decrypt(base64url_decode(n["k"]), link.key_bytes)[:16])
                except Exception:
                    continue
        cands.append(link.key_bytes)
        # dedupe, keep order
        cands = list(dict.fromkeys(cands))

        from swarm.providers.mega import fold_key
        out: list[FileSpec] = []
        tree_root = next((n["h"] for n in resp.get("f", [])
                          if n.get("t") == 1 and n.get("p") == link.handle), link.handle)
        for node in resp.get("f", []):
            if node.get("t") != 0:      # 0=file, 1=folder, 2=root
                continue
            if not node.get("k") or not node.get("a"):
                continue
            spec = self._decrypt_file_node(node, cands, nodes, tree_root)
            if spec is not None:
                out.append(spec)
        return out

    def _decrypt_file_node(self, node: dict, key_cands: list[bytes],
                           nodes: dict[str, dict], tree_root: str) -> FileSpec | None:
        """Try every candidate key on this node's k blob; attr-decrypt decides."""
        from swarm.providers.mega import fold_key
        attr_blob = base64url_decode(node["a"])
        for kc in key_cands:
            try:
                enc = base64url_decode(node["k"].split(":", 1)[1])
                compkey = self._ecb_decrypt(enc, kc)
                if len(compkey) != 32 or not any(compkey):
                    continue
                content = fold_key(compkey)
                attr = decrypt_attr(attr_blob, content)
                name = attr.get("n")
                if not name:
                    continue
                return FileSpec(
                    handle=node["h"],
                    key=compkey,
                    size=int(node.get("s", 0)),
                    name=name,
                    relpath=self._relpath(node, nodes, name),
                    share_handle=tree_root,
                    expected_mac=compkey[24:32],
                )
            except Exception:
                continue
        return None

    async def file_url(self, handle: str, share_handle: str | None = None,
                       proxy: str | None = None) -> tuple[str, int]:
        """CDN download URL for a file.

        Folder files: body {"a":"g","g":1,"ssl":2,"n":<file handle>} with the
        share handle as the API's &n= query context (verified). Single links
        use {"a":"g","p":<handle>}.
        Returns (url, size).
        """
        if share_handle:
            body = {"a": "g", "g": 1, "ssl": 2, "n": handle}
            ctx = share_handle
        else:
            body = {"a": "g", "p": handle, "ssl": 2}
            ctx = handle
        resp = await self.api(body, handle=ctx, proxy=proxy)
        if isinstance(resp, int):
            self._raise(resp)
        if not isinstance(resp, dict):
            raise MegaError(-1, f"unexpected a:g response: {str(resp)[:120]}")
        url = resp.get("g")
        if not url:
            raise MegaError(-1, f"no CDN url in a:g response: {str(resp)[:120]}")
        return str(url), int(resp.get("s", 0))

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
