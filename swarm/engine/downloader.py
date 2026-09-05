"""Chunk worker: downloads + decrypts one file through leased proxies.

The worker owns no network session of its own — `cdn_get` (and the MegaClient
inside it) are injected, making every path testable. On quota errors (509 /
-4) the current proxy is released as `quota`, a new one is leased, and the
download continues at the next unfinished chunk. Chunks are independent, so
resume == skipping finished chunks.
"""

from __future__ import annotations

import asyncio
import enum
import time
from pathlib import Path

from swarm.providers.mega import (
    ParsedLink,
    chunk_table,
    ctr_crypt,
    prepare_key,
)
from swarm.providers.mega_api import FileSpec


class DownloadResult(enum.Enum):
    COMPLETE = "complete"
    PENDING = "pending"      # no proxy available now; resumable
    FAILED = "failed"        # hard, non-quota failure
    CANCELLED = "cancelled"


class Lease:
    """Opaque handle returned by the pool; the pool fills `proxy`."""

    def __init__(self, proxy: str | None):
        self.proxy = proxy

    def report(self, outcome: str) -> None:  # pragma: no cover - pool fills
        raise NotImplementedError


class ChunkWorker:
    def __init__(
        self,
        spec: FileSpec,
        pool,
        cdn_get,                      # async (url, headers, proxy) -> resp(200: read())
        dest: str | Path,
        chunks_state: str = "",
        on_progress=None,             # callable(bytes_done_delta)
        chunk_timeout_s: float = 30.0,
        max_lease_wait_s: float = 120.0,
        mega_client=None,             # for URL re-fetch after rotation (phase 3)
    ):
        self.spec = spec
        self.pool = pool
        self.cdn_get = cdn_get
        self.dest = Path(dest)
        self.dest.mkdir(parents=True, exist_ok=True)
        self.table = chunk_table(spec.size)
        self.chunks_state = list(chunks_state) if chunks_state else ["0"] * len(self.table)
        if len(self.chunks_state) < len(self.table):
            self.chunks_state += ["0"] * (len(self.table) - len(self.chunks_state))
        self.on_progress = on_progress
        self.chunk_timeout_s = chunk_timeout_s
        self.max_lease_wait_s = max_lease_wait_s
        self.mega_client = mega_client
        self.path = self.dest / Path(spec.relpath).name
        self._cancel = asyncio.Event()

    def cancel(self):
        self._cancel.set()

    def _bytes_done(self) -> int:
        return sum(ln for bit, (_, ln) in zip(self.chunks_state, self.table) if bit == "1")

    def _write_chunk(self, offset: int, data: bytes) -> None:
        with open(self.path, "r+b" if self.path.exists() else "wb") as f:
            f.seek(offset)
            f.write(data)

    async def _lease_with_wait(self, exclude: set[str] | None = None):
        deadline = time.monotonic() + self.max_lease_wait_s
        while time.monotonic() < deadline:
            if self._cancel.is_set():
                return None
            lease = await self.pool.lease(exclude=exclude)
            if lease is not None:
                return lease
            await asyncio.sleep(0.25)
        return None

    async def _fetch_chunk(self, url: str, offset: int, length: int, proxy: str) -> bytes:
        headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
        resp = await self.cdn_get(url, headers=headers, proxy=proxy)
        if resp.status == 509:
            raise QuotaSignal()
        if resp.status != 200:
            raise IOError(f"CDN HTTP {resp.status}")
        data = await asyncio.wait_for(resp.read(), timeout=self.chunk_timeout_s)
        if len(data) != length:
            raise IOError(f"short read: {len(data)} != {length}")
        return data

    async def _ensure_url(self, lease) -> str:
        """Return a CDN URL, re-fetching through the current proxy if needed.

        Folder files need the share handle as API context (&n=<share>):
        body {"a":"g","g":1,"ssl":2,"n":<file handle>}, URL &n=<share>.
        Single public file links use the p: form instead.
        """
        if self.spec.url is not None:
            return self.spec.url
        if self.mega_client is None:
            raise IOError("no url and no mega client to fetch one")
        if self.spec.share_handle:
            url, size = await self.mega_client.file_url(
                self.spec.handle, self.spec.share_handle, proxy=lease.proxy)
        else:
            spec = await self.mega_client.file_info(
                ParsedLink("file", self.spec.handle, self.spec.key), proxy=lease.proxy)
            url = spec.url
        self.spec.url = url
        return self.spec.url  # type: ignore[return-value]

    async def run(self) -> DownloadResult:
        aes_key, nonce, mac_key = prepare_key(self.spec.key)
        current: Lease | None = None
        url: str | None = None

        try:
            while any(b == "0" for b in self.chunks_state):
                if self._cancel.is_set():
                    return DownloadResult.CANCELLED

                if current is None:
                    current = await self._lease_with_wait()
                    if current is None:
                        return DownloadResult.PENDING
                    self.pool.release_placeholder(current) if hasattr(self.pool, "release_placeholder") else None
                    url = None  # force URL fetch through the new proxy

                if url is None:
                    try:
                        url = await self._ensure_url(current)
                    except QuotaSignal:
                        self.pool.release(current, "quota")
                        current = None
                        continue
                    except Exception:
                        self.pool.release(current, "fail")
                        current = None
                        continue

                # next unfinished chunk
                idx = self.chunks_state.index("0")
                offset, length = self.table[idx]

                try:
                    data = await self._fetch_chunk(url, offset, length, current.proxy)
                    plain = ctr_crypt(data, aes_key, nonce, offset)
                    self._write_chunk(offset, plain)
                    self.chunks_state[idx] = "1"
                    if self.on_progress:
                        self.on_progress(length)
                except QuotaSignal:
                    self.pool.release(current, "quota")
                    current = None
                    url = None
                    continue
                except (asyncio.TimeoutError, IOError, OSError):
                    self.pool.release(current, "fail")
                    current = None
                    url = None
                    continue

            # finished: optional MAC verification happens in the orchestrator
            return DownloadResult.COMPLETE
        finally:
            if current is not None:
                self.pool.release(current, "ok")


class QuotaSignal(Exception):
    """Raised by the fetch layer on HTTP 509 from the CDN."""
