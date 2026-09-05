"""Tests for the chunk worker: proxy use, 509/-4 rotation, resume, integrity."""

import asyncio
import json

import pytest

from swarm.providers.mega import (
    base64url_encode,
    chunk_table,
    ctr_crypt,
    fold_key,
    prepare_key,
    ParsedLink,
)
from swarm.providers.mega_api import FileSpec
from swarm.engine.downloader import ChunkWorker, DownloadResult


# ── fixtures ───────────────────────────────────────────────────────────

KEY = bytes(range(32))
AES_KEY, NONCE, EXPECTED_MAC = prepare_key(KEY)   # EXPECTED_MAC = k[24:32]


def make_spec(size: int, url="https://gfs1.storage.mega.nz/file/H") -> FileSpec:
    return FileSpec(handle="H", key=KEY, size=size, name="f.bin", url=url, relpath="f.bin")


def encrypt_offset(plain: bytes, offset: int) -> bytes:
    return ctr_crypt(plain, AES_KEY, NONCE, offset)


class FakeCDN:
    """Serves a synthetic file's encrypted bytes; can simulate failures."""

    def __init__(self, file_bytes: bytes):
        self.file = file_bytes
        self.requests: list[tuple[int, int, str | None]] = []  # (start, end, proxy)
        self.fail_once_starts: set[int] = set()  # 509 only on FIRST request to that offset

    async def get(self, url, headers=None, proxy=None):
        rng = headers["Range"]  # bytes=a-b
        a, b = rng.replace("bytes=", "").split("-")
        a, b = int(a), int(b)
        self.requests.append((a, b, proxy))

        if a in self.fail_once_starts:
            self.fail_once_starts.discard(a)

            class FailResp:
                status = 509
                async def read(self):
                    return b""
                async def __aenter__(self): return self
                async def __aexit__(self, *e): return False
            return FailResp()

        data = encrypt_offset(self.file[a:b + 1], a)

        class Resp:
            status = 200
            def __init__(self, data):
                self._data = data
            async def read(self, n=-1):
                return self._data
            async def __aenter__(self): return self
            async def __aexit__(self, *e): return False

        return Resp(data)


class FakeLease:
    def __init__(self, proxy):
        self.proxy = proxy
        self.reports = []

    def report(self, outcome):
        self.reports.append(outcome)


class FakePool:
    def __init__(self, proxies: list[str]):
        self._proxies = list(proxies)
        self.leased: list[str] = []
        self.released: list[tuple[str, str]] = []

    async def lease(self, exclude=None):
        if not self._proxies:
            await asyncio.sleep(0)
            return None
        proxy = self._proxies.pop(0)
        self.leased.append(proxy)
        return FakeLease(proxy)

    def release(self, lease, outcome: str):
        self.released.append((lease.proxy, outcome))


@pytest.mark.asyncio
async def test_download_small_file_through_proxy(tmp_path):
    file = bytes(range(256)) * 1024  # 256 KiB -> two chunks (128K + 128K)
    spec = make_spec(len(file))
    cdn = FakeCDN(file)
    pool = FakePool(["http://p1:8080"])
    worker = ChunkWorker(spec, pool, cdn_get=cdn.get, dest=tmp_path)

    result = await worker.run()
    assert result == DownloadResult.COMPLETE
    out = (tmp_path / "f.bin").read_bytes()
    assert out == file  # decrypted content matches exactly
    # every request went through the leased proxy
    assert all(proxy == "http://p1:8080" for _, _, proxy in cdn.requests)
    # lease was released as good
    assert ("http://p1:8080", "ok") in pool.released


@pytest.mark.asyncio
async def test_quota_on_second_chunk_rotates_and_resumes(tmp_path):
    file = bytes(range(256)) * 1024  # 256 KiB, 2 chunks
    spec = make_spec(len(file))
    cdn = FakeCDN(file)
    cdn.fail_once_starts = {131072}  # 509 on first attempt of chunk 2 only
    pool = FakePool(["http://p1:8080", "http://p2:8080"])
    worker = ChunkWorker(spec, pool, cdn_get=cdn.get, dest=tmp_path)

    result = await worker.run()
    assert result == DownloadResult.COMPLETE
    # first proxy burned on chunk 2, second proxy finished the job
    assert cdn.requests[0][2] == "http://p1:8080"    # chunk 1 via p1
    assert any(proxy == "http://p2:8080" for _, _, proxy in cdn.requests)
    assert ("http://p1:8080", "quota") in pool.released
    assert ("http://p2:8080", "ok") in pool.released
    # file intact despite rotation
    assert (tmp_path / "f.bin").read_bytes() == file


@pytest.mark.asyncio
async def test_resume_skips_completed_chunks(tmp_path):
    file = bytes(range(256)) * 1024
    spec = make_spec(len(file))
    cdn = FakeCDN(file)
    # Pre-mark chunk 0 as done (as if resuming): first 128K already on disk
    (tmp_path / "f.bin").write_bytes(file[:131072])
    table = chunk_table(len(file))
    chunks_state = "1" + "0" * (len(table) - 1)

    pool = FakePool(["http://p1:8080"])
    worker = ChunkWorker(spec, pool, cdn_get=cdn.get, dest=tmp_path,
                         chunks_state=chunks_state)
    result = await worker.run()
    assert result == DownloadResult.COMPLETE
    # only chunk 2 was fetched
    fetched_starts = {start for start, _, _ in cdn.requests}
    assert fetched_starts == {131072}
    assert (tmp_path / "f.bin").read_bytes() == file


@pytest.mark.asyncio
async def test_pool_exhaustion_returns_pending(tmp_path):
    file = bytes(1000)
    spec = make_spec(len(file))
    cdn = FakeCDN(file)
    pool = FakePool([])  # no proxies at all
    worker = ChunkWorker(spec, pool, cdn_get=cdn.get, dest=tmp_path,
                         max_lease_wait_s=0.05)
    result = await worker.run()
    assert result == DownloadResult.PENDING  # job stays resumable, not failed


# ── integrity: streaming MAC (megajs) vs expected k[24:32] ─────────────

def test_verify_file_mac_matches_reference(tmp_path):
    """Engine._verify_file (segmented-CBC optimized) must agree with the
    auditable block-by-block reference stream_mac() — and catch corruption.

    (Ground truth of the scheme itself: real-download validation, see
    scripts/debug_mac.py — a synthetic key's k[24:32] is not a MAC of
    synthetic data, so the reference's output is the oracle here.)
    """
    from swarm.engine.jobs import Engine
    from swarm.providers.mega import stream_mac
    from swarm.store import Store

    size = 3 * 131072 + 5000   # crosses posNext boundaries 128K,384K + tail
    plain = bytes((i * 7 + 3) % 256 for i in range(size))
    # build a CONSISTENT 32B key the way MEGA stores one: content = k[:16] XOR
    # k[16:32], nonce = k[16:24], mac = k[24:32] — with mac = stream_mac of
    # the plaintext under (content, nonce)
    content_key = bytes(range(16))
    nonce = bytes(range(16, 24))
    mac = stream_mac(plain, content_key, nonce)
    second = nonce + mac
    first = bytes(a ^ b for a, b in zip(content_key, second))   # so fold == content
    key = first + second
    assert prepare_key(key) == (content_key, nonce, mac)
    spec = FileSpec(handle="H", key=key, size=size, name="f.bin", relpath="f.bin")
    store = Store(str(tmp_path / "db.sqlite"))
    engine = Engine.__new__(Engine)   # only _verify_file is exercised
    out = tmp_path / "f.bin"
    out.write_bytes(plain)

    # optimized verify agrees with the reference scheme on the same bytes
    ok, err = engine._verify_file(spec, out)
    assert ok, f"optimized verify disagrees with reference stream_mac: {err}"

    # corruption must be caught: flip one byte
    bad = bytearray(plain)
    bad[200000] ^= 0xFF
    out.write_bytes(bytes(bad))
    ok, err = engine._verify_file(spec, out)
    assert not ok and "MAC mismatch" in (err or "")
