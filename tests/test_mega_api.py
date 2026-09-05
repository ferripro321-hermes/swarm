"""Tests for the async MEGA API client (mocked HTTP + one real net test)."""

import json

import pytest

from swarm.providers.mega_api import MegaClient
from swarm.providers.mega import MegaError, QuotaExceeded, parse_link


def _cs_response(payload):
    return json.dumps([payload])


class FakeResponse:
    def __init__(self, text, status=200):
        self.status = status
        self._text = text

    async def text(self):
        return self._text

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, url, data=None, timeout=None, proxy=None):
        self.calls.append({"url": url, "data": data, "proxy": proxy, "timeout": timeout})
        if not self.responses:
            raise AssertionError("unexpected extra API call")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


@pytest.mark.asyncio
async def test_file_info_parses_response():
    link = parse_link("https://mega.nz/file/HANDLE123#" + _b64key())
    session = FakeSession([
        FakeResponse(_cs_response({
            "g": "https://gfs301.storage.mega.nz/file/HANDLE123",
            "s": 123456,
            "at": _attr_blob("song.mp3", link.key_bytes[:16]),
            "h": "HANDLE123",
        }))
    ])
    client = MegaClient(session=session)
    spec = await client.file_info(link)
    assert spec.handle == "HANDLE123"
    assert spec.size == 123456
    assert spec.name == "song.mp3"
    assert spec.url.startswith("https://gfs")
    # API call went to the cs endpoint with the file handle
    call = session.calls[0]
    assert "g.api.mega.co.nz" in call["url"]
    assert "n=HANDLE123" in call["url"]
    body = json.loads(call["data"])
    assert body[0]["a"] == "g" and body[0]["p"] == "HANDLE123"


@pytest.mark.asyncio
async def test_file_info_maps_quota_error():
    link = parse_link("https://mega.nz/file/H#"+_b64key())
    session = FakeSession([FakeResponse(_cs_response(-4))])
    client = MegaClient(session=session)
    with pytest.raises(QuotaExceeded):
        await client.file_info(link)


@pytest.mark.asyncio
async def test_api_error_generic():
    link = parse_link("https://mega.nz/file/H#"+_b64key())
    session = FakeSession([FakeResponse(_cs_response(-9))])
    client = MegaClient(session=session)
    with pytest.raises(MegaError) as ei:
        await client.file_info(link)
    assert ei.value.code == -9


@pytest.mark.asyncio
async def test_folder_tree_resolves_paths():
    from swarm.providers.mega import fold_key
    link = parse_link("https://mega.nz/folder/FH#"+_b64key16())
    # f response: nodes 'h' with parent 'p', file attrs encrypted with the
    # FOLDED node key (as in real shares), node k ECB-encrypted with share key
    node_key = bytes(range(32))           # the real node file key (compkey)
    node_attr_key = fold_key(node_key)
    share_key = _b64key16()
    f1_attr = _attr_blob("song one.mp3", node_attr_key)
    f2_attr = _attr_blob("sub/song two.mp3", node_attr_key)
    session = FakeSession([
        FakeResponse(_cs_response({
            "f": [
                {"h": "NODE1", "p": "FH", "t": 0, "s": 100, "a": f1_attr,
                 "k": f"NODE1:{_enc_node_key(node_key)}"},
                {"h": "NODE2", "p": "NODE1", "t": 0, "s": 200, "a": f2_attr,
                 "k": f"NODE2:{_enc_node_key(node_key)}"},
            ],
            "ok": [{"h": "FH", "k": _enc_share_key(share_key, link.key_bytes)}],
        }))
    ])
    client = MegaClient(session=session)
    files = await client.folder_tree(link)
    assert len(files) == 2
    assert files[0].name == "song one.mp3"
    assert files[0].size == 100
    assert files[0].key == node_key
    # share handle + expected MAC must ride along (URL refetch / verification)
    assert files[0].share_handle == "FH"
    assert files[0].expected_mac == node_key[24:32]


# ── helpers ────────────────────────────────────────────────────────────

def _b64key():
    from swarm.providers.mega import base64url_encode
    return base64url_encode(bytes(range(32)))


def _b64key16():
    from swarm.providers.mega import base64url_encode
    return base64url_encode(bytes(range(16)))


def _attr_blob(name: str, aes_key: bytes) -> str:
    from swarm.providers.mega import base64url_encode
    from Crypto.Cipher import AES
    payload = json.dumps({"n": name}).encode()
    body = len(payload) + 4
    pad = (16 - body % 16) % 16
    plain = b"MEGA" + payload + b"\x00" * pad
    # real attrs: AES-CBC zero-IV (ECB never decrypts live shares)
    return base64url_encode(AES.new(aes_key, AES.MODE_CBC, b"\x00" * 16).encrypt(plain))


def _enc_node_key(node_key: bytes) -> str:
    """AES-ECB(share_key, node_key) — the real node 'k' format."""
    from swarm.providers.mega import base64url_encode
    from Crypto.Cipher import AES
    return base64url_encode(AES.new(_FAKE_SHARE_KEY, AES.MODE_ECB).encrypt(node_key))


_FAKE_SHARE_KEY = bytes(range(16))


def _enc_share_key(share_key: str, master: bytes) -> str:
    """Encrypt the 16-byte share key with the folder link key (AES-ECB)."""
    from swarm.providers.mega import base64url_encode
    from Crypto.Cipher import AES
    raw = _FAKE_SHARE_KEY
    return base64url_encode(AES.new(master, AES.MODE_ECB).encrypt(raw))
