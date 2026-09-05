"""Tests for MEGA link parsing, chunk math and crypto (pure functions, no network)."""

import pytest

from swarm.providers.mega import (
    base64url_decode,
    base64url_encode,
    chunk_table,
    chunk_starts,
    prepare_key,
    parse_link,
    ctr_crypt,
    decrypt_attr,
    stream_mac,
    ParsedLink,
)


# ── base64url (MEGA variant: no padding, -_ alphabet) ──────────────────

def test_base64url_roundtrip():
    data = bytes(range(64))
    enc = base64url_encode(data)
    assert "=" not in enc and "+" not in enc and "/" not in enc
    assert base64url_decode(enc) == data


def test_base64url_decode_pads():
    # 32 bytes -> 43 chars without padding; decode must handle it
    enc = base64url_encode(bytes(32))
    assert len(enc) == 43
    assert base64url_decode(enc) == bytes(32)


# ── link parsing ───────────────────────────────────────────────────────

def test_parse_file_link_new_format():
    key = base64url_encode(bytes(range(32)))
    link = f"https://mega.nz/file/XxY123#{key}"
    p = parse_link(link)
    assert isinstance(p, ParsedLink)
    assert p.kind == "file"
    assert p.handle == "XxY123"
    assert len(p.key_bytes) == 32


def test_parse_file_link_legacy():
    key = base64url_encode(bytes(32))
    p = parse_link(f"https://mega.nz/#!XxY123!{key}")
    assert p.kind == "file"
    assert p.handle == "XxY123"
    assert len(p.key_bytes) == 32


def test_parse_folder_link_new_format():
    key = base64url_encode(bytes(16))
    p = parse_link(f"https://mega.nz/folder/FolderHandle#{key}")
    assert p.kind == "folder"
    assert p.handle == "FolderHandle"
    assert len(p.key_bytes) == 16  # folder share keys are 16 bytes


def test_parse_folder_link_legacy():
    key = base64url_encode(bytes(16))
    p = parse_link(f"https://mega.nz/#F!FolderHandle!{key}")
    assert p.kind == "folder"


def test_parse_link_rejects_garbage():
    with pytest.raises(ValueError):
        parse_link("https://example.com/nothing")
    with pytest.raises(ValueError):
        parse_link("https://mega.nz/file/onlyhandle")


# ── chunk math ─────────────────────────────────────────────────────────

def test_chunk_starts_first_chunks():
    starts = list(chunk_starts(9))
    assert starts[:5] == [0, 131072, 393216, 917504, 1966080]
    assert starts[7] == 16646144  # first full 8 MiB chunk starts here
    assert starts[8] - starts[7] == 8 * 1024 * 1024  # and every one after is 8 MiB


def test_chunk_table_exact_sizes():
    # tiny file: one chunk
    assert chunk_table(100) == [(0, 100)]
    # exactly one base chunk
    assert chunk_table(131072) == [(0, 131072)]
    # 128K + 256K + tail
    table = chunk_table(131072 + 262144 + 10)
    assert table == [(0, 131072), (131072, 262144), (393216, 10)]
    # 8 MiB cap holds for big files
    table = chunk_table(32 * 1024 * 1024)
    assert max(length for _, length in table) == 8 * 1024 * 1024
    assert sum(length for _, length in table) == 32 * 1024 * 1024


# ── crypto ─────────────────────────────────────────────────────────────

def test_prepare_key_splits_32byte_key():
    # megajs scheme (byte-verified against a real download, 2026-09-05):
    # content key = k[:16] XOR k[16:32]; nonce = k[16:24];
    # expected file MAC = k[24:32] (stored in the key blob — no mac key exists)
    key = bytes(range(32))
    aes_key, nonce, mac = prepare_key(key)
    assert aes_key == bytes(a ^ b for a, b in zip(key[:16], key[16:32]))
    assert nonce == key[16:24]
    assert mac == key[24:32]


def test_prepare_key_fold_is_real():
    # the fold must actually XOR the halves (the old split returned k[:16] raw)
    key = bytes(range(32))
    aes_key, _, _ = prepare_key(key)
    assert aes_key != key[:16]


def test_ctr_crypt_symmetric():
    from Crypto.Cipher import AES
    key = bytes(16)
    data = b"hello world" * 100
    enc = ctr_crypt(data, key, bytes(8), 0)
    assert enc != data
    dec = ctr_crypt(enc, key, bytes(8), 0)
    assert dec == data


def test_ctr_crypt_offset_dependence():
    key = bytes(range(16))
    nonce = bytes(8)
    data = bytes(64)
    a = ctr_crypt(data, key, nonce, 0)
    b = ctr_crypt(data, key, nonce, 16)
    assert a != b  # counter includes offset/16


def test_decrypt_attr_roundtrip():
    from Crypto.Cipher import AES
    import json as _json
    aes_key = bytes(range(16))
    payload = _json.dumps({"n": "test song.mp3", "c": "0,0,0"}).encode()
    # MEGA attr format: b"MEGA" + json padded (with zeros) so len % 16 == 0
    body = len(payload) + 4  # +4 for the MEGA prefix
    pad = (16 - body % 16) % 16
    plain = b"MEGA" + payload + b"\x00" * pad
    assert len(plain) % 16 == 0
    # attrs are AES-CBC with a ZERO IV (ECB never decrypts — sweep-verified)
    enc = AES.new(aes_key, AES.MODE_CBC, b"\x00" * 16).encrypt(plain)
    assert decrypt_attr(enc, aes_key) == {"n": "test song.mp3", "c": "0,0,0"}


def test_stream_mac_matches_segmented_cbc_derivation():
    """Naive reference stream_mac == independent segmented-CBC derivation.

    The chain c_i = E(p_i ⊕ c_{i-1}) with c_0 = seed IS a CBC encryption
    seeded nonce||nonce; snapshots follow megajs's posNext schedule (128K
    steps growing to 1M). Ground truth for the whole scheme was established
    against a REAL download (0.8 MB MP4, computed == k[24:32]) — see
    scripts/debug_mac.py + references/mega-crypto.md.
    """
    from Crypto.Cipher import AES

    from swarm.providers.mega import stream_mac
    key = bytes(range(32))
    aes_key, nonce, _ = prepare_key(key)
    data = bytes((i * 11) % 256 for i in range(200000))

    seed = nonce + nonce
    ec = AES.new(aes_key, AES.MODE_ECB)
    snaps, pos, nxt, inc = [], 0, 131072, 131072
    while pos < len(data):
        seg = data[pos:min(nxt, len(data))]
        if len(seg) % 16:
            seg += b"\x00" * (16 - len(seg) % 16)
        snaps.append(AES.new(aes_key, AES.MODE_CBC, seed).encrypt(seg)[-16:])
        pos += len(seg)
        if pos == nxt and pos < len(data):
            if inc < 1048576:
                inc += 131072
            nxt += inc
    if pos == nxt:          # EOF exactly on a boundary → trailing reseeded seed
        snaps.append(seed)
    acc = bytearray(16)
    for m in snaps:
        acc = bytearray(ec.encrypt(bytes(a ^ b for a, b in zip(acc, m))))
    w = [int.from_bytes(bytes(acc[i:i + 4]), "big") for i in (0, 4, 8, 12)]
    manual = (w[0] ^ w[1]).to_bytes(4, "big") + (w[2] ^ w[3]).to_bytes(4, "big")
    assert stream_mac(data, aes_key, nonce) == manual
