"""MEGA provider: link parsing, chunk math, crypto, API client.

Public-link support only (file + folder). Byte-level details follow the
mega.py client and MegaBasterd's MegaAPI.
"""

from __future__ import annotations

import asyncio
import base64
import json
import math
import re
import secrets
import struct
import time
from dataclasses import dataclass, field

from Crypto.Cipher import AES

MEGA_API_URL = "https://g.api.mega.co.nz/cs"

# MEGA standard chunk sizes: 128 KiB doubling every chunk, cap 8 MiB.
CHUNK_FIRST = 128 * 1024
CHUNK_MAX = 8 * 1024 * 1024


class MegaError(Exception):
    def __init__(self, code: int, message: str = ""):
        self.code = code
        super().__init__(f"MEGA error {code}: {message or _err_name(code)}")


class QuotaExceeded(MegaError):
    """API error -4: this IP's transfer quota is exhausted."""

    def __init__(self):
        super().__init__(-4, "quota exceeded for this IP")


def _err_name(code: int) -> str:
    return {
        -1: "EINTERNAL", -2: "EARGS", -3: "EAGAIN", -4: "ERATELIMIT/EAQUOTA",
        -5: "EFAULT", -6: "EACCESS", -8: "EEXPIRED", -9: "ENOENT",
        -11: "EOVERQUOTA", -12: "EBLOCKED", -14: "ETEMPUNAVAIL",
    }.get(code, "EUNKNOWN")


# ── base64url (MEGA variant) ───────────────────────────────────────────

def base64url_decode(data: str) -> bytes:
    padded = data + "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(padded)


def base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


# ── link parsing ───────────────────────────────────────────────────────

@dataclass
class ParsedLink:
    kind: str          # "file" | "folder"
    handle: str        # public handle (file id or folder id)
    key_bytes: bytes   # raw key from the fragment


_FILE_PATTERNS = [
    re.compile(r"mega\.nz/file/([A-Za-z0-9_-]+)#([A-Za-z0-9_-]+)"),
    re.compile(r"mega\.nz/#!([A-Za-z0-9_-]+)!([A-Za-z0-9_-]+)"),
    re.compile(r"mega\.nz/# !?([A-Za-z0-9_-]+)!([A-Za-z0-9_-]+)"),  # tolerant
]
_FOLDER_PATTERNS = [
    re.compile(r"mega\.nz/folder/([A-Za-z0-9_-]+)#([A-Za-z0-9_-]+)"),
    re.compile(r"mega\.nz/#F!([A-Za-z0-9_-]+)!([A-Za-z0-9_-]+)"),
    re.compile(r"mega\.nz/#F!([A-Za-z0-9_-]+)!([A-Za-z0-9_-]+)"),
]


def parse_link(link: str) -> ParsedLink:
    link = link.strip()
    for pat in _FOLDER_PATTERNS:
        m = pat.search(link)
        if m:
            handle, key = m.group(1), m.group(2)
            raw = base64url_decode(key)
            if len(raw) != 16:
                raise ValueError(f"folder key must be 16 bytes, got {len(raw)}")
            return ParsedLink("folder", handle, raw)
    for pat in _FILE_PATTERNS:
        m = pat.search(link)
        if m:
            handle, key = m.group(1), m.group(2)
            raw = base64url_decode(key)
            if len(raw) != 32:
                raise ValueError(f"file key must be 32 bytes, got {len(raw)}")
            return ParsedLink("file", handle, raw)
    raise ValueError(f"unrecognized MEGA link: {link[:80]}")


# ── chunk math ─────────────────────────────────────────────────────────

def chunk_starts(count: int):
    """First `count` chunk start offsets.

    MEGA sizes: 128K, 256K, 512K, 1M, 2M, 4M, then 8 MiB for every
    subsequent chunk ( MegaBasterd's exact sequence ).
    """
    start = 0
    size = CHUNK_FIRST
    for _ in range(count):
        yield start
        start += size
        size = min(size * 2, CHUNK_MAX)


def chunk_table(file_size: int) -> list[tuple[int, int]]:
    """[(offset, length)] covering [0, file_size) with MEGA's exact chunking."""
    if file_size <= 0:
        return []
    table: list[tuple[int, int]] = []
    est = max(1, math.ceil(file_size / CHUNK_MAX) + 8)
    all_starts = list(chunk_starts(est))
    for i, start in enumerate(all_starts):
        if start >= file_size:
            break
        end = all_starts[i + 1] if i + 1 < len(all_starts) else start + CHUNK_MAX
        table.append((start, min(end, file_size) - start))
    return table


# ── crypto ─────────────────────────────────────────────────────────────

def prepare_key(key32: bytes) -> tuple[bytes, bytes, bytes]:
    """Split a 32-byte file key into (aes_key16, nonce8, mac_key16).

    Derivation per mega.py / MegaBasterd: aes key = k[0:16];
    nonce = k[16:24]; mac key = k[24:32] + k[16:20] + k[20:24].
    """
    if len(key32) != 32:
        raise ValueError("file key must be 32 bytes")
    aes_key = key32[:16]
    nonce = key32[16:24]
    mac_key = key32[24:32] + key32[16:20] + key32[20:24]
    return aes_key, nonce, mac_key


def decrypt_attr(attr: bytes, aes_key: bytes) -> dict:
    """Decrypt an `at` attribute blob (AES-ECB, 'MEGA' + JSON + pad)."""
    if len(attr) % 16:
        attr = attr[: len(attr) - (len(attr) % 16)]
    dec = AES.new(aes_key, AES.MODE_ECB).decrypt(attr)
    if not dec.startswith(b"MEGA"):
        # some nodes lack the prefix; try raw JSON parse
        try:
            return json.loads(dec.decode("utf-8", "ignore"))
        except Exception:
            raise ValueError("attr decryption failed (no MEGA prefix)")
    dec = dec[4:].rstrip(b"\x00")
    return json.loads(dec.decode("utf-8", "ignore"))


def _ctr_cipher(aes_key: bytes, nonce: bytes, offset: int):
    nonce_counter = int.from_bytes(nonce + (0).to_bytes(8, "big"), "big")
    nonce_counter += offset // 16
    return AES.new(aes_key, AES.MODE_CTR, nonce=b"", initial_value=nonce_counter.to_bytes(16, "big"))


def ctr_crypt(data: bytes, aes_key: bytes, nonce: bytes, offset: int) -> bytes:
    """AES-CTR transform (encrypt==decrypt) for a chunk at byte offset."""
    return _ctr_cipher(aes_key, nonce, offset).encrypt(data)


def chunk_mac(data: bytes, mac_key: bytes, nonce: bytes, offset: int) -> bytes:
    """CBC-MAC over one chunk, seeded with nonce||offset/16 (per MEGA)."""
    mac_cipher = AES.new(mac_key, AES.MODE_ECB)
    mac = nonce + (offset // 16 // 1048576).to_bytes(8, "big")
    for i in range(0, len(data), 16):
        block = data[i:i + 16].ljust(16, b"\x00")
        mac = mac_cipher.encrypt(bytes(a ^ b for a, b in zip(mac, block)))
    return mac


def meta_mac(chunk_macs: list[bytes]) -> bytes:
    """XOR-fold chunk MACs (8 bytes each) into the final file MAC."""
    out = bytearray(8)
    for cm in chunk_macs:
        for i in range(8):
            out[i] ^= cm[i] ^ cm[i + 8]
    return bytes(out)
