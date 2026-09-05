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

def fold_key(key32: bytes) -> bytes:
    """Content key = k[:16] XOR k[16:32] (MEGA file-key fold)."""
    return bytes(a ^ b for a, b in zip(key32[:16], key32[16:32]))


def prepare_key(key32: bytes) -> tuple[bytes, bytes, bytes]:
    """Split a 32-byte MEGA file key into (content_key16, nonce8, mac8).

    Per megajs/go-mega (verified against a real download):
      - content/CTR key = k[:16] XOR k[16:32]
      - CTR nonce       = k[16:24]
      - expected MAC    = k[24:32]  (the file MAC is stored in the key blob)
    """
    if len(key32) != 32:
        raise ValueError("file key must be 32 bytes")
    return fold_key(key32), key32[16:24], key32[24:32]


def decrypt_attr(attr: bytes, aes_key: bytes) -> dict:
    """Decrypt an `at` attribute blob (AES-CBC zero-IV, 'MEGA' + JSON + pad)."""
    if len(attr) % 16:
        attr = attr[: len(attr) - (len(attr) % 16)]
    dec = AES.new(aes_key, AES.MODE_CBC, b"\x00" * 16).decrypt(attr)
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


def stream_mac(plain: bytes, aes_key: bytes, nonce: bytes) -> bytes:
    """Reference streaming file MAC (megajs MAC class, block-by-block).

    Byte-verified ground truth against a real download (2026-09-05):
    running CBC-ENCRYPT chain seeded nonce||nonce, snapshot + reseed at the
    megajs posNext schedule (increments of 128K growing up to 1M — NOT the
    download chunk table), trailing segment always captured, then XOR-fold
    condense. Engine._verify_file computes the same value with per-segment
    CBC optimization; this naive form stays as the auditable reference.
    """
    ec = AES.new(aes_key, AES.MODE_ECB)
    mac = bytearray(nonce + nonce)
    macs: list[bytes] = []
    pos, pos_next, increment = 0, 131072, 131072
    for i in range(0, len(plain), 16):
        block = plain[i:i + 16].ljust(16, b"\x00")
        for j in range(16):
            mac[j] ^= block[j]
        mac = bytearray(ec.encrypt(bytes(mac)))
        pos += 16
        if pos >= pos_next:
            macs.append(bytes(mac))
            mac = bytearray(nonce + nonce)
            if increment < 1048576:
                increment += 131072
            pos_next += increment
    macs.append(bytes(mac))
    acc = bytearray(16)
    for m in macs:
        for j in range(16):
            acc[j] ^= m[j]
        acc = bytearray(ec.encrypt(bytes(acc)))
    w = [int.from_bytes(bytes(acc[i:i + 4]), "big") for i in (0, 4, 8, 12)]
    return (w[0] ^ w[1]).to_bytes(4, "big") + (w[2] ^ w[3]).to_bytes(4, "big")
