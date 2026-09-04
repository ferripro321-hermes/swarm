"""Raw SOCKS5 handshake against a NordVPN endpoint — shows exactly what the port says.

Usage: .venv/bin/python scripts/probe_nord_raw.py host [port]
"""

from __future__ import annotations

import os
import socket
import struct
import sys

HOST = sys.argv[1] if len(sys.argv) > 1 else "de701.nordvpn.com"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 1080
USER = os.environ.get("NORD_USER", "")
PASS = os.environ.get("NORD_PASS", "")

print(f"tcp connect {HOST}:{PORT} …")
try:
    s = socket.create_connection((HOST, PORT), timeout=10)
except OSError as e:
    print(f"connect FAILED: {e!r}")
    raise SystemExit(1)
print("tcp open")

# greeting: ver=5, methods=[no-auth(0), userpass(2)]
s.sendall(b"\x05\x02\x00\x02")
try:
    reply = s.recv(2)
except socket.timeout:
    print("greeting: NO REPLY (timeout) — port blackholed or non-SOCKS")
    raise SystemExit(1)
if not reply:
    print("greeting: connection closed — not a SOCKS5 listener")
    raise SystemExit(1)
ver, method = reply[0], reply[1]
print(f"greeting: ver={ver} chosen_method={method}"
      + {0: " (no-auth!)", 2: " (user/pass)", 255: " (NO ACCEPTABLE METHODS)"}[method])

if method == 2:
    u = USER.encode()
    p = PASS.encode()
    s.sendall(b"\x01" + bytes([len(u)]) + u + bytes([len(p)]) + p)
    r = s.recv(2)
    if len(r) < 2:
        print("auth reply: empty — listener closed after auth")
        raise SystemExit(1)
    print(f"auth: ver={r[0]} status={r[1]} ({'OK' if r[1] == 0 else 'REJECTED'})")
    if r[1] != 0:
        raise SystemExit(1)

# CONNECT to api.ipify.org:80
s.sendall(b"\x05\x01\x00\x03" + bytes([13]) + b"api.ipify.org" + struct.pack(">H", 80))
try:
    r = s.recv(64)
except socket.timeout:
    print("CONNECT: no reply (timeout)")
    raise SystemExit(1)
if len(r) < 4:
    print("CONNECT: closed", r)
    raise SystemExit(1)
print(f"connect-reply: ver={r[0]} rep={r[1]} ({'granted' if r[1] == 0 else 'refused'})")
if r[1] == 0:
    s.sendall(b"GET /?format=json HTTP/1.0\r\nHost: api.ipify.org\r\n\r\n")
    data = s.recv(512)
    print("---- response ----")
    print(data.decode(errors="replace")[:400])
s.close()
