"""Scan common ports on a host with short timeouts.

Usage: .venv/bin/python scripts/port_scan.py host [port,port,...]
Default ports: 22,80,443,1080,1085,1194,4443,51820,8080,8118,9021,3000
"""

from __future__ import annotations

import socket
import sys

HOST = sys.argv[1]
PORTS = ([int(p) for p in sys.argv[2].split(",")] if len(sys.argv) > 2
         else [22, 80, 443, 1080, 1085, 1194, 4443, 51820, 8080, 8118, 9021, 3000])

try:
    ips = socket.gethostbyname_ex(HOST)[2]
    print(f"{HOST} -> {', '.join(ips)}")
except OSError as e:
    print(f"DNS FAILED: {e!r}")
    raise SystemExit(1)

for port in PORTS:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    try:
        s.connect((ips[0], port))
        print(f"  {port:5d} OPEN")
        s.close()
    except OSError as e:
        print(f"  {port:5d} closed/filtered ({type(e).__name__})")
    finally:
        s.close()
