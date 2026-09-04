"""Gentle re-probe after the full scan: does 407 persist? Do exits rotate per connection?

Usage: NORD_USER=... NORD_PASS=... .venv/bin/python scripts/probe_gentle.py
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import ssl

USER = os.environ.get("NORD_USER", "")
PASS = os.environ.get("NORD_PASS", "")
TOKEN = base64.b64encode(f"{USER}:{PASS}".encode()).decode()
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


async def connect_once(ip: str, host: str, target: str = "api.ipify.org") -> str:
    """One CONNECT through the proxy; return status line or exit ip."""
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(ip, 89, ssl=ctx, server_hostname=host), timeout=12)
    writer.write((f"CONNECT {target}:80 HTTP/1.1\r\nHost: {target}:80\r\n"
                  f"Proxy-Authorization: Basic {TOKEN}\r\n\r\n").encode())
    await writer.drain()
    data = await asyncio.wait_for(reader.read(2048), timeout=12)
    head = data.split(b"\r\n\r\n")[0].decode(errors="replace")
    status = head.splitlines()[0] if head else "(no reply)"
    if " 200" in status:
        writer.write(f"GET /?format=json HTTP/1.0\r\nHost: {target}\r\n\r\n".encode())
        await writer.drain()
        body = await asyncio.wait_for(reader.read(1024), timeout=12)
        txt = body.decode(errors="replace").split("\r\n\r\n")[-1].strip()
        writer.close()
        return f"200 exit={txt}"
    writer.close()
    return status[:50]


async def main() -> None:
    with open("/opt/data/swarm/data/nord_port89_working.json") as f:
        working = json.load(f)
    with open("/opt/data/swarm/data/nord_proxy_servers.json") as f:
        candidates = json.load(f)
    working_set = {w["hostname"] for w in working}

    # 1) a server that 407'd in the scan, probed standalone & gently
    forty = [c for c in candidates if c["cc"] == "ES" and c["hostname"] not in working_set][:3]
    for c in forty:
        try:
            print(f"former-407 {c['hostname']:22s} -> {await connect_once(c['ip'], c['hostname'])}")
        except Exception as e:
            print(f"former-407 {c['hostname']:22s} -> {type(e).__name__}: {str(e)[:50]}")

    # 2) exit-IP stability: 4 fresh connections to two working servers
    for w in working[:2]:
        host, ip = w["hostname"], w["ip"]
        outs = []
        for _ in range(4):
            try:
                outs.append((await connect_once(ip, host)).replace("200 exit=", ""))
            except Exception as e:
                outs.append(type(e).__name__)
            await asyncio.sleep(1)
        print(f"stability {host:22s} -> {outs}")


asyncio.run(main())
