"""Full port-89 scan of proxy-enabled Nord servers (from nord_servers.py output).

Usage: NORD_USER=... NORD_PASS=... .venv/bin/python scripts/scan_nord89_full.py [concurrency]
Reads data/nord_proxy_servers.json -> writes data/nord_port89_working.json (merged).
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import ssl
import sys
import time

USER = os.environ.get("NORD_USER", "")
PASS = os.environ.get("NORD_PASS", "")
CONC = int(sys.argv[1]) if len(sys.argv) > 1 else 150

with open("/opt/data/swarm/data/nord_proxy_servers.json") as f:
    candidates = json.load(f)
print(f"scanning {len(candidates)} proxy-enabled servers, concurrency {CONC}")

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE
TOKEN = base64.b64encode(f"{USER}:{PASS}".encode()).decode()
SEM = asyncio.Semaphore(CONC)


async def probe(c: dict) -> dict | None:
    async with SEM:
        out = {**c, "ok": False}
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(c["ip"], 89, ssl=ctx, server_hostname=c["hostname"]),
                timeout=12)
            writer.write((f"CONNECT api.ipify.org:80 HTTP/1.1\r\nHost: api.ipify.org:80\r\n"
                          f"Proxy-Authorization: Basic {TOKEN}\r\n\r\n").encode())
            await writer.drain()
            data = await asyncio.wait_for(reader.read(2048), timeout=15)
            head = data.split(b"\r\n\r\n")[0].decode(errors="replace")
            if " 200" in head:
                writer.write(b"GET /?format=json HTTP/1.0\r\nHost: api.ipify.org\r\n\r\n")
                await writer.drain()
                body = await asyncio.wait_for(reader.read(1024), timeout=12)
                txt = body.decode(errors="replace").split("\r\n\r\n")[-1].strip()
                try:
                    out["exit_ip"] = json.loads(txt)["ip"]
                except Exception:
                    out["exit_ip"] = None
                out["ok"] = True
            else:
                out["err"] = head.splitlines()[0][:40] if head else "empty"
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
        except Exception as e:
            out["err"] = f"{type(e).__name__}"
        return out if out["ok"] else None


async def main() -> None:
    t0 = time.monotonic()
    results = await asyncio.gather(*(probe(c) for c in candidates))
    working = [r for r in results if r]
    print(f"working: {len(working)}/{len(candidates)} in {time.monotonic()-t0:.0f}s")
    by_cc: dict[str, int] = {}
    for r in working:
        by_cc[r["cc"]] = by_cc.get(r["cc"], 0) + 1
    print("by country:", dict(sorted(by_cc.items(), key=lambda kv: -kv[1])))
    with open("/opt/data/swarm/data/nord_port89_working.json", "w") as f:
        json.dump(working, f, indent=1)
    print("saved")


asyncio.run(main())
