"""Test port-89 TLS CONNECT proxy across a sample of Nord proxy-enabled servers.

Usage: NORD_USER=... NORD_PASS=... .venv/bin/python scripts/test_port89.py [sample_n]
Reads data/nord_proxy_servers.json, samples across countries, reports success rate
and exit IPs. Saves working endpoints to data/nord_port89_working.json
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import random
import ssl
import sys
import time

import aiohttp

USER = os.environ.get("NORD_USER", "")
PASS = os.environ.get("NORD_PASS", "")
SAMPLE = int(sys.argv[1]) if len(sys.argv) > 1 else 40

with open("/opt/data/swarm/data/nord_proxy_servers.json") as f:
    candidates = json.load(f)

by_cc: dict[str, list] = {}
for c in candidates:
    by_cc.setdefault(c["cc"], []).append(c)

# spread the sample evenly over countries
per_cc = max(1, SAMPLE // len(by_cc))
sample: list[dict] = []
rng = random.Random(7)
for cc in sorted(by_cc):
    pool = by_cc[cc]
    rng.shuffle(pool)
    sample.extend(pool[:per_cc])
rng.shuffle(sample)
sample = sample[:SAMPLE]
print(f"testing {len(sample)} servers on port 89 (TLS CONNECT)…")


async def probe(c: dict) -> dict:
    host, cc, ip = c["hostname"], c["cc"], c["ip"]
    token = base64.b64encode(f"{USER}:{PASS}".encode()).decode()
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    out = {**c, "ok": False, "exit_ip": None, "err": None}
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, 89, ssl=ctx, server_hostname=host), timeout=12)
        req = (f"CONNECT api.ipify.org:80 HTTP/1.1\r\n"
               f"Host: api.ipify.org:80\r\n"
               f"Proxy-Authorization: Basic {token}\r\n\r\n")
        writer.write(req.encode())
        await writer.drain()
        data = await asyncio.wait_for(reader.read(2048), timeout=12)
        head = data.split(b"\r\n\r\n")[0].decode(errors="replace")
        status = head.splitlines()[0] if head else ""
        if " 200" in status:
            writer.write(b"GET /?format=json HTTP/1.0\r\nHost: api.ipify.org\r\n\r\n")
            await writer.drain()
            body = await asyncio.wait_for(reader.read(1024), timeout=12)
            txt = body.decode(errors="replace").split("\r\n\r\n")[-1]
            try:
                out["exit_ip"] = json.loads(txt.strip())["ip"]
            except Exception:
                out["exit_ip"] = txt.strip()[:40]
            out["ok"] = True
        else:
            out["err"] = status[:60] or "empty reply"
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
    except Exception as e:
        out["err"] = f"{type(e).__name__}: {str(e)[:60]}"
    return out


async def main() -> None:
    t0 = time.monotonic()
    results = await asyncio.gather(*(probe(c) for c in sample))
    ok = [r for r in results if r["ok"]]
    print(f"\n{len(ok)}/{len(results)} WORKING  ({time.monotonic()-t0:.0f}s)")
    errkinds: dict[str, int] = {}
    for r in results:
        if not r["ok"]:
            key = (r["err"] or "?").split(":")[0][:30]
            errkinds[key] = errkinds.get(key, 0) + 1
    print("failure kinds:", errkinds)
    uniq = {r["exit_ip"] for r in ok}
    print(f"distinct exit IPs: {len(uniq)}")
    for r in ok[:10]:
        print(f"  ✅ {r['hostname']:22s} {r['cc']} load={r['load']:3d} exit={r['exit_ip']}")
    with open("/opt/data/swarm/data/nord_port89_working.json", "w") as f:
        json.dump(ok, f, indent=1)
    print("saved working list")


asyncio.run(main())
