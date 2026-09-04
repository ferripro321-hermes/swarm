"""Fetch Nord server list, count proxy-service servers per target country.

Usage: .venv/bin/python scripts/nord_servers.py [CC,CC,...]
Default: ES,FR,DE,BE,NL,SE,CH,AT,PT,PL,CZ,LU
Writes the proxy-enabled candidate list to data/nord_proxy_servers.json
"""

from __future__ import annotations

import json
import sys
import urllib.request

TARGETS = sys.argv[1].split(",") if len(sys.argv) > 1 else \
    ["ES", "FR", "DE", "BE", "NL", "SE", "CH", "AT", "PT", "PL", "CZ", "LU"]

req = urllib.request.Request(
    "https://api.nordvpn.com/v1/servers?limit=0&groups%5B%5D=legacy-standard",
    headers={"User-Agent": "Mozilla/5.0"})
servers = json.load(urllib.request.urlopen(req, timeout=60))
print(f"total servers: {len(servers)}")

candidates = []
for s in servers:
    if not s.get("locations"):
        continue
    cc = s["locations"][0]["country"]["code"]
    if cc not in TARGETS:
        continue
    services = [x["identifier"] for x in s.get("services", [])]
    if "proxy" in services:
        candidates.append({
            "hostname": s["hostname"], "ip": s["station"], "cc": cc,
            "city": (s["locations"][0].get("city") or {}).get("name", ""),
            "load": s["load"],
        })

by_cc: dict[str, list] = {}
for c in candidates:
    by_cc.setdefault(c["cc"], []).append(c)
print("proxy-enabled servers in target countries:")
for cc in sorted(by_cc):
    avg_load = sum(x["load"] for x in by_cc[cc]) // max(1, len(by_cc[cc]))
    print(f"  {cc}: {len(by_cc[cc]):4d}  (avg load {avg_load}%)")

out = "/opt/data/swarm/data/nord_proxy_servers.json"
with open(out, "w") as f:
    json.dump(candidates, f, indent=1)
print(f"saved {len(candidates)} candidates -> {out}")
