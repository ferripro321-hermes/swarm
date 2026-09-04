# Swarm 🐝

**Self-hosted proxy-rotating downloader — pull large MEGA folders without fighting the per-IP quota.**

Swarm downloads big public MEGA folders by spreading chunk requests across a pool of public proxies it fetches, benchmarks against MEGA itself, grades, and rotates automatically. When a proxy's IP burns (HTTP 509 / API error `-4`), Swarm bans it for a cooldown, leases the next-best proxy, re-fetches the download URL through it, and resumes the file at the exact chunk where it stopped.

> Named *Swarm* because many slow IPs act as one fast downloader. The provider/job layer is generic — more sources planned.

## How it works

```
Flask :6970 ──> asyncio Engine (thread)
                 ├── MEGA provider      parse links · walk folders · decrypt chunks · verify MAC
                 ├── ProxyPool          fetch → bench → grade → lease → ban → refresh
                 └── Orchestrator       N files × M chunk workers · rotation · resume
                        ↓
                 SQLite (WAL): jobs, files, proxies, events — crash-safe resume
```

**Why proxy per-worker:** MEGA only enforces the per-IP quota on the download-URL request + CDN chunk GETs. Each worker routes exactly that through its leased proxy; rotation swaps the whole unit so a mid-file burn never corrupts anything.

**Why bench against MEGA:** most public proxies pass generic "can you reach Google" checks but die on MEGA's CDN. Swarm's benchmark stage 2 hits `g.api.mega.co.nz` directly — garbage is eliminated *before* it wastes your download.

## Quick start

```bash
cd /opt/data/swarm
uv venv .venv && uv pip install --python .venv/bin/python -e .
# or: .venv/bin/pip install flask aiohttp aiohttp-socks pycryptodome pyyaml requests

.venv/bin/python app.py
# → 🚂 Swarm listening on http://0.0.0.0:6970
```

Open the dashboard, paste a `mega.nz/folder/...#...` link, hit **Start**. The first run fetches and benches proxies automatically (takes a few minutes); subsequent runs reuse the graded pool.

### CLI QA (optional)

```bash
.venv/bin/python scripts/qa_quota.py <mega-link> --max-requests 200
```

Downloads real bytes and reports rotation stats — the acceptance test for the whole pipeline.

## Configuration (`config.yaml`)

| Section | Key | Default | Meaning |
|---|---|---|---|
| server | port | 6970 | web UI + API |
| engine | max_parallel_files | 3 | files downloading at once |
| engine | workers_per_file | 4 | chunk workers per file |
| proxy | ban_ttl_h | 6 | cooldown for quota-burned proxies |
| proxy | fail_ban_after | 3 | consecutive failures → dead |
| proxy | sources | monosans + ProxyScrape | raw list URLs |
| proxy | refresh_min | 30 | pool refresh cadence |
| proxy.bench | min_throughput_kbps | 250 | below this → dead |
| proxy.bench | speed_url | mega.nz/secureboot.js | MEGA edge asset used to measure throughput |
| downloads | dest | data/downloads | default download root |

Env overrides: `SWARM_<SECTION>_<KEY>` (e.g. `SWARM_SERVER_PORT=8080`).

## API

```
POST /api/jobs              {link, dest?}       → 202 {job_id}
GET  /api/jobs              list with per-file progress
GET  /api/jobs/:id
DELETE /api/jobs/:id        cancel
POST /api/jobs/:id/pause | resume
GET  /api/proxies           ?state=&limit=
POST /api/proxies/import    {text} or {url}     → import + bench
POST /api/proxies/bench     re-bench 'new' now
GET  /api/proxies/stats
GET  /api/events            ?limit=&since_id=
GET  /api/stream            SSE live progress
GET  /api/health
```

## Proxy states

```
new ──bench──> ready ⇄ leased
                 │  quota → cooldown (ban_ttl_h) → back to ready
                 │  fail ×N → dead
```

## Tests

```bash
.venv/bin/python -m pytest tests/ -q          # 53 tests, no network
```

## Honest notes

- **Public proxies are slow** (50–500 KB/s each). The win is parallelism — `workers_per_file × max_parallel_files`. For sustained speed, plug a paid proxy list into `/api/proxies/import`.
- Public links only (no login) in v1; nothing encrypted-without-key.
- Quota-circumvention is ToS-gray — personal-use tool.
- MAC verification catches proxy-mangled bytes; a corrupt file is marked, not silently kept.

## Roadmap

- [ ] Paid proxy provider presets
- [ ] Direct HTTP / yt-dlp providers on the same job+pool machinery
- [ ] Per-file proxy pinning (UI)
- [ ] Docker image + Unraid template

---
MIT — built by Ferripro321 + ferripro321-hermes
