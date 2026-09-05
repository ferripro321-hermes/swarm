"""SQLite store for Swarm. WAL mode; one connection per operation (thread-safe)."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    link TEXT NOT NULL,
    dest TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',   -- queued|running|paused|done|failed|cancelled
    provider TEXT NOT NULL DEFAULT 'mega',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    relpath TEXT NOT NULL,
    size INTEGER NOT NULL,
    handle TEXT NOT NULL,
    key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',  -- pending|downloading|verifying|done|failed|corrupt|cancelled
    bytes_done INTEGER NOT NULL DEFAULT 0,
    chunks_state TEXT NOT NULL DEFAULT '',   -- '1' per finished chunk
    share_handle TEXT,                       -- folder share handle (URL refetch on rotation)
    expected_mac TEXT,                       -- hex k[24:32]; '' = no MAC in key (single links)
    error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE(job_id, relpath)
);
CREATE TABLE IF NOT EXISTS proxies (
    url TEXT PRIMARY KEY,                    -- scheme://host[:port] or host:port
    protocol TEXT NOT NULL DEFAULT 'http',
    source TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT 'new',       -- new|testing|ready|leased|cooldown|dead
    score REAL,
    latency_ms REAL,
    throughput_kbps REAL,
    country TEXT,
    asn TEXT,
    fail_count INTEGER NOT NULL DEFAULT 0,
    quota_count INTEGER NOT NULL DEFAULT 0,
    last_banned_at REAL,
    last_benched_at REAL,
    exit_ip TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    kind TEXT NOT NULL,                      -- rotation|quota|proxy_dead|job|file|error|...
    message TEXT NOT NULL,
    job_id INTEGER,
    file_id INTEGER,
    proxy TEXT
);
CREATE INDEX IF NOT EXISTS idx_files_job ON files(job_id);
CREATE INDEX IF NOT EXISTS idx_proxies_state ON proxies(state);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts DESC);
"""


class Store:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        with self._conn() as c:
            c.executescript(_SCHEMA)
            # lightweight migrations: files.share_handle / files.expected_mac
            existing = {r[1] for r in c.execute("PRAGMA table_info(files)").fetchall()}
            if "share_handle" not in existing:
                c.execute("ALTER TABLE files ADD COLUMN share_handle TEXT")
            if "expected_mac" not in existing:
                c.execute("ALTER TABLE files ADD COLUMN expected_mac TEXT NOT NULL DEFAULT ''")

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=30000")
            self._local.conn = conn
        return conn

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    # ── jobs ──────────────────────────────────────────────────────────
    def create_job(self, link: str, dest: str, provider: str = "mega") -> int:
        now = time.time()
        with self._conn() as c:
            cur = c.execute(
                "INSERT INTO jobs (link, dest, provider, created_at, updated_at) VALUES (?,?,?,?,?)",
                (link, dest, provider, now, now),
            )
            return int(cur.lastrowid)

    def _job_to_dict(self, row: sqlite3.Row, with_files: bool = False) -> dict[str, Any]:
        d = dict(row)
        if with_files:
            with self._conn() as c:
                rows = c.execute(
                    "SELECT * FROM files WHERE job_id=? ORDER BY id", (d["id"],)
                ).fetchall()
            d["files"] = [dict(r) for r in rows]
        return d

    def get_job(self, job_id: int, with_files: bool = True) -> dict[str, Any] | None:
        with self._conn() as c:
            row = c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            return None
        return self._job_to_dict(row, with_files=with_files)

    def list_jobs(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [self._job_to_dict(r, with_files=True) for r in rows]

    def set_job_status(self, job_id: int, status: str) -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE jobs SET status=?, updated_at=? WHERE id=?", (status, time.time(), job_id)
            )

    # ── files ─────────────────────────────────────────────────────────
    def add_file(self, job_id: int, name: str, size: int, handle: str, key: str, relpath: str,
                 share_handle: str | None = None, expected_mac: str = "") -> int:
        now = time.time()
        with self._conn() as c:
            cur = c.execute(
                """INSERT OR IGNORE INTO files
                   (job_id, name, relpath, size, handle, key, share_handle, expected_mac,
                    created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (job_id, name, relpath, size, handle, key, share_handle, expected_mac, now, now),
            )
            if cur.lastrowid is not None and cur.lastrowid > 0:
                return int(cur.lastrowid)
            row = c.execute(
                "SELECT id FROM files WHERE job_id=? AND relpath=?", (job_id, relpath)
            ).fetchone()
            return int(row["id"])

    def get_file(self, file_id: int) -> dict[str, Any] | None:
        with self._conn() as c:
            row = c.execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()
        return dict(row) if row else None

    def update_file_progress(self, file_id: int, bytes_done: int, chunks_state: str) -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE files SET bytes_done=?, chunks_state=?, updated_at=? WHERE id=?",
                (bytes_done, chunks_state, time.time(), file_id),
            )

    def set_file_status(self, file_id: int, status: str, error: str | None = None) -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE files SET status=?, error=?, updated_at=? WHERE id=?",
                (status, error, time.time(), file_id),
            )

    # ── proxies ───────────────────────────────────────────────────────
    def upsert_proxy(self, url: str, protocol: str = "http", source: str = "", country: str | None = None,
                     asn: str | None = None, exit_ip: str | None = None) -> None:
        now = time.time()
        with self._conn() as c:
            c.execute(
                """INSERT INTO proxies (url, protocol, source, country, asn, exit_ip, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?)
                   ON CONFLICT(url) DO UPDATE SET
                     country=COALESCE(excluded.country, proxies.country),
                     asn=COALESCE(excluded.asn, proxies.asn),
                     exit_ip=COALESCE(excluded.exit_ip, proxies.exit_ip),
                     updated_at=excluded.updated_at""",
                (url, protocol, source, country, asn, exit_ip, now, now),
            )

    def get_proxy(self, url: str) -> dict[str, Any] | None:
        with self._conn() as c:
            row = c.execute("SELECT * FROM proxies WHERE url=?", (url,)).fetchone()
        return dict(row) if row else None

    def set_proxy_state(self, url: str, state: str, score: float | None = None,
                        latency_ms: float | None = None, throughput_kbps: float | None = None,
                        last_benched: bool = False) -> None:
        now = time.time()
        with self._conn() as c:
            c.execute(
                """UPDATE proxies SET state=?,
                     score=COALESCE(?, score),
                     latency_ms=COALESCE(?, latency_ms),
                     throughput_kbps=COALESCE(?, throughput_kbps),
                     last_benched_at=CASE WHEN ? THEN ? ELSE last_benched_at END,
                     updated_at=? WHERE url=?""",
                (state, score, latency_ms, throughput_kbps, 1 if last_benched else 0, now, now, url),
            )

    def bump_proxy_fail(self, url: str, dead_after: int) -> bool:
        """Increment fail_count; return True if the proxy should be marked dead."""
        with self._conn() as c:
            c.execute("UPDATE proxies SET fail_count=fail_count+1, updated_at=? WHERE url=?",
                      (time.time(), url))
            row = c.execute("SELECT fail_count FROM proxies WHERE url=?", (url,)).fetchone()
        return bool(row and row["fail_count"] >= dead_after)

    def bump_proxy_quota(self, url: str) -> None:
        with self._conn() as c:
            c.execute("UPDATE proxies SET quota_count=quota_count+1, updated_at=? WHERE url=?",
                      (time.time(), url))

    def mark_proxy_banned(self, url: str, until: float) -> None:
        with self._conn() as c:
            c.execute(
                "UPDATE proxies SET state='cooldown', last_banned_at=?, updated_at=? WHERE url=?",
                (until, time.time(), url),
            )

    def get_proxies_by_state(self, state: str, limit: int = 1000) -> list[dict[str, Any]]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM proxies WHERE state=? ORDER BY score DESC LIMIT ?",
                (state, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def list_proxies(self, limit: int = 5000) -> list[dict[str, Any]]:
        with self._conn() as c:
            rows = c.execute("SELECT * FROM proxies ORDER BY url LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def proxy_stats(self) -> dict[str, int]:
        with self._conn() as c:
            rows = c.execute("SELECT state, COUNT(*) AS n FROM proxies GROUP BY state").fetchall()
        stats = {r["state"]: r["n"] for r in rows}
        return {
            "new": stats.get("new", 0),
            "testing": stats.get("testing", 0),
            "ready": stats.get("ready", 0),
            "leased": stats.get("leased", 0),
            "cooldown": stats.get("cooldown", 0),
            "dead": stats.get("dead", 0),
        }

    # ── events ────────────────────────────────────────────────────────
    def add_event(self, kind: str, message: str, job_id: int | None = None,
                  file_id: int | None = None, proxy: str | None = None) -> None:
        with self._conn() as c:
            c.execute(
                "INSERT INTO events (ts, kind, message, job_id, file_id, proxy) VALUES (?,?,?,?,?,?)",
                (time.time(), kind, message, job_id, file_id, proxy),
            )

    def get_events(self, limit: int = 100, since_id: int | None = None) -> list[dict[str, Any]]:
        with self._conn() as c:
            if since_id is not None:
                rows = c.execute(
                    "SELECT * FROM events WHERE id>? ORDER BY id DESC LIMIT ?",
                    (since_id, limit),
                ).fetchall()
            else:
                rows = c.execute(
                    "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
                ).fetchall()
        return [dict(r) for r in rows]
