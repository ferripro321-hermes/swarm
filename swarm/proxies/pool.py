"""Proxy pool: lease/release state machine with score-ordered selection.

Single-owner design: all pool state lives on the asyncio event loop that the
engine runs on (Flask threads reach it via run_coroutine_threadsafe).
States: ready ⇄ leased; quota → cooldown(ttl); fail×N → dead.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass


@dataclass
class Lease:
    proxy: str
    released: bool = False


class ProxyPool:
    def __init__(self, store, ban_ttl_s: float = 6 * 3600.0,
                 fail_ban_after: int = 3, bench_fn=None):
        self.store = store
        self.ban_ttl_s = ban_ttl_s
        self.fail_ban_after = fail_ban_after
        self.bench_fn = bench_fn          # async (url) -> BenchResult (refresh loop)
        self._ready: dict[str, float] = {}       # url -> score (available now)
        self._leased: set[str] = set()
        self._cooldown: dict[str, float] = {}    # url -> unban time
        self._lock = asyncio.Lock()

    # ── startup ────────────────────────────────────────────────────────
    def prime_from_store(self) -> None:
        """Load ready + cooldown proxies from the store (engine boot)."""
        now = time.time()
        for p in self.store.get_proxies_by_state("ready"):
            self._ready[p["url"]] = p.get("score") or 0.0
        for p in self.store.get_proxies_by_state("leased"):
            # orphaned leases (crash) → back to ready
            self._ready[p["url"]] = p.get("score") or 0.0
            self.store.set_proxy_state(p["url"], "ready")
        for p in self.store.get_proxies_by_state("cooldown"):
            until = (p.get("last_banned_at") or 0) + self.ban_ttl_s
            if until > now:
                self._cooldown[p["url"]] = until
            else:
                self._ready[p["url"]] = p.get("score") or 0.0
                self.store.set_proxy_state(p["url"], "ready")

    # ── leasing ────────────────────────────────────────────────────────
    async def lease(self, exclude: set[str] | None = None) -> Lease | None:
        async with self._lock:
            self._expire_cooldowns()
            best_url, best_score = None, -1.0
            for url, score in self._ready.items():
                if exclude and url in exclude:
                    continue
                if url in self._leased:
                    continue
                if score > best_score:
                    best_url, best_score = url, score
            if best_url is None:
                return None
            self._leased.add(best_url)
            self.store.set_proxy_state(best_url, "leased")
            return Lease(best_url)

    async def re_lease_same(self, url: str) -> Lease:
        """Re-acquire a specific proxy (test helper / sticky retry)."""
        async with self._lock:
            self._leased.add(url)
            self.store.set_proxy_state(url, "leased")
            return Lease(url)

    # ── release outcomes ───────────────────────────────────────────────
    def release(self, lease: Lease, outcome: str) -> None:
        """ok | quota | fail"""
        if lease.released:
            return
        lease.released = True
        url = lease.proxy
        self._leased.discard(url)

        if outcome == "ok":
            self._ready[url] = self._ready.get(url, 0.0)
            self.store.set_proxy_state(url, "ready")
            return

        if outcome == "quota":
            self.store.bump_proxy_quota(url)
            until = time.time() + self.ban_ttl_s
            self._cooldown[url] = until
            self._ready.pop(url, None)
            self.store.mark_proxy_banned(url, until)
            self.store.add_event("quota", f"{url} quota-burned; cooldown {self.ban_ttl_s/3600:.1f}h")
            return

        # fail
        dead = self.store.bump_proxy_fail(url, self.fail_ban_after)
        self._ready.pop(url, None)
        if dead:
            self.store.set_proxy_state(url, "dead")
            self.store.add_event("proxy_dead", f"{url} dead after {self.fail_ban_after} failures")
        else:
            self._ready[url] = self._ready.get(url, 0.0)
            self.store.set_proxy_state(url, "ready")

    # ── benchmarking integration ───────────────────────────────────────
    def add_result(self, url: str, ok: bool, score: float | None,
                   latency_ms: float | None = None,
                   throughput_kbps: float | None = None) -> None:
        """Called by the refresher after benching a proxy."""
        self.store.upsert_proxy(url)
        if ok and score and score > 0:
            self.store.set_proxy_state(url, "ready", score=score,
                                       latency_ms=latency_ms,
                                       throughput_kbps=throughput_kbps,
                                       last_benched=True)
            self._ready.setdefault(url, score)
        else:
            self.store.set_proxy_state(url, "dead", last_benched=True)
            self._ready.pop(url, None)

    # ── introspection ──────────────────────────────────────────────────
    def _expire_cooldowns(self) -> None:
        now = time.time()
        expired = [u for u, until in self._cooldown.items() if until <= now]
        for url in expired:
            del self._cooldown[url]
            self._ready.setdefault(url, 0.0)
            self.store.set_proxy_state(url, "ready")

    def stats(self) -> dict[str, int]:
        self._expire_cooldowns()
        return {
            "ready": len([u for u in self._ready if u not in self._leased]),
            "leased": len(self._leased),
            "cooldown": len(self._cooldown),
            "dead": len(self.store.get_proxies_by_state("dead")),
        }
