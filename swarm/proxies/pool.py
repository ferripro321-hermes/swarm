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
                 fail_ban_after: int = 3, bench_fn=None,
                 mode: str = "public", nord_max_leases: int = 4,
                 throttle_cooldown_s: float = 90.0):
        self.store = store
        self.ban_ttl_s = ban_ttl_s
        self.fail_ban_after = fail_ban_after
        self._throttle_cooldown_s = throttle_cooldown_s
        self.bench_fn = bench_fn          # async (url) -> BenchResult (refresh loop)
        # mode: "public" leases nothing from Nord URLs; "nord" leases ONLY
        # Nord URLs. Either/or, no mixing.
        self.mode = mode
        self.nord_max_leases = max(1, nord_max_leases)
        self._ready: dict[str, float] = {}       # url -> score (available now)
        self._leased: set[str] = set()
        self._cooldown: dict[str, float] = {}    # url -> unban time
        self._throttle_strikes: dict[str, int] = {}   # url -> consecutive throttle passes
        self._lock = asyncio.Lock()

    # ── startup ────────────────────────────────────────────────────────
    def prime_from_store(self) -> None:
        """Load ready + cooldown proxies from the store (engine boot).

        Only rows matching proxy.mode are loaded for leasing — rows from the
        other mode stay in the DB untouched so a config switch doesn't need a
        re-bench.
        """
        from swarm.proxies.nord import is_nord_url
        now = time.time()

        def keep(url: str) -> bool:
            return is_nord_url(url) if self.mode == "nord" else not is_nord_url(url)

        for p in self.store.get_proxies_by_state("ready"):
            if keep(p["url"]):
                self._ready[p["url"]] = p.get("score") or 0.0
        for p in self.store.get_proxies_by_state("leased"):
            # orphaned leases (crash) → back to ready
            if not keep(p["url"]):
                continue
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
    def _mode_ok(self, url: str) -> bool:
        from swarm.proxies.nord import is_nord_url
        if self.mode == "nord":
            return is_nord_url(url)
        return not is_nord_url(url)

    def _nord_leased_count(self) -> int:
        from swarm.proxies.nord import is_nord_url
        return sum(1 for u in self._leased if is_nord_url(u))

    async def lease(self, exclude: set[str] | None = None) -> Lease | None:
        async with self._lock:
            self._expire_cooldowns()
            nord_in_pool = self.mode == "nord"
            if nord_in_pool and self._nord_leased_count() >= self.nord_max_leases:
                return None            # curated-set cap: never over-lease Nord
            best_url, best_score = None, -1.0
            for url, score in self._ready.items():
                if exclude and url in exclude:
                    continue
                if url in self._leased:
                    continue
                if not self._mode_ok(url):
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
        """ok | quota | throttle | fail"""
        if lease.released:
            return
        lease.released = True
        url = lease.proxy
        self._leased.discard(url)

        if outcome == "ok":
            self._ready[url] = self._ready.get(url, 0.0)
            self.store.set_proxy_state(url, "ready")
            return

        if outcome == "throttle":
            # temporary auth rate-limit (Nord): short cooldown, no fail strike.
            # The exit keeps its grade and returns to ready afterwards.
            until = time.time() + self._throttle_cooldown_s
            self._cooldown[url] = until
            self._ready.pop(url, None)
            self.store.mark_proxy_banned(url, until)
            self.store.add_event("throttle", f"{url} auth-throttled; cooldown 90s")
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
                   throughput_kbps: float | None = None,
                   throttled: bool = False) -> None:
        """Called by the refresher after benching a proxy.

        Results for the *other* mode's URLs are still recorded (they update
        grades in SQLite) but are never leased thanks to the mode gate.
        A throttle result (Nord auth rate-limit) does NOT kill an endpoint:
        it keeps its previous grade/ready state; consecutive throttle passes
        back off the re-verification pressure (bench_skip).
        """
        self.store.upsert_proxy(url)
        if throttled:
            strikes = self._throttle_strikes.get(url, 0) + 1
            self._throttle_strikes[url] = strikes
            self.store.add_event("throttle",
                                 f"{url} bench throttled x{strikes}; grade kept")
            # row stays whatever it was — the engine only re-queues throttled
            # URLs for benching with exponential backoff (bench_skip_minutes)
            return
        self._throttle_strikes.pop(url, None)
        if ok and score and score > 0:
            self.store.set_proxy_state(url, "ready", score=score,
                                       latency_ms=latency_ms,
                                       throughput_kbps=throughput_kbps,
                                       last_benched=True)
            self._ready[url] = score
        else:
            self.store.set_proxy_state(url, "dead", last_benched=True)
            self._ready.pop(url, None)

    def throttle_skip_minutes(self, url: str) -> float:
        """Exponential backoff before re-benching a throttled endpoint.

        2^n minutes, capped at 32: 1, 2, 4, 8, 16, 32, 32... (strike count
        resets on the first non-throttled result).
        """
        strikes = self._throttle_strikes.get(url, 1)
        return float(min(2 ** max(0, strikes - 1), 32))

    # ── introspection ──────────────────────────────────────────────────
    def _expire_cooldowns(self) -> None:
        now = time.time()
        expired = [u for u, until in self._cooldown.items() if until <= now]
        for url in expired:
            del self._cooldown[url]
            self._ready.setdefault(url, 0.0)
            self.store.set_proxy_state(url, "ready")

    def stats(self) -> dict[str, int | str]:
        from swarm.proxies.nord import is_nord_url
        self._expire_cooldowns()

        def keep(url: str) -> bool:
            return is_nord_url(url) if self.mode == "nord" else not is_nord_url(url)

        dead = [p for p in self.store.get_proxies_by_state("dead", limit=5000) if keep(p["url"])]
        out: dict[str, int | str] = {
            "mode": self.mode,
            "ready": len([u for u in self._ready
                          if u not in self._leased and self._mode_ok(u)]),
            "leased": len([u for u in self._leased if self._mode_ok(u)]),
            "cooldown": len(self._cooldown),
            "dead": len(dead),
            "nord_leases_cap": self.nord_max_leases if self.mode == "nord" else 0,
        }
        # raw store counters (mode-filtered), kept for the dashboard/import flow
        for state in ("new", "testing"):
            rows = self.store.get_proxies_by_state(state, limit=5000)
            out[state] = len([p for p in rows if keep(p["url"])])
        return out
