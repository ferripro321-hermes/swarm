"""Tests for the proxy pool: lease, release outcomes, bans, refresh."""

import pytest

from swarm.proxies.pool import ProxyPool


class StoreStub:
    """Minimal store double with the pool-facing API."""

    def __init__(self):
        self.proxies: dict[str, dict] = {}
        self.events: list[tuple] = []

    def upsert_proxy(self, url, protocol="http", source="", **kw):
        self.proxies.setdefault(url, {"url": url, "state": "new", "score": None,
                                      "fail_count": 0, "quota_count": 0,
                                      "last_banned_at": None, "last_benched_at": None,
                                      "throughput_kbps": None, "latency_ms": None})

    def get_proxy(self, url):
        return self.proxies.get(url)

    def set_proxy_state(self, url, state, score=None, latency_ms=None,
                        throughput_kbps=None, last_benched=False):
        if url in self.proxies:
            self.proxies[url]["state"] = state
            if score is not None: self.proxies[url]["score"] = score
            if latency_ms is not None: self.proxies[url]["latency_ms"] = latency_ms
            if throughput_kbps is not None: self.proxies[url]["throughput_kbps"] = throughput_kbps
            if last_benched: self.proxies[url]["last_benched_at"] = 1

    def get_proxies_by_state(self, state, limit=1000):
        return [p for p in self.proxies.values() if p["state"] == state]

    def bump_proxy_fail(self, url, dead_after):
        p = self.proxies[url]; p["fail_count"] += 1
        return p["fail_count"] >= dead_after

    def bump_proxy_quota(self, url):
        self.proxies[url]["quota_count"] += 1

    def mark_proxy_banned(self, url, until):
        self.proxies[url]["state"] = "cooldown"
        self.proxies[url]["last_banned_at"] = until

    def add_event(self, kind, message, **kw):
        self.events.append((kind, message))


@pytest.fixture
def pool():
    s = StoreStub()
    for i in range(3):
        s.upsert_proxy(f"http://p{i}:1")
    p = ProxyPool(s, ban_ttl_s=3600, fail_ban_after=3)
    # pre-seed ready proxies with scores
    for i in range(3):
        s.set_proxy_state(f"http://p{i}:1", "ready", score=50.0 + i)
    p.prime_from_store()
    return p


@pytest.mark.asyncio
async def test_lease_round_robin_and_release(pool):
    l1 = await pool.lease()
    assert l1 is not None and l1.proxy == "http://p2:1"  # highest score first
    pool.release(l1, "ok")
    l2 = await pool.lease()
    assert l2.proxy == l1.proxy  # released back → available again, still best score


@pytest.mark.asyncio
async def test_quota_release_bans_proxy(pool):
    l1 = await pool.lease()
    burned = l1.proxy
    pool.release(l1, "quota")
    # burned proxy must not be leased again while cooling down
    l2 = await pool.lease()
    assert l2 is not None
    assert l2.proxy != burned
    stats = pool.stats()
    assert stats["cooldown"] == 1


@pytest.mark.asyncio
async def test_hard_fail_bans_then_kills(pool):
    s = pool.store
    l = await pool.lease()
    url = l.proxy
    pool.release(l, "fail")
    pool.release(await pool.re_lease_same(url), "fail")
    # third failure should mark dead
    l3 = await pool.re_lease_same(url)
    pool.release(l3, "fail")
    assert s.proxies[url]["state"] == "dead"


@pytest.mark.asyncio
async def test_exhaustion_blocks_until_release(pool):
    leases = []
    for _ in range(3):
        leases.append(await pool.lease())
    assert all(l is not None for l in leases)
    # pool now empty
    got = await pool.lease()
    assert got is None
    # release one → leasable again
    pool.release(leases[0], "ok")
    got = await pool.lease()
    assert got is not None


@pytest.mark.asyncio
async def test_stats_shape(pool):
    stats = pool.stats()
    assert {"ready", "leased", "cooldown", "dead"} <= set(stats)
