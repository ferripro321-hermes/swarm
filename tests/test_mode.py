"""Tests for proxy.mode gating: either/or, never mixed, nord lease cap."""

import pytest

from swarm.proxies.nord import is_nord_url
from swarm.proxies.pool import ProxyPool, Lease
from tests.test_pool import StoreStub


def make_pool(store, mode: str, nord_max_leases: int = 4) -> ProxyPool:
    return ProxyPool(store, mode=mode, nord_max_leases=nord_max_leases)


def seed(store, urls: list[str], score: float = 50.0):
    for u in urls:
        store.upsert_proxy(u, protocol="http", source="test")
        store.set_proxy_state(u, "ready", score=score)


def test_is_nord_url():
    assert is_nord_url("https://u:p@nl816.nordvpn.com:89")
    assert is_nord_url("socks5://u:p@nl.socks.nordhold.net:1080")
    assert not is_nord_url("http://1.2.3.4:8080")
    assert not is_nord_url("socks5://5.6.7.8:1080")


@pytest.mark.asyncio
async def test_public_mode_never_leases_nord():
    s = StoreStub()
    seed(s, ["https://u:p@nl816.nordvpn.com:89"], score=99.0)   # nord looks "best"
    seed(s, ["http://1.2.3.4:8080"], score=10.0)
    pool = make_pool(s, "public")
    pool.prime_from_store()
    l1 = await pool.lease()
    assert l1 is not None and l1.proxy == "http://1.2.3.4:8080"
    pool.release(l1, "ok")
    l2 = await pool.lease()
    assert l2 is not None and l2.proxy == "http://1.2.3.4:8080"  # nord never, even at 99


@pytest.mark.asyncio
async def test_nord_mode_never_leases_public():
    s = StoreStub()
    seed(s, ["http://1.2.3.4:8080", "https://u:p@nl816.nordvpn.com:89"])
    pool = make_pool(s, "nord")
    pool.prime_from_store()
    l = await pool.lease()
    assert l is not None and is_nord_url(l.proxy)
    assert await pool.lease() is None          # only one nord row seeded


@pytest.mark.asyncio
async def test_nord_mode_respects_lease_cap():
    s = StoreStub()
    seed(s, [f"https://u:p@nl8{i}.nordvpn.com:89" for i in range(6)])
    pool = make_pool(s, "nord", nord_max_leases=2)
    pool.prime_from_store()
    first = [await pool.lease() for _ in range(2)]
    assert all(l is not None and is_nord_url(l.proxy) for l in first)
    assert await pool.lease() is None          # cap hit
    pool.release(first[0], "ok")
    again = await pool.lease()
    assert again is not None                   # cap freed


@pytest.mark.asyncio
async def test_stats_are_mode_filtered():
    s = StoreStub()
    seed(s, ["http://1.2.3.4:8080", "https://u:p@nl816.nordvpn.com:89"])
    pool = make_pool(s, "nord")
    pool.prime_from_store()
    stats = pool.stats()
    assert stats["mode"] == "nord"
    assert stats["ready"] == 1
    assert stats["nord_leases_cap"] == 4
