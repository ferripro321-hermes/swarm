"""API tests using the Flask test client with a fake engine loop."""

import asyncio
import threading
import time

import pytest

from swarm.store import Store
from swarm.proxies.pool import ProxyPool
from swarm.engine.jobs import Engine
from swarm.web.api import make_api_blueprint


class _Settings:
    class engine:
        max_parallel_files = 2
        workers_per_file = 2
        chunk_timeout_s = 5
        url_timeout_s = 5
    class proxy:
        ban_ttl_h = 6
        fail_ban_after = 3
        class bench:
            connect_timeout_s = 1
            mega_probe_timeout_s = 1
            speed_cap_mb = 0.1
            speed_timeout_s = 1
            min_throughput_kbps = 250
    class downloads:
        dest = "data/downloads"


@pytest.fixture()
def client(tmp_path):
    store = Store(str(tmp_path / "t.db"))
    pool = ProxyPool(store)
    settings = _Settings()
    loop = asyncio.new_event_loop()
    t = threading.Thread(target=loop.run_forever, daemon=True)
    t.start()

    engine = Engine(settings, store, pool)

    from flask import Flask
    app = Flask(__name__)
    bp = make_api_blueprint(engine, store, loop)
    app.register_blueprint(bp)

    yield app.test_client()

    loop.call_soon_threadsafe(loop.stop)


def test_health(client):
    r = client.get("/api/health")
    assert r.status_code == 200
    assert r.get_json()["ok"] is True


def test_create_job_requires_link(client):
    r = client.post("/api/jobs", json={})
    assert r.status_code == 400


def test_create_job_rejects_bad_link(client):
    r = client.post("/api/jobs", json={"link": "not-a-mega-link"})
    assert r.status_code in (400, 502)  # parse error (ValueError path)
    assert "error" in r.get_json()


def test_jobs_empty_list(client):
    r = client.get("/api/jobs")
    assert r.status_code == 200
    assert r.get_json()["jobs"] == []


def test_proxy_import_text(client):
    r = client.post("/api/proxies/import", json={"text": "http://1.2.3.4:8080\n9.9.9.9:3128"})
    assert r.status_code == 200
    assert r.get_json()["imported"] == 2
    stats = client.get("/api/proxies/stats").get_json()
    assert stats["new"] == 2


def test_proxy_import_requires_body(client):
    r = client.post("/api/proxies/import", json={})
    assert r.status_code == 400


def test_events_endpoint(client):
    r = client.get("/api/events")
    assert r.status_code == 200
    assert "events" in r.get_json()
