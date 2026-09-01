"""Tests for the SQLite store: jobs, files, proxies, events."""

import sqlite3

import pytest

from swarm.store import Store


@pytest.fixture()
def store(tmp_path):
    s = Store(str(tmp_path / "test.db"))
    yield s
    s.close()


def test_create_job_and_get(store):
    job_id = store.create_job(link="https://mega.nz/folder/abc#def", dest="data/downloads/j1")
    job = store.get_job(job_id)
    assert job["id"] == job_id
    assert job["link"].endswith("#def")
    assert job["status"] == "queued"


def test_job_with_files_progress(store):
    job_id = store.create_job(link="x", dest="d")
    f1 = store.add_file(job_id, name="a.bin", size=1000, handle="h1", key="k1", relpath="a.bin")
    store.update_file_progress(f1, bytes_done=400, chunks_state="111000")
    job = store.get_job(job_id)
    assert job["files"][0]["bytes_done"] == 400
    assert job["files"][0]["chunks_state"] == "111000"
    assert job["files"][0]["status"] == "pending"


def test_file_status_transitions(store):
    job_id = store.create_job(link="x", dest="d")
    fid = store.add_file(job_id, name="a.bin", size=10, handle="h", key="k", relpath="a.bin")
    store.set_file_status(fid, "downloading")
    assert store.get_job(job_id)["files"][0]["status"] == "downloading"
    store.set_file_status(fid, "done")
    assert store.get_job(job_id)["files"][0]["status"] == "done"


def test_proxy_upsert_and_states(store):
    store.upsert_proxy("http://1.2.3.4:8080", protocol="http", source="test")
    p = store.get_proxy("http://1.2.3.4:8080")
    assert p["state"] == "new"
    store.set_proxy_state("http://1.2.3.4:8080", "ready", score=72.5, latency_ms=120.0, throughput_kbps=900.0)
    p = store.get_proxy("http://1.2.3.4:8080")
    assert p["state"] == "ready"
    assert p["score"] == pytest.approx(72.5)


def test_proxies_by_state(store):
    store.upsert_proxy("p1", protocol="http", source="t")
    store.upsert_proxy("p2", protocol="http", source="t")
    store.upsert_proxy("p3", protocol="http", source="t")
    store.set_proxy_state("p1", "ready", score=10)
    store.set_proxy_state("p2", "ready", score=90)
    ready = store.get_proxies_by_state("ready")
    assert [p["url"] for p in ready] == ["p2", "p1"]  # score desc


def test_events_append(store):
    job_id = store.create_job(link="x", dest="d")
    store.add_event("rotation", "proxy burned", job_id=job_id)
    events = store.get_events(limit=5)
    assert events[0]["kind"] == "rotation"
    assert events[0]["job_id"] == job_id


def test_store_is_threadsafe_connection_per_op(store, tmp_path):
    # Each op opens its own connection (WAL); simulate concurrent-ish use
    store.upsert_proxy("x", protocol="http", source="t")
    Store(str(tmp_path / "test.db")).upsert_proxy("y", protocol="http", source="t")
    assert len(store.list_proxies()) == 2
