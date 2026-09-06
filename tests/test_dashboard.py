"""Tests for dashboard endpoints: jobs_summary + paginated file lists."""

import pytest

from swarm.store import Store


@pytest.fixture()
def store(tmp_path):
    s = Store(str(tmp_path / "db.sqlite"))
    jid = s.create_job(link="https://mega.nz/folder/X#k", dest=str(tmp_path / "dl"))
    for i in range(250):
        status = "done" if i < 200 else ("failed" if i < 230 else "pending")
        s.add_file(jid, name=f"f{i:03d}.mp4", size=1000 + i,
                   handle=f"H{i}", key="00" * 32, relpath=f"f{i:03d}.mp4")
        s.set_file_status(jid * 1000 + i, status) if False else None
    # set statuses properly via file ids
    rows = c = None
    with s._conn() as c:
        rows = c.execute("SELECT id FROM files WHERE job_id=? ORDER BY id", (jid,)).fetchall()
    for idx, r in enumerate(rows):
        status = "done" if idx < 200 else ("failed" if idx < 230 else "pending")
        s.set_file_status(r["id"], status)
    # partial progress on two pending files
    with s._conn() as c:
        for fid, done in ((rows[230]["id"], 400), (rows[231]["id"], 100)):
            c.execute("UPDATE files SET status='downloading', bytes_done=? WHERE id=?", (done, fid))
    return s, jid


def test_jobs_summary_aggregates(store):
    s, jid = store
    jobs = s.jobs_summary()
    assert len(jobs) == 1
    j = jobs[0]
    assert j["id"] == jid
    assert j["files_total"] == 250
    assert j["files_done"] == 200
    assert j["files_failed"] == 30
    assert j["files_downloading"] == 2
    assert j["bytes_done"] == 500          # the two partial downloads
    assert j["bytes_total"] == sum(1000 + i for i in range(250))
    assert len(j["active"]) == 2           # only the in-flight rows ride along


def test_list_files_pagination_and_filters(store):
    s, jid = store
    files, total = s.list_files(jid, status="done", limit=10)
    assert total == 200 and len(files) == 10

    files, total = s.list_files(jid, status="downloading")
    assert total == 2 and {f["bytes_done"] for f in files} == {400, 100}

    files, total = s.list_files(jid, q="f24")
    assert total == 10 and all("f24" in f["name"] for f in files)

    files, _ = s.list_files(jid, sort="name", dir="asc", limit=5)
    assert [f["name"] for f in files] == [f"f{i:03d}.mp4" for i in range(5)]

    files, _ = s.list_files(jid, limit=100, offset=240)
    assert len(files) == 10                # 250 total, offset 240
