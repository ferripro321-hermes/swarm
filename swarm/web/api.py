"""Flask API: jobs, proxies, events, SSE progress."""

from __future__ import annotations

import asyncio
import json
import queue
import threading

from flask import Blueprint, jsonify, request, Response

from swarm.store import Store


def make_api_blueprint(engine, store: Store, loop) -> Blueprint:
    bp = Blueprint("api", __name__, url_prefix="/api")

    def run_async(coro):
        """Submit a coroutine to the engine's event loop and wait for the result."""
        fut = asyncio.run_coroutine_threadsafe(coro, loop)
        return fut.result(timeout=300)

    # ── jobs ──────────────────────────────────────────────────────────
    @bp.post("/jobs")
    def create_job():
        data = request.get_json(silent=True) or {}
        link = (data.get("link") or "").strip()
        if not link:
            return jsonify(error="link required"), 400
        dest = (data.get("dest") or "").strip() or None
        try:
            job_id = run_async(engine.create_job(link, dest))
        except ValueError as e:
            return jsonify(error=str(e)), 400
        except Exception as e:
            return jsonify(error=f"failed to inspect link: {e}"), 502
        run_async(engine.start_job(job_id))
        return jsonify(job_id=job_id), 202

    @bp.get("/jobs")
    def list_jobs():
        return jsonify(jobs=store.list_jobs())

    @bp.get("/jobs/summary")
    def jobs_summary():
        return jsonify(jobs=store.jobs_summary())

    @bp.get("/jobs/<int:job_id>/files")
    def list_job_files(job_id: int):
        files, total = store.list_files(
            job_id,
            status=request.args.get("status") or None,
            q=request.args.get("q") or None,
            sort=request.args.get("sort", "bytes_done"),
            dir=request.args.get("dir", "desc"),
            limit=min(int(request.args.get("limit", 100)), 500),
            offset=int(request.args.get("offset", 0)),
        )
        return jsonify(files=files, total=total)

    @bp.get("/jobs/<int:job_id>")
    def get_job(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            return jsonify(error="not found"), 404
        return jsonify(job)

    @bp.delete("/jobs/<int:job_id>")
    def cancel_job(job_id: int):
        run_async(engine.cancel_job(job_id))
        return jsonify(ok=True)

    @bp.post("/jobs/<int:job_id>/pause")
    def pause_job(job_id: int):
        run_async(engine.pause_job(job_id))
        return jsonify(ok=True)

    @bp.post("/jobs/<int:job_id>/resume")
    def resume_job(job_id: int):
        run_async(engine.start_job(job_id))
        return jsonify(ok=True)

    # ── proxies ───────────────────────────────────────────────────────
    @bp.get("/proxies")
    def list_proxies():
        state = request.args.get("state")
        limit = min(int(request.args.get("limit", 500)), 5000)
        if state:
            proxies = store.get_proxies_by_state(state, limit=limit)
        else:
            proxies = store.list_proxies(limit=limit)
        return jsonify(proxies=proxies, stats=engine.pool.stats())

    @bp.post("/proxies/import")
    def import_proxies():
        data = request.get_json(silent=True) or {}
        text = data.get("text") or ""
        url = data.get("url") or ""
        if not text and not url:
            return jsonify(error="text or url required"), 400
        if url:
            run_async(engine.enqueue_source(url))
            return jsonify(ok=True, queued=url), 202
        from swarm.proxies.sources import parse_proxy_lines
        entries = parse_proxy_lines(text, source="manual")
        for e in entries:
            store.upsert_proxy(e.url, protocol=e.protocol, source=e.source)
        return jsonify(ok=True, imported=len(entries))

    @bp.post("/proxies/bench")
    def bench_now():
        run_async(engine.bench_new_now())
        return jsonify(ok=True)

    @bp.get("/proxies/stats")
    def proxy_stats():
        return jsonify(engine.pool.stats())

    # ── events ────────────────────────────────────────────────────────
    @bp.get("/events")
    def events():
        limit = min(int(request.args.get("limit", 100)), 1000)
        since = request.args.get("since_id")
        return jsonify(events=store.get_events(limit=limit,
                                               since_id=int(since) if since else None))

    # ── SSE stream ────────────────────────────────────────────────────
    @bp.get("/stream")
    def stream():
        def generate():
            last_event_id = 0
            import time as _t
            while True:
                events = store.get_events(limit=50, since_id=last_event_id)
                if events:
                    last_event_id = max(e["id"] for e in events)
                    for e in reversed(events):   # chronological
                        yield f"id: {e['id']}\ndata: {json.dumps(e, default=str)}\n\n"
                # lightweight state poll (jobs + proxy stats)
                payload = {
                    "jobs": [{ "id": j["id"], "status": j["status"],
                               "files": [{"id": f["id"], "name": f["name"],
                                          "status": f["status"],
                                          "bytes_done": f["bytes_done"],
                                          "size": f["size"]}
                                         for f in j["files"]]} for j in store.list_jobs(limit=20)],
                    "proxies": engine.pool.stats(),
                }
                yield f"event: state\ndata: {json.dumps(payload, default=str)}\n\n"
                _t.sleep(1.0)

        resp = Response(generate(), mimetype="text/event-stream")
        resp.headers["Cache-Control"] = "no-cache"
        resp.headers["X-Accel-Buffering"] = "no"
        return resp

    # ── ui ────────────────────────────────────────────────────────────
    @bp.get("/health")
    def health():
        return jsonify(ok=True, stats=engine.pool.stats())

    return bp
