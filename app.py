"""Swarm entrypoint: Flask app + asyncio engine thread.

Run: .venv/bin/python app.py   (port 6970 by default)
"""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path

from flask import Flask, send_from_directory

from swarm.config import load_settings
from swarm.store import Store
from swarm.proxies.pool import ProxyPool
from swarm.engine.jobs import Engine
from swarm.web.api import make_api_blueprint


def _engine_thread(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


def create_app(config_path: str | None = None) -> tuple[Flask, Engine, asyncio.AbstractEventLoop]:
    settings = load_settings(config_path)
    store = Store(settings.db_path)
    pool = ProxyPool(store, ban_ttl_s=settings.proxy.ban_ttl_h * 3600,
                     fail_ban_after=settings.proxy.fail_ban_after)

    loop = asyncio.new_event_loop()
    engine = Engine(settings, store, pool)
    # Prime the pool inside the engine loop (it will own it)
    asyncio.run_coroutine_threadsafe(_prime(pool), loop)

    t = threading.Thread(target=_engine_thread, args=(loop,), daemon=True,
                         name="swarm-engine")
    t.start()

    # start the proxy refresh loop on the engine loop
    asyncio.run_coroutine_threadsafe(engine.refresh_proxies_forever(), loop)

    app = Flask(__name__, static_folder=None)

    @app.get("/")
    def index():
        static_dir = Path(__file__).parent / "swarm" / "web" / "static"
        return send_from_directory(static_dir, "index.html")

    @app.get("/static/<path:filename>")
    def static_files(filename: str):
        static_dir = Path(__file__).parent / "swarm" / "web" / "static"
        return send_from_directory(static_dir, filename)

    api = make_api_blueprint(engine, store, loop)
    app.register_blueprint(api)

    app.engine = engine  # type: ignore[attr-defined]
    app.store = store    # type: ignore[attr-defined]
    return app, engine, loop


async def _prime(pool: ProxyPool) -> None:
    pool.prime_from_store()


def main() -> None:
    app, engine, loop = create_app("config.yaml")
    host = app.engine.settings.server.host
    port = app.engine.settings.server.port
    print(f"🚂 Swarm listening on http://{host}:{port}")
    try:
        app.run(host=host, port=port, threaded=True, use_reloader=False)
    finally:
        asyncio.run_coroutine_threadsafe(engine.shutdown(), loop).result(timeout=10)


if __name__ == "__main__":
    main()
