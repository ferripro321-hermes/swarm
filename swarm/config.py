"""Swarm configuration: YAML file + environment overrides into typed dataclasses."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 6970


@dataclass
class EngineConfig:
    max_parallel_files: int = 3
    workers_per_file: int = 4
    chunk_timeout_s: float = 30.0
    url_timeout_s: float = 20.0


@dataclass
class BenchConfig:
    connect_timeout_s: float = 5.0
    mega_probe_timeout_s: float = 8.0
    speed_cap_mb: float = 3.0
    speed_timeout_s: float = 10.0
    min_throughput_kbps: float = 250.0
    rebench_after_s: float = 3600.0
    speed_url: str = "https://mega.nz/secureboot.js"


@dataclass
class ProxyConfig:
    ban_ttl_h: float = 6.0
    fail_ban_after: int = 3
    sources: list[str] = field(default_factory=lambda: [
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/all.txt",
        "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000",
    ])
    refresh_min: float = 30.0
    bench: BenchConfig = field(default_factory=BenchConfig)


@dataclass
class DownloadsConfig:
    dest: str = "data/downloads"


@dataclass
class NordConfig:
    """NordVPN proxy endpoints (service credentials from the Nord dashboard)."""
    user: str = ""
    password: str = ""
    countries: list[str] = field(default_factory=lambda: ["ES", "FR", "DE", "BE", "NL", "SE"])
    port89: bool = True              # scan undocumented TLS-CONNECT (port 89) servers
    scan_concurrency: int = 120

    @property
    def enabled(self) -> bool:
        return bool(self.user and self.password)


@dataclass
class Settings:
    server: ServerConfig = field(default_factory=ServerConfig)
    engine: EngineConfig = field(default_factory=EngineConfig)
    proxy: ProxyConfig = field(default_factory=ProxyConfig)
    downloads: DownloadsConfig = field(default_factory=DownloadsConfig)
    nord: NordConfig = field(default_factory=NordConfig)
    db_path: str = "data/swarm.db"


def _apply_section(dc, data: dict | None, prefix: str) -> None:
    for key, value in (data or {}).items():
        attr = str(key)
        env_key = f"SWARM_{prefix}_{attr}".upper()
        if os.getenv(env_key) is not None:
            value = os.getenv(env_key)
        if not hasattr(dc, attr):
            continue
        current = getattr(dc, attr)
        if isinstance(current, bool):
            setattr(dc, attr, str(value).lower() in ("1", "true", "yes"))
        elif isinstance(current, list):
            # list attrs (e.g. nord.countries): accept YAML lists or comma strings
            if isinstance(value, str):
                value = [v.strip() for v in value.split(",") if v.strip()]
            setattr(dc, attr, value)
        elif isinstance(current, int) and not isinstance(current, bool):
            setattr(dc, attr, int(str(value)))
        elif isinstance(current, float):
            setattr(dc, attr, float(str(value)))
        else:
            setattr(dc, attr, value)


def load_settings(config_path: str | Path | None = None) -> Settings:
    """Load settings from config.yaml, then env overrides (SWARM_SECTION_KEY)."""
    settings = Settings()
    path = Path(config_path) if config_path else Path("config.yaml")
    raw: dict = {}
    if path.exists():
        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
        raw = loaded if isinstance(loaded, dict) else {}
    proxy_raw = dict(raw.get("proxy") or {})
    bench_raw = proxy_raw.pop("bench", None)
    _apply_section(settings.server, raw.get("server"), "SERVER")
    _apply_section(settings.engine, raw.get("engine"), "ENGINE")
    _apply_section(settings.proxy, proxy_raw, "PROXY")
    _apply_section(settings.proxy.bench, bench_raw if isinstance(bench_raw, dict) else {}, "BENCH")
    _apply_section(settings.downloads, raw.get("downloads"), "DOWNLOADS")
    _apply_section(settings.nord, raw.get("nord"), "NORD")
    env_db = os.getenv("SWARM_DB_PATH")
    if env_db:
        settings.db_path = env_db
    elif raw.get("db_path"):
        settings.db_path = str(raw["db_path"])
    # Ensure paths exist
    Path(settings.db_path).parent.mkdir(parents=True, exist_ok=True)
    Path(settings.downloads.dest).mkdir(parents=True, exist_ok=True)
    return settings
