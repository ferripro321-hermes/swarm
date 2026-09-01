"""Single-instance guard: PID-based lock so two Flask workers never double-run the engine."""

from __future__ import annotations

import os
from pathlib import Path


class EngineAlreadyRunning(Exception):
    pass


class EngineLock:
    def __init__(self, path: str):
        self.path = Path(path)
        self.acquired = False

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            try:
                old_pid = int(self.path.read_text().strip())
            except (ValueError, OSError):
                old_pid = None
            if old_pid is not None and _pid_alive(old_pid):
                return False
            # stale lock — break it
            try:
                self.path.unlink()
            except OSError:
                return False
        try:
            self.path.write_text(str(os.getpid()))
            self.acquired = True
            return True
        except OSError:
            return False

    def release(self) -> None:
        if self.acquired and self.path.exists():
            try:
                if self.path.read_text().strip() == str(os.getpid()):
                    self.path.unlink()
            except OSError:
                pass
        self.acquired = False

    def __enter__(self) -> "EngineLock":
        if not self.acquire():
            raise EngineAlreadyRunning(f"engine already running (lock: {self.path})")
        return self

    def __exit__(self, *exc) -> None:
        self.release()


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists but not ours
