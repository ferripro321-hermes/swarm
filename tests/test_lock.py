"""Tests for the single-instance engine lock."""

import os

import pytest

from swarm.engine.lock import EngineLock, EngineAlreadyRunning


def test_lock_acquire_and_release(tmp_path):
    lock_path = str(tmp_path / "engine.lock")
    with EngineLock(lock_path) as l1:
        assert l1.acquired
    # released -> can acquire again
    with EngineLock(lock_path) as l2:
        assert l2.acquired


def test_lock_prevents_second_instance(tmp_path):
    lock_path = str(tmp_path / "engine.lock")
    l1 = EngineLock(lock_path)
    assert l1.acquire()
    try:
        l2 = EngineLock(lock_path)
        assert not l2.acquire()
    finally:
        l1.release()


def test_lock_stale_pid_file_is_broken(tmp_path):
    # A lock file whose pid no longer exists must be breakable
    lock_path = str(tmp_path / "engine.lock")
    lock_path_obj = tmp_path / "engine.lock"
    lock_path_obj.write_text("999999999")  # pid almost certainly not running
    l = EngineLock(lock_path)
    assert l.acquire()
    l.release()
