from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

import pytest

from rem.utils.lock import FileLock, LockInfo


@pytest.fixture  # type: ignore[misc]
def tmp_target(tmp_path: Path) -> Path:
    return tmp_path.joinpath("resource.txt")


def lock_path_for(target: Path) -> Path:
    return Path(str(target) + ".lock")


def test_acquire_and_release_with_lockfile(tmp_target: Path) -> None:
    lock_path = lock_path_for(tmp_target)
    with FileLock(tmp_target) as lock:
        assert lock_path.exists()
    assert not lock_path.exists()


def test_nonblocking_raises_when_held(tmp_target: Path) -> None:
    lock1 = FileLock(tmp_target)
    lock1.acquire()
    try:
        lock2 = FileLock(tmp_target)
        with pytest.raises(BlockingIOError):
            lock2.acquire(blocking=False)
    finally:
        lock1.release()
    assert not lock_path_for(tmp_target).exists()


def test_timeout_expires(tmp_target: Path) -> None:
    lock1 = FileLock(tmp_target)
    lock1.acquire()
    try:
        lock2 = FileLock(tmp_target)
        t0 = time.monotonic()
        with pytest.raises(TimeoutError):
            lock2.acquire(blocking=True, timeout=0.1, poll_interval=0.02)
        assert time.monotonic() - t0 >= 0.1
    finally:
        lock1.release()


def test_context_manager_releases_on_exception(tmp_target: Path) -> None:
    try:
        with FileLock(tmp_target):
            raise RuntimeError("testraise")
    except RuntimeError:
        pass
    assert not lock_path_for(tmp_target).exists()


def test_mutual_exclusion_between_threads(tmp_target: Path) -> None:
    order = []

    def worker(
        name: str, start_evt: threading.Event, done_evt: threading.Event
    ) -> None:
        start_evt.wait()
        with FileLock(tmp_target):
            order.append(name)
            time.sleep(0.05)
        done_evt.set()

    s1, s2 = threading.Event(), threading.Event()
    d1, d2 = threading.Event(), threading.Event()
    t1 = threading.Thread(target=worker, args=("A", s1, d1))
    t2 = threading.Thread(target=worker, args=("B", s2, d2))
    t1.start()
    t2.start()
    s1.set()
    s2.set()
    d1.wait()
    d2.wait()
    t1.join()
    t2.join()

    # Both ran, but one after the other
    assert order in (["A", "B"], ["B", "A"])


def test_stale_lock_breaking(tmp_target: Path) -> None:
    # Manually create a stale-looking lock
    lp = lock_path_for(tmp_target)
    lp.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lp, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
    try:
        info = LockInfo(pid=999999, hostname="testhost", timestamp=time.time() - 9999)
        os.write(fd, info.to_json().encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)

    # New lock with stale_after should break and acquire
    lock2 = FileLock(tmp_target, stale_after=0.5)
    lock2.acquire(blocking=True, timeout=1.0, poll_interval=0.05)
    try:
        assert lp.exists()  # exists while held
    finally:
        lock2.release()
    assert not lp.exists()
