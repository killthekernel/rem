from __future__ import annotations

import json
import os
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Optional

from rem.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class LockInfo:
    pid: int
    hostname: str
    timestamp: float

    def to_json(self) -> str:
        return json.dumps(
            {
                "pid": self.pid,
                "hostname": self.hostname,
                "timestamp": self.timestamp,
            }
        )

    @staticmethod
    def from_json(path: Path) -> Optional[LockInfo]:
        try:
            with path.open("r") as f:
                data = json.load(f)
            return LockInfo(
                pid=int(data["pid"]),
                hostname=str(data["hostname"]),
                timestamp=float(data["timestamp"]),
            )
        except Exception as e:
            logger.error(f"Failed to read lock file {path}: {e}")
            return None


class FileLock:
    """
    Cross-platform file lock using atomic creation and unlinking of a lock file.

    Acquire the lock using atomic creation (O_CREAT | O_EXCL). If exists, lock is held by another process.
    Hold the lock by keeping the file descriptor open, write LockInfo for traceability.
    Release the lock by closing the file descriptor and unlinking the lock file.
    """

    def __init__(
        self,
        target_path: Path,
        suffix: str = ".lock",
        stale_after: Optional[float] = 10.0,
    ) -> None:
        self.lock_path = Path(str(target_path) + suffix)
        self._fd: Optional[int] = None
        self._stale_after = stale_after

    def acquire(
        self,
        blocking: bool = True,
        timeout: Optional[float] = None,
        poll_interval: float = 0.05,
    ) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        start = time.monotonic()

        while True:
            try:
                # Atomic creation of the lock file
                self._fd = os.open(
                    self.lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644
                )
                info = LockInfo(
                    pid=os.getpid(),
                    hostname=socket.gethostname(),
                    timestamp=time.time(),
                )
                os.write(self._fd, info.to_json().encode("utf-8"))
                os.fsync(self._fd)
                logger.debug(
                    f"Acquired lock: {self.lock_path} by PID {info.pid} on {info.hostname}"
                )
                return
            except FileExistsError:
                if not blocking:
                    msg = f"Lock busy (non-blocking): {self.lock_path} is held by another process."
                    logger.warning(msg)
                    raise BlockingIOError(f"Lock busy: {self.lock_path}")
                if timeout is not None and (time.monotonic() - start) >= timeout:
                    msg = f"Timeout while waiting for lock on {self.lock_path}."
                    logger.error(msg)
                    raise TimeoutError(msg)

                if self._stale_after is not None:
                    existing_info = LockInfo.from_json(self.lock_path)
                    if (
                        existing_info is not None
                        and (time.time() - existing_info.timestamp) > self._stale_after
                    ):
                        logger.warning(
                            f"Stale lock detected at {self.lock_path}, removing."
                        )
                        try:
                            self._break_stale_lock(existing_info)
                        except Exception:
                            pass  # Ignore errors, will retry acquiring the lock
                time.sleep(poll_interval)

    def _break_stale_lock(self, info: LockInfo) -> None:
        try:
            self.lock_path.unlink()
        except FileNotFoundError:
            pass

    def release(self) -> None:
        if self._fd is not None:
            try:
                os.close(self._fd)
            except Exception as e:
                logger.error(f"Error closing lock file descriptor: {e}")
            finally:
                self._fd = None
        try:
            self.lock_path.unlink()
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.error(f"Error removing lock file {self.lock_path}: {e}")

    def __enter__(self) -> FileLock:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.release()
