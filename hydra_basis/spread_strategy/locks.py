"""Per-symbol OS lock shared by the standalone runner and the dispatcher.

The OS releases the lock if the process dies; a lock file held by another
process is never deleted.
"""
from __future__ import annotations

import hashlib
import os
from contextlib import contextmanager
from pathlib import Path

LOCK_DIR = Path("data/spread_strategies")


def lock_path(symbol: str, mode: str) -> Path:
    return LOCK_DIR / f"{hashlib.sha256(symbol.encode()).hexdigest()[:16]}.{mode}.lock"


class SymbolLock:
    def __init__(self, path: Path):
        self.path = path
        self._handle = None

    def acquire(self):
        if self._handle is not None:
            return  # already held by this instance (e.g. a paused group being resumed)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        try:
            if self.path.stat().st_size == 0:
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise RuntimeError(f"another spread strategy owns this symbol ({self.path.name})") from exc
        self._handle = handle

    def release(self):
        handle, self._handle = self._handle, None
        if handle is None:
            return
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


@contextmanager
def symbol_lock(path: Path):
    lock = SymbolLock(path)
    lock.acquire()
    try:
        yield
    finally:
        lock.release()
