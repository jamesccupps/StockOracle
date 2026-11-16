"""
An exclusive lock across threads and processes, for a short read-modify-write
of a file the GUI and the terminal both touch. Hold it only around file I/O,
never across network calls.
"""
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Union

_thread_locks: Dict[str, threading.RLock] = {}
_guard = threading.Lock()


def _thread_lock(path: Path) -> threading.RLock:
    with _guard:
        return _thread_locks.setdefault(str(path.resolve()), threading.RLock())


@contextmanager
def file_lock(lock_path: Union[str, Path], timeout: float = 10.0):
    lock_path = Path(lock_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with _thread_lock(lock_path):
        with open(lock_path, "a+b") as fh:
            if os.name == "nt":
                import msvcrt
                deadline = time.monotonic() + timeout
                while True:
                    try:
                        fh.seek(0)
                        msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                        break
                    except OSError:
                        if time.monotonic() > deadline:
                            raise TimeoutError(f"{lock_path} held for more than {timeout}s")
                        time.sleep(0.02)
                try:
                    yield
                finally:
                    fh.seek(0)
                    msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(fh, fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(fh, fcntl.LOCK_UN)
