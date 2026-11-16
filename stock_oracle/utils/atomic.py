"""
Crash-safe file writes.

The GUI and the terminal are separate processes sharing one data folder, so
a file written in place can be read half-written, or left truncated by a
crash. Write to a temp file in the same folder and swap it in with
os.replace, which is atomic on NTFS and POSIX.
"""
import os
import time
from pathlib import Path
from typing import Union


def _replace(tmp: Path, path: Path):
    # Windows: a scanner/indexer can hold the just-written target for a few
    # ms, and os.replace then fails with WinError 5
    for attempt in range(40):
        try:
            os.replace(tmp, path)
            return
        except PermissionError:
            if attempt == 39:
                raise
            time.sleep(0.025)


def write_bytes_atomic(path: Union[str, Path], data: bytes):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_bytes(data)
        _replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def write_text_atomic(path: Union[str, Path], text: str, encoding: str = "utf-8"):
    write_bytes_atomic(path, text.encode(encoding))
