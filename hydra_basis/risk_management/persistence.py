from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


def atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    """Write ``text`` to ``path`` atomically.

    The content is written to a temporary file in the same directory and then
    ``os.replace``d onto the destination. ``os.replace`` is atomic on both POSIX
    and Windows, so a reader (or a crash) never observes a partially written
    file. This matters for the risk manager: a truncated registry/state file
    would crash every subsequent load and lose track of open positions.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding=encoding) as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


def atomic_write_json(path: Path, payload: Any, *, indent: int = 2, sort_keys: bool = True) -> None:
    atomic_write_text(path, json.dumps(payload, indent=indent, sort_keys=sort_keys))


def read_json_with_recovery(path: Path, *, default: Any = None) -> Any:
    """Read JSON from ``path``, tolerating a corrupt/half-written file.

    On a JSON decode error the corrupt file is preserved as ``<path>.corrupt``
    for later inspection and ``default`` is returned instead of raising, so a
    single bad write cannot permanently wedge the risk manager. A missing file
    also returns ``default``.
    """
    if not path.exists():
        return default
    text = path.read_text(encoding="utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        corrupt_path = path.with_suffix(path.suffix + ".corrupt")
        try:
            corrupt_path.write_text(text, encoding="utf-8")
            print(
                f"recovered from corrupt json at {path}; preserved copy at {corrupt_path}",
                flush=True,
            )
        except Exception:
            pass
        return default
