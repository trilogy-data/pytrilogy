"""Write file artifacts through a staging path and one atomic rename.

A writer that dies part-way (a failed query, an OOM kill) otherwise leaves a
truncated file under the target's own name, and the next multi-file scan
reads it as a real artifact. Here the bytes land in a ``.trilogy-staging``
sibling directory and the target is replaced by a single rename, so it is
either the previous complete file or the new complete file.

Staging lives in a subdirectory rather than beside the target because
DuckDB's ``*`` glob matches dot-prefixed siblings: a stray staging file in the
target's directory would still be scanned.

Remote targets (``gs://`` and friends) write in place. Object stores expose
no rename, and their uploads are already all-or-nothing.
"""

from __future__ import annotations

import glob
import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

STAGING_DIR = ".trilogy-staging"
_CREATE_ATTEMPTS = 5


def is_remote_target(target: str) -> bool:
    return "://" in target


def _sweep(staging: Path, pattern: str) -> None:
    for old in staging.glob(pattern):
        try:
            old.unlink()
        except OSError:
            pass


def _claim(staging: Path, name: str, token: str) -> Path:
    """Create the staging file, re-making the directory if a sibling writer
    removed it as empty between our mkdir and our touch."""
    tmp = staging / f"{name}.{token}.tmp"
    for attempt in range(_CREATE_ATTEMPTS):
        staging.mkdir(exist_ok=True)
        try:
            tmp.touch()
            return tmp
        except FileNotFoundError:
            if attempt == _CREATE_ATTEMPTS - 1:
                raise
    raise AssertionError("unreachable")


def _remove_if_empty(staging: Path) -> None:
    try:
        staging.rmdir()
    except OSError:
        pass


@contextmanager
def staged_write(target: str) -> Iterator[str]:
    """Yield the path to write instead of ``target``.

    On a clean exit the target is replaced by the staged file in one rename;
    on any exception the staged file is deleted and the target is untouched.
    A remote target yields itself.
    """
    if is_remote_target(target):
        yield target
        return
    final = Path(target)
    if not final.parent.is_dir():
        raise FileNotFoundError(
            f"cannot write '{target}': directory '{final.parent}' does not exist"
        )
    staging = final.parent / STAGING_DIR
    token = uuid.uuid4().hex[:8]
    try:
        staging.mkdir(exist_ok=True)
        # What an earlier writer of this target left behind when killed,
        # under our name or DuckDB's own tmp_ prefix on it.
        for prefix in ("", "tmp_"):
            _sweep(staging, f"{prefix}{glob.escape(final.name)}.*.tmp")
        tmp = _claim(staging, final.name, token)
        yield str(tmp)
        os.replace(tmp, final)
    finally:
        # Writers may stage beside the path they were given (DuckDB's own
        # ``tmp_`` prefix), so sweep by token rather than unlinking one path.
        _sweep(staging, f"*{token}*")
        _remove_if_empty(staging)


def write_text_staged(path: Path, content: str) -> None:
    with staged_write(str(path)) as tmp:
        Path(tmp).write_text(content, encoding="utf-8")
