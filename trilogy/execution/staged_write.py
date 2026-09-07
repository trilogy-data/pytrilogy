"""Write file artifacts through a staging path and one atomic rename.

A writer that dies part-way (a failed query, an OOM kill) otherwise leaves a
truncated file under the target's own name, and the next multi-file scan
reads it as a real artifact. Here the bytes land in a ``.trilogy-staging``
sibling directory and the target is replaced by a single rename, so it is
either the previous complete file or the new complete file.

Staging lives in a subdirectory rather than beside the target because
DuckDB's ``*`` glob matches dot-prefixed siblings: a stray staging file in the
target's directory would still be scanned.

The configured ``[staging] path`` is used instead when it is a local
directory on the same filesystem as the target, keeping every scratch file in
one place. A rename is only atomic within one filesystem, which is why the
sibling directory remains the fallback rather than the other way round.

Remote targets stage the same way, under a ``.trilogy-staging`` key prefix,
and finalize with a server-side copy followed by a delete. Object stores
expose no rename, but a copy is atomic in the only sense that matters here:
the destination is replaced whole or not at all.

They are not all-or-nothing on their own, which is what this module first
assumed. DuckDB writes ``gcs://`` through its S3-compatible multipart
uploader, and a writer that dies part-way can still finalize the parts it
had already sent -- observed as a 143 MB parquet carrying a ``PAR1`` header,
page data and no footer, sitting under the real object name and breaking
every multi-file scan that read it.

GCS is reached through its S3-compatible endpoint with the same
``GOOGLE_HMAC_KEY``/``GOOGLE_HMAC_SECRET`` pair DuckDB writes with, not
through pyarrow's GcsFileSystem: the write and the swap have to be one
identity, or a pipeline authorised to publish cannot finalize what it wrote.
A target we cannot build a filesystem for writes in place, as before -- no
credentials is a reason to lose the protection, never the write.
"""

from __future__ import annotations

import glob
import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from trilogy.constants import logger

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


def _same_filesystem(a: Path, b: Path) -> bool:
    try:
        return os.stat(a).st_dev == os.stat(b).st_dev
    except OSError:
        return False


def _shared_root(staging_root: str | None, parent: Path) -> Path | None:
    """The configured staging root, when the target can be renamed out of it."""
    if staging_root is None or is_remote_target(staging_root):
        return None
    root = Path(staging_root)
    try:
        root.mkdir(parents=True, exist_ok=True)
    except OSError:
        return None
    return root if _same_filesystem(root, parent) else None


_GCS_SCHEMES = ("gs", "gcs")
_GCS_S3_ENDPOINT = "storage.googleapis.com"
# Schemes whose ``<scheme>://<rest>`` splits cleanly into the ``bucket/key``
# path pyarrow addresses objects by, so the staged key round-trips back into a
# URI the writer can be handed. Anything else -- ``file://`` most of all, whose
# path is platform-shaped -- keeps the old write-in-place behaviour.
_OBJECT_STORE_SCHEMES = (*_GCS_SCHEMES, "s3")


def split_remote(uri: str) -> tuple[str, str]:
    """``gcs://bucket/a/b`` -> ``("gcs", "bucket/a/b")``."""
    scheme, _, rest = uri.partition("://")
    return scheme, rest


def _remote_filesystem(scheme: str, uri: str):
    """A pyarrow filesystem able to copy and delete at ``uri``, or None.

    None means "write in place": the caller keeps the pre-staging behaviour
    rather than failing a write it could otherwise complete.
    """
    from pyarrow import fs as pafs

    if scheme in _GCS_SCHEMES:
        key = os.environ.get("GOOGLE_HMAC_KEY")
        secret = os.environ.get("GOOGLE_HMAC_SECRET")
        if not key or not secret:
            return None
        return pafs.S3FileSystem(
            access_key=key,
            secret_key=secret,
            endpoint_override=_GCS_S3_ENDPOINT,
            scheme="https",
        )
    if scheme not in _OBJECT_STORE_SCHEMES:
        return None
    try:
        filesystem, _ = pafs.FileSystem.from_uri(uri)
    except Exception:
        return None
    return filesystem


def _delete_remote(filesystem, path: str) -> None:
    """Best-effort: a staged key we cannot collect costs storage, and the
    next writer of this target sweeps it. Never worth failing a good publish
    for, but worth saying out loud -- a rising count of these is a
    credential or permission problem, not noise."""
    try:
        filesystem.delete_file(path)
    except Exception as exc:
        logger.warning("Could not remove staged object %s: %s", path, exc)


def _sweep_remote(filesystem, prefix: str, name: str) -> None:
    """Drop what an earlier, killed writer of this same target left staged."""
    from pyarrow import fs as pafs

    try:
        entries = filesystem.get_file_info(
            pafs.FileSelector(prefix, allow_not_found=True)
        )
    except Exception:
        return
    for entry in entries:
        base = entry.path.rsplit("/", 1)[-1]
        if base.startswith(f"{name}.") and base.endswith(".tmp"):
            _delete_remote(filesystem, entry.path)


def _remove_if_empty(staging: Path) -> None:
    try:
        staging.rmdir()
    except OSError:
        pass


@contextmanager
def staged_write(target: str, staging_root: str | None = None) -> Iterator[str]:
    """Yield the path to write instead of ``target``.

    On a clean exit the target is replaced by the staged file in one rename;
    on any exception the staged file is deleted and the target is untouched.
    A remote target stages under a key prefix and is finalized by a
    server-side copy. ``staging_root`` is a per-process scratch
    directory (the executor's ``[staging] path`` subdir) to prefer over the
    sibling directory; it is used only when a rename out of it can succeed.
    """
    if is_remote_target(target):
        scheme, path = split_remote(target)
        filesystem = _remote_filesystem(scheme, target)
        if filesystem is None:
            yield target
            return
        head, _, name = path.rpartition("/")
        prefix = f"{head}/{STAGING_DIR}" if head else STAGING_DIR
        staged = f"{prefix}/{name}.{uuid.uuid4().hex[:8]}.tmp"
        _sweep_remote(filesystem, prefix, name)
        try:
            yield f"{scheme}://{staged}"
            # Either the destination is replaced whole or it is untouched;
            # a failure here leaves the previous object in place, which is
            # the state a partial write used to destroy.
            filesystem.copy_file(staged, path)
        finally:
            _delete_remote(filesystem, staged)
        return
    final = Path(target)
    if not final.parent.is_dir():
        raise FileNotFoundError(
            f"cannot write '{target}': directory '{final.parent}' does not exist"
        )
    shared = _shared_root(staging_root, final.parent)
    staging = shared or final.parent / STAGING_DIR
    token = uuid.uuid4().hex[:8]
    try:
        staging.mkdir(exist_ok=True)
        # What an earlier writer of this target left behind when killed,
        # under our name or DuckDB's own tmp_ prefix on it. A shared root is
        # per process and cleaned at exit, and the same basename there may
        # belong to another target, so only the sibling directory is swept.
        if shared is None:
            for prefix in ("", "tmp_"):
                _sweep(staging, f"{prefix}{glob.escape(final.name)}.*.tmp")
        tmp = _claim(staging, final.name, token)
        yield str(tmp)
        os.replace(tmp, final)
    finally:
        # Writers may stage beside the path they were given (DuckDB's own
        # ``tmp_`` prefix), so sweep by token rather than unlinking one path.
        _sweep(staging, f"*{token}*")
        if shared is None:
            _remove_if_empty(staging)


def write_text_staged(path: Path, content: str) -> None:
    with staged_write(str(path)) as tmp:
        Path(tmp).write_text(content, encoding="utf-8")
