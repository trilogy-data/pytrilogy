"""Storage backends for the `trilogy file` CLI command.

The backend abstraction exists so that `trilogy file` can target remote
stores (GCS, S3, remote git models) in the future without CLI changes.
Only the local filesystem backend ships today; additional backends can
register themselves via :func:`register_backend`.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Protocol
from urllib.parse import urlparse


class FileOperationError(Exception):
    """Base error for file backend operations."""


class FileNotFoundError(
    FileOperationError
):  # noqa: A001 - intentional shadow for API clarity
    """Raised when a backend cannot locate the requested path."""


@dataclass(frozen=True)
class FileEntry:
    """Metadata about a single file discovered by a backend."""

    path: str
    size: int
    is_dir: bool = False


class FileBackend(Protocol):
    """Minimal CRUD interface for file-like storage backends."""

    scheme: str

    def list(self, path: str, recursive: bool = False) -> list[FileEntry]: ...

    def read(self, path: str) -> bytes: ...

    def write(self, path: str, data: bytes) -> None: ...

    def delete(self, path: str, recursive: bool = False) -> None: ...

    def exists(self, path: str) -> bool: ...

    def move(self, src: str, dst: str) -> None: ...


class LocalFileBackend:
    """Backend that maps directly onto the local filesystem."""

    scheme = "file"

    def _resolve(self, path: str) -> Path:
        parsed = urlparse(path)
        raw = parsed.path if parsed.scheme == self.scheme else path
        return Path(raw).expanduser()

    def list(self, path: str, recursive: bool = False) -> list[FileEntry]:
        target = self._resolve(path)
        if not target.exists():
            raise FileNotFoundError(f"No such path: {path}")
        entries: list[FileEntry] = []
        if target.is_file():
            entries.append(
                FileEntry(path=str(target), size=target.stat().st_size, is_dir=False)
            )
            return entries
        iterator: Iterable[Path] = target.rglob("*") if recursive else target.iterdir()
        for item in sorted(iterator):
            try:
                size = item.stat().st_size if item.is_file() else 0
            except OSError:
                continue
            entries.append(FileEntry(path=str(item), size=size, is_dir=item.is_dir()))
        return entries

    def read(self, path: str) -> bytes:
        target = self._resolve(path)
        if not target.exists() or not target.is_file():
            raise FileNotFoundError(f"No such file: {path}")
        return target.read_bytes()

    def write(self, path: str, data: bytes) -> None:
        target = self._resolve(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)

    def delete(self, path: str, recursive: bool = False) -> None:
        target = self._resolve(path)
        if not target.exists():
            raise FileNotFoundError(f"No such path: {path}")
        if target.is_dir():
            if not recursive:
                raise FileOperationError(
                    f"{path} is a directory; pass recursive=True to remove it."
                )
            shutil.rmtree(target)
        else:
            target.unlink()

    def exists(self, path: str) -> bool:
        return self._resolve(path).exists()

    def move(self, src: str, dst: str) -> None:
        source = self._resolve(src)
        destination = self._resolve(dst)
        if not source.exists():
            raise FileNotFoundError(f"No such path: {src}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(destination))


_BACKENDS: dict[str, Callable[[], FileBackend]] = {
    "": lambda: LocalFileBackend(),
    "file": lambda: LocalFileBackend(),
}


def register_backend(scheme: str, factory: Callable[[], FileBackend]) -> None:
    """Register a backend factory for a URL scheme (e.g. ``gs``, ``s3``)."""
    _BACKENDS[scheme] = factory


def _scheme_for(path: str) -> str:
    # Windows drive letters (e.g. "C:\\foo") would otherwise be parsed as a
    # URL scheme by urlparse; detect and treat them as local paths.
    if len(path) >= 2 and path[1] == ":" and path[0].isalpha():
        return ""
    return urlparse(path).scheme


def get_backend(path: str) -> FileBackend:
    """Return the backend responsible for *path* based on its URL scheme."""
    scheme = _scheme_for(path)
    factory = _BACKENDS.get(scheme)
    if factory is None:
        raise FileOperationError(
            f"No backend registered for scheme {scheme!r}. "
            f"Known schemes: {sorted(s for s in _BACKENDS if s) or ['(local)']}."
        )
    return factory()
