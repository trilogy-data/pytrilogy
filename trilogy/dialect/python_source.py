"""Shared plumbing for Python script datasources.

A python datasource is a script run via ``uv run`` that writes an Arrow IPC
stream to stdout. DuckDB consumes that stream directly (see
``trilogy.dialect.duckdb``); warehouses that cannot execute local processes
stage the stream to object storage first (see
``trilogy.dialect.bigquery_staging``). Everything both paths share lives here.
"""

from __future__ import annotations

import hashlib
import re
import shlex
import subprocess
import tempfile
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa

MAX_ATTEMPTS = 3
RETRY_DELAYS_SECONDS = (0.5, 1.5)
RETRYABLE_UV_ERROR_MARKERS = (
    "failed to acquire",
    "file lock",
    "database is locked",
    "resource temporarily unavailable",
    "being used by another process",
    "the process cannot access the file",
    "access is denied",
    "os error 32",
)

UV_RUN_PREFIX = ("uv", "run", "--no-project", "--quiet")

# Identifier-safe stem for generated staging table names.
_UNSAFE_IDENTIFIER = re.compile(r"[^A-Za-z0-9_]")
_MAX_STEM_LENGTH = 32


class PythonDatasourceError(RuntimeError):
    """A python datasource script failed to produce an Arrow stream."""

    def __init__(
        self,
        script: str,
        return_code: int,
        stderr: str,
        cause: Exception | None = None,
    ):
        self.script = script
        self.return_code = return_code
        self.stderr = stderr
        detail = stderr.strip() or (str(cause) if cause else "no output")
        super().__init__(
            f"Python datasource script '{script}' failed "
            f"(exit code {return_code}): {detail}"
        )


def is_retryable_uv_error(message: str) -> bool:
    normalized = message.lower()
    return any(marker in normalized for marker in RETRYABLE_UV_ERROR_MARKERS)


def build_uv_command(script: str, args: str = "") -> list[str]:
    return [*UV_RUN_PREFIX, script, *shlex.split(args)]


def source_key(location: str, args: str = "") -> str:
    """Stable digest of a script invocation, used to name staged artifacts."""
    return hashlib.md5(f"{location}|{args}".encode()).hexdigest()


def staged_object_name(location: str, args: str = "", prefix: str = "") -> str:
    """Readable, identifier-safe name for the artifact a script invocation stages.

    Keyed on script path + args rather than script contents so a given script
    maps to one stable artifact that is refreshed in place; hashing contents
    would leave an unbounded trail of orphans behind every edit.
    """
    stem = _UNSAFE_IDENTIFIER.sub("_", Path(location).stem)[:_MAX_STEM_LENGTH]
    return f"{prefix}{stem}_{source_key(location, args)[:12]}"


def retry_delay(attempt: int) -> float:
    return RETRY_DELAYS_SECONDS[min(attempt, len(RETRY_DELAYS_SECONDS)) - 1]


def _stream_once(
    script: str,
    args: str,
    write: Callable[[pa.Schema, Iterator[Any]], int],
) -> tuple[int, int, str, Exception | None]:
    """One attempt. Returns ``(return_code, rows, stderr, failure)``.

    stderr goes to a temp file rather than a pipe: nothing drains it while we
    read stdout, and a pipe would deadlock on a chatty script.
    """
    import pyarrow as pa

    rows = 0
    failure: Exception | None = None
    with tempfile.TemporaryFile() as error_file:
        process = subprocess.Popen(
            build_uv_command(script, args),
            stdout=subprocess.PIPE,
            stderr=error_file,
        )
        try:
            reader = pa.ipc.open_stream(process.stdout)
            rows = write(reader.schema, reader)
        except Exception as e:
            failure = e
            process.kill()
        finally:
            if process.stdout:
                process.stdout.close()
            return_code = process.wait()
        error_file.seek(0)
        stderr = error_file.read().decode("utf-8", errors="replace")
    return return_code, rows, stderr, failure


def stream_script(
    script: str,
    args: str,
    write: Callable[[pa.Schema, Iterator[Any]], int],
    max_attempts: int = MAX_ATTEMPTS,
) -> int:
    """Run a python datasource script, handing its record batches to ``write``.

    ``write`` receives the Arrow schema and a batch iterator and returns the
    number of rows it consumed; it is called once per attempt, so it must be
    able to start over (e.g. reopen its output stream). Transient uv cache-lock
    errors are retried before the failure is surfaced.
    """
    for attempt in range(1, max_attempts + 1):
        return_code, rows, stderr, failure = _stream_once(script, args, write)
        if return_code == 0 and failure is None:
            return rows
        if attempt < max_attempts and is_retryable_uv_error(stderr):
            time.sleep(retry_delay(attempt))
            continue
        raise PythonDatasourceError(script, return_code, stderr, failure)
    raise PythonDatasourceError(script, 1, "exhausted retries")


@dataclass
class ParquetStreamWriter:
    """A ``stream_script`` writer that streams batches out as parquet.

    Batches are written as they arrive, so neither the full Arrow stream nor the
    parquet file is ever held in memory or spilled to local disk.
    """

    open_sink: Callable[[], IO[bytes]]
    compression: str = "snappy"

    def __call__(self, schema: pa.Schema, batches: Iterator[Any]) -> int:
        import pyarrow.parquet as pq

        rows = 0
        with self.open_sink() as sink, pq.ParquetWriter(
            sink, schema, compression=self.compression
        ) as writer:
            for batch in batches:
                writer.write_batch(batch)
                rows += batch.num_rows
        return rows


def open_uri_sink(uri: str) -> Callable[[], IO[bytes]]:
    """Return a factory opening a writable binary stream at ``uri``.

    Resolves local paths, ``gs://`` and ``s3://`` through pyarrow's filesystem
    layer, which authenticates from the ambient environment (ADC for GCS).
    """
    from pyarrow import fs as pafs

    filesystem, path = pafs.FileSystem.from_uri(normalize_object_uri(uri))
    # Object stores have no directories, but a local filesystem will not create
    # the parent of a nested staging path on its own.
    if isinstance(filesystem, pafs.LocalFileSystem):
        parent = path.rsplit("/", 1)[0]
        if parent:
            filesystem.create_dir(parent, recursive=True)
    return lambda: filesystem.open_output_stream(path)


def normalize_object_uri(uri: str) -> str:
    """pyarrow registers GCS under the ``gs`` scheme only; accept ``gcs://`` too."""
    if uri.startswith("gcs://"):
        return "gs://" + uri[len("gcs://") :]
    return uri
