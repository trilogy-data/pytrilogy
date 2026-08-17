"""Shared plumbing for Python script datasources.

A python datasource is a script run via ``uv run`` that writes an Arrow IPC
stream to stdout. DuckDB consumes that stream directly (see
``trilogy.dialect.duckdb``); warehouses that cannot execute local processes
stage the stream to object storage first (see
``trilogy.dialect.bigquery_staging``). Everything both paths share lives here.
"""

from __future__ import annotations

import hashlib
import json
import re
import shlex
import subprocess
import tempfile
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any

from trilogy.io.errors import ERROR_PREFIX, SCRIPT_ERROR_EXIT_CODE
from trilogy.io.runner import METADATA_PREFIX
from trilogy.io.sinks import normalize_object_uri, open_uri_sink

if TYPE_CHECKING:
    import pyarrow as pa

__all__ = [
    "MAX_ATTEMPTS",
    "RETRYABLE_UV_ERROR_MARKERS",
    "RETRY_DELAYS_SECONDS",
    "ParquetStreamWriter",
    "PythonDatasourceError",
    "build_uv_command",
    "is_retryable_uv_error",
    "normalize_object_uri",
    "open_uri_sink",
    "retry_delay",
    "script_metadata",
    "source_key",
    "staged_object_name",
    "stream_script",
]

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
        self.reported = parse_script_error(stderr)
        # A script using trilogy.io states its own failure on one line; falling
        # back to raw stderr means dumping a traceback plus whatever uv logged.
        if self.reported:
            detail = f"{self.reported['type']}: {self.reported['message']}"
        else:
            detail = stderr.strip() or (str(cause) if cause else "no output")
        super().__init__(
            f"Python datasource script '{script}' failed "
            f"(exit code {return_code}): {detail}"
        )


def parse_script_error(stderr: str) -> dict[str, Any] | None:
    """The structured failure a ``trilogy.io`` script writes ahead of its traceback."""
    for line in stderr.splitlines():
        if line.startswith(ERROR_PREFIX):
            try:
                return json.loads(line[len(ERROR_PREFIX) :])
            except json.JSONDecodeError:
                return None
    return None


def is_retryable_uv_error(message: str) -> bool:
    normalized = message.lower()
    return any(marker in normalized for marker in RETRYABLE_UV_ERROR_MARKERS)


def is_retryable(return_code: int, stderr: str) -> bool:
    """Whether a failed attempt is worth repeating.

    A script that exits ``SCRIPT_ERROR_EXIT_CODE`` has told us directly whether
    its own failure was transient, so believe it rather than pattern-matching
    stderr. Everything else is uv's exit, where the marker list is all we have.
    """
    reported = parse_script_error(stderr)
    if return_code == SCRIPT_ERROR_EXIT_CODE and reported is not None:
        return bool(reported.get("retryable"))
    return is_retryable_uv_error(stderr)


def script_metadata(schema: pa.Schema) -> dict[str, str]:
    """Sideband facts the script attached to its stream (contract, watermark)."""
    return {
        key.decode()[len(METADATA_PREFIX) :]: value.decode()
        for key, value in (schema.metadata or {}).items()
        if key.decode().startswith(METADATA_PREFIX)
    }


def build_uv_command(script: str, args: str = "") -> list[str]:
    return [*UV_RUN_PREFIX, script, *shlex.split(args)]


def build_script_command(script: str) -> list[str]:
    """How to invoke ``script``: uv for python, anything else runs directly."""
    if Path(script).suffix == ".py":
        return build_uv_command(script)
    return [script]


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
        if attempt < max_attempts and is_retryable(return_code, stderr):
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
