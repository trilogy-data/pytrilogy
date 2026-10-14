"""A VGI worker that streams python datasource scripts into DuckDB.

The opt-in alternative to the ``uv_run`` macro (``python_datasource_transport``
on ``DuckDBConfig``). DuckDB attaches this module once as a pooled worker and
renders each script as ``trilogy_io.main.script(path, args)``; the worker runs
the script as ``uv_run`` does (a compiled source runs directly) and hands its
Arrow batches over the VGI pipe, so nothing is staged to disk and no shell pipe
is involved.

Requires a DuckDB that can ``LOAD vgi`` (Query.Farm's ``haybarn``) and the
``vgi-python`` package in the interpreter DuckDB launches this worker with.
"""

from __future__ import annotations

import json
import subprocess
import tempfile
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import IO, Annotated, Any

import pyarrow as pa
from vgi import Arg, Worker
from vgi.catalog import Catalog, Schema
from vgi.invocation import BindResponse
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from trilogy.dialect.python_source import (
    MAX_ATTEMPTS,
    VGI_FAILURE_MARKER,
    build_script_command,
    is_retryable,
    retry_delay,
)

CATALOG_NAME = "trilogy_io"


def _failure(
    script: str, return_code: int, stderr: str, cause: Exception | None = None
) -> Exception:
    payload = {"script": script, "return_code": return_code, "stderr": stderr}
    if cause is not None and not stderr.strip():
        payload["stderr"] = str(cause)
    return RuntimeError(f"{VGI_FAILURE_MARKER} {json.dumps(payload)}")


def _drain(stderr: IO[bytes]) -> str:
    stderr.seek(0)
    text = stderr.read().decode("utf-8", errors="replace")
    stderr.close()
    return text


@dataclass
class _Run:
    process: subprocess.Popen
    reader: pa.RecordBatchStreamReader
    stderr: IO[bytes]
    script: str
    started: float = field(default_factory=time.monotonic)

    def close(self, kill: bool = False) -> int:
        if kill:
            self.process.kill()
        if self.process.stdout:
            self.process.stdout.close()
        return self.process.wait()

    def fail(self, return_code: int, cause: Exception | None = None) -> Exception:
        return _failure(self.script, return_code, _drain(self.stderr), cause)


# Runs started by bind (which needs the schema) and handed to the scan that
# follows. Keyed by invocation; a list because one query may scan the same
# script twice. ``_ACTIVE`` holds runs a scan is consuming.
_PENDING: dict[tuple[str, str], list[_Run]] = defaultdict(list)
_ACTIVE: dict[str, _Run] = {}
# A bind with no scan (EXPLAIN, a failed plan) leaves its run behind.
_ORPHAN_SECONDS = 300.0


def start(script: str, args: str) -> _Run:
    """Run the script until its schema arrives; retry transient uv failures."""
    attempt = 1
    while True:
        # Owned by the _Run, which outlives this scope.
        stderr = tempfile.TemporaryFile()  # noqa: SIM115
        process = subprocess.Popen(
            build_script_command(script, args),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=stderr,
        )
        try:
            return _Run(process, pa.ipc.open_stream(process.stdout), stderr, script)
        except Exception as e:
            process.kill()
            if process.stdout:
                process.stdout.close()
            return_code = process.wait()
            text = _drain(stderr)
            if attempt < MAX_ATTEMPTS and is_retryable(return_code, text):
                time.sleep(retry_delay(attempt))
                attempt += 1
                continue
            raise _failure(script, return_code, text, e)


def _reap_orphans() -> None:
    cutoff = time.monotonic() - _ORPHAN_SECONDS
    for runs in _PENDING.values():
        for run in [r for r in runs if r.started < cutoff]:
            runs.remove(run)
            run.close(kill=True)
            run.stderr.close()


def _claim(script: str, args: str) -> _Run:
    pending = _PENDING.get((script, args))
    if pending:
        return pending.pop(0)
    # Bind ran in another pooled worker process; start our own.
    return start(script, args)


@dataclass(frozen=True, kw_only=True)
class ScriptArgs:
    path: Annotated[str, Arg(0, doc="Path to the datasource script")]
    args: Annotated[str, Arg(1, doc="Command line passed to the script")]


@dataclass(kw_only=True)
class ScriptState(ArrowSerializableDataclass):
    scan: str = ""


@init_single_worker
class Script(TableFunctionGenerator[ScriptArgs, ScriptState]):
    class Meta:
        name = "script"
        description = "Stream a trilogy python datasource script"
        projection_pushdown = True

    @classmethod
    def on_bind(cls, params: BindParams[ScriptArgs]) -> BindResponse:
        _reap_orphans()
        run = start(params.args.path, params.args.args)
        _PENDING[(params.args.path, params.args.args)].append(run)
        return BindResponse(output_schema=run.reader.schema.remove_metadata())

    @classmethod
    def initial_state(cls, params: ProcessParams[ScriptArgs]) -> ScriptState:
        scan = uuid.uuid4().hex
        _ACTIVE[scan] = _claim(params.args.path, params.args.args)
        return ScriptState(scan=scan)

    @classmethod
    def process(
        cls, params: ProcessParams[ScriptArgs], state: ScriptState, out: OutputCollector
    ) -> None:
        run = _ACTIVE[state.scan]
        try:
            batch = run.reader.read_next_batch()
        except StopIteration:
            del _ACTIVE[state.scan]
            return_code = run.close()
            if return_code != 0:
                raise run.fail(return_code)
            run.stderr.close()
            out.finish()
            return
        except Exception as e:
            del _ACTIVE[state.scan]
            raise run.fail(run.close(kill=True), e)
        out.emit(_project(batch, params.output_schema))


def _project(batch: pa.RecordBatch, schema: pa.Schema) -> pa.RecordBatch:
    if batch.schema.names == schema.names:
        return batch.replace_schema_metadata(None)
    return pa.RecordBatch.from_arrays(
        [batch.column(name) for name in schema.names], schema=schema
    )


class TrilogyIOWorker(Worker):
    catalog = Catalog(
        name=CATALOG_NAME,
        default_schema="main",
        schemas=[Schema(path=["main"], functions=[Script])],
    )


def main() -> Any:
    return TrilogyIOWorker().run()


if __name__ == "__main__":
    main()
