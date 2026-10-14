"""Serve a source as a pooled VGI worker: ``run(fn)`` under ``--vgi``.

DuckDB (haybarn, with the ``vgi`` extension) attaches the program once and
calls ``source(args)`` per scan, so ``fn`` runs in a warm process instead of a
fresh one per query. ``args`` is the command line the source would otherwise
be launched with, parsed by the same ``parse_args``: the contract does not fork,
it is just delivered per call.

The only module in ``trilogy.io`` that imports ``vgi``; ``run`` loads it lazily.
"""

from __future__ import annotations

import json
import shlex
import sys
import uuid
from collections import defaultdict
from collections.abc import Callable
from contextlib import redirect_stdout
from dataclasses import dataclass
from functools import partial
from typing import Annotated, Any, ClassVar

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

from trilogy.io.errors import SCRIPT_ERROR_EXIT_CODE, VGI_FAILURE_MARKER
from trilogy.io.runner import error_report, parse_args, resolve

CATALOG_NAME = "trilogy_source"
FUNCTION_NAME = "source"


# The protocol owns fd 1 (vgi_rpc opens it once at startup), so anything the
# source prints while it runs is sent to stderr instead of into the stream.
def _quiet() -> redirect_stdout:
    return redirect_stdout(sys.stderr)


# Readers resolved at bind (which needs the schema), claimed by the scan that
# follows; a list per args because one query may scan the same request twice.
_PENDING: dict[str, list[pa.RecordBatchReader]] = defaultdict(list)
_ACTIVE: dict[str, pa.RecordBatchReader] = {}


def failure(error: BaseException) -> RuntimeError:
    """The error DuckDB relays, carrying what a one-shot run would have exited with."""
    payload = {
        "script": sys.argv[0],
        "return_code": SCRIPT_ERROR_EXIT_CODE,
        "stderr": error_report(error),
    }
    return RuntimeError(f"{VGI_FAILURE_MARKER} {json.dumps(payload)}")


def _project(batch: pa.RecordBatch, schema: pa.Schema) -> pa.RecordBatch:
    if batch.schema.names == schema.names:
        return batch.replace_schema_metadata(None)
    return pa.RecordBatch.from_arrays(
        [batch.column(name) for name in schema.names], schema=schema
    )


@dataclass(frozen=True, kw_only=True)
class SourceArgs:
    args: Annotated[str, Arg(0, doc="The source's command line, e.g. '--limit 5'")]


@dataclass(kw_only=True)
class SourceState(ArrowSerializableDataclass):
    scan: str = ""


@init_single_worker
class Source(TableFunctionGenerator[SourceArgs, SourceState]):
    """The served function; ``resolver`` is bound by ``serve``."""

    resolver: ClassVar[Callable[[str], pa.RecordBatchReader]]

    class Meta:
        name = FUNCTION_NAME
        description = "Run this trilogy source with a contract command line"
        projection_pushdown = True

    @classmethod
    def on_bind(cls, params: BindParams[SourceArgs]) -> BindResponse:
        try:
            with _quiet():
                reader = cls.resolver(params.args.args)
        except BaseException as e:
            raise failure(e) from e
        _PENDING[params.args.args].append(reader)
        return BindResponse(output_schema=reader.schema.remove_metadata())

    @classmethod
    def initial_state(cls, params: ProcessParams[SourceArgs]) -> SourceState:
        pending = _PENDING.get(params.args.args)
        try:
            # Bind ran in another pooled process when nothing is waiting here.
            with _quiet():
                reader = pending.pop(0) if pending else cls.resolver(params.args.args)
        except BaseException as e:
            raise failure(e) from e
        scan = uuid.uuid4().hex
        _ACTIVE[scan] = reader
        return SourceState(scan=scan)

    @classmethod
    def process(
        cls,
        params: ProcessParams[SourceArgs],
        state: SourceState,
        out: OutputCollector,
    ) -> None:
        try:
            with _quiet():
                batch = _ACTIVE[state.scan].read_next_batch()
        except StopIteration:
            del _ACTIVE[state.scan]
            out.finish()
            return
        except BaseException as e:
            del _ACTIVE[state.scan]
            raise failure(e) from e
        out.emit(_project(batch, params.output_schema))


class SourceWorker(Worker):
    catalog = Catalog(
        name=CATALOG_NAME,
        default_schema="main",
        schemas=[Schema(path=["main"], functions=[Source])],
    )


def resolve_args(
    fn: Callable, schema: pa.Schema | None, watermark: Any, args: str
) -> pa.RecordBatchReader:
    """``fn``'s reader for one call's command line, exactly as a one-shot run."""
    try:
        invocation = parse_args(shlex.split(args))
    except SystemExit as e:  # argparse already wrote why to stderr
        raise ValueError(f"invalid source arguments: {args!r}") from e
    return resolve(fn, invocation, schema, watermark)


def serve(fn: Callable, schema: pa.Schema | None = None, watermark: Any = None) -> None:
    Source.resolver = staticmethod(partial(resolve_args, fn, schema, watermark))  # type: ignore[assignment]
    SourceWorker().run()
