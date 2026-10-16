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

from trilogy.io.contract import pushdown_parameters
from trilogy.io.errors import SCRIPT_ERROR_EXIT_CODE, VGI_FAILURE_MARKER, ContractError
from trilogy.io.runner import Program, error_report, parse_args, resolve, select

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
    declared: ClassVar[Callable[[str], pa.Schema | None]]

    class Meta:
        name = FUNCTION_NAME
        description = "Run this trilogy source with a contract command line"
        projection_pushdown = True

    @classmethod
    def on_bind(cls, params: BindParams[SourceArgs]) -> BindResponse:
        # Binds run one after another; leaving the source's run to the scan
        # lets two scans of one query run at once, in separate workers.
        declared = cls.declared(params.args.args)
        if declared is not None:
            return BindResponse(output_schema=declared)
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
    program: Program, schema: pa.Schema | None, watermark: Any, args: str
) -> pa.RecordBatchReader:
    """The reader for one call's command line, exactly as a one-shot run.

    ``--source`` rides in ``args``, so one worker serves every source of a
    program.
    """
    try:
        invocation = parse_args(shlex.split(args))
    except SystemExit as e:  # argparse already wrote why to stderr
        raise ValueError(f"invalid source arguments: {args!r}") from e
    selected = select(program, invocation.source, schema, watermark)
    return resolve(selected.fn, invocation, selected.schema, selected.watermark)


def declared_schema(
    program: Program, schema: pa.Schema | None, args: str
) -> pa.Schema | None:
    """The schema a call will produce, when it is known without running the source.

    Only a declared schema the request cannot reshape qualifies: a source that
    projects its own columns, or a request naming a column the schema lacks,
    must run so that its answer (or its error) is the one-shot one.
    """
    try:
        invocation = parse_args(shlex.split(args))
        selected = select(program, invocation.source, schema)
    except (SystemExit, ContractError):
        return None
    if selected.schema is None or "columns" in pushdown_parameters(selected.fn):
        return None
    request = invocation.request
    named = [
        *(request.columns or ()),
        *(f.column for f in request.filters),
        *(s.column for s in request.order_by),
    ]
    if any(name not in selected.schema.names for name in named):
        return None
    if request.columns:
        return pa.schema([selected.schema.field(name) for name in request.columns])
    return selected.schema.remove_metadata()


def serve(
    program: Program, schema: pa.Schema | None = None, watermark: Any = None
) -> None:
    Source.resolver = staticmethod(partial(resolve_args, program, schema, watermark))  # type: ignore[assignment]
    Source.declared = staticmethod(partial(declared_schema, program, schema))  # type: ignore[assignment]
    SourceWorker().run()
