"""``run(fn)`` -- the whole interface a script author needs.

Parsed with argparse, not click: ``import click`` costs ~270ms and DuckDB runs
these scripts once per query, so the fast path stays stdlib-only. Authors who
want their own subcommands can compose ``trilogy.io.click_options`` into a click
group instead.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import traceback
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from trilogy.io import describe as describe_module
from trilogy.io.adapters import to_reader
from trilogy.io.contract import (
    CONTRACT_VERSION,
    SourceRequest,
    apply,
    bind,
    effective_pushdown,
    pushdown_parameters,
    request_from_strings,
)
from trilogy.io.errors import (
    ERROR_PREFIX,
    SCRIPT_ERROR_EXIT_CODE,
    VGI_FLAG,
    ContractError,
)
from trilogy.io.sinks import Format, write

METADATA_PREFIX = "trilogy."

# Transient by nature: the consumer may retry these rather than failing the
# query. A bug in the script is not retryable and must surface immediately.
RETRYABLE_ERRORS = (ConnectionError, TimeoutError, BlockingIOError)

SOURCE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

# What a program serves: one function, or several by name (``--source``).
Program = Callable | Mapping[str, Callable]


@dataclass(frozen=True)
class SourceOptions:
    schema: pa.Schema | None = None
    watermark: Any | None = None


# Filled by ``trilogy.io.source``: a mapping of sources reads each one's options
# here, since ``run`` cannot take one schema for all of them.
SOURCE_OPTIONS: dict[Callable, SourceOptions] = {}


@dataclass(frozen=True)
class Selected:
    fn: Callable
    schema: pa.Schema | None
    watermark: Any | None
    name: str | None


@dataclass(frozen=True)
class Invocation:
    request: SourceRequest
    fmt: Format
    output: str | None
    describe: bool
    source: str | None = None


def build_parser(prog: str | None = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=prog, description="A trilogy data source. Writes Arrow IPC to stdout."
    )
    parser.add_argument("--limit", type=int, help="maximum rows to emit")
    parser.add_argument("--columns", help="comma-separated columns to project")
    parser.add_argument(
        "--filter",
        action="append",
        default=[],
        dest="filters",
        metavar="'col op value'",
        help="row predicate, repeatable (e.g. --filter 'state in [\"CA\"]')",
    )
    parser.add_argument(
        "--order-by",
        dest="order_by",
        help="comma-separated sort keys, e.g. 'score:desc,id'",
    )
    parser.add_argument("--since", help="watermark low bound")
    parser.add_argument(
        "--partition",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="partition selector, repeatable",
    )
    parser.add_argument(
        "--format",
        dest="fmt",
        choices=[f.value for f in Format],
        default=Format.ARROW.value,
    )
    parser.add_argument("--output", help="destination URI; default stdout")
    parser.add_argument(
        "--describe",
        action="store_true",
        help="print this source's schema and pushdown support as JSON",
    )
    parser.add_argument(
        "--source", help="which source to run, for a program that serves several"
    )
    return parser


def parse_args(
    argv: Sequence[str] | None = None, prog: str | None = None
) -> Invocation:
    parsed = build_parser(prog).parse_args(argv)
    return Invocation(
        request=request_from_strings(
            limit=parsed.limit,
            columns=parsed.columns,
            filters=parsed.filters,
            order_by=parsed.order_by,
            since=parsed.since,
            partition=parsed.partition,
        ),
        fmt=Format(parsed.fmt),
        output=parsed.output,
        describe=parsed.describe,
        source=parsed.source,
    )


def stamp(
    reader: pa.RecordBatchReader, meta: Mapping[str, Any]
) -> pa.RecordBatchReader:
    """Attach sideband metadata to the stream's schema.

    Schema-level key-value metadata rides inside the IPC stream and survives
    the parquet staging path, so the consumer reads it off ``reader.schema``
    with no extra plumbing. Only facts known before the first batch can go here
    -- the schema is written first, so row counts cannot.
    """
    encoded = {
        f"{METADATA_PREFIX}{k}".encode(): str(v).encode()
        for k, v in meta.items()
        if v is not None
    }
    if not encoded:
        return reader
    schema = reader.schema.with_metadata({**(reader.schema.metadata or {}), **encoded})
    return pa.RecordBatchReader.from_batches(
        schema, (pa.RecordBatch.from_arrays(b.columns, schema=schema) for b in reader)
    )


def resolve(
    fn: Callable,
    invocation: Invocation,
    schema: pa.Schema | None = None,
    watermark: Any | None = None,
) -> pa.RecordBatchReader:
    """Call the source function and shape its output to the request."""
    pushdown = effective_pushdown(pushdown_parameters(fn), invocation.request)
    reader = to_reader(fn(**bind(fn, invocation.request, pushdown)), schema)
    reader = apply(reader, invocation.request, pushdown)
    return stamp(
        reader,
        {
            "contract": str(CONTRACT_VERSION),
            "pushdown": ",".join(pushdown),
            "watermark": watermark,
        },
    )


def check_program(
    program: Program, schema: pa.Schema | None, watermark: Any | None
) -> None:
    """Reject a malformed program before it serves anything."""
    if not isinstance(program, Mapping):
        return
    if schema is not None or watermark is not None:
        raise TypeError(
            "run() takes each source's schema and watermark from @source(...) "
            "when given several sources"
        )
    for name in program:
        if not SOURCE_NAME.fullmatch(name):
            raise ValueError(f"source name {name!r} is not an identifier")


def select(
    program: Program,
    name: str | None,
    schema: pa.Schema | None = None,
    watermark: Any | None = None,
) -> Selected:
    """The source ``--source`` names, with the options it runs under."""
    if not isinstance(program, Mapping):
        if name is not None:
            raise ContractError(
                f"unknown source '{name}': this program serves a single source"
            )
        options = SOURCE_OPTIONS.get(program, SourceOptions())
        return Selected(
            program,
            schema if schema is not None else options.schema,
            watermark if watermark is not None else options.watermark,
            None,
        )
    available = f"Available sources: {', '.join(sorted(program))}"
    if name is None:
        raise ContractError(
            "this program serves several sources; choose one with --source. "
            + available
        )
    if name not in program:
        raise ContractError(f"unknown source '{name}'. {available}")
    fn = program[name]
    options = SOURCE_OPTIONS.get(fn, SourceOptions())
    return Selected(fn, options.schema, options.watermark, name)


def run(
    fn: Program,
    *,
    schema: pa.Schema | None = None,
    watermark: Any | None = None,
    argv: Sequence[str] | None = None,
) -> int:
    """Run ``fn`` as a trilogy data source. Returns a process exit code.

    ``fn`` may return a pyarrow table or reader, any dataframe implementing the
    Arrow PyCapsule interface, a pandas frame, a list of dicts, or an iterator
    of any of those. A ``schema`` given here is authoritative -- the output is
    cast onto it whatever the return type. If it declares parameters named for
    contract fields
    (``limit``, ``columns``, ``filters``, ``since``, ``partition``) they are
    bound and it owns them; everything else is enforced on the output stream.

    ``fn`` may also map names to functions: one program serving several
    sources, chosen per call by ``--source``.
    """
    check_program(fn, schema, watermark)
    args = sys.argv[1:] if argv is None else list(argv)
    if VGI_FLAG in args:
        # Imported only here: vgi is an optional dependency, and the default
        # one-shot path must stay as cheap as it is.
        from trilogy.io.vgi import serve

        serve(fn, schema=schema, watermark=watermark)
        return 0
    try:
        invocation = parse_args(argv)
        selected = select(fn, invocation.source, schema, watermark)
        if invocation.describe:
            # Deliberately the unnarrowed request: describe reports what the
            # source produces, not what this particular invocation would return.
            reader = to_reader(
                selected.fn(**bind(selected.fn, SourceRequest())), selected.schema
            )
            print(
                json.dumps(
                    describe_module.payload(
                        reader.schema,
                        pushdown_parameters(selected.fn),
                        sys.argv[0],
                        selected.name,
                    ),
                    indent=2,
                )
            )
            return 0
        write(
            resolve(selected.fn, invocation, selected.schema, selected.watermark),
            invocation.fmt,
            invocation.output,
        )
        return 0
    except SystemExit as e:  # argparse already reported the problem
        return int(e.code or 0)
    except BaseException as e:
        _report(e)
        return SCRIPT_ERROR_EXIT_CODE


def error_report(error: BaseException) -> str:
    """Machine-readable line first, then the traceback a human needs."""
    line = json.dumps(
        {
            "type": type(error).__name__,
            "message": str(error),
            "contract": CONTRACT_VERSION,
            "retryable": isinstance(error, RETRYABLE_ERRORS),
        }
    )
    return f"{ERROR_PREFIX}{line}\n" + "".join(traceback.format_exception(error))


def _report(error: BaseException) -> None:
    sys.stderr.write(error_report(error))
    sys.stderr.flush()


def main(fn: Program, **kwargs: Any) -> None:
    """``run`` that exits the process, for use under ``if __name__``."""
    raise SystemExit(run(fn, **kwargs))
