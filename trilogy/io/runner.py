"""``run(fn)`` -- the whole interface a script author needs.

Parsed with argparse, not click: ``import click`` costs ~270ms and DuckDB runs
these scripts once per query, so the fast path stays stdlib-only. Authors who
want their own subcommands can compose ``trilogy.io.click_options`` into a click
group instead.
"""

from __future__ import annotations

import argparse
import json
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
from trilogy.io.errors import ERROR_PREFIX, SCRIPT_ERROR_EXIT_CODE
from trilogy.io.sinks import Format, write

METADATA_PREFIX = "trilogy."

# Transient by nature: the consumer may retry these rather than failing the
# query. A bug in the script is not retryable and must surface immediately.
RETRYABLE_ERRORS = (ConnectionError, TimeoutError, BlockingIOError)


@dataclass(frozen=True)
class Invocation:
    request: SourceRequest
    fmt: Format
    output: str | None
    describe: bool


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


def run(
    fn: Callable,
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
    """
    try:
        invocation = parse_args(argv)
        if invocation.describe:
            # Deliberately the unnarrowed request: describe reports what the
            # source produces, not what this particular invocation would return.
            reader = to_reader(fn(**bind(fn, SourceRequest())), schema)
            print(
                json.dumps(
                    describe_module.payload(
                        reader.schema, pushdown_parameters(fn), sys.argv[0]
                    ),
                    indent=2,
                )
            )
            return 0
        write(
            resolve(fn, invocation, schema, watermark),
            invocation.fmt,
            invocation.output,
        )
        return 0
    except SystemExit as e:  # argparse already reported the problem
        return int(e.code or 0)
    except BaseException as e:
        _report(e)
        return SCRIPT_ERROR_EXIT_CODE


def _report(error: BaseException) -> None:
    """Machine-readable line first, then the traceback a human needs."""
    sys.stderr.write(
        ERROR_PREFIX
        + json.dumps(
            {
                "type": type(error).__name__,
                "message": str(error),
                "contract": CONTRACT_VERSION,
                "retryable": isinstance(error, RETRYABLE_ERRORS),
            }
        )
        + "\n"
    )
    traceback.print_exc(file=sys.stderr)
    sys.stderr.flush()


def main(fn: Callable, **kwargs: Any) -> None:
    """``run`` that exits the process, for use under ``if __name__``."""
    raise SystemExit(run(fn, **kwargs))
