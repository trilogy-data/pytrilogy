"""Make a python function a trilogy data source.

    #!/usr/bin/env -S uv run
    # /// script
    # dependencies = ["pandas", "pytrilogy"]
    # ///
    import pandas as pd
    from trilogy.io import run

    def landmarks() -> pd.DataFrame:
        return pd.read_csv("https://example.org/landmarks.csv")

    if __name__ == "__main__":
        run(landmarks)

The function may return anything :func:`trilogy.io.adapters.to_reader` knows
how to convert, and gets a command line -- ``--limit``, ``--filter``,
``--columns``, ``--format``, ``--describe`` -- for free.

This package is deliberately leaf-level: it costs a pyarrow import and nothing
else, because DuckDB runs these scripts once per query. Do not import
``trilogy.core`` from here.
"""

from collections.abc import Callable, Sequence
from typing import Any

import pyarrow as pa

from trilogy.io.adapters import register_adapter, to_reader
from trilogy.io.contract import Filter, Sort, SourceRequest
from trilogy.io.errors import ContractError, TrilogyIOError
from trilogy.io.runner import main, parse_args, resolve, run
from trilogy.io.sinks import Format, write

__all__ = [
    "ContractError",
    "Filter",
    "Format",
    "Sort",
    "SourceRequest",
    "TrilogyIOError",
    "emit",
    "main",
    "register_adapter",
    "run",
    "source",
    "to_reader",
    "write",
]


def emit(
    fn: Callable,
    *,
    schema: pa.Schema | None = None,
    argv: Sequence[str] | None = None,
) -> None:
    """Backwards-compatible entrypoint: run ``fn`` and let errors propagate.

    Predates :func:`run` and is still what the published wheel's examples use.
    Unlike ``run`` it does not trap exceptions or return an exit code, but it
    goes through the same pipeline, so scripts written against it pick up the
    adapters and the contract flags unchanged.
    """
    invocation = parse_args(argv)
    write(resolve(fn, invocation, schema), invocation.fmt, invocation.output)


def source(
    fn: Callable | None = None,
    *,
    schema: pa.Schema | None = None,
    watermark: Any | None = None,
) -> Any:
    """Mark a function as a data source, attaching ``.cli()`` to it.

    ``schema`` is authoritative: the output is cast onto it, and it is the only
    thing that can describe a result with no rows.

    The function stays directly callable, so it is still testable as a plain
    function::

        @source(schema=SCHEMA)
        def landmarks(limit: int | None = None) -> pd.DataFrame: ...

        if __name__ == "__main__":
            landmarks.cli()
    """

    def decorate(target: Callable) -> Callable:
        target.cli = lambda argv=None: main(  # type: ignore[attr-defined]
            target, schema=schema, watermark=watermark, argv=argv
        )
        return target

    if fn is not None:
        return decorate(fn)
    return decorate
