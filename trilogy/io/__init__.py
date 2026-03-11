import inspect
from collections.abc import Iterator
from typing import Callable, get_type_hints

import pyarrow as pa

from trilogy.io.arrow import emit_arrow, emit_arrow_batches


def emit(fn: Callable) -> None:
    hints = get_type_hints(fn)
    return_hint = hints.get("return")

    if return_hint is None:
        raise TypeError(f"{fn.__name__} must have a return type hint")

    origin = getattr(return_hint, "__origin__", None)

    # Iterator[pa.RecordBatch]
    if origin in (Iterator, inspect.Parameter.empty) or (
        origin is not None and issubclass(origin, Iterator)
    ):
        args = getattr(return_hint, "__args__", ())
        if args and args[0] is pa.RecordBatch:
            batches = fn()
            first = next(batches)

            def _chain():
                yield first
                yield from batches

            emit_arrow_batches(_chain(), first.schema)
            return

    # pa.Table
    if return_hint is pa.Table:
        emit_arrow(fn())
        return

    raise TypeError(
        f"Unsupported return type: {return_hint}. Expected pa.Table or Iterator[pa.RecordBatch]"
    )
