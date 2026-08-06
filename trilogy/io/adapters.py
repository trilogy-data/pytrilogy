"""Turn whatever a source function returned into an Arrow batch reader.

Probes are duck-typed and ordered, so no optional dependency is imported unless
an object of that kind actually shows up. Streaming inputs stay streaming --
nothing here materializes a generator into a table.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from typing import Any

import pyarrow as pa

from trilogy.io.errors import TrilogyIOError

Probe = Callable[[Any], bool]
Convert = Callable[[Any, "pa.Schema | None"], pa.RecordBatchReader]

_ADAPTERS: list[tuple[Probe, Convert]] = []


def register_adapter(predicate: Probe, converter: Convert | None = None) -> Any:
    """Teach the wrapper about another return type.

    Usable directly or as a decorator. Later registrations are probed first, so
    a caller can override a built-in.
    """
    if converter is not None:
        _ADAPTERS.append((predicate, converter))
        return converter

    def decorate(fn: Convert) -> Convert:
        _ADAPTERS.append((predicate, fn))
        return fn

    return decorate


def to_reader(obj: Any, schema: pa.Schema | None = None) -> pa.RecordBatchReader:
    for predicate, converter in reversed(_ADAPTERS):
        if predicate(obj):
            return converter(obj, schema)
    raise TrilogyIOError(
        f"Cannot convert {type(obj).__module__}.{type(obj).__qualname__} to an "
        "Arrow stream. Return a pyarrow Table, a dataframe implementing the "
        "Arrow PyCapsule interface, a list of dicts, or an iterator of any of "
        "those -- or register_adapter() your own."
    )


def empty_reader(schema: pa.Schema | None) -> pa.RecordBatchReader:
    if schema is None:
        raise TrilogyIOError(
            "Cannot infer a schema from an empty result. Pass schema= so the "
            "source still describes its columns when it has no rows."
        )
    return pa.RecordBatchReader.from_batches(schema, iter(()))


def conform(batch: pa.RecordBatch, schema: pa.Schema) -> pa.RecordBatch:
    """Cast a batch onto the stream's schema; a no-op in the common case."""
    if batch.schema.equals(schema):
        return batch
    return pa.Table.from_batches([batch]).cast(schema).combine_chunks().to_batches()[0]


def _module_root(obj: Any) -> str:
    return type(obj).__module__.partition(".")[0]


def _chain(
    head: pa.RecordBatchReader, rest: Iterator[Any], schema: pa.Schema
) -> Iterator[pa.RecordBatch]:
    for batch in head:
        yield conform(batch, schema)
    for item in rest:
        for batch in to_reader(item, schema):
            yield conform(batch, schema)


def _from_iterable(obj: Any, schema: pa.Schema | None) -> pa.RecordBatchReader:
    """Peek one element to learn the schema, then stream the rest behind it."""
    iterator = iter(obj)
    try:
        first = next(iterator)
    except StopIteration:
        return empty_reader(schema)
    head = to_reader(first, schema)
    resolved = schema or head.schema
    return pa.RecordBatchReader.from_batches(resolved, _chain(head, iterator, resolved))


def _from_pylist(obj: Any, schema: pa.Schema | None) -> pa.RecordBatchReader:
    if not obj:
        return empty_reader(schema)
    return pa.Table.from_pylist(list(obj), schema=schema).to_reader()


def _from_pydict(obj: Any, schema: pa.Schema | None) -> pa.RecordBatchReader:
    return pa.Table.from_pydict(dict(obj), schema=schema).to_reader()


def _from_capsule_array(obj: Any, schema: pa.Schema | None) -> pa.RecordBatchReader:
    batch = pa.record_batch(obj)
    return pa.RecordBatchReader.from_batches(batch.schema, iter((batch,)))


def _from_pandas(obj: Any, schema: pa.Schema | None) -> pa.RecordBatchReader:
    return pa.Table.from_pandas(obj, schema=schema, preserve_index=False).to_reader()


# --- built-in probes ---------------------------------------------------------
# Registration is most-generic first, because probing runs in reverse: the last
# registration wins, which is what lets a caller override a built-in. Keep the
# capsule probes below the concrete pyarrow ones -- a pa.Table exposes
# __arrow_c_stream__ too, but to_reader() preserves its existing chunking.

register_adapter(
    lambda o: isinstance(o, (list, tuple)) or hasattr(o, "__next__"), _from_iterable
)
register_adapter(lambda o: isinstance(o, Mapping), _from_pydict)
register_adapter(
    lambda o: isinstance(o, (list, tuple))
    and all(isinstance(item, Mapping) for item in o),
    _from_pylist,
)
register_adapter(lambda o: hasattr(o, "__arrow_c_array__"), _from_capsule_array)
register_adapter(
    lambda o: hasattr(o, "__arrow_c_stream__"),
    lambda o, schema: pa.RecordBatchReader.from_stream(o),
)
# pandas 2 predates the capsule interface. Matched on the module name so pandas
# is never imported on behalf of a script that does not use it.
register_adapter(
    lambda o: _module_root(o) == "pandas" and type(o).__name__ == "DataFrame",
    _from_pandas,
)
register_adapter(
    lambda o: isinstance(o, pa.RecordBatch),
    lambda o, schema: pa.RecordBatchReader.from_batches(o.schema, iter((o,))),
)
register_adapter(lambda o: isinstance(o, pa.Table), lambda o, schema: o.to_reader())
register_adapter(lambda o: isinstance(o, pa.RecordBatchReader), lambda o, schema: o)
register_adapter(lambda o: o is None, lambda o, schema: empty_reader(schema))
