"""What trilogy may ask a source script for, and how that ask is honored.

A request is satisfied one of two ways. If the source function declares a
matching parameter it is bound and the function owns it -- an API-backed source
can push the filter to the API, which is the point of having a contract at all.
Whatever the function does not declare, ``apply`` enforces on the Arrow stream.

So the contract is always honored; declaring a parameter only changes *where*.
Adding a field here means adding a fallback below, after which existing scripts
keep working and silently gain the feature.
"""

from __future__ import annotations

import inspect
import json
import re
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field, fields, replace
from typing import Any

import pyarrow as pa

from trilogy.io.errors import ContractError

CONTRACT_VERSION = 1

# Bumping this is a wire-format change; the rust crate parses the same strings.
OPERATORS = ("!=", ">=", "<=", "=", ">", "<", "not in", "in", "like")

_FILTER = re.compile(
    r"^\s*(?P<column>[A-Za-z_][A-Za-z0-9_.]*)\s*"
    r"(?P<op>!=|>=|<=|=|>|<|\bnot\s+in\b|\bin\b|\blike\b)\s*"
    r"(?P<value>.*?)\s*$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Filter:
    column: str
    op: str
    value: Any

    @classmethod
    def parse(cls, text: str) -> Filter:
        match = _FILTER.match(text)
        if not match:
            raise ContractError(
                f"Could not parse filter {text!r}. Expected '<column> <op> <value>' "
                f"where op is one of {', '.join(OPERATORS)}."
            )
        op = " ".join(match.group("op").lower().split())
        return cls(match.group("column"), op, _parse_value(match.group("value")))

    def render(self) -> str:
        return f"{self.column} {self.op} {json.dumps(self.value, default=str)}"


def _parse_value(raw: str) -> Any:
    """JSON first, so numbers, lists and null work; bare words stay strings."""
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw.strip("'\"")


@dataclass(frozen=True)
class Sort:
    column: str
    descending: bool = False

    @classmethod
    def parse(cls, text: str) -> Sort:
        column, _, direction = text.partition(":")
        normalized = direction.strip().lower() or "asc"
        if normalized not in ("asc", "desc"):
            raise ContractError(
                f"Could not parse sort {text!r}. Expected '<column>' or "
                "'<column>:asc' / '<column>:desc'."
            )
        return cls(column.strip(), normalized == "desc")

    def render(self) -> str:
        return f"{self.column}:{'desc' if self.descending else 'asc'}"


@dataclass(frozen=True)
class SourceRequest:
    limit: int | None = None
    columns: tuple[str, ...] | None = None
    filters: tuple[Filter, ...] = ()
    order_by: tuple[Sort, ...] = ()
    since: Any | None = None
    partition: Mapping[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "limit": self.limit,
            "columns": list(self.columns) if self.columns else None,
            "filters": [f.render() for f in self.filters],
            "order_by": [s.render() for s in self.order_by],
            "since": self.since,
            "partition": dict(self.partition),
        }


#: Parameter names a source function may declare to take ownership of a field.
PUSHDOWN_PARAMETERS = tuple(f.name for f in fields(SourceRequest))


def pushdown_parameters(fn: Callable) -> tuple[str, ...]:
    """Which contract fields ``fn`` declares -- the whole request if it asks."""
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return ()
    if _request_parameter(signature) is not None:
        return PUSHDOWN_PARAMETERS
    return tuple(n for n in PUSHDOWN_PARAMETERS if n in signature.parameters)


def effective_pushdown(
    declared: tuple[str, ...], request: SourceRequest
) -> tuple[str, ...]:
    """Narrow ``declared`` to the fields it is safe to hand over for this request.

    ``limit`` only composes if it is applied last. A source that truncates to
    ``limit`` rows and then has a filter applied -- or has its output sorted --
    returns the wrong rows, so whenever narrowing or ordering is still being
    enforced locally the limit comes back to the fallback with them.
    """
    narrowing_is_local = (request.filters and "filters" not in declared) or (
        request.order_by and "order_by" not in declared
    )
    if narrowing_is_local:
        return tuple(name for name in declared if name != "limit")
    return declared


def _request_parameter(signature: inspect.Signature) -> str | None:
    for name, parameter in signature.parameters.items():
        if name == "request" or parameter.annotation in (
            SourceRequest,
            "SourceRequest",
            "SourceRequest | None",
        ):
            return name
    return None


def bind(
    fn: Callable, request: SourceRequest, pushdown: tuple[str, ...] | None = None
) -> dict[str, Any]:
    """Keyword arguments to call ``fn`` with for this request.

    ``pushdown`` is the set of fields the function is allowed to own; anything
    outside it is withheld so the fallback stays authoritative. A field the
    caller left unset is omitted too, so the function's own default applies
    rather than being overwritten with None.
    """
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return {}
    allowed = PUSHDOWN_PARAMETERS if pushdown is None else pushdown
    whole = _request_parameter(signature)
    if whole is not None:
        return {whole: _withhold(request, allowed)}
    unset = SourceRequest()
    return {
        name: getattr(request, name)
        for name in allowed
        if name in signature.parameters
        and getattr(request, name) != getattr(unset, name)
    }


def _withhold(request: SourceRequest, allowed: tuple[str, ...]) -> SourceRequest:
    """Blank the fields a whole-request function is not being trusted with."""
    if set(allowed) == set(PUSHDOWN_PARAMETERS):
        return request
    unset = SourceRequest()
    return replace(
        request,
        **{n: getattr(unset, n) for n in PUSHDOWN_PARAMETERS if n not in allowed},
    )


# --- fallback application ----------------------------------------------------


def apply(
    reader: pa.RecordBatchReader, request: SourceRequest, pushed_down: tuple[str, ...]
) -> pa.RecordBatchReader:
    """Enforce every contract field the function did not take ownership of.

    ``since`` and ``partition`` are semantic -- only the source knows what
    column carries its watermark or how it is partitioned -- so they have no
    fallback and are simply ignored when not pushed down.
    """
    schema = reader.schema
    filters = () if "filters" in pushed_down else request.filters
    columns = None if "columns" in pushed_down else request.columns
    limit = None if "limit" in pushed_down else request.limit
    order_by = () if "order_by" in pushed_down else request.order_by

    if filters:
        _validate_columns(schema, [f.column for f in filters], "filter on")
    if order_by:
        _validate_columns(schema, [s.column for s in order_by], "sort by")
    if columns:
        _validate_columns(schema, columns, "project")
        schema = pa.schema(
            [schema.field(name) for name in columns], metadata=schema.metadata
        )

    if not (filters or columns or order_by or limit is not None):
        return reader
    if order_by:
        # Sorting cannot stream: the last batch can hold the first row. This is
        # why a source that can order itself should claim ``order_by``.
        return _sorted_reader(reader, filters, order_by, columns, limit, schema)
    return pa.RecordBatchReader.from_batches(
        schema, _transform(reader, filters, columns, limit)
    )


def _sorted_reader(
    reader: pa.RecordBatchReader,
    filters: tuple[Filter, ...],
    order_by: tuple[Sort, ...],
    columns: tuple[str, ...] | None,
    limit: int | None,
    schema: pa.Schema,
) -> pa.RecordBatchReader:
    """Filter, then sort, then limit, then project -- in that order."""
    batches = [b for b in _transform(reader, filters, None, None)]
    table = (
        pa.Table.from_batches(batches, reader.schema)
        if batches
        else reader.schema.empty_table()
    )
    table = table.sort_by(
        [(s.column, "descending" if s.descending else "ascending") for s in order_by]
    )
    if limit is not None:
        table = table.slice(0, limit)
    if columns:
        table = table.select(list(columns))
    return pa.RecordBatchReader.from_batches(schema, table.to_batches())


def _validate_columns(
    schema: pa.Schema, columns: list[str] | tuple[str, ...], verb: str
) -> None:
    missing = [name for name in columns if name not in schema.names]
    if missing:
        raise ContractError(
            f"Cannot {verb} {', '.join(missing)}: not produced by this source. "
            f"Available columns: {', '.join(schema.names)}."
        )


def _transform(
    reader: pa.RecordBatchReader,
    filters: tuple[Filter, ...],
    columns: tuple[str, ...] | None,
    limit: int | None,
) -> Iterator[pa.RecordBatch]:
    emitted = 0
    for batch in reader:
        for predicate in filters:
            batch = batch.filter(_mask(batch, predicate))
        if columns:
            batch = batch.select(list(columns))
        if limit is not None:
            if emitted + batch.num_rows > limit:
                batch = batch.slice(0, limit - emitted)
            emitted += batch.num_rows
        if batch.num_rows:
            yield batch
        # Stop pulling once satisfied rather than draining the source.
        if limit is not None and emitted >= limit:
            return


def _mask(batch: pa.RecordBatch, predicate: Filter) -> pa.Array:
    import pyarrow.compute as pc

    column = batch.column(batch.schema.get_field_index(predicate.column))
    value = predicate.value
    if predicate.op == "=":
        return pc.equal(column, value)
    if predicate.op == "!=":
        return pc.not_equal(column, value)
    if predicate.op == ">":
        return pc.greater(column, value)
    if predicate.op == ">=":
        return pc.greater_equal(column, value)
    if predicate.op == "<":
        return pc.less(column, value)
    if predicate.op == "<=":
        return pc.less_equal(column, value)
    if predicate.op == "in":
        return pc.is_in(column, value_set=pa.array(_as_set(value)))
    if predicate.op == "not in":
        return pc.invert(pc.is_in(column, value_set=pa.array(_as_set(value))))
    if predicate.op == "like":
        return pc.match_like(column, str(value))
    raise ContractError(f"Unsupported filter operator {predicate.op!r}")


def _as_set(value: Any) -> list[Any]:
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]
