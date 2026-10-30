from collections.abc import Callable
from typing import ClassVar

from trilogy.core.enums import DatePart, FunctionType
from trilogy.core.models.core import DataType
from trilogy.dialect.base import BaseDialect, render_true_divide, render_unsupported


def day_of_week(expr: str) -> str:
    # dow is 0 on Sunday; Trilogy's day_of_week is 1 on Sunday, as on DuckDB
    return f"(date_part('dow', {expr}) + 1)"


EPOCH_WIDTH = {DatePart.HOUR: 3600, DatePart.MINUTE: 60, DatePart.SECOND: 1}


def date_diff(first: str, second: str, grain: DatePart) -> str:
    """The part boundaries crossed, as DuckDB counts them; an interval's
    fields would count the elapsed time instead."""
    grain = DatePart(grain)
    if grain == DatePart.YEAR:
        return f"(date_part('year', {second}) - date_part('year', {first}))"
    elif grain == DatePart.QUARTER:
        return (
            f"(4 * {date_diff(first, second, DatePart.YEAR)}"
            f" + date_part('quarter', {second}) - date_part('quarter', {first}))"
        )
    elif grain == DatePart.MONTH:
        return (
            f"(12 * {date_diff(first, second, DatePart.YEAR)}"
            f" + date_part('month', {second}) - date_part('month', {first}))"
        )
    elif grain == DatePart.DAY:
        return f"(({second})::date - ({first})::date)"
    elif grain == DatePart.WEEK:
        return f"((({second})::date - ({first})::date) / 7)"
    elif grain in EPOCH_WIDTH:
        epoch = "extract(epoch from date_trunc('{part}', ({expr})::timestamp))"
        return (
            f"CAST(({epoch.format(part=grain.value, expr=second)}"
            f" - {epoch.format(part=grain.value, expr=first)}) / {EPOCH_WIDTH[grain]} AS BIGINT)"
        )
    else:
        raise NotImplementedError(f"Date diff not implemented for grain {grain}")


def render_cast(args: list[str], types: list) -> str:
    # float to integer is half-to-even here; through numeric it is half-away
    if types[0].data_type in (DataType.FLOAT, DataType.DOUBLE) and types[
        1
    ].data_type in (DataType.INTEGER, DataType.BIGINT):
        return f"cast(({args[0]})::numeric as {args[1]})"
    return f"cast({args[0]} as {args[1]})"


FUNCTION_MAP = {
    # no fold over an array
    FunctionType.ARRAY_REDUCE: render_unsupported("array_reduce"),
    FunctionType.BIT_XOR: lambda x, types: f"({x[0]} # {x[1]})",
    FunctionType.SPLIT: lambda x, types: f"string_to_array({x[0]}, {x[1]})",
    FunctionType.DATE_TRUNCATE: lambda x, types: f"date_trunc('{x[1]}', {x[0]})",
    # the offset may be an expression, which an interval string cannot hold
    FunctionType.DATE_ADD: lambda x, types: f"({x[0]} + ({x[2]}) * INTERVAL '1 {x[1]}')",
    FunctionType.DATE_SUB: lambda x, types: f"({x[0]} - ({x[2]}) * INTERVAL '1 {x[1]}')",
    FunctionType.DATE_PART: lambda x, types: (
        day_of_week(x[0]) if x[1] == "day_of_week" else f"date_part('{x[1]}', {x[0]})"
    ),
    FunctionType.DAY_OF_WEEK: lambda x, types: day_of_week(x[0]),
    FunctionType.WEEK: lambda x, types: f"date_part('week', {x[0]})",
    FunctionType.DATE_DIFF: lambda x, types: date_diff(x[0], x[1], x[2]),
    FunctionType.DIVIDE: lambda x, types: render_true_divide(
        "double precision", x, types
    ),
    FunctionType.CAST: lambda x, types: render_cast(x, types),
    FunctionType.IS_NULL: lambda x, types: f"{x[0]} IS NULL",
    FunctionType.CURRENT_DATETIME: lambda x, types: "LOCALTIMESTAMP",
    FunctionType.CURRENT_TIMESTAMP: lambda x, types: "CURRENT_TIMESTAMP",
    FunctionType.DATETIME_LITERAL: lambda x, types: (
        f"timestamptz '{x}'" if x.tzinfo else f"timestamp '{x}'"
    ),
}

MAX_IDENTIFIER_LENGTH = 50


class PostgresDialect(BaseDialect):
    FUNCTION_MAP: ClassVar[dict[FunctionType, Callable[..., str]]] = {
        **BaseDialect.FUNCTION_MAP,
        **FUNCTION_MAP,
    }
    DATATYPE_MAP: ClassVar[dict[DataType, str]] = {
        **BaseDialect.DATATYPE_MAP,
        DataType.DATETIME: "timestamp",
    }
    QUOTE_CHARACTER = '"'
    SUPPORTS_AGGREGATE_GROUPING_MODES = True
    # `relation "orders" does not exist` / `schema "analytics" does not exist`.
    # Anchored on the object kind: bare `does not exist` also covers a missing
    # type or function, which are query bugs rather than a missing source.
    TABLE_NOT_FOUND_PATTERN = r'(relation|schema) ".+" does not exist'
    # quoted when unqualified (`column "updated_at" does not exist`), unquoted
    # when qualified (`column base.updated_at does not exist`)
    COLUMN_NOT_FOUND_PATTERN = r"column .+ does not exist"

    def get_table_primary_keys(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[str]:
        """Uses pg_catalog for more reliable constraint information than information_schema."""
        if schema:
            pk_query = f"""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = '{schema}.{table_name}'::regclass
            AND i.indisprimary
            ORDER BY a.attnum
            """
        else:
            pk_query = f"""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = '{table_name}'::regclass
            AND i.indisprimary
            ORDER BY a.attnum
            """

        rows = executor.execute_raw_sql(pk_query).fetchall()
        return [row[0] for row in rows]
