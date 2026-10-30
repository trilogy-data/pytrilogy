from collections.abc import Callable
from typing import ClassVar

from jinja2 import Template

from trilogy.core.enums import FunctionType
from trilogy.core.models.core import DataType
from trilogy.dialect.base import (
    BaseDialect,
    TableColumn,
    render_true_divide,
    render_unsupported,
)

MONTH_NAME_CASE = (
    "CASE CAST(strftime('%m', {expr}) AS INTEGER) "
    "WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March' "
    "WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June' "
    "WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September' "
    "WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December' END"
)

DAY_NAME_CASE = (
    "CASE CAST(strftime('%w', {expr}) AS INTEGER) "
    "WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday' "
    "WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday' "
    "WHEN 6 THEN 'Saturday' END"
)


# the ISO week is the one holding its Thursday; %W counts Monday weeks from 0
ISO_WEEK = "CAST((strftime('%j', date({expr}, '-3 days', 'weekday 4')) - 1) / 7 + 1 AS INTEGER)"

# date_diff counts the part's boundaries crossed, as DuckDB does: the
# difference of both sides truncated to the part, never the elapsed time
TRUNCATED_EPOCH = {
    "hour": ("%Y-%m-%d %H:00:00", 3600),
    "minute": ("%Y-%m-%d %H:%M:00", 60),
    "second": ("%Y-%m-%d %H:%M:%S", 1),
}

INTEGER_TYPES = (DataType.INTEGER, DataType.BIGINT)
FRACTIONAL_TYPES = (DataType.FLOAT, DataType.DOUBLE, DataType.NUMERIC, DataType.NUMBER)


def date_diff(first: str, second: str, part: str) -> str:
    part = part.lower()
    days = f"(julianday(date({second})) - julianday(date({first})))"
    if part == "day":
        return f"CAST({days} AS INTEGER)"
    if part == "week":
        return f"CAST({days} / 7 AS INTEGER)"
    if part == "month":
        return (
            f"((CAST(strftime('%Y', {second}) AS INTEGER) - CAST(strftime('%Y', {first}) AS INTEGER)) * 12"
            f" + (CAST(strftime('%m', {second}) AS INTEGER) - CAST(strftime('%m', {first}) AS INTEGER)))"
        )
    if part == "year":
        return f"(CAST(strftime('%Y', {second}) AS INTEGER) - CAST(strftime('%Y', {first}) AS INTEGER))"
    if part in TRUNCATED_EPOCH:
        fmt, width = TRUNCATED_EPOCH[part]
        epoch = "CAST(strftime('%s', strftime('{fmt}', {expr})) AS INTEGER)"
        return (
            f"(({epoch.format(fmt=fmt, expr=second)}"
            f" - {epoch.format(fmt=fmt, expr=first)}) / {width})"
        )
    raise ValueError(f"Unsupported date diff part for sqlite: {part}")


def render_cast(args: list[str], types: list) -> str:
    # SQLite truncates a fraction cast to an integer; other engines round it
    if (
        len(types) == 2
        and types[0].data_type in FRACTIONAL_TYPES
        and types[1].data_type in INTEGER_TYPES
    ):
        return f"cast(round({args[0]}) as {args[1]})"
    return f"cast({args[0]} as {args[1]})"


def date_modifier(unit: str, amount: str, subtract: bool = False) -> str:
    multiplier = "-" if subtract else ""
    normalized = unit.lower()
    mapping = {
        "day": "day",
        "week": "day",
        "month": "month",
        "quarter": "month",
        "year": "year",
        "hour": "hour",
        "minute": "minute",
        "second": "second",
    }
    if normalized not in mapping:
        raise ValueError(f"Unsupported date unit for sqlite: {unit}")
    factor = "7" if normalized == "week" else "3" if normalized == "quarter" else "1"
    final_amount = (
        f"(({multiplier}({amount}))*{factor})"
        if factor != "1"
        else f"{multiplier}({amount})"
    )
    return f"({final_amount}) || ' {mapping[normalized]}'"


def date_part(expr: str, part: str) -> str:
    part_lower = part.lower()
    mapping = {
        "year": "CAST(strftime('%Y', {expr}) AS INTEGER)",
        "month": "CAST(strftime('%m', {expr}) AS INTEGER)",
        "day": "CAST(strftime('%d', {expr}) AS INTEGER)",
        "hour": "CAST(strftime('%H', {expr}) AS INTEGER)",
        "minute": "CAST(strftime('%M', {expr}) AS INTEGER)",
        "second": "CAST(strftime('%S', {expr}) AS INTEGER)",
        "week": ISO_WEEK,
        "day_of_week": "(CAST(strftime('%w', {expr}) AS INTEGER) + 1)",
    }
    if part_lower == "quarter":
        return f"(((CAST(strftime('%m', {expr}) AS INTEGER)-1)/3)+1)"
    if part_lower not in mapping:
        raise ValueError(f"Unsupported date part for sqlite: {part}")
    return mapping[part_lower].format(expr=expr)


def date_truncate(expr: str, part: str) -> str:
    part_lower = part.lower()
    if part_lower == "day":
        return f"datetime(date({expr}))"
    if part_lower == "month":
        return f"datetime(strftime('%Y-%m-01 00:00:00', {expr}))"
    if part_lower == "year":
        return f"datetime(strftime('%Y-01-01 00:00:00', {expr}))"
    if part_lower == "hour":
        return f"datetime(strftime('%Y-%m-%d %H:00:00', {expr}))"
    if part_lower == "minute":
        return f"datetime(strftime('%Y-%m-%d %H:%M:00', {expr}))"
    if part_lower == "second":
        return f"datetime(strftime('%Y-%m-%d %H:%M:%S', {expr}))"
    if part_lower == "week":
        return f"datetime(date({expr}, '-' || ((CAST(strftime('%w', {expr}) AS INTEGER)+6) % 7) || ' days'))"
    if part_lower == "quarter":
        return (
            "datetime(printf('%04d-%02d-01 00:00:00', "
            f"CAST(strftime('%Y', {expr}) AS INTEGER), "
            f"(((CAST(strftime('%m', {expr}) AS INTEGER)-1)/3)*3)+1))"
        )
    raise ValueError(f"Unsupported date truncation for sqlite: {part}")


FUNCTION_MAP = {
    # no fold over an array
    FunctionType.ARRAY_REDUCE: render_unsupported("array_reduce"),
    FunctionType.CAST: lambda x, types: render_cast(x, types),
    FunctionType.DIVIDE: lambda x, types: render_true_divide("REAL", x, types),
    FunctionType.BIT_XOR: lambda x, types: f"(({x[0]} | {x[1]}) - ({x[0]} & {x[1]}))",
    # concat() skips NULLs (coalesce-wrapped for pre-3.44 sqlite); || propagates
    FunctionType.CONCAT: lambda args, types: (
        "(" + " || ".join([f"coalesce({a}, '')" for a in args]) + ")"
    ),
    FunctionType.CONTAINS: lambda args, types: f"(instr(lower({args[0]}), lower({args[1]})) > 0)",
    FunctionType.BOOL_OR: lambda args, types: f"max(CAST({args[0]} as integer))",
    FunctionType.BOOL_AND: lambda args, types: f"min(CAST({args[0]} as integer))",
    FunctionType.CURRENT_DATE: lambda x, types: "date('now')",
    FunctionType.CURRENT_DATETIME: lambda x, types: "datetime('now')",
    FunctionType.TIMESTAMP: lambda x, types: f"datetime({x[0]})",
    FunctionType.DATE_LITERAL: lambda x, types: f"date('{x}')",
    FunctionType.DATETIME_LITERAL: lambda x, types: f"datetime('{x}')",
    FunctionType.DATE_ADD: lambda x, types: f"datetime({x[0]}, {date_modifier(x[1], x[2])})",
    FunctionType.DATE_SUB: lambda x, types: f"datetime({x[0]}, {date_modifier(x[1], x[2], subtract=True)})",
    FunctionType.DATE_PART: lambda x, types: date_part(x[0], x[1]),
    FunctionType.DATE_TRUNCATE: lambda x, types: date_truncate(x[0], x[1]),
    FunctionType.DATE_DIFF: lambda x, types: date_diff(x[0], x[1], x[2]),
    FunctionType.DAY: lambda x, types: f"CAST(strftime('%d', {x[0]}) AS INTEGER)",
    FunctionType.WEEK: lambda x, types: ISO_WEEK.format(expr=x[0]),
    FunctionType.MONTH: lambda x, types: f"CAST(strftime('%m', {x[0]}) AS INTEGER)",
    FunctionType.QUARTER: lambda x, types: f"(((CAST(strftime('%m', {x[0]}) AS INTEGER)-1)/3)+1)",
    FunctionType.YEAR: lambda x, types: f"CAST(strftime('%Y', {x[0]}) AS INTEGER)",
    FunctionType.HOUR: lambda x, types: f"CAST(strftime('%H', {x[0]}) AS INTEGER)",
    FunctionType.MINUTE: lambda x, types: f"CAST(strftime('%M', {x[0]}) AS INTEGER)",
    FunctionType.SECOND: lambda x, types: f"CAST(strftime('%S', {x[0]}) AS INTEGER)",
    FunctionType.DAY_OF_WEEK: lambda x, types: f"(CAST(strftime('%w', {x[0]}) AS INTEGER) + 1)",
    FunctionType.MONTH_NAME: lambda x, types: MONTH_NAME_CASE.format(expr=x[0]),
    FunctionType.DAY_NAME: lambda x, types: DAY_NAME_CASE.format(expr=x[0]),
    FunctionType.FORMAT_TIME: lambda x, types: f"strftime({x[1]}, {x[0]})",
    FunctionType.PARSE_TIME: lambda x, types: f"datetime({x[0]})",
    FunctionType.RANDOM: lambda x, types: f"(abs(random()) % {x[0]})",
}

SQLITE_CREATE_TABLE_SQL_TEMPLATE = Template("""
CREATE TABLE {% if create_mode == "create_if_not_exists" %}IF NOT EXISTS {% endif %}{{ name }} (
{%- for column in columns %}
    {{ column.name }} {{ type_map[column.name] }}{% if not loop.last %},{% endif %}
{%- endfor %}
);
""".strip())

DATATYPE_MAP: dict[DataType, str] = {
    DataType.STRING: "TEXT",
    DataType.INTEGER: "INTEGER",
    DataType.FLOAT: "REAL",
    DataType.DOUBLE: "REAL",
    DataType.BOOL: "INTEGER",
    DataType.NUMERIC: "NUMERIC",
    DataType.DATE: "TEXT",
    DataType.DATETIME: "TEXT",
    DataType.TIMESTAMP: "TEXT",
}


class SQLiteDialect(BaseDialect):
    FUNCTION_MAP: ClassVar[dict[FunctionType, Callable[..., str]]] = {
        **BaseDialect.FUNCTION_MAP,
        **FUNCTION_MAP,
    }
    DATATYPE_MAP: ClassVar[dict[DataType, str]] = {
        **BaseDialect.DATATYPE_MAP,
        **DATATYPE_MAP,
    }
    QUOTE_CHARACTER = '"'
    LIMIT_PARENTHESIZED = True
    CREATE_TABLE_SQL_TEMPLATE = SQLITE_CREATE_TABLE_SQL_TEMPLATE
    SUPPORTS_ARRAYS = False
    TABLE_NOT_FOUND_PATTERN = "no such table"
    COLUMN_NOT_FOUND_PATTERN = "no such column"
    # no native ILIKE; emulate via case-folded LIKE
    ILIKE_AS_LOWER_LIKE = ("lower", "like", "not ")

    def compile_create_table_statements(self, target, create_mode) -> list[str]:
        statements = super().compile_create_table_statements(target, create_mode)
        if create_mode.value == "create_or_replace":
            return [
                f"DROP TABLE IF EXISTS {self.safe_quote(target.name)};",
                *statements,
            ]
        return statements

    def get_table_schema(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[TableColumn]:
        schema_name = schema or "main"
        rows = executor.execute_raw_sql(
            f"PRAGMA {schema_name}.table_info('{table_name}')"
        ).fetchall()
        # cid, name, type, notnull, dflt_value, pk
        return [self.make_table_column(row[1], row[2], not row[3], "") for row in rows]

    def get_table_primary_keys(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[str]:
        schema_name = schema or "main"
        rows = executor.execute_raw_sql(
            f"PRAGMA {schema_name}.table_info('{table_name}')"
        ).fetchall()
        return [row[1] for row in rows if row[5] > 0]
