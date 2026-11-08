from collections.abc import Callable
from typing import Any, ClassVar

from jinja2 import Template

from trilogy.core.enums import ComparisonOperator, FunctionType, NullSort
from trilogy.core.like_patterns import to_glob
from trilogy.core.models.core import DataType
from trilogy.dialect.base import (
    BaseDialect,
    TableColumn,
    guard_divisor,
    offset_keeps_date,
    render_present_extreme,
    render_true_divide,
    render_unsupported,
)
from trilogy.dialect.base import (
    render_mod as base_render_mod,
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


TEMPORAL_CASTS = {
    DataType.DATE: "date",
    DataType.DATETIME: "datetime",
    DataType.TIMESTAMP: "datetime",
}


def render_cast(args: list[str], types: list) -> str:
    # SQLite truncates a fraction cast to an integer; other engines round it
    if (
        len(types) == 2
        and types[0].data_type in FRACTIONAL_TYPES
        and types[1].data_type in INTEGER_TYPES
    ):
        return f"cast(round({args[0]}) as {args[1]})"
    # dates are text here, so a cast to TEXT keeps the time a date drops
    if len(types) == 2 and types[1].data_type in TEMPORAL_CASTS:
        return f"{TEMPORAL_CASTS[types[1].data_type]}({args[0]})"
    return f"cast({args[0]} as {args[1]})"


def render_mod(args: list[str], types: list) -> str:
    """`%` casts both sides to an integer (`7.5 % 3` is 1); `mod` keeps the
    fraction, but answers a REAL for integers too."""
    if any(getattr(t, "data_type", t) in FRACTIONAL_TYPES for t in types):
        return f"mod({args[0]}, {guard_divisor(args[1])})"
    return base_render_mod(args)


MONTH_FACTOR = {"month": 1, "quarter": 3, "year": 12}
OFFSET_UNIT = {
    "day": ("day", 1),
    "week": ("day", 7),
    "hour": ("hour", 1),
    "minute": ("minute", 1),
    "second": ("second", 1),
}


def month_offset(expr: str, months: str) -> str:
    """The same day `months` later, clamped to the month's last day as every
    other engine does; the `months` modifier overflows (Jan 31 + 1 is Mar 2)."""
    first = f"date({expr}, 'start of month', ({months}) || ' months')"
    last_day = f"CAST(strftime('%d', {first}, '+1 month', '-1 day') AS INTEGER)"
    day = f"min(CAST(strftime('%d', {expr}) AS INTEGER), {last_day})"
    return f"date({first}, ({day} - 1) || ' days')"


def date_offset(args: list[str], types: list, subtract: bool = False) -> str:
    expr, unit, amount = args[0], args[1].lower(), args[2]
    sign = "-" if subtract else ""
    keep_date = offset_keeps_date(args, types)
    if unit in MONTH_FACTOR:
        shifted = month_offset(expr, f"{sign}({amount}) * {MONTH_FACTOR[unit]}")
        return shifted if keep_date else f"datetime({shifted} || ' ' || time({expr}))"
    if unit not in OFFSET_UNIT:
        raise ValueError(f"Unsupported date unit for sqlite: {args[1]}")
    name, factor = OFFSET_UNIT[unit]
    modifier = f"({sign}({amount}) * {factor}) || ' {name}'"
    return f"{'date' if keep_date else 'datetime'}({expr}, {modifier})"


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
    # day and coarser are typed DATE (`get_date_trunc_output`), so render a date
    part_lower = part.lower()
    if part_lower == "day":
        return f"date({expr})"
    if part_lower == "month":
        return f"date({expr}, 'start of month')"
    if part_lower == "year":
        return f"date({expr}, 'start of year')"
    if part_lower == "hour":
        return f"datetime(strftime('%Y-%m-%d %H:00:00', {expr}))"
    if part_lower == "minute":
        return f"datetime(strftime('%Y-%m-%d %H:%M:00', {expr}))"
    if part_lower == "second":
        return f"datetime(strftime('%Y-%m-%d %H:%M:%S', {expr}))"
    if part_lower == "week":
        return f"date({expr}, '-' || ((CAST(strftime('%w', {expr}) AS INTEGER)+6) % 7) || ' days')"
    if part_lower == "quarter":
        return (
            "date(printf('%04d-%02d-01', "
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
    FunctionType.DATE_ADD: lambda x, types: date_offset(x, types),
    FunctionType.DATE_SUB: lambda x, types: date_offset(x, types, subtract=True),
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
    FunctionType.MOD: render_mod,
    # the multi-argument max/min are greatest/least, NULL if any argument is
    FunctionType.GREATEST: render_present_extreme("max"),
    FunctionType.LEAST: render_present_extreme("min"),
    FunctionType.STRPOS: lambda x, types: f"instr({x[0]}, {x[1]})",
    # native: the reference semantics (formal/Trilogy/Strings.lean, `substr`)
    FunctionType.SUBSTRING: lambda x, types: f"SUBSTRING({x[0]},{x[1]},{x[2]})",
    # SQLAlchemy registers a Python `floor` here, which raises on NULL
    FunctionType.FLOOR: lambda x, types: (
        f"(CAST({x[0]} AS INTEGER) - ({x[0]} < CAST({x[0]} AS INTEGER)))"
    ),
    FunctionType.CEIL: lambda x, types: (
        f"(CAST({x[0]} AS INTEGER) + ({x[0]} > CAST({x[0]} AS INTEGER)))"
    ),
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


# `to_glob` over a pattern only known at run time: the escaped specials are
# parked on control characters while the wildcards are rewritten. A pattern
# holding those control characters itself is not translated faithfully.
_RUNTIME_GLOB_STEPS = (
    ("'\\\\'", "char(1)"),
    ("'\\%'", "char(2)"),
    ("'\\_'", "char(3)"),
    ("'\\'", "''"),
    ("'['", "'[[]'"),
    ("'*'", "'[*]'"),
    ("'?'", "'[?]'"),
    ("'%'", "'*'"),
    ("'_'", "'?'"),
    ("char(1)", "'\\'"),
    ("char(2)", "'%'"),
    ("char(3)", "'_'"),
)


def runtime_glob(pattern_sql: str) -> str:
    for old, new in _RUNTIME_GLOB_STEPS:
        pattern_sql = f"replace({pattern_sql}, {old}, {new})"
    return pattern_sql


class SQLiteDialect(BaseDialect):
    NULL_SORT = NullSort.SMALLEST
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

    def render_like(
        self,
        left_sql: str,
        pattern,
        operator: ComparisonOperator,
        render: Callable[[Any], str],
    ) -> str:
        """SQLite's LIKE ignores ASCII case; GLOB is its case-sensitive match."""
        if operator not in (ComparisonOperator.LIKE, ComparisonOperator.NOT_LIKE):
            return super().render_like(left_sql, pattern, operator, render)
        if isinstance(pattern, str):
            glob = self.render_string_literal(to_glob(pattern))
        else:
            glob = runtime_glob(render(pattern))
        negate = "not " if operator == ComparisonOperator.NOT_LIKE else ""
        return f"({negate}{left_sql} glob {glob})"

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
