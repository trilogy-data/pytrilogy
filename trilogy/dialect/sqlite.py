from typing import Any, Callable, Mapping

from jinja2 import Template

from trilogy.core.enums import FunctionType, WindowType
from trilogy.core.models.core import DataType
from trilogy.dialect.base import BaseDialect

WINDOW_FUNCTION_MAP: Mapping[WindowType, Callable[[Any, Any, Any], str]] = {}

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
        f"(({multiplier}{amount})*{factor})"
        if factor != "1"
        else f"{multiplier}{amount}"
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
        "week": "CAST(strftime('%W', {expr}) AS INTEGER)",
        "day_of_week": "(CAST(strftime('%w', {expr}) AS INTEGER) + 1)",
    }
    if part_lower == "quarter":
        return "(((CAST(strftime('%m', {expr}) AS INTEGER)-1)/3)+1)".format(expr=expr)
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
    FunctionType.COUNT: lambda args, types: f"count({args[0]})",
    FunctionType.SUM: lambda args, types: f"sum({args[0]})",
    FunctionType.AVG: lambda args, types: f"avg({args[0]})",
    FunctionType.LENGTH: lambda args, types: f"length({args[0]})",
    FunctionType.CONCAT: lambda args, types: f"({' || '.join(args)})",
    FunctionType.ILIKE: lambda args, types: f"(lower({args[0]}) like lower({args[1]}))",
    FunctionType.CONTAINS: lambda args, types: f"(instr(lower({args[0]}), lower({args[1]})) > 0)",
    FunctionType.BOOL_OR: lambda args, types: f"max(CAST({args[0]} as integer))",
    FunctionType.BOOL_AND: lambda args, types: f"min(CAST({args[0]} as integer))",
    FunctionType.CURRENT_DATE: lambda x, types: "date('now')",
    FunctionType.CURRENT_DATETIME: lambda x, types: "datetime('now')",
    FunctionType.DATE: lambda x, types: f"date({x[0]})",
    FunctionType.DATETIME: lambda x, types: f"datetime({x[0]})",
    FunctionType.TIMESTAMP: lambda x, types: f"datetime({x[0]})",
    FunctionType.DATE_LITERAL: lambda x, types: f"date('{x}')",
    FunctionType.DATETIME_LITERAL: lambda x, types: f"datetime('{x}')",
    FunctionType.DATE_ADD: lambda x, types: f"datetime({x[0]}, {date_modifier(x[1], x[2])})",
    FunctionType.DATE_SUB: lambda x, types: f"datetime({x[0]}, {date_modifier(x[1], x[2], subtract=True)})",
    FunctionType.DATE_PART: lambda x, types: date_part(x[0], x[1]),
    FunctionType.DATE_TRUNCATE: lambda x, types: date_truncate(x[0], x[1]),
    FunctionType.DATE_DIFF: lambda x, types: (
        f"CAST((julianday({x[1]}) - julianday({x[0]})) AS INTEGER)"
        if x[2].lower() == "day"
        else (
            f"CAST((julianday({x[1]}) - julianday({x[0]})) / 7 AS INTEGER)"
            if x[2].lower() == "week"
            else (
                f"((CAST(strftime('%Y', {x[1]}) AS INTEGER) - CAST(strftime('%Y', {x[0]}) AS INTEGER)) * 12 + (CAST(strftime('%m', {x[1]}) AS INTEGER) - CAST(strftime('%m', {x[0]}) AS INTEGER)))"
                if x[2].lower() == "month"
                else (
                    f"(CAST(strftime('%Y', {x[1]}) AS INTEGER) - CAST(strftime('%Y', {x[0]}) AS INTEGER))"
                    if x[2].lower() == "year"
                    else (
                        f"CAST((julianday({x[1]}) - julianday({x[0]})) * 24 AS INTEGER)"
                        if x[2].lower() == "hour"
                        else (
                            f"CAST((julianday({x[1]}) - julianday({x[0]})) * 24 * 60 AS INTEGER)"
                            if x[2].lower() == "minute"
                            else f"CAST((julianday({x[1]}) - julianday({x[0]})) * 24 * 60 * 60 AS INTEGER)"
                        )
                    )
                )
            )
        )
    ),
    FunctionType.DAY: lambda x, types: "CAST(strftime('%d', {}) AS INTEGER)".format(
        x[0]
    ),
    FunctionType.WEEK: lambda x, types: "CAST(strftime('%W', {}) AS INTEGER)".format(
        x[0]
    ),
    FunctionType.MONTH: lambda x, types: "CAST(strftime('%m', {}) AS INTEGER)".format(
        x[0]
    ),
    FunctionType.QUARTER: lambda x, types: "(((CAST(strftime('%m', {}) AS INTEGER)-1)/3)+1)".format(
        x[0]
    ),
    FunctionType.YEAR: lambda x, types: "CAST(strftime('%Y', {}) AS INTEGER)".format(
        x[0]
    ),
    FunctionType.HOUR: lambda x, types: "CAST(strftime('%H', {}) AS INTEGER)".format(
        x[0]
    ),
    FunctionType.MINUTE: lambda x, types: "CAST(strftime('%M', {}) AS INTEGER)".format(
        x[0]
    ),
    FunctionType.SECOND: lambda x, types: "CAST(strftime('%S', {}) AS INTEGER)".format(
        x[0]
    ),
    FunctionType.DAY_OF_WEEK: lambda x, types: "(CAST(strftime('%w', {}) AS INTEGER) + 1)".format(
        x[0]
    ),
    FunctionType.MONTH_NAME: lambda x, types: MONTH_NAME_CASE.format(expr=x[0]),
    FunctionType.DAY_NAME: lambda x, types: DAY_NAME_CASE.format(expr=x[0]),
    FunctionType.FORMAT_TIME: lambda x, types: f"strftime({x[1]}, {x[0]})",
    FunctionType.PARSE_TIME: lambda x, types: f"datetime({x[0]})",
    FunctionType.RANDOM: lambda x, types: f"(abs(random()) % {x[0]})",
}

FUNCTION_GRAIN_MATCH_MAP = {
    **FUNCTION_MAP,
    FunctionType.COUNT_DISTINCT: lambda args, types: f"CASE WHEN {args[0]} IS NOT NULL THEN 1 ELSE 0 END",
    FunctionType.COUNT: lambda args, types: f"CASE WHEN {args[0]} IS NOT NULL THEN 1 ELSE 0 END",
    FunctionType.SUM: lambda args, types: f"{args[0]}",
    FunctionType.AVG: lambda args, types: f"{args[0]}",
    FunctionType.MAX: lambda args, types: f"{args[0]}",
    FunctionType.MIN: lambda args, types: f"{args[0]}",
}

SQLITE_SQL_TEMPLATE = Template("""{%- if output %}
{{output}}
{% endif %}{%- if ctes %}
WITH {% if recursive%}RECURSIVE{% endif %}{% for cte in ctes %}
{{cte.name}} as (
{{cte.statement}}){% if not loop.last %},{% else %}
{% endif %}{% endfor %}{% endif %}
{%- if full_select -%}
{{full_select}}
{%- else -%}
SELECT
{%- for select in select_columns %}
    {{ select }}{% if not loop.last %},{% endif %}{% endfor %}
{% if base %}FROM
    {{ base }}{% endif %}{% if joins %}
{%- for join in joins %}
    {{ join }}{% endfor %}{% endif %}
{%- if where %}
WHERE
    {{ where }}
{% endif -%}{%- if group_by %}
GROUP BY {% for group in group_by %}
    {{group}}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}{% if having %}
HAVING
    {{ having }}
{% endif %}{%- if order_by %}
ORDER BY {% for order in order_by %}
    {{ order }}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}
{%- if limit is not none %}
LIMIT ({{ limit }}){% endif %}{% endif %}
""")

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
    DataType.BOOL: "INTEGER",
    DataType.NUMERIC: "NUMERIC",
    DataType.DATE: "TEXT",
    DataType.DATETIME: "TEXT",
    DataType.TIMESTAMP: "TEXT",
}


class SQLiteDialect(BaseDialect):
    WINDOW_FUNCTION_MAP = {**BaseDialect.WINDOW_FUNCTION_MAP, **WINDOW_FUNCTION_MAP}
    FUNCTION_MAP = {**BaseDialect.FUNCTION_MAP, **FUNCTION_MAP}
    FUNCTION_GRAIN_MATCH_MAP = {
        **BaseDialect.FUNCTION_GRAIN_MATCH_MAP,
        **FUNCTION_GRAIN_MATCH_MAP,
    }
    DATATYPE_MAP = {**BaseDialect.DATATYPE_MAP, **DATATYPE_MAP}
    QUOTE_CHARACTER = '"'
    SQL_TEMPLATE = SQLITE_SQL_TEMPLATE
    CREATE_TABLE_SQL_TEMPLATE = SQLITE_CREATE_TABLE_SQL_TEMPLATE
    TABLE_NOT_FOUND_PATTERN = "no such table"
    COLUMN_NOT_FOUND_PATTERN = "no such column"

    def compile_create_table_statement(self, target, create_mode):
        statement = super().compile_create_table_statement(target, create_mode)
        if create_mode.value == "create_or_replace":
            table_name = self.safe_quote(target.name)
            return f"DROP TABLE IF EXISTS {table_name};\n{statement.replace('CREATE TABLE ', 'CREATE TABLE ', 1)}"
        return statement

    def get_table_schema(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[tuple]:
        schema_name = schema or "main"
        rows = executor.execute_raw_sql(
            f"PRAGMA {schema_name}.table_info('{table_name}')"
        ).fetchall()
        output: list[tuple] = []
        for row in rows:
            # cid, name, type, notnull, dflt_value, pk
            output.append((row[1], row[2], "NO" if row[3] else "YES", ""))
        return output

    def get_table_primary_keys(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[str]:
        schema_name = schema or "main"
        rows = executor.execute_raw_sql(
            f"PRAGMA {schema_name}.table_info('{table_name}')"
        ).fetchall()
        return [row[1] for row in rows if row[5] > 0]
