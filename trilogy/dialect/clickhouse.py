from typing import TYPE_CHECKING, Any, Dict, Optional

from jinja2 import Template

from trilogy.core.enums import FunctionType, UnnestMode
from trilogy.core.models.core import DataType, MapWrapper
from trilogy.dialect.base import AGGREGATE_GRAIN_MATCH_MAP, BaseDialect

if TYPE_CHECKING:
    from trilogy.core.models.execute import CTE, UnionCTE


def _ch_struct(args, types):
    # trilogy passes args as alternating [value, 'name', value, 'name', ...]
    # and types similarly. Build a CH named-tuple via cast so attr-access works.
    values = args[::2]
    names = [n.strip("'\"") for n in args[1::2]]
    value_types = list(types[::2]) if types else [None] * len(values)
    type_strs = [
        DATATYPE_MAP.get(t, t.value) if t is not None else "Nullable(Nothing)"
        for t in value_types
    ]
    fields = ", ".join(f"{n} {t}" for n, t in zip(names, type_strs))
    return f"cast(tuple({', '.join(values)}), 'Tuple({fields})')"


def _ch_log(args):
    base = str(args[1]).strip()
    if base in ("10", "10.0"):
        return f"log10({args[0]})"
    if base in ("2", "2.0"):
        return f"log2({args[0]})"
    return f"(ln({args[0]}) / ln({args[1]}))"


def _ch_hash(val, hash_type):
    name = hash_type.strip("'\"").lower()
    funcs = {"md5": "MD5", "sha1": "SHA1", "sha256": "SHA256", "sha512": "SHA512"}
    if name not in funcs:
        raise ValueError(f"Unsupported hash type for ClickHouse: {hash_type}")
    return f"lower(hex({funcs[name]}({val})))"


# ClickHouse uses backticks and is mostly forgiving on EXTRACT/CAST syntax,
# but several functions diverge meaningfully from the BaseDialect defaults.
FUNCTION_MAP = {
    # constants
    FunctionType.CURRENT_DATE: lambda x, types: "today()",
    FunctionType.CURRENT_DATETIME: lambda x, types: "now()",
    FunctionType.CURRENT_TIMESTAMP: lambda x, types: "now()",
    # math
    FunctionType.POWER: lambda x, types: f"pow({x[0]}, {x[1]})",
    FunctionType.RANDOM: lambda x, types: "randCanonical()",
    FunctionType.LOG: lambda x, types: _ch_log(x),
    FunctionType.MOD: lambda x, types: f"modulo({x[0]}, {x[1]})",
    # string
    FunctionType.STRPOS: lambda x, types: f"position({x[0]}, {x[1]})",
    FunctionType.CONTAINS: lambda x, types: f"position({x[0]}, {x[1]}) > 0",
    FunctionType.REGEXP_CONTAINS: lambda x, types: f"match({x[0]}, {x[1]})",
    FunctionType.REGEXP_EXTRACT: lambda x, types: f"extract({x[0]}, {x[1]})",
    FunctionType.REGEXP_REPLACE: lambda x, types: f"replaceRegexpAll({x[0]}, {x[1]}, {x[2]})",
    FunctionType.REPLACE: lambda x, types: f"replaceAll({x[0]}, {x[1]}, {x[2]})",
    FunctionType.SPLIT: lambda x, types: f"splitByString({x[1]}, {x[0]})",
    FunctionType.HASH: lambda x, types: _ch_hash(x[0], x[1]),
    FunctionType.HEX: lambda x, types: f"hex({x[0]})",
    # aggregates with no direct CH equivalent
    FunctionType.BOOL_OR: lambda x, types: f"max(toUInt8({x[0]})) = 1",
    FunctionType.BOOL_AND: lambda x, types: f"min(toUInt8({x[0]})) = 1",
    FunctionType.ANY: lambda x, types: f"any({x[0]})",
    FunctionType.ARRAY_AGG: lambda x, types: f"groupArray({x[0]})",
    # date extract — use to* family for direct integer return
    FunctionType.SECOND: lambda x, types: f"toSecond({x[0]})",
    FunctionType.MINUTE: lambda x, types: f"toMinute({x[0]})",
    FunctionType.HOUR: lambda x, types: f"toHour({x[0]})",
    FunctionType.DAY: lambda x, types: f"toDayOfMonth({x[0]})",
    FunctionType.DAY_OF_WEEK: lambda x, types: f"toDayOfWeek({x[0]})",
    FunctionType.MONTH: lambda x, types: f"toMonth({x[0]})",
    FunctionType.QUARTER: lambda x, types: f"toQuarter({x[0]})",
    FunctionType.YEAR: lambda x, types: f"toYear({x[0]})",
    FunctionType.MONTH_NAME: lambda x, types: f"dateName('month', {x[0]})",
    FunctionType.DAY_NAME: lambda x, types: f"dateName('weekday', {x[0]})",
    FunctionType.FORMAT_TIME: lambda x, types: f"formatDateTime({x[0]}, {x[1]})",
    FunctionType.PARSE_TIME: lambda x, types: f"parseDateTime({x[0]}, {x[1]})",
    # CH versions ≥25 alias dateAdd/date_add to `plus` (binary +) with a 3-arg
    # mismatch error. INTERVAL syntax is the portable form.
    FunctionType.DATE_TRUNCATE: lambda x, types: f"date_trunc('{x[1]}', {x[0]})",
    FunctionType.DATE_PART: lambda x, types: f"EXTRACT({x[1]} FROM {x[0]})",
    FunctionType.DATE_ADD: lambda x, types: f"({x[0]} + INTERVAL {x[2]} {x[1]})",
    FunctionType.DATE_SUB: lambda x, types: f"({x[0]} - INTERVAL {x[2]} {x[1]})",
    FunctionType.DATE_DIFF: lambda x, types: f"dateDiff('{x[2]}', {x[0]}, {x[1]})",
    # toWeek mode 1 = ISO (Mon-Sun); default mode 0 returns Sun-Sat with off-by-one
    FunctionType.WEEK: lambda x, types: f"toWeek({x[0]}, 1)",
    # casts
    FunctionType.DATE: lambda x, types: f"toDate({x[0]})",
    FunctionType.DATETIME: lambda x, types: f"toDateTime64({x[0]}, 3)",
    FunctionType.TIMESTAMP: lambda x, types: f"toDateTime64({x[0]}, 3)",
    FunctionType.DATE_LITERAL: lambda x, types: f"toDate('{x}')",
    FunctionType.DATETIME_LITERAL: lambda x, types: f"toDateTime64('{x}', 3)",
    # arrays
    FunctionType.UNNEST: lambda x, types: f"arrayJoin({x[0]})",
    FunctionType.ARRAY: lambda x, types: f"[{', '.join(x)}]",
    FunctionType.ARRAY_SUM: lambda x, types: f"arraySum({x[0]})",
    FunctionType.ARRAY_DISTINCT: lambda x, types: f"arrayDistinct({x[0]})",
    FunctionType.ARRAY_SORT: lambda x, types: f"arraySort({x[0]})",
    FunctionType.ARRAY_TO_STRING: lambda x, types: f"arrayStringConcat({x[0]}, {x[1]})",
    # CH lambda syntax flips arg order vs. trilogy default (lambda first)
    FunctionType.ARRAY_TRANSFORM: lambda x, types: f"arrayMap({x[1]} -> {x[2]}, {x[0]})",
    FunctionType.ARRAY_FILTER: lambda x, types: f"arrayFilter({x[1]} -> {x[2]}, {x[0]})",
    FunctionType.GENERATE_ARRAY: lambda x, types: f"range({x[0]}, ({x[1]}) + 1, {x[2]})",
    # maps
    FunctionType.MAP_KEYS: lambda x, types: f"mapKeys({x[0]})",
    FunctionType.MAP_VALUES: lambda x, types: f"mapValues({x[0]})",
    # struct: CH named tuples require an explicit Tuple(name type, ...) cast for
    # attribute access by name. Types come from trilogy via the types arg.
    FunctionType.STRUCT: _ch_struct,
}

FUNCTION_GRAIN_MATCH_MAP = {
    **FUNCTION_MAP,
    **AGGREGATE_GRAIN_MATCH_MAP,
}


# CAST target type names used in `cast(x as <type>)` and column DDL
DATATYPE_MAP: dict[DataType, str] = {
    DataType.STRING: "String",
    DataType.BYTES: "String",
    DataType.INTEGER: "Int64",
    DataType.BIGINT: "Int64",
    DataType.FLOAT: "Float64",
    DataType.NUMBER: "Float64",
    DataType.BOOL: "Bool",
    DataType.NUMERIC: "Decimal64(4)",
    DataType.DATE: "Date",
    DataType.DATETIME: "DateTime64(3)",
    DataType.TIMESTAMP: "DateTime64(3)",
    DataType.MAP: "Map",
    DataType.ARRAY: "Array",
}

# Reverse map for information_schema column type strings.
# CH returns concrete types like "String", "UInt32", "Nullable(Int64)", etc.
# Nullable wrappers and parametric types are stripped before lookup at the
# call site; this map only needs the base type names.
DB_COLUMN_TYPE_MAP: dict[str, DataType] = {
    "string": DataType.STRING,
    "fixedstring": DataType.STRING,
    "uint8": DataType.INTEGER,
    "uint16": DataType.INTEGER,
    "uint32": DataType.INTEGER,
    "uint64": DataType.INTEGER,
    "int8": DataType.INTEGER,
    "int16": DataType.INTEGER,
    "int32": DataType.INTEGER,
    "int64": DataType.INTEGER,
    "float32": DataType.FLOAT,
    "float64": DataType.FLOAT,
    "bool": DataType.BOOL,
    "boolean": DataType.BOOL,
    "decimal": DataType.NUMERIC,
    "date": DataType.DATE,
    "date32": DataType.DATE,
    "datetime": DataType.DATETIME,
    "datetime64": DataType.DATETIME,
    "array": DataType.ARRAY,
    "map": DataType.MAP,
    "tuple": DataType.STRUCT,
}


CLICKHOUSE_SQL_TEMPLATE = Template("""{%- if ctes %}
WITH {% if recursive %}RECURSIVE {% endif %}{% for cte in ctes %}
{{cte.name}} as (
{{cte.statement}}){% if not loop.last %},{% endif %}{% endfor %}{% endif %}
{%- if full_select -%}
{{full_select}}
{% else -%}
SELECT
{%- for select in select_columns %}
    {{ select }}{% if not loop.last %},{% endif %}{% endfor %}
{% if base %}FROM
    {{ base }}{% endif %}{% if joins %}{% for join in joins %}
    {{ join }}{% endfor %}{% endif %}{% if where %}
WHERE
    {{ where }}{% endif %}{%- if group_by %}
GROUP BY {% for group in group_by %}
    {{group}}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}{% if having %}
HAVING
    {{ having }}{% endif %}{% if qualify %}
QUALIFY
    {{ qualify }}{% endif %}{%- if order_by %}
ORDER BY {% for order in order_by %}
    {{ order }}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}
{%- if limit is not none %}
LIMIT {{ limit }}{% endif %}{% endif %}
""")


class ClickhouseDialect(BaseDialect):
    FUNCTION_MAP = {**BaseDialect.FUNCTION_MAP, **FUNCTION_MAP}
    FUNCTION_GRAIN_MATCH_MAP = {
        **BaseDialect.FUNCTION_GRAIN_MATCH_MAP,
        **FUNCTION_GRAIN_MATCH_MAP,
    }
    DATATYPE_MAP = {**BaseDialect.DATATYPE_MAP, **DATATYPE_MAP}
    DB_COLUMN_TYPE_MAP = {**BaseDialect.DB_COLUMN_TYPE_MAP, **DB_COLUMN_TYPE_MAP}
    QUOTE_CHARACTER = "`"
    SQL_TEMPLATE = CLICKHOUSE_SQL_TEMPLATE
    SUPPORTS_QUALIFY = True
    # CH doesn't accept arrayJoin as a FROM-clause table function; DIRECT mode
    # emits `SELECT arrayJoin(...) AS alias` with no FROM, which CH supports.
    UNNEST_MODE = UnnestMode.DIRECT
    SUPPORTS_AGGREGATE_GROUPING_MODES = True
    TABLE_NOT_FOUND_PATTERN = "Table .* doesn't exist"
    COLUMN_NOT_FOUND_PATTERN = "Unknown identifier"

    def render_map_literal(
        self,
        e: "MapWrapper[Any, Any]",
        cte: "Optional[CTE | UnionCTE]" = None,
        cte_map: "Optional[Dict[str, CTE | UnionCTE]]" = None,
        raise_invalid: bool = False,
    ) -> str:
        # CH uses map(k1, v1, k2, v2, ...). Avoids the `key:value` syntax that
        # SQLAlchemy's text() reads as bound parameters.
        parts: list[str] = []
        for k, v in e.items():
            parts.append(
                self.render_expr(
                    k, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid
                )
            )
            parts.append(
                self.render_expr(
                    v, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid
                )
            )
        return f"map({', '.join(parts)})"
