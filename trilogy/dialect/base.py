from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date, datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Union,
    cast,
)

if TYPE_CHECKING:
    from trilogy.dialect.config import DialectConfig
    from trilogy.engine import ResultProtocol

from jinja2 import Template

from trilogy.constants import (
    CONFIG,
    DEFAULT_NAMESPACE,
    MagicConstants,
    Rendering,
    logger,
)
from trilogy.core.constants import UNNEST_NAME
from trilogy.core.enums import (
    AddressType,
    AggregateGroupingMode,
    ComparisonOperator,
    CreateMode,
    DatePart,
    Derivation,
    FunctionClass,
    FunctionType,
    GroupMode,
    Modifier,
    Ordering,
    PersistMode,
    ShowCategory,
    UnnestMode,
    WindowType,
)
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.internal import DEFAULT_CONCEPTS
from trilogy.core.models.author import ArgBinding, arg_to_datatype
from trilogy.core.models.build import (
    BoolExpr,
    BuildAggregateWrapper,
    BuildBetween,
    BuildCaseElse,
    BuildCaseSimpleWhen,
    BuildCaseWhen,
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildExpr,
    BuildFilterItem,
    BuildFunction,
    BuildMultiSelectLineage,
    BuildNavigationWindowItem,
    BuildNumberingWindowItem,
    BuildOrderItem,
    BuildParamaterizedConceptReference,
    BuildParenthetical,
    BuildRowsetItem,
    BuildSubselectComparison,
    BuildSubselectItem,
    BuildWindowItem,
)
from trilogy.core.models.core import (
    CONCRETE_TYPES,
    ArrayType,
    DataType,
    EnumType,
    ListWrapper,
    MapType,
    MapWrapper,
    NumericType,
    StructType,
    TraitDataType,
    TupleWrapper,
)
from trilogy.core.models.datasource import Address, Datasource, RawColumnExpr
from trilogy.core.models.environment import Environment
from trilogy.core.models.execute import (
    CTE,
    CompiledCTE,
    RecursiveCTE,
    UnionCTE,
)
from trilogy.core.processing.condition_utility import (
    condition_implies,
    contains_window,
    decompose_condition,
    is_scalar_condition,
)
from trilogy.core.processing.utility import sort_select_output
from trilogy.core.query_processor import (
    process_chart,
    process_copy,
    process_persist,
    process_query,
)
from trilogy.core.statements.author import (
    ChartStatement,
    ConceptDeclarationStatement,
    CopyStatement,
    CreateStatement,
    FunctionDeclaration,
    ImportStatement,
    MergeStatementV2,
    MockStatement,
    MultiSelectStatement,
    PersistStatement,
    PublishStatement,
    RawSQLStatement,
    RowsetDerivationStatement,
    SelectStatement,
    ShowStatement,
    ValidateStatement,
)
from trilogy.core.statements.execute import (
    PROCESSED_STATEMENT_TYPES,
    ProcessedChartCopyStatement,
    ProcessedChartStatement,
    ProcessedCopyStatement,
    ProcessedCreateStatement,
    ProcessedMockStatement,
    ProcessedPublishStatement,
    ProcessedQuery,
    ProcessedQueryPersist,
    ProcessedRawSQLStatement,
    ProcessedShowStatement,
    ProcessedStaticValueOutput,
    ProcessedValidateStatement,
)
from trilogy.core.table_processor import (
    CreateTableInfo,
    datasource_to_create_table_info,
    process_create_statement,
)
from trilogy.core.utility import safe_quote
from trilogy.dialect.common import render_join, render_unnest
from trilogy.hooks.base_hook import BaseHook
from trilogy.utility import safe_open


@dataclass
class TableColumn:
    """A single column from a table schema, as returned by ``get_table_schema``.

    ``raw_db_type`` is the dialect's own type string; ``trilogy_type`` is that
    string mapped through the dialect's type map (``normalize_db_type``), so
    callers never have to re-run the mapping themselves.
    """

    column_name: str
    raw_db_type: str
    trilogy_type: DataType
    is_nullable: bool = True
    comment: str | None = None


def nullable_from_str(value: object) -> bool:
    """Interpret an information_schema ``is_nullable`` value as a bool.

    Nullable unless the value is explicitly ``NO`` (case-insensitive)."""
    return str(value).strip().upper() != "NO"


def null_wrapper(lval: str, rval: str, modifiers: list[Modifier]) -> str:

    if Modifier.NULLABLE in modifiers:
        return f"({lval} = {rval} or ({lval} is null and {rval} is null))"
    return f"{lval} = {rval}"


LOGGER_PREFIX = "[RENDERING]"

WINDOW_ITEMS = (BuildNumberingWindowItem, BuildNavigationWindowItem)
FILTER_ITEMS = (BuildFilterItem,)


ARITHMETIC_OPERATORS = (
    FunctionType.ADD,
    FunctionType.SUBTRACT,
    FunctionType.DIVIDE,
    FunctionType.MULTIPLY,
)
# Infix parents that render as binary operators; arithmetic args must be
# parenthesized for precedence. Non-infix function calls (abs, cast, sum)
# already delimit their args with call parens and don't need extra wrapping.
INFIX_PARENTS_REQUIRING_PARENS = ARITHMETIC_OPERATORS + (
    FunctionType.MOD,
    FunctionType.POWER,
)


def _needs_arithmetic_parentheses(
    expr: Any, parent_operator: FunctionType | None = None
) -> bool:
    if (
        parent_operator is not None
        and parent_operator not in INFIX_PARENTS_REQUIRING_PARENS
    ):
        return False
    if isinstance(expr, BuildFunction):
        return expr.operator in ARITHMETIC_OPERATORS
    if isinstance(expr, BuildRowsetItem):
        return _needs_arithmetic_parentheses(expr.content)
    return (
        isinstance(expr, BuildConcept)
        and expr.lineage is not None
        and _needs_arithmetic_parentheses(expr.lineage)
    )


AGGREGATE_ITEMS = (BuildAggregateWrapper,)
FUNCTION_ITEMS = (BuildFunction,)
PARENTHETICAL_ITEMS = (BuildParenthetical,)


def _is_build_row_tuple(x: Any) -> bool:
    """True for a ROW_TUPLE operand of composite (row-wise) membership."""
    return isinstance(x, BuildFunction) and x.operator == FunctionType.ROW_TUPLE


# Datatypes whose CONSTANT values can be inlined into SQL without
# parameterisation. INTEGER / BOOL round-trip cleanly through engine
# parsing; FLOAT is excluded because DuckDB parses dotted literals as
# DECIMAL, which would change result types from float to Decimal.
INLINE_SAFE_PARAM_DATATYPES = frozenset({DataType.INTEGER, DataType.BOOL})


def _constant_bindable(lineage: BuildFunction) -> bool:
    """A CONSTANT whose value is a MagicConstants (e.g. NULL) can't be bound — no
    driver can transform the enum into a value. Render it inline instead."""
    return not (lineage.arguments and isinstance(lineage.arguments[0], MagicConstants))


CASE_WHEN_ITEMS = (BuildCaseWhen,)
CASE_ELSE_ITEMS = (BuildCaseElse,)
SUBSELECT_COMPARISON_ITEMS = (BuildSubselectComparison,)
SUBSELECT_ITEMS = (BuildSubselectItem,)
COMPARISON_ITEMS = (BuildComparison,)
CONDITIONAL_ITEMS = (BuildConditional,)
BETWEEN_ITEMS = (BuildBetween,)

BASE_INVALID = "INVALID_REFERENCE_BUG"


def INVALID_REFERENCE_STRING(x: Any, callsite: str = ""):
    # Always embed the reason `x` (the unsourceable concept/why) so it survives
    # into the rendered SQL; the final strict-mode guard extracts it to raise an
    # actionable error instead of a generic "this should never occur".
    if not callsite:
        return f"{BASE_INVALID}<{x}>"
    return f"{BASE_INVALID}_{callsite}<{x}>"


def extract_invalid_reference_reasons(sql: str) -> list[str]:
    """Pull the embedded reasons out of any INVALID_REFERENCE_BUG sentinels in
    rendered SQL (order-preserving, de-duplicated). Each sentinel is
    ``INVALID_REFERENCE_BUG[_callsite]<reason>``; an unadorned bare sentinel
    contributes no reason."""
    import re

    return list(dict.fromkeys(re.findall(rf"{BASE_INVALID}\w*<([^>]*)>", sql)))


def _window_over_clause(window: str, sort: str) -> str:
    if window and sort:
        return f"over (partition by {window} order by {sort} )"
    if window:
        return f"over (partition by {window})"
    if sort:
        return f"over (order by {sort} )"
    return "over ()"


def numbering_window_factory(name: str) -> Callable[[str, str], str]:
    def render(window: str, sort: str) -> str:
        return f"{name}() {_window_over_clause(window, sort)}"

    return render


def navigation_window_factory(name: str) -> Callable[[str, str, str, int | None], str]:
    def render(concept: str, window: str, sort: str, offset: int | None = None) -> str:
        if offset is not None:
            base = f"{name}({concept}, {offset})"
        else:
            base = f"{name}({concept})"
        return f"{base} {_window_over_clause(window, sort)}"

    return render


NUMBERING_WINDOW_FUNCTION_MAP: dict[WindowType, Callable[[str, str], str]] = {
    WindowType.RANK: numbering_window_factory("rank"),
    WindowType.DENSE_RANK: numbering_window_factory("dense_rank"),
    WindowType.ROW_NUMBER: numbering_window_factory("row_number"),
}

NAVIGATION_WINDOW_FUNCTION_MAP: dict[
    WindowType, Callable[[str, str, str, int | None], str]
] = {
    WindowType.LAG: navigation_window_factory("lag"),
    WindowType.LEAD: navigation_window_factory("lead"),
    WindowType.SUM: navigation_window_factory("sum"),
    WindowType.COUNT: navigation_window_factory("count"),
    WindowType.AVG: navigation_window_factory("avg"),
    WindowType.MAX: navigation_window_factory("max"),
    WindowType.MIN: navigation_window_factory("min"),
}

DATATYPE_MAP: dict[DataType, str] = {
    DataType.STRING: "string",
    DataType.BYTES: "bytes",
    DataType.INTEGER: "int",
    DataType.FLOAT: "float",
    DataType.BOOL: "bool",
    DataType.NUMERIC: "numeric",
    DataType.MAP: "map",
    DataType.DATE: "date",
    DataType.DATETIME: "datetime",
    DataType.ARRAY: "list",
}

COMPLEX_DATATYPE_MAP = {
    DataType.ARRAY: lambda x: f"{x}[]",
}

# Maps lowercased DB type names (as returned by information_schema, SQLite's
# PRAGMA table_info, or DuckDB's DESCRIBE) to DataType. `normalize_db_type`
# strips any "(...)" precision suffix before lookup. These are the cross-dialect
# standard SQL spellings, so dialects without an override (Postgres, SQLite,
# Presto, SQL Server) still resolve common columns; dialects with engine-specific
# spellings extend or override this map.
DB_COLUMN_TYPE_MAP: dict[str, DataType] = {
    # integer
    "int": DataType.INTEGER,
    "integer": DataType.INTEGER,
    "int4": DataType.INTEGER,
    "int2": DataType.INTEGER,
    "smallint": DataType.INTEGER,
    "tinyint": DataType.INTEGER,
    "mediumint": DataType.INTEGER,
    "signed": DataType.INTEGER,
    # bigint
    "bigint": DataType.BIGINT,
    "int8": DataType.BIGINT,
    "long": DataType.BIGINT,
    # float
    "float": DataType.FLOAT,
    "float4": DataType.FLOAT,
    "float8": DataType.FLOAT,
    "real": DataType.FLOAT,
    "double": DataType.FLOAT,
    "double precision": DataType.FLOAT,
    # numeric / decimal
    "numeric": DataType.NUMERIC,
    "decimal": DataType.NUMERIC,
    "money": DataType.NUMERIC,
    # string
    "string": DataType.STRING,
    "char": DataType.STRING,
    "nchar": DataType.STRING,
    "varchar": DataType.STRING,
    "nvarchar": DataType.STRING,
    "character": DataType.STRING,
    "character varying": DataType.STRING,
    "varying character": DataType.STRING,
    "bpchar": DataType.STRING,
    "text": DataType.STRING,
    "ntext": DataType.STRING,
    "tinytext": DataType.STRING,
    "mediumtext": DataType.STRING,
    "longtext": DataType.STRING,
    "clob": DataType.STRING,
    "uuid": DataType.STRING,
    "uniqueidentifier": DataType.STRING,
    # boolean
    "bool": DataType.BOOL,
    "boolean": DataType.BOOL,
    "bit": DataType.BOOL,
    # bytes
    "bytes": DataType.BYTES,
    "blob": DataType.BYTES,
    "bytea": DataType.BYTES,
    "binary": DataType.BYTES,
    "varbinary": DataType.BYTES,
    # date / time
    "date": DataType.DATE,
    "datetime": DataType.DATETIME,
    "datetime2": DataType.DATETIME,
    "smalldatetime": DataType.DATETIME,
    "timestamp": DataType.TIMESTAMP,
    "timestamp without time zone": DataType.DATETIME,
    "timestamp with time zone": DataType.TIMESTAMP,
    "timestamptz": DataType.TIMESTAMP,
    "datetimeoffset": DataType.TIMESTAMP,
    # complex
    "map": DataType.MAP,
    "array": DataType.ARRAY,
    "list": DataType.ARRAY,
}


def render_case(args):
    return "CASE\n\t" + "\n\t".join(args) + "\n\tEND"


def render_simple_case(args):
    output_args = []
    for arg in args[1:]:
        if arg.strip().startswith("ELSE"):
            output_args.append(arg)
        else:
            output_args.append(f"WHEN {args[0]} = {arg}")
    return "CASE\n\t" + "\n\t".join(output_args) + "\n\tEND"


def struct_arg(args):
    return [f"{x[1]}: {x[0]}" for x in zip(args[::2], args[1::2])]


def hash_from_args(val, hash_type):
    hash_type = hash_type[1:-1]
    if hash_type.lower() == "md5":
        return f"md5({val})"
    elif hash_type.lower() == "sha1":
        return f"sha1({val})"
    elif hash_type.lower() == "sha256":
        return f"sha256({val})"
    elif hash_type.lower() == "sha512":
        return f"sha512({val})"
    else:
        raise ValueError(f"Unsupported hash type: {hash_type}")


FUNCTION_MAP = {
    # generic types
    FunctionType.ALIAS: lambda x, types: f"{x[0]}",
    FunctionType.NOOP: lambda x, types: f"{x[0]}",
    FunctionType.GROUP: lambda x, types: f"{x[0]}",
    FunctionType.CONSTANT: lambda x, types: f"{x[0]}",
    FunctionType.TYPED_CONSTANT: lambda x, types: f"{x[0]}",
    FunctionType.COALESCE: lambda x, types: f"coalesce({','.join(x)})",
    FunctionType.GREATEST: lambda x, types: f"greatest({','.join(x)})",
    FunctionType.LEAST: lambda x, types: f"least({','.join(x)})",
    FunctionType.NULLIF: lambda x, types: f"nullif({x[0]},{x[1]})",
    FunctionType.CAST: lambda x, types: f"cast({x[0]} as {x[1]})",
    FunctionType.CASE: lambda x, types: render_case(x),
    FunctionType.SIMPLE_CASE: lambda x, types: render_simple_case(x),
    FunctionType.SPLIT: lambda x, types: f"split({x[0]}, {x[1]})",
    FunctionType.IS_NULL: lambda x, types: f"{x[0]} is null",
    FunctionType.BOOL: lambda x, types: f"CASE WHEN {x[0]} THEN TRUE ELSE FALSE END",
    FunctionType.PARENTHETICAL: lambda x, types: f"({x[0]})",
    # Complex
    FunctionType.INDEX_ACCESS: lambda x, types: f"{x[0]}[{x[1]}]",
    FunctionType.MAP_ACCESS: lambda x, types: f"{x[0]}[{x[1]}]",
    FunctionType.UNNEST: lambda x, types: f"unnest({x[0]})",
    FunctionType.DATE_SPINE: lambda x, types: f"""unnest(
        generate_series(
            {x[0]},
            {x[1]},
            INTERVAL '1 day'
        )
    )""",
    FunctionType.RECURSE_EDGE: lambda x, types: f"CASE WHEN {x[1]} IS NULL THEN {x[0]} ELSE {x[1]} END",
    FunctionType.ATTR_ACCESS: lambda x, types: f"""{x[0]}.{x[1].replace("'", "")}""",
    FunctionType.STRUCT: lambda x, types: f"{{{', '.join(struct_arg(x))}}}",
    FunctionType.ARRAY: lambda x, types: f"[{', '.join(x)}]",
    FunctionType.ROW_TUPLE: lambda x, types: f"({', '.join(x)})",
    FunctionType.DATE_LITERAL: lambda x, types: f"date '{x}'",
    FunctionType.DATETIME_LITERAL: lambda x, types: f"datetime '{x}'",
    # MAP
    FunctionType.MAP_KEYS: lambda x, types: f"map_keys({x[0]})",
    FunctionType.MAP_VALUES: lambda x, types: f"map_values({x[0]})",
    # ARRAY
    FunctionType.GENERATE_ARRAY: lambda x, types: f"generate_series({x[0]}, {x[1]}, {x[2]})",
    FunctionType.ARRAY_SUM: lambda x, types: f"array_sum({x[0]})",
    FunctionType.ARRAY_DISTINCT: lambda x, types: f"array_distinct({x[0]})",
    FunctionType.ARRAY_SORT: lambda x, types: f"array_sort({x[0]})",
    FunctionType.ARRAY_TRANSFORM: lambda args, types: (
        f"array_transform({args[0]}, {args[1]} -> {args[2]})"
    ),
    FunctionType.ARRAY_TO_STRING: lambda args, types: (
        f"array_to_string({args[0]}, {args[1]})"
    ),
    FunctionType.ARRAY_FILTER: lambda args, types: (
        f"array_filter({args[0]}, {args[1]} -> {args[2]})"
    ),
    # math
    FunctionType.ADD: lambda x, types: " + ".join(x),
    FunctionType.ABS: lambda x, types: f"abs({x[0]})",
    FunctionType.SUBTRACT: lambda x, types: " - ".join(x),
    FunctionType.DIVIDE: lambda x, types: " / ".join(x),
    FunctionType.MULTIPLY: lambda x, types: " * ".join(x),
    FunctionType.ROUND: lambda x, types: f"round({x[0]},{x[1]})",
    FunctionType.FLOOR: lambda x, types: f"floor({x[0]})",
    FunctionType.CEIL: lambda x, types: f"ceil({x[0]})",
    FunctionType.MOD: lambda x, types: f"{x[0]} % {x[1]}",
    FunctionType.POWER: lambda x, types: f"{x[0]} ** {x[1]}",
    FunctionType.SQRT: lambda x, types: f"sqrt({x[0]})",
    FunctionType.RANDOM: lambda x, types: "random()",
    FunctionType.LOG: lambda x, types: (
        f"log({x[0]})" if x[1] == 10 else f"log({x[0]}, {x[1]})"
    ),
    # aggregate types
    FunctionType.COUNT_DISTINCT: lambda x, types: f"count(distinct {x[0]})",
    FunctionType.COUNT: lambda x, types: f"count({x[0]})",
    FunctionType.GROUPING: lambda x, types: f"grouping({','.join(x)})",
    FunctionType.GROUPING_ID: lambda x, types: f"grouping_id({','.join(x)})",
    FunctionType.SUM: lambda x, types: f"sum({x[0]})",
    FunctionType.ARRAY_AGG: lambda x, types: f"array_agg({x[0]})",
    FunctionType.LENGTH: lambda x, types: f"length({x[0]})",
    FunctionType.AVG: lambda x, types: f"avg({x[0]})",
    FunctionType.STDDEV: lambda x, types: f"stddev_samp({x[0]})",
    FunctionType.VARIANCE: lambda x, types: f"var_samp({x[0]})",
    FunctionType.MAX: lambda x, types: f"max({x[0]})",
    FunctionType.MIN: lambda x, types: f"min({x[0]})",
    FunctionType.ANY: lambda x, types: f"any_value({x[0]})",
    FunctionType.BOOL_OR: lambda x, types: f"bool_or({x[0]})",
    FunctionType.BOOL_AND: lambda x, types: f"bool_and({x[0]})",
    # string types
    FunctionType.UPPER: lambda x, types: f"UPPER({x[0]}) ",
    FunctionType.LOWER: lambda x, types: f"LOWER({x[0]}) ",
    FunctionType.SUBSTRING: lambda x, types: f"SUBSTRING({x[0]},{x[1]},{x[2]})",
    FunctionType.STRPOS: lambda x, types: f"STRPOS({x[0]},{x[1]})",
    FunctionType.CONTAINS: lambda x, types: f"CONTAINS({x[0]},{x[1]})",
    FunctionType.REGEXP_CONTAINS: lambda x, types: f"REGEXP_CONTAINS({x[0]},{x[1]})",
    FunctionType.REGEXP_EXTRACT: lambda x, types: f"REGEXP_EXTRACT({x[0]},{x[1]})",
    FunctionType.REGEXP_REPLACE: lambda x, types: f"REGEXP_REPLACE({x[0]},{x[1]}, {x[2]})",
    FunctionType.TRIM: lambda x, types: f"TRIM({x[0]})",
    FunctionType.LTRIM: lambda x, types: f"LTRIM({x[0]})",
    FunctionType.RTRIM: lambda x, types: f"RTRIM({x[0]})",
    FunctionType.REPLACE: lambda x, types: f"REPLACE({x[0]},{x[1]},{x[2]})",
    FunctionType.HASH: lambda x, types: hash_from_args(x[0], x[1]),
    FunctionType.HEX: lambda x, types: f"HEX({x[0]})",
    # FunctionType.NOT_LIKE: lambda x: f" CASE WHEN {x[0]} like {x[1]} THEN 0 ELSE 1 END",
    # date types
    FunctionType.DATE_TRUNCATE: lambda x, types: f"date_trunc({x[0]},{x[1]})",
    FunctionType.DATE_PART: lambda x, types: f"date_part({x[0]},{x[1]})",
    FunctionType.DATE_ADD: lambda x, types: f"date_add({x[0]},{x[1]}, {x[2]})",
    FunctionType.DATE_SUB: lambda x, types: f"date_sub({x[0]},{x[1]}, {x[2]})",
    FunctionType.DATE_DIFF: lambda x, types: f"date_diff({x[0]},{x[1]}, {x[2]})",
    FunctionType.DATE: lambda x, types: f"date({x[0]})",
    FunctionType.DATETIME: lambda x, types: f"datetime({x[0]})",
    FunctionType.TIMESTAMP: lambda x, types: f"timestamp({x[0]})",
    FunctionType.SECOND: lambda x, types: f"second({x[0]})",
    FunctionType.MINUTE: lambda x, types: f"minute({x[0]})",
    FunctionType.HOUR: lambda x, types: f"hour({x[0]})",
    FunctionType.DAY: lambda x, types: f"day({x[0]})",
    FunctionType.DAY_NAME: lambda x, types: f"dayname({x[0]})",
    FunctionType.DAY_OF_WEEK: lambda x, types: f"day_of_week({x[0]})",
    FunctionType.WEEK: lambda x, types: f"week({x[0]})",
    FunctionType.MONTH: lambda x, types: f"month({x[0]})",
    FunctionType.MONTH_NAME: lambda x, types: f"monthname({x[0]})",
    FunctionType.FORMAT_TIME: lambda x, types: f"strftime({x[0]}, {x[1]})",
    FunctionType.PARSE_TIME: lambda x, types: f"strptime({x[0]}, {x[1]})",
    FunctionType.QUARTER: lambda x, types: f"quarter({x[0]})",
    FunctionType.YEAR: lambda x, types: f"year({x[0]})",
    FunctionType.GEO_FROM_TEXT: lambda x, types: f"ST_GeomFromText({x[0]})",
    FunctionType.GEO_POINT: lambda x, types: f"ST_Point({x[0]},{x[1]})",
    FunctionType.GEO_DISTANCE: lambda x, types: f"ST_Distance({x[0]},{x[1]})",
    FunctionType.GEO_X: lambda x, types: f"ST_X({x[0]})",
    FunctionType.GEO_Y: lambda x, types: f"ST_Y({x[0]})",
    FunctionType.GEO_CENTROID: lambda x, types: f"ST_Centroid({x[0]})",
    FunctionType.GEO_TRANSFORM: lambda x, types: f"ST_Transform({x[0]}, {x[1]}, {x[2]})",
    # string types
    FunctionType.CONCAT: lambda x, types: f"concat({','.join(x)})",
    # constant types
    FunctionType.CURRENT_DATE: lambda x, types: "current_date()",
    FunctionType.CURRENT_DATETIME: lambda x, types: "current_datetime()",
    FunctionType.CURRENT_TIMESTAMP: lambda x, types: "current_timestamp()",
}

FUNCTION_GRAIN_MATCH_MAP = {
    **FUNCTION_MAP,
    FunctionType.COUNT_DISTINCT: lambda args, types: f"CASE WHEN{args[0]} IS NOT NULL THEN 1 ELSE 0 END",
    FunctionType.COUNT: lambda args, types: f"CASE WHEN {args[0]} IS NOT NULL THEN 1 ELSE 0 END",
    FunctionType.SUM: lambda args, types: f"{args[0]}",
    FunctionType.AVG: lambda args, types: f"{args[0]}",
    FunctionType.MAX: lambda args, types: f"{args[0]}",
    FunctionType.MIN: lambda args, types: f"{args[0]}",
    FunctionType.ANY: lambda args, types: f"{args[0]}",
}


GENERIC_SQL_TEMPLATE: Template = Template("""{%- if ctes %}
WITH {% if recursive%} RECURSIVE {% endif %}{% for cte in ctes %}
{{cte.name}} as (
{{cte.statement}}){% if not loop.last %},{% endif %}{% endfor %}{% endif %}
{%- if full_select -%}
{{full_select}}
{% else -%}
SELECT
{%- if limit is not none %}
TOP {{ limit }}{% endif %}
{%- for select in select_columns %}
\t{{ select }}{% if not loop.last %},{% endif %}{% endfor %}
{% if base %}FROM
\t{{ base }}{% endif %}{% if joins %}{% for join in joins %}
\t{{ join }}{% endfor %}{% endif %}{% if where %}
WHERE
\t{{ where }}{% endif %}{%- if group_by %}
GROUP BY {% for group in group_by %}
\t{{group}}{% if not loop.last %},{% endif %}{% endfor %}{% endif %}{% if having %}
HAVING
\t{{ having }}{% endif %}{%- if order_by %}
ORDER BY{% for order in order_by %}
\t{{ order }}{% if not loop.last %},{% endif %}{% endfor %}
{% endif %}{% endif %}
""")


CREATE_TABLE_SQL_TEMPLATE = Template("""
CREATE {% if create_mode == "create_or_replace" %}OR REPLACE TABLE{% elif create_mode == "create_if_not_exists" %}TABLE IF NOT EXISTS{% else %}TABLE{% endif %} {{ name }} (
{%- for column in columns %}
    {{ column.name }} {{ type_map[column.name] }}{% if column.comment %} COMMENT '{{ column.comment }}'{% endif %}{% if not loop.last %},{% endif %}
{%- endfor %}
)
{%- if partition_keys %}
PARTITIONED BY (
{%- for partition_key in partition_keys %}
    {{ partition_key }}{% if not loop.last %},{% endif %}
{%- endfor %}
)
{%- endif %};
""".strip())


def safe_get_cte_value(
    coalesce: Callable,
    cte: CTE | UnionCTE,
    c: BuildConcept,
    quote_char: str,
    render_expr: Callable,
    use_map: dict[str, set[str]],
) -> Optional[str]:
    address = c.address
    raw = cte.source_map.get(address, None)

    def _format(source: str, rendered) -> str:
        if isinstance(rendered, RawColumnExpr):
            return rendered.text
        if isinstance(rendered, FUNCTION_ITEMS):
            return f"{render_expr(rendered, cte=cte, raise_invalid=True)}"
        # Translate the source_map token to the actual SQL alias: identity for
        # a normal CTE, the raw-table alias for an inlined DatasourceCTE.
        alias = cte.source_key_for(source) if isinstance(cte, CTE) else source
        return f"{quote_char}{alias}{quote_char}.{safe_quote(rendered, quote_char)}"

    if not raw:
        # No explicit source, but an inlined datasource may still expose this
        # concept as a raw column (not in its pruned projection).
        if isinstance(cte, CTE):
            inlined = cte.inlined_parent_providing(c)
            if inlined is not None:
                use_map[inlined.name].add(c.address)
                return _format(inlined.name, inlined.consumer_column(c))
        return None

    if isinstance(raw, str):
        rendered = cte.get_alias(c, raw)
        use_map[raw].add(c.address)
        return _format(raw, rendered)
    if isinstance(raw, list) and len(raw) == 1:
        rendered = cte.get_alias(c, raw[0])
        use_map[raw[0]].add(c.address)
        return _format(raw[0], rendered)
    for x in raw:
        use_map[x].add(c.address)
    return coalesce(
        sorted([_format(x, cte.get_alias(c, x)) for x in raw]),
        [],
    )


def get_grouped_aggregate_wrapper(
    concept: BuildConcept,
) -> BuildAggregateWrapper | None:
    lineage = concept.lineage
    if isinstance(lineage, BuildAggregateWrapper):
        return lineage
    if isinstance(lineage, BuildRowsetItem) and isinstance(
        lineage.content.lineage, BuildAggregateWrapper
    ):
        return lineage.content.lineage
    return None


class BaseDialect:
    NUMBERING_WINDOW_FUNCTION_MAP = NUMBERING_WINDOW_FUNCTION_MAP
    NAVIGATION_WINDOW_FUNCTION_MAP = NAVIGATION_WINDOW_FUNCTION_MAP
    FUNCTION_MAP = FUNCTION_MAP
    FUNCTION_GRAIN_MATCH_MAP = FUNCTION_GRAIN_MATCH_MAP
    QUOTE_CHARACTER = "`"
    SQL_TEMPLATE = GENERIC_SQL_TEMPLATE
    CREATE_TABLE_SQL_TEMPLATE = CREATE_TABLE_SQL_TEMPLATE
    DATATYPE_MAP = DATATYPE_MAP
    COMPLEX_DATATYPE_MAP = COMPLEX_DATATYPE_MAP
    DB_COLUMN_TYPE_MAP = DB_COLUMN_TYPE_MAP
    UNNEST_MODE = UnnestMode.CROSS_APPLY
    GROUP_MODE = GroupMode.AUTO
    SUPPORTS_AGGREGATE_GROUPING_MODES = False
    # Whether HAVING can reference a SELECT-list output alias. True for
    # DuckDB/BigQuery/Snowflake/MySQL; false for standard SQL (Postgres, MSSQL).
    # Gates the aggregate-predicate -> HAVING pushdown and lets that HAVING
    # render by alias instead of re-inlining the aggregate expression.
    SUPPORTS_ALIAS_IN_HAVING = False
    # Whether the dialect supports a QUALIFY clause, used to lower a window
    # function appearing in a `having` condition. False dialects reject instead.
    SUPPORTS_QUALIFY = False
    # Whether this dialect can produce a full-result summary — per-column stats
    # over the query with its output LIMIT removed. Off by default; gates whether
    # `run` returns it. Dialects that set it True must override
    # ``summarize_result``.
    SUPPORTS_RESULT_SUMMARY = False
    EXPLAIN_KEYWORD = "EXPLAIN"
    NULL_WRAPPER = staticmethod(null_wrapper)
    ALIAS_ORDER_REFERENCING_ALLOWED = True
    TABLE_NOT_FOUND_PATTERN: str | None = None  # Dialect-specific pattern to match
    HTTP_NOT_FOUND_PATTERN: str | None = None  # Pattern for HTTP 404 errors (e.g., GCS)
    COLUMN_NOT_FOUND_PATTERN: str | None = None  # Pattern for column-not-found errors

    def render_map_literal(
        self,
        e: "MapWrapper[Any, Any]",
        cte: Optional["CTE | UnionCTE"] = None,
        cte_map: Optional[Dict[str, "CTE | UnionCTE"]] = None,
        raise_invalid: bool = False,
    ) -> str:
        # Default DuckDB-style; CH and others override.
        items = ",".join(
            f"{self.render_expr(k, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)}"
            f":{self.render_expr(v, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)}"
            for k, v in e.items()
        )
        return f"MAP {{{items}}}"

    def __init__(
        self,
        rendering: Rendering | None = None,
        config: "DialectConfig | None" = None,
    ):
        self.rendering = rendering or CONFIG.rendering
        self.config = config
        self.used_map: dict[str, set[str]] = defaultdict(set)

    def render_source(self, address: Address) -> str:
        if address.type == AddressType.QUERY:
            return f"({address.location})"
        if address.is_file:
            if address.type == AddressType.SQL:
                with safe_open(address.location) as f:
                    return f"({f.read()})"
            return f"'{address.location}'"
        return self.safe_quote(address.location)

    def make_table_column(
        self,
        column_name: str,
        raw_db_type: str,
        is_nullable: bool = True,
        comment: str | None = None,
    ) -> TableColumn:
        """Build a TableColumn, resolving ``trilogy_type`` via the dialect map."""
        return TableColumn(
            column_name=column_name,
            raw_db_type=raw_db_type,
            trilogy_type=self.normalize_db_type(raw_db_type),
            is_nullable=is_nullable,
            comment=comment,
        )

    def _columns_from_info_schema_rows(self, rows) -> list[TableColumn]:
        """Map (column_name, data_type, is_nullable, comment) rows to TableColumns."""
        return [
            self.make_table_column(r[0], r[1], nullable_from_str(r[2]), r[3])
            for r in rows
        ]

    def get_table_schema(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[TableColumn]:
        """Return per-column schema info via information_schema."""
        query = f"""
        SELECT
            column_name,
            data_type,
            is_nullable,
            '' as column_comment
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        """
        if schema:
            query += f" AND table_schema = '{schema}'"
        query += " ORDER BY ordinal_position"
        return self._columns_from_info_schema_rows(
            executor.execute_raw_sql(query).fetchall()
        )

    def normalize_db_type(self, db_type: str) -> DataType:
        """Map a database type string (from information_schema) to a DataType enum."""
        key = db_type.lower().split("(")[0].strip()
        return self.DB_COLUMN_TYPE_MAP.get(key, DataType.UNKNOWN)

    def get_table_columns(
        self, executor, table_name: str, schema: str | None = None
    ) -> dict[str, DataType] | None:
        """Return a name→DataType mapping for an existing table, or None if not found.

        An empty result is treated as table-not-found since all real tables have at
        least one column.
        """
        rows = self.get_table_schema(executor, table_name, schema)
        if not rows:
            return None
        return {row.column_name.lower(): row.trilogy_type for row in rows}

    def list_tables(self, executor, schema: str | None = None) -> list[tuple[str, str]]:
        """Return (table_name, table_type) for tables and views via
        information_schema. System schemas are excluded unless ``schema`` is
        passed explicitly."""
        query = "SELECT table_name, table_type FROM information_schema.tables"
        if schema:
            query += f" WHERE table_schema = '{schema}'"
        else:
            query += (
                " WHERE table_schema NOT IN "
                "('information_schema', 'pg_catalog', 'pg_toast', 'system')"
            )
        query += " ORDER BY table_name"
        return [(r[0], r[1]) for r in executor.execute_raw_sql(query).fetchall()]

    def refine_runtime_value_type_for_validation(
        self,
        executor,
        value: Any,
        inferred_type: CONCRETE_TYPES,
        expected_type: CONCRETE_TYPES,
        result_type: CONCRETE_TYPES | None = None,
    ) -> CONCRETE_TYPES:
        return inferred_type

    def get_result_column_types_for_validation(
        self, result: Any
    ) -> dict[str, CONCRETE_TYPES] | None:
        return None

    def get_table_primary_keys(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[str]:
        raise NotImplementedError

    def get_table_sample(
        self,
        executor,
        table_name: str,
        schema: str | None = None,
        sample_size: int = 10000,
    ) -> list[tuple]:
        if schema:
            qualified_name = f"{schema}.{table_name}"
        else:
            qualified_name = table_name

        sample_query = (
            f"SELECT * FROM {self.safe_quote(qualified_name)} LIMIT {sample_size}"
        )
        rows = executor.execute_raw_sql(sample_query).fetchall()
        return rows

    def get_table_last_modified(
        self, executor, table_name: str, schema: str | None = None
    ) -> str | None:
        from datetime import datetime, timezone

        return datetime.now(timezone.utc).isoformat()

    def hash_column_value(self, column_name: str) -> str:
        return f"md5(CAST({self.safe_quote(column_name)} AS VARCHAR))"

    def aggregate_checksum(self, hash_expr: str) -> str:
        return f"BIT_XOR(hash({hash_expr}))"

    def render_order_item(
        self,
        order_item: BuildOrderItem,
        cte: CTE | UnionCTE,
    ) -> str:
        # check if it's in our output select projection
        # and we can just reference by there directly and save
        # on re-expression (smaller output query)
        if (
            isinstance(order_item.expr, BuildConcept)
            and order_item.expr.address in cte.output_columns
            and order_item.expr.address not in cte.hidden_concepts
            and self.ALIAS_ORDER_REFERENCING_ALLOWED
        ):
            if cte.source_map.get(order_item.expr.address, []):
                # if it is sourced from somewhere, we need to reference the alias directly
                return f"{self.render_expr(order_item.expr, cte=cte, )} {order_item.order.value}"
            # otherwise we've derived it, safe to use alias
            return f"{self.QUOTE_CHARACTER}{order_item.expr.safe_address}{self.QUOTE_CHARACTER} {order_item.order.value}"
        return (
            f"{self.render_expr(order_item.expr, cte=cte, )} {order_item.order.value}"
        )

    def _canonical_render_siblings(
        self, c: BuildConcept, cte: CTE | UnionCTE
    ) -> list[BuildConcept]:
        """Concepts in this CTE sharing ``c``'s canonical lineage but bound under
        a different address. A concept materialized-as-root (lineage stripped
        because its canonical lineage is precomputed) has no source_map entry of
        its own when the plan binds a canonical sibling instead — e.g. inline
        ``year(flight_date)`` selected alongside the named auto ``flight_year``.
        The sibling produces the same SQL expression, so rendering through it
        satisfies ``c``. A canonically-equivalent concept under the *same*
        address but carrying lineage (the un-stripped duplicate) also qualifies."""
        siblings: list[BuildConcept] = []
        for source in (cte.output_columns, cte.source.output_concepts):
            for other in source:
                if other is c or any(other is s for s in siblings):
                    continue
                if other.canonical_address != c.canonical_address:
                    continue
                if other.lineage is None and not cte.source_map.get(other.address, []):
                    continue
                siblings.append(other)
        return siblings

    def _grain_key_membership_redirect(
        self, c: BuildConcept, cte: CTE | UnionCTE
    ) -> str | None:
        """Borrow the renderable column of a grain-key semijoin's left operand.

        A post-aggregation grain-key membership (`key in (filter key where ...)`,
        built by `_rewrite_having_finer_dims_to_membership`) filters exactly the
        CTE's grain key, so its left operand IS that grain key's materialized
        column. When a scoped INNER join collapsed the grain key onto a twin and
        the projected grain-key output dead-ends on the dropped side (q64), the
        membership-left is the same logical key and renders fine here — use it.

        Tightly gated: only a single-component-grain CTE whose membership-left is
        itself a single same-grain key, and only as a fallback when the normal
        render already produced the INVALID_REFERENCE sentinel."""
        if not isinstance(cte, CTE) or cte.condition is None:
            return None
        grain = cte.grain.components
        if len(grain) != 1 or c.address not in grain:
            return None

        def _memberships(node: Any) -> list[BuildSubselectComparison]:
            if isinstance(node, BuildSubselectComparison):
                return [node]
            acc: list[BuildSubselectComparison] = []
            for attr in ("left", "right", "content"):
                child = getattr(node, attr, None)
                if child is not None:
                    acc += _memberships(child)
            return acc

        for comp in _memberships(cte.condition):
            if comp.operator not in (
                ComparisonOperator.IN,
                ComparisonOperator.NOT_IN,
            ):
                continue
            left = comp.left
            # Existence-set semijoin shape (both operands single same-grain
            # concepts): a grain-key membership, not a literal value-list `in
            # (1, 2)`. Only such a semijoin guarantees the left IS the grain key.
            if not isinstance(left, BuildConcept) or len(left.grain.components) != 1:
                continue
            if not isinstance(comp.right, BuildConcept):
                continue
            try:
                rendered = self._render_concept_sql(left, cte, raise_invalid=True)
            except ValueError:
                continue
            if rendered and BASE_INVALID not in rendered:
                return rendered
        return None

    def render_concept_sql(
        self,
        c: BuildConcept,
        cte: CTE | UnionCTE,
        alias: bool = True,
        raise_invalid: bool = False,
    ) -> str:
        result = None
        if not isinstance(c, BuildConcept):
            raise SyntaxError(f"Expected BuildConcept, got {type(c)} {c}")
        candidates: list[BuildConcept] = []
        if c.pseudonyms:
            candidates += [y for y in [cte.get_concept(x) for x in c.pseudonyms] if y]
            for candidate in list(candidates):
                candidates += [
                    y
                    for y in [cte.get_concept(x) for x in candidate.pseudonyms]
                    if y and y.address != c.address and y not in candidates
                ]
        if c.lineage is None and not cte.source_map.get(c.address, []):
            candidates += self._canonical_render_siblings(c, cte)
        if candidates:
            logger.debug(
                f"{LOGGER_PREFIX} [{c.address}] render candidates are {[x.address for x in candidates]}"
            )
            for candidate in [c] + candidates:
                try:
                    logger.debug(
                        f"{LOGGER_PREFIX} [{c.address}] Attempting rendering w/ candidate {candidate.address}"
                    )
                    result = self._render_concept_sql(
                        candidate,
                        cte,
                        raise_invalid=True,
                    )
                    if result:
                        break
                except ValueError:
                    continue
        if not result:
            result = self._render_concept_sql(
                c,
                cte,
                raise_invalid=raise_invalid,
            )
        if result is not None and BASE_INVALID in result:
            redirect = self._grain_key_membership_redirect(c, cte)
            if redirect is not None:
                result = redirect
        if alias:
            return f"{result} as {self.QUOTE_CHARACTER}{c.safe_address}{self.QUOTE_CHARACTER}"
        return result

    def _render_concept_sql(
        self,
        c: BuildConcept,
        cte: CTE | UnionCTE,
        raise_invalid: bool = False,
    ) -> str:
        # only recurse while it's in sources of the current cte
        logger.debug(
            f"{LOGGER_PREFIX} [{c.address}] Starting rendering loop on cte: {cte.name}"
        )
        if cte.group_to_grain and c.address in {
            concept.address for concept in cte.rollup_concepts
        }:
            rolled = safe_get_cte_value(
                self.FUNCTION_MAP[FunctionType.COALESCE],
                cte,
                c,
                self.QUOTE_CHARACTER,
                self.render_expr,
                self.used_map,
            )
            if not rolled:
                rolled = INVALID_REFERENCE_STRING(
                    f"Missing rollup source reference to {c.address}"
                )
            return self.FUNCTION_MAP[FunctionType.SUM]([rolled], [])

        # check if it's not inherited AND no pseudonyms are inherited
        if c.lineage and cte.source_map.get(c.address, []) == []:
            logger.debug(
                f"{LOGGER_PREFIX} [{c.address}] rendering concept with lineage that is not already existing"
            )
            if isinstance(c.lineage, WINDOW_ITEMS):
                rendered_order_components = [
                    f"{self.render_expr(x.expr, cte, raise_invalid=raise_invalid)} {x.order.value}"
                    for x in c.lineage.order_by
                ]
                rendered_over_components = [
                    self.render_concept_sql(
                        x, cte, alias=False, raise_invalid=raise_invalid
                    )
                    for x in c.lineage.over
                ]
                window_str = ",".join(rendered_over_components)
                sort_str = ",".join(rendered_order_components)
                rval: str | None
                if isinstance(c.lineage, BuildNumberingWindowItem):
                    rval = self.NUMBERING_WINDOW_FUNCTION_MAP[c.lineage.type](
                        window_str, sort_str
                    )
                else:
                    rval = self.NAVIGATION_WINDOW_FUNCTION_MAP[c.lineage.type](
                        self.render_expr(
                            c.lineage.content,
                            cte=cte,
                            raise_invalid=raise_invalid,
                        ),
                        window_str,
                        sort_str,
                        c.lineage.offset,
                    )
            elif isinstance(c.lineage, FILTER_ITEMS):
                # When the CTE's WHERE already restricts rows to those satisfying
                # the filter's predicate (either exact match or a superset that
                # implies it), the per-row CASE WHEN is redundant — emit just
                # the content.
                if cte.condition is not None and (
                    cte.condition == c.lineage.where.conditional
                    or condition_implies(cte.condition, c.lineage.where.conditional)
                ):
                    rval = self.render_expr(
                        c.lineage.content, cte=cte, raise_invalid=raise_invalid
                    )
                else:
                    rval = f"CASE WHEN {self.render_expr(c.lineage.where.conditional, cte=cte)} THEN {self.render_expr(c.lineage.content, cte=cte, raise_invalid=raise_invalid)} ELSE NULL END"
            elif isinstance(c.lineage, BuildRowsetItem):
                rval = f"{self.render_concept_sql(c.lineage.content, cte=cte, alias=False, raise_invalid=raise_invalid)}"
            elif isinstance(c.lineage, SUBSELECT_ITEMS):
                rval = self._render_subselect(c, cte, raise_invalid=raise_invalid)
            elif isinstance(c.lineage, BuildMultiSelectLineage):
                if c.address in c.lineage.calculated_derivations:
                    assert c.lineage.derive is not None
                    for x in c.lineage.derive.items:
                        if x.address == c.address:
                            rval = self.render_expr(
                                x.expr,
                                cte=cte,
                                raise_invalid=raise_invalid,
                            )
                            break
                else:
                    rval = f"{self.render_concept_sql(c.lineage.find_source(c, cte), cte=cte, alias=False, raise_invalid=raise_invalid)}"
            elif isinstance(c.lineage, SUBSELECT_COMPARISON_ITEMS):
                # A named/projected membership (`auto flag <- a in b`) renders
                # via render_expr so its RHS becomes an existence subquery
                # rather than a bare (unjoinable) column reference. Parenthesize
                # for the same precedence reason as BuildComparison below.
                rval = f"({self.render_expr(c.lineage, cte=cte, raise_invalid=raise_invalid)})"
            elif isinstance(c.lineage, BuildComparison):
                # Route through render_comparison so dialect operator overrides
                # apply (e.g. SQLite has no native ILIKE). Parenthesize: an
                # inlined boolean comparison may itself become the operand of
                # another comparison (e.g. `flag = true` over `flag <- x > 5`),
                # and `x > 5 = true` is a SQL precedence error.
                rval = f"({self.render_comparison(c.lineage.left, c.lineage.right, c.lineage.operator, cte=cte, raise_invalid=raise_invalid)})"
            elif isinstance(c.lineage, (*CONDITIONAL_ITEMS, *BETWEEN_ITEMS)):
                # A named boolean predicate (`auto x <- a and b`, `... between ...`)
                # renders as its full expression; parenthesize for the same
                # precedence reason as BuildComparison above.
                rval = f"({self.render_expr(c.lineage, cte=cte, raise_invalid=raise_invalid)})"
            elif isinstance(c.lineage, AGGREGATE_ITEMS):
                args = [
                    self.render_expr(v, cte)  # , alias=False)
                    for v in c.lineage.function.arguments
                ]
                if cte.group_to_grain:
                    rval = self.FUNCTION_MAP[c.lineage.function.operator](args, [])
                else:
                    # EXPERIMENT: guard removed, unconditional collapse
                    rval = f"{self.FUNCTION_GRAIN_MATCH_MAP[c.lineage.function.operator](args, [])}"
            elif (
                isinstance(c.lineage, FUNCTION_ITEMS)
                and c.lineage.operator == FunctionType.UNION
            ):
                local_matched = [
                    x
                    for x in c.lineage.arguments
                    if isinstance(x, BuildConcept) and x.address in cte.output_columns
                ]
                # if we're sorting by the output of the union
                if not local_matched:
                    rval = c.safe_address
                else:
                    rval = self.render_expr(local_matched[0], cte)
            elif (
                isinstance(c.lineage, FUNCTION_ITEMS)
                and c.lineage.operator == FunctionType.CONSTANT
                and self.rendering.parameters is True
                and c.datatype.data_type != DataType.MAP
                and c.datatype.data_type not in INLINE_SAFE_PARAM_DATATYPES
                and _constant_bindable(c.lineage)
            ):
                rval = f":{c.safe_address}"
            else:
                args = []
                types = []
                for arg in c.lineage.arguments:
                    if _needs_arithmetic_parentheses(arg, c.lineage.operator):
                        args.append(
                            self.render_expr(
                                BuildParenthetical(content=cast(BuildExpr, arg)),
                                cte=cte,
                                raise_invalid=raise_invalid,
                            )
                        )
                    else:
                        args.append(
                            self.render_expr(arg, cte=cte, raise_invalid=raise_invalid)
                        )
                    types.append(arg_to_datatype(arg))

                if cte.group_to_grain:
                    rval = f"{self.FUNCTION_MAP[c.lineage.operator](args, types)}"
                else:

                    rval = f"{self.FUNCTION_GRAIN_MATCH_MAP[c.lineage.operator](args, types)}"
        else:
            logger.debug(
                f"{LOGGER_PREFIX} [{c.address}] Rendering basic lookup from {cte.source_map.get(c.address,None)}"
            )

            parent = cte.source_map.get(c.address, None)
            has_multiple_sources = isinstance(parent, list) and len(parent) > 1
            raw_content = None if has_multiple_sources else cte.get_alias(c)
            if parent and not has_multiple_sources:
                self.used_map[parent[0]].add(c.address)
            if isinstance(raw_content, RawColumnExpr):
                rval = raw_content.text
            elif isinstance(raw_content, FUNCTION_ITEMS):
                rval = self.render_expr(
                    raw_content, cte=cte, raise_invalid=raise_invalid
                )
            else:
                rval = safe_get_cte_value(
                    self.FUNCTION_MAP[FunctionType.COALESCE],
                    cte,
                    c,
                    self.QUOTE_CHARACTER,
                    self.render_expr,
                    self.used_map,
                )
                if not rval:
                    # unions won't have a specific source mapped; just use a generic column reference
                    # we shouldn't ever have an expression at this point, so will be safe
                    if isinstance(cte, UnionCTE):
                        rval = c.safe_address
                    else:
                        if raise_invalid:
                            raise ValueError(
                                f"Invalid reference string found in query: {rval}, this should never occur. Please report this issue."
                            )
                        rval = INVALID_REFERENCE_STRING(
                            f"Missing source reference to {c.address}"
                        )
        # Pre-aggregated COUNT columns sourced from a sparse materialization
        # leak NULL through a LEFT/FULL JOIN when a dim row has no matching
        # fact row. The granular path's `count(...)` returns 0 in that case
        # (count over an empty group is 0). Coalesce to keep the two paths
        # result-equivalent. SUM is left alone — `SUM` over an empty group
        # is NULL in both paths.
        if (
            isinstance(c.lineage, BuildAggregateWrapper)
            and c.lineage.function.operator == FunctionType.COUNT
            and not cte.group_to_grain
            and isinstance(cte, CTE)
            and any(n.address == c.address for n in cte.nullable_concepts)
            # A multiselect-align merge CTE is the exception: a NULL count there
            # means "this entity is absent from this arm", not "0 facts", and
            # must stay NULL so a cross-arm comparison (q64 `cnt_00 <= cnt_99`)
            # excludes single-arm rows. Coalescing to 0 would let them through.
            and not any(
                isinstance(o.lineage, BuildMultiSelectLineage)
                for o in cte.output_columns
            )
        ):
            rval = self.FUNCTION_MAP[FunctionType.COALESCE]([rval, "0"], [])
        assert rval is not None
        return rval

    def _render_subselect(
        self,
        c: BuildConcept,
        cte: CTE | UnionCTE,
        raise_invalid: bool = False,
    ) -> str:
        lineage: BuildSubselectItem = c.lineage  # type: ignore
        q = self.QUOTE_CHARACTER
        content_sources = cte.source_map.get(lineage.content.address, [])
        if not content_sources:
            return INVALID_REFERENCE_STRING(
                f"Missing subselect source for {lineage.content.address}"
            )
        source_cte = content_sources[0]
        # Track that the content concept is used from its source CTE
        self.used_map[source_cte].add(lineage.content.address)
        inner_alias = f"_sq_{c.safe_address[:20]}"
        content_col = f"{inner_alias}.{q}{lineage.content.safe_address}{q}"

        def _to_inner(rendered: str) -> str:
            """Replace inner CTE references with inner alias (leave outer as-is)."""
            result = rendered.replace(f"{source_cte}.", f"{inner_alias}.")
            if q:
                result = result.replace(f"{q}{source_cte}{q}.", f"{inner_alias}.")
            return result

        # Concepts referenced by the subselect (content, where, order_by)
        inner_addrs = {a.address for a in lineage.concept_arguments}

        # Build inner WHERE parts
        where_parts: list[str] = []

        # Correlation: concepts that are in both the subselect's arguments
        # AND the CTE output columns (the outer context), excluding the
        # content concept and the subselect concept itself.
        # Skip entirely for cross-datasource subselects (outer_arguments present)
        # - correlation is handled by explicit outer refs in ORDER BY/WHERE.
        if not lineage.outer_arguments:
            for col in cte.output_columns:
                if col.address == c.address:
                    continue
                if col.address == lineage.content.address:
                    continue
                if col.address not in inner_addrs:
                    continue
                if source_cte not in cte.source_map.get(col.address, []):
                    continue
                outer_ref = f"{source_cte}.{q}{col.safe_address}{q}"
                inner_ref = f"{inner_alias}.{q}{col.safe_address}{q}"
                where_parts.append(f"{inner_ref} = {outer_ref}")

        # User-specified WHERE filter
        if lineage.where:
            cond = lineage.where.conditional
            # Bare concept ref in WHERE = correlation key (e.g. "where category"),
            # not a boolean filter — skip rendering it as a WHERE clause.
            if not isinstance(cond, BuildConcept):
                rendered = self.render_expr(cond, cte=cte, raise_invalid=raise_invalid)
                where_parts.append(_to_inner(rendered))

        # ORDER BY
        order_parts: list[str] = []
        if lineage.order_by:
            for oi in lineage.order_by:
                rendered = self.render_expr(
                    oi.expr, cte=cte, raise_invalid=raise_invalid
                )
                order_parts.append(f"{_to_inner(rendered)} {oi.order.value}")

        # Build inner subquery (handles ORDER BY + LIMIT)
        inner_select = f"SELECT {content_col} FROM {source_cte} AS {inner_alias}"
        if where_parts:
            inner_select += f" WHERE {' AND '.join(where_parts)}"
        if order_parts:
            inner_select += f" ORDER BY {', '.join(order_parts)}"
        if lineage.limit is not None:
            inner_select += f" LIMIT {lineage.limit}"

        # Wrap with array aggregation
        return f"(SELECT array_agg(_sr.{q}{lineage.content.safe_address}{q}) FROM ({inner_select}) _sr)"

    def render_array_unnest(
        self,
        left,
        right,
        operator: ComparisonOperator,
        cte: CTE | UnionCTE | None = None,
        cte_map: Optional[Dict[str, CTE | UnionCTE]] = None,
        raise_invalid: bool = False,
    ):
        return f"{self.render_expr(left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)} {operator.value} {self.render_expr(right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)}"

    def render_comparison(
        self,
        left,
        right,
        operator: ComparisonOperator,
        cte: CTE | UnionCTE | None = None,
        cte_map: Optional[Dict[str, CTE | UnionCTE]] = None,
        raise_invalid: bool = False,
        materialized_addresses: set[str] | None = None,
    ) -> str:
        """Default rendering for a binary comparison. Dialects override when an
        operator needs translation (e.g. SQLite ``ILIKE``)."""
        return f"{self.render_expr(left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses)} {operator.value} {self.render_expr(right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses)}"

    def _resolve_existence_column(
        self,
        rc: BuildConcept,
        cte: CTE | UnionCTE | None,
        cte_map: Optional[Dict[str, CTE | UnionCTE]],
        raise_invalid: bool,
    ) -> tuple[str, str]:
        """Resolve a right-hand membership concept to (from_clause, column_ref)
        against its existence source, mirroring the single-column path (including
        inlined-parent physical columns)."""
        lookup_cte = cte
        if cte_map and not lookup_cte:
            lookup_cte = cte_map.get(rc.address)
        assert lookup_cte, "Subselects must be rendered with a CTE in context"
        if rc.address not in lookup_cte.existence_source_map:
            lookup = lookup_cte.source_map.get(
                rc.address,
                [INVALID_REFERENCE_STRING(f"Missing source reference to {rc.address}")],
            )
        else:
            lookup = lookup_cte.existence_source_map[rc.address]
        target = (
            lookup[0]
            if lookup
            else INVALID_REFERENCE_STRING(f"Missing source CTE for {rc.address}")
        )
        inlined_parent = (
            cte.inlined_parent_for_source(target) if isinstance(cte, CTE) else None
        )
        if inlined_parent is not None:
            assert isinstance(cte, CTE)
            target = cte.source_key_for(target)
            self.used_map[target].add(rc.address)
            new_base = inlined_parent.datasource.safe_location
            phys = inlined_parent.consumer_column(rc)
            if isinstance(phys, str):
                col_ref = f"{target}.{self.QUOTE_CHARACTER}{phys}{self.QUOTE_CHARACTER}"
            elif isinstance(phys, RawColumnExpr):
                col_ref = phys.text
            else:
                col_ref = self.render_expr(
                    phys, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid
                )
            return f"{new_base} as {target}", col_ref
        self.used_map[target].add(rc.address)
        col_ref = (
            f"{target}.{self.QUOTE_CHARACTER}{rc.safe_address}{self.QUOTE_CHARACTER}"
        )
        return target, col_ref

    def render_composite_membership(
        self,
        left: BuildFunction,
        right: BuildFunction,
        operator: ComparisonOperator,
        cte: CTE | UnionCTE | None = None,
        cte_map: Optional[Dict[str, CTE | UnionCTE]] = None,
        raise_invalid: bool = False,
        materialized_addresses: set[str] | None = None,
    ) -> str:
        """Render row-wise membership `(a, b) in (set.a, set.b)` as a multi-column
        existence subquery: `(a, b) IN (select x, y from cte where x is not null
        and y is not null)`. All right components share one existence CTE."""
        left_sql = ", ".join(
            self.render_expr(
                a,
                cte=cte,
                cte_map=cte_map,
                raise_invalid=raise_invalid,
                materialized_addresses=materialized_addresses,
            )
            for a in left.arguments
        )
        from_clause = ""
        cols: list[str] = []
        for rc in right.arguments:
            assert isinstance(
                rc, BuildConcept
            ), "composite membership operands must be concepts"
            from_clause, col_ref = self._resolve_existence_column(
                rc, cte, cte_map, raise_invalid
            )
            cols.append(col_ref)
        select_list = ", ".join(cols)
        not_null = " and ".join(f"{c} is not null" for c in cols)
        return (
            f"({left_sql}) {operator.value} "
            f"(select {select_list} from {from_clause} where {not_null})"
        )

    def _render_expression_membership_subselect(
        self,
        right: BuildFunction,
        cte: CTE | UnionCTE | None,
        cte_map: Optional[Dict[str, CTE | UnionCTE]],
        raise_invalid: bool,
    ) -> str | None:
        """Render an expression-typed membership RHS (`... in (rs.col::string)`,
        a concat of rowset columns, etc.) as a `(select <expr> from <cte> where
        ...)` existence subquery. The single-BuildConcept path does this for a
        bare RHS; an expression RHS otherwise emits its inner column refs against
        a CTE that's never in a FROM (Binder: table not found). Returns None when
        this isn't an existence membership, so the caller falls back to the
        default inline rendering."""
        concepts = list(right.concept_arguments)
        if not concepts:
            return None
        lookup_cte = cte
        if cte_map and not lookup_cte:
            lookup_cte = cte_map.get(concepts[0].address)
        if lookup_cte is None:
            return None
        # Only a genuine existence membership (every referenced concept sourced
        # via the existence map) needs subselect-wrapping; a local scalar
        # expression must keep the default inline rendering.
        if any(c.address not in lookup_cte.existence_source_map for c in concepts):
            return None
        from_clauses: set[str] = set()
        null_cols: list[str] = []
        for rc in concepts:
            from_clause, col_ref = self._resolve_existence_column(
                rc, cte, cte_map, raise_invalid
            )
            # inlined-parent physical-column form ("<base> as <target>"); the
            # full-expression render below can't redirect to physical columns,
            # so let it fall back rather than emit inconsistent SQL.
            if " as " in from_clause:
                return None
            from_clauses.add(from_clause)
            null_cols.append(col_ref)
        if len(from_clauses) != 1:
            return None
        from_clause = next(iter(from_clauses))
        inner = self.render_expr(
            right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid
        )
        not_null = " and ".join(f"{c} is not null" for c in null_cols)
        return f"(select {inner} from {from_clause} where {not_null})"

    def render_expr(
        self,
        e: Union[
            BuildConcept,
            BuildFunction,
            BuildConditional,
            BuildBetween,
            BuildAggregateWrapper,
            BuildComparison,
            BuildCaseWhen,
            BuildCaseSimpleWhen,
            BuildCaseElse,
            BuildSubselectComparison,
            BuildSubselectItem,
            BuildWindowItem,
            BuildFilterItem,
            BuildParenthetical,
            BuildParamaterizedConceptReference,
            BuildMultiSelectLineage,
            BuildRowsetItem,
            str,
            int,
            list,
            bool,
            float,
            date,
            datetime,
            DataType,
            TraitDataType,
            MagicConstants,
            MapWrapper[Any, Any],
            MapType,
            NumericType,
            StructType,
            ArrayType,
            ListWrapper[Any],
            TupleWrapper[Any],
            DatePart,
        ],
        cte: Optional[CTE | UnionCTE] = None,
        cte_map: Optional[Dict[str, CTE | UnionCTE]] = None,
        raise_invalid: bool = False,
        materialized_addresses: set[str] | None = None,
    ) -> str:
        # ``materialized_addresses`` is the set of concept addresses available
        # as SELECT-list aliases at this point in the render. Propagated through
        # every recursion except the aggregate-function branch — dialects can
        # reference an alias from a top-level position in HAVING and from
        # inside CASE/arithmetic/non-aggregate functions, but not from inside
        # an aggregate (DuckDB resolves aggregate inputs against FROM, not the
        # projection).
        if isinstance(e, SUBSELECT_COMPARISON_ITEMS):
            right: Any = e.right
            while isinstance(right, BuildParenthetical):
                right = right.content
            if (
                isinstance(e.left, BuildFunction)
                and _is_build_row_tuple(e.left)
                and _is_build_row_tuple(right)
            ):
                return self.render_composite_membership(
                    e.left,
                    right,
                    e.operator,
                    cte=cte,
                    cte_map=cte_map,
                    raise_invalid=raise_invalid,
                    materialized_addresses=materialized_addresses,
                )
            if isinstance(right, BuildConcept):
                # An array-valued RHS (e.g. `x in split(s, ',')`) holds a list per
                # row; `x IN (select arr_col ...)` compares a scalar to an array and
                # the DB rejects it. Unnest the array column so the subselect yields
                # one scalar per element.
                rhs_is_array = isinstance(right.datatype, ArrayType)
                # we won't always have an existnce map
                # so fall back to the normal map
                lookup_cte = cte
                if cte_map and not lookup_cte:
                    lookup_cte = cte_map.get(right.address)

                assert lookup_cte, "Subselects must be rendered with a CTE in context"
                if right.address not in lookup_cte.existence_source_map:
                    lookup = lookup_cte.source_map.get(
                        right.address,
                        [
                            INVALID_REFERENCE_STRING(
                                f"Missing source reference to {right.address}"
                            )
                        ],
                    )
                else:
                    lookup = lookup_cte.existence_source_map[right.address]
                if len(lookup) > 0:
                    target = lookup[0]
                else:
                    target = INVALID_REFERENCE_STRING(
                        f"Missing source CTE for {right.address}"
                    )
                assert cte, "CTE must be provided for inlined CTEs"
                inlined_parent = (
                    cte.inlined_parent_for_source(target)
                    if isinstance(cte, CTE)
                    else None
                )
                if inlined_parent is not None:
                    target = cte.source_key_for(target)
                    self.used_map[target].add(right.address)
                    new_base = inlined_parent.datasource.safe_location
                    # The inlined parent exposes raw table columns, so look up
                    # the physical column for `right` rather than emitting the
                    # logical concept name.
                    phys = inlined_parent.consumer_column(right)
                    if isinstance(phys, str):
                        col_ref = f"{target}.{self.QUOTE_CHARACTER}{phys}{self.QUOTE_CHARACTER}"
                    elif isinstance(phys, RawColumnExpr):
                        col_ref = phys.text
                    else:
                        col_ref = self.render_expr(
                            phys,
                            cte=cte,
                            cte_map=cte_map,
                            raise_invalid=raise_invalid,
                        )
                    sel_ref = f"unnest({col_ref})" if rhs_is_array else col_ref
                    return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses)} {e.operator.value} (select {sel_ref} from {new_base} as {target} where {col_ref} is not null)"
                self.used_map[target].add(right.address)
                col = f"{target}.{self.QUOTE_CHARACTER}{right.safe_address}{self.QUOTE_CHARACTER}"
                sel = f"unnest({col})" if rhs_is_array else col
                return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses)} {e.operator.value} (select {sel} from {target} where {col} is not null)"
            elif isinstance(right, BuildParamaterizedConceptReference):
                if isinstance(right.concept.lineage, BuildFunction) and isinstance(
                    right.concept.lineage.arguments[0], ListWrapper
                ):
                    return self.render_array_unnest(
                        e.left,
                        right,
                        e.operator,
                        cte=cte,
                        cte_map=cte_map,
                        raise_invalid=raise_invalid,
                    )
                return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses)} {e.operator.value} {self.render_expr(right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)}"
            elif isinstance(
                right,
                (ListWrapper, TupleWrapper, BuildParenthetical),
            ):
                return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses)} {e.operator.value} {self.render_expr(right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)}"

            # An expression RHS over existence-sourced rowset columns
            # (`x::string in (rs.col::string)`, or a concat of several rs cols).
            # The bare-BuildConcept path above wraps the referenced CTE in a
            # `select ... from <cte>` subselect; an expression RHS must do the
            # same, else its inner column refs dangle (Binder: table not found).
            if isinstance(right, BuildFunction):
                subselect = self._render_expression_membership_subselect(
                    right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid
                )
                if subselect is not None:
                    return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses)} {e.operator.value} {subselect}"

            return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses)} {e.operator.value} ({self.render_expr(right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid)})"
        elif isinstance(e, COMPARISON_ITEMS):
            return self.render_comparison(
                e.left,
                e.right,
                e.operator,
                cte=cte,
                cte_map=cte_map,
                raise_invalid=raise_invalid,
                materialized_addresses=materialized_addresses,
            )
        elif isinstance(e, CONDITIONAL_ITEMS):
            return f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses)} {e.operator.value} {self.render_expr(e.right, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses)}"
        elif isinstance(e, BETWEEN_ITEMS):
            return (
                f"{self.render_expr(e.left, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses)} "
                f"BETWEEN {self.render_expr(e.low, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses)} "
                f"AND {self.render_expr(e.high, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses)}"
            )
        elif isinstance(e, WINDOW_ITEMS):
            rendered_order_components = [
                f"{self.render_expr(x.expr, cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses)} {x.order.value}"
                for x in e.order_by
            ]
            rendered_over_components = [
                self.render_expr(
                    x,
                    cte,
                    cte_map=cte_map,
                    raise_invalid=raise_invalid,
                    materialized_addresses=materialized_addresses,
                )
                for x in e.over
            ]
            window_str = ",".join(rendered_over_components)
            sort_str = ",".join(rendered_order_components)
            if isinstance(e, BuildNumberingWindowItem):
                return self.NUMBERING_WINDOW_FUNCTION_MAP[e.type](window_str, sort_str)
            return self.NAVIGATION_WINDOW_FUNCTION_MAP[e.type](
                self.render_expr(
                    e.content,
                    cte=cte,
                    cte_map=cte_map,
                    raise_invalid=raise_invalid,
                    materialized_addresses=materialized_addresses,
                ),
                window_str,
                sort_str,
                e.offset,
            )
        elif isinstance(e, PARENTHETICAL_ITEMS):
            # conditions need to be nested in parentheses
            if isinstance(e.content, list):
                return f"( {','.join([self.render_expr(x, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses) for x in e.content])} )"
            return f"( {self.render_expr(e.content, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses)} )"
        elif isinstance(e, CASE_WHEN_ITEMS):
            return f"WHEN {self.render_expr(e.comparison, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses) } THEN {self.render_expr(e.expr, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses) }"
        elif isinstance(e, BuildCaseSimpleWhen):
            return f"{self.render_expr(e.value_expr, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses)} THEN {self.render_expr(e.expr, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses)}"
        elif isinstance(e, CASE_ELSE_ITEMS):
            return f"ELSE {self.render_expr(e.expr, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses) }"
        elif isinstance(e, FUNCTION_ITEMS):
            # propagate aliases for scalar functions, drop for aggregates
            # (DuckDB resolves aggregate inputs against FROM, not projection)
            arg_aliases = (
                None
                if e.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
                else materialized_addresses
            )
            arguments = []
            for arg in e.arguments:
                if _needs_arithmetic_parentheses(arg, e.operator):
                    arguments.append(
                        self.render_expr(
                            BuildParenthetical(content=cast(BuildExpr, arg)),
                            cte=cte,
                            cte_map=cte_map,
                            raise_invalid=raise_invalid,
                            materialized_addresses=arg_aliases,
                        )
                    )
                else:
                    arguments.append(
                        self.render_expr(
                            arg,
                            cte=cte,
                            cte_map=cte_map,
                            raise_invalid=raise_invalid,
                            materialized_addresses=arg_aliases,
                        )
                    )
            if cte and cte.group_to_grain:
                return self.FUNCTION_MAP[e.operator](
                    arguments, [arg_to_datatype(x) for x in arguments]
                )

            return self.FUNCTION_GRAIN_MATCH_MAP[e.operator](
                arguments, [arg_to_datatype(x) for x in arguments]
            )
        elif isinstance(e, AGGREGATE_ITEMS):
            # aggregate input columns must resolve from FROM, not the
            # projection — don't propagate alias addresses into the function
            return self.render_expr(
                e.function, cte, cte_map=cte_map, raise_invalid=raise_invalid
            )
        elif isinstance(e, FILTER_ITEMS):
            return f"CASE WHEN {self.render_expr(e.where.conditional,cte=cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses)} THEN {self.render_expr(e.content, cte, cte_map=cte_map, raise_invalid=raise_invalid, materialized_addresses=materialized_addresses)} ELSE NULL END"
        elif isinstance(e, BuildConcept):
            if (
                isinstance(e.lineage, FUNCTION_ITEMS)
                and e.lineage.operator == FunctionType.CONSTANT
                and self.rendering.parameters is True
                and e.datatype.data_type != DataType.MAP
                and e.datatype.data_type not in INLINE_SAFE_PARAM_DATATYPES
                and _constant_bindable(e.lineage)
                # only bind the literal where it's first materialized; if it's
                # already a column in a source CTE (e.g. an ORDER BY term sourced
                # from a join), reference that column instead of re-emitting the
                # bind param — a bare param is illegal in ORDER BY.
                and not (cte and cte.source_map.get(e.address))
            ):
                return f":{e.safe_address}"
            if (
                materialized_addresses is not None
                and e.address in materialized_addresses
            ):
                # we're at a top-level position in a HAVING on an alias-capable
                # dialect and the SELECT already computes this expression
                # under e.safe_address
                return f"{self.QUOTE_CHARACTER}{e.safe_address}{self.QUOTE_CHARACTER}"
            if cte:
                return self.render_concept_sql(
                    e,
                    cte,
                    alias=False,
                    raise_invalid=raise_invalid,
                )
            elif cte_map:
                self.used_map[cte_map[e.address].name].add(e.address)
                return f"{cte_map[e.address].name}.{self.QUOTE_CHARACTER}{e.safe_address}{self.QUOTE_CHARACTER}"
            return f"{self.QUOTE_CHARACTER}{e.safe_address}{self.QUOTE_CHARACTER}"
        elif isinstance(e, bool):
            return f"{e}"
        elif isinstance(e, str):
            return f"'{e}'"
        elif isinstance(e, (int, float)):
            return str(e)
        elif isinstance(e, TupleWrapper):
            return f"({','.join([self.render_expr(x, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid) for x in e])})"
        elif isinstance(e, MapWrapper):
            return self.render_map_literal(
                e, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid
            )
        elif isinstance(e, ListWrapper):
            return f"{self.FUNCTION_MAP[FunctionType.ARRAY]([self.render_expr(x, cte=cte, cte_map=cte_map, raise_invalid=raise_invalid) for x in e], [])}"
        elif isinstance(e, DataType):
            return self.DATATYPE_MAP.get(e, e.value)
        elif isinstance(e, DatePart):
            return str(e.value)
        elif isinstance(e, NumericType):
            return f"{self.DATATYPE_MAP[DataType.NUMERIC]}({e.precision},{e.scale})"
        elif isinstance(e, MagicConstants):
            if e == MagicConstants.NULL:
                return "null"
            return str(e.value)
        elif isinstance(e, datetime):
            return self.FUNCTION_MAP[FunctionType.DATETIME_LITERAL](e, [])
        elif isinstance(e, date):
            return self.FUNCTION_MAP[FunctionType.DATE_LITERAL](e, [])
        elif isinstance(e, EnumType):
            return self.render_expr(e.data_type, cte=cte, cte_map=cte_map)  # type: ignore[arg-type]
        elif isinstance(e, TraitDataType):
            return self.render_expr(e.type, cte=cte, cte_map=cte_map)  # type: ignore[arg-type]
        elif isinstance(e, ArgBinding):
            return e.name
        elif isinstance(e, Ordering):
            return str(e.value)
        elif isinstance(e, ArrayType):
            return f"{self.COMPLEX_DATATYPE_MAP[DataType.ARRAY](self.render_expr(e.value_data_type, cte=cte, cte_map=cte_map))}"  # type: ignore[arg-type]
        elif isinstance(e, list):
            return f"{self.FUNCTION_MAP[FunctionType.ARRAY]([self.render_expr(x, cte=cte, cte_map=cte_map) for x in e], [])}"
        elif isinstance(e, BuildParamaterizedConceptReference):
            inline_safe = e.concept.datatype.data_type in INLINE_SAFE_PARAM_DATATYPES
            if self.rendering.parameters and not inline_safe:
                if e.concept.namespace == DEFAULT_NAMESPACE:
                    return f":{e.concept.name}"
                return f":{e.concept.address.replace('.', '_')}"
            elif e.concept.lineage:
                return self.render_expr(e.concept.lineage, cte=cte, cte_map=cte_map)
            return f"{self.QUOTE_CHARACTER}{e.concept.address}{self.QUOTE_CHARACTER}"

        else:
            raise ValueError(f"Unable to render type {type(e)} {e}")

    @staticmethod
    def _grouped_output_is_parent_passthrough(
        concept: BuildConcept,
        cte: CTE | UnionCTE,
        parents_by_name: dict[str, CTE | UnionCTE],
    ) -> bool:
        # A non-standard-grouped output is a "passthrough" when its source_map
        # points exclusively at parent CTEs that already applied the same
        # rollup/cube/grouping-sets aggregation. Re-emitting GROUP BY here
        # would double-aggregate the parent's output (or, for ROLLUP, error
        # because the passthrough columns aren't inside an aggregate).
        if not isinstance(cte, CTE):
            return False
        aggregate = get_grouped_aggregate_wrapper(concept)
        if aggregate is None or aggregate.grouping == AggregateGroupingMode.STANDARD:
            return False
        sources = cte.source_map.get(concept.address, [])
        if not sources:
            return False
        by_addresses = [c.address for c in aggregate.by]
        for src in sources:
            parent = parents_by_name.get(src)
            if parent is None:
                return False
            parent_concept = next(
                (c for c in parent.output_columns if c.address == concept.address),
                None,
            )
            if parent_concept is None:
                return False
            parent_aggregate = get_grouped_aggregate_wrapper(parent_concept)
            if parent_aggregate is None:
                return False
            if parent_aggregate.grouping != aggregate.grouping:
                return False
            if [c.address for c in parent_aggregate.by] != by_addresses:
                return False
        return True

    @staticmethod
    def _has_local_aggregate(cte: CTE | UnionCTE) -> bool:
        """True if the CTE computes an aggregate locally (not merely passing one
        through from a parent). Such a CTE must emit a GROUP BY even when its only
        rollup output is a passthrough — which would otherwise short-circuit the
        GROUP BY via ``_all_grouped_outputs_are_passthrough`` and leave the local
        aggregate ungrouped (invalid SQL)."""
        if not isinstance(cte, CTE):
            return False
        return any(
            get_grouped_aggregate_wrapper(c) is not None
            and not cte.source_map.get(c.address)
            for c in cte.output_columns
        )

    @classmethod
    def _all_grouped_outputs_are_passthrough(cls, cte: CTE | UnionCTE) -> bool:
        if not isinstance(cte, CTE) or not cte.parent_ctes:
            return False
        grouped_concepts = [
            c
            for c in cte.output_columns
            if (agg := get_grouped_aggregate_wrapper(c)) is not None
            and agg.grouping != AggregateGroupingMode.STANDARD
        ]
        if not grouped_concepts:
            return False
        parents_by_name = {p.name: p for p in cte.parent_ctes}
        return all(
            cls._grouped_output_is_parent_passthrough(c, cte, parents_by_name)
            for c in grouped_concepts
        )

    def _get_aggregate_grouping(
        self, cte: CTE | UnionCTE
    ) -> tuple[AggregateGroupingMode, list[BuildConcept], list[list[BuildConcept]]]:
        grouped = [
            aggregate
            for c in cte.output_columns
            if (aggregate := get_grouped_aggregate_wrapper(c)) is not None
            and aggregate.grouping != AggregateGroupingMode.STANDARD
            # A passthrough rollup (already aggregated upstream, selected here as a
            # plain column) must not drive this CTE's grouping mode — re-emitting
            # its ROLLUP would double-aggregate. Only locally-computed grouped
            # aggregates set the mode.
            and not cte.source_map.get(c.address)
        ]
        if not grouped:
            return AggregateGroupingMode.STANDARD, [], []

        first = grouped[0]
        mode = first.grouping
        by = first.by
        grouping_sets = first.grouping_sets
        by_addresses = [c.address for c in by]
        grouping_set_addresses = [
            [c.address for c in grouping_set] for grouping_set in grouping_sets
        ]
        for aggregate in grouped[1:]:
            if aggregate.grouping != mode:
                raise ValueError("Cannot mix aggregate grouping modes in one CTE")
            if [c.address for c in aggregate.by] != by_addresses:
                raise ValueError("Aggregate grouping dimensions must match in one CTE")
            if [
                [c.address for c in grouping_set]
                for grouping_set in aggregate.grouping_sets
            ] != grouping_set_addresses:
                raise ValueError("Aggregate grouping sets must match in one CTE")
        return mode, by, grouping_sets

    def _render_grouping_concept(
        self,
        concept: BuildConcept,
        cte: CTE | UnionCTE,
        select_index: dict[str, int],
        rendered_to_index: dict[str, int],
    ) -> str:
        if self.GROUP_MODE == GroupMode.AUTO:
            return self.render_concept_sql(concept, cte, alias=False)
        if concept.address in select_index:
            return str(select_index[concept.address])
        sql = self.render_concept_sql(concept, cte, alias=False)
        existing_idx = rendered_to_index.get(sql)
        if existing_idx is not None:
            return str(existing_idx)
        return sql

    def _rendered_select_index(
        self, cte: CTE | UnionCTE, select_index: dict[str, int]
    ) -> dict[str, int]:
        rendered_to_index: dict[str, int] = {}
        output_by_address = {c.address: c for c in cte.output_columns}
        for addr, idx in select_index.items():
            concept = output_by_address.get(addr)
            if concept is None:
                continue
            sql = self.render_concept_sql(concept, cte, alias=False)
            rendered_to_index[sql] = idx
        return rendered_to_index

    def _render_grouping_mode(
        self,
        cte: CTE | UnionCTE,
        select_index: dict[str, int],
    ) -> list[str] | None:
        mode, by, grouping_sets = self._get_aggregate_grouping(cte)
        if mode == AggregateGroupingMode.STANDARD:
            return None
        if not self.SUPPORTS_AGGREGATE_GROUPING_MODES:
            raise NotImplementedError(
                f"{self.__class__.__name__} does not support aggregate grouping mode {mode.value}"
            )

        rendered_to_index = self._rendered_select_index(cte, select_index)

        def render_concepts(concepts: list[BuildConcept]) -> str:
            return ", ".join(
                self._render_grouping_concept(c, cte, select_index, rendered_to_index)
                for c in concepts
            )

        if mode == AggregateGroupingMode.ROLLUP:
            return [f"ROLLUP ({render_concepts(by)})"]
        if mode == AggregateGroupingMode.CUBE:
            return [f"CUBE ({render_concepts(by)})"]
        if mode == AggregateGroupingMode.GROUPING_SETS:
            rendered_sets = []
            for grouping_set in grouping_sets:
                rendered_sets.append(f"({render_concepts(grouping_set)})")
            return [f"GROUPING SETS ({', '.join(rendered_sets)})"]
        raise ValueError(f"Unsupported aggregate grouping mode {mode}")

    @staticmethod
    def _constant_output_group_by_fallback(cte: CTE | UnionCTE) -> list[str]:
        # Dedupe to one row when group_to_grain is set but every output is a
        # plain constant and there is a real source below, e.g.
        # `where cond select 1 as test` over a multi-row table.
        if not isinstance(cte, CTE) or not cte.render_from_clause:
            return []
        if not cte.output_columns:
            return []
        if all(c.derivation == Derivation.CONSTANT for c in cte.output_columns):
            return ["1"]
        return []

    def render_cte_group_by(
        self, cte: CTE | UnionCTE, select_index: dict[str, int]
    ) -> Optional[list[str]]:

        if not cte.group_to_grain:
            return None

        if self._all_grouped_outputs_are_passthrough(
            cte
        ) and not self._has_local_aggregate(cte):
            return None

        grouping_mode = self._render_grouping_mode(cte, select_index)
        if grouping_mode is not None:
            return grouping_mode

        constant_output_fallback = self._constant_output_group_by_fallback(cte)

        if self.GROUP_MODE == GroupMode.AUTO:
            result = sorted(
                {
                    self.render_concept_sql(c, cte, alias=False)
                    for c in cte.group_concepts
                }
            )
            if not result:
                return constant_output_fallback
            return result

        # Build reverse map from rendered SQL to index for resolving
        # hidden concepts that render identically to visible ones
        rendered_to_index = self._rendered_select_index(cte, select_index)
        seen: set[int] = set()
        seen_sql: set[str] = set()
        indices: list[int] = []
        fallbacks: list[str] = []
        for c in cte.group_concepts:
            sql = self.render_concept_sql(c, cte, alias=False)
            # two group keys that resolve to the same source expression are
            # redundant: grouping by (x, x) == grouping by (x). Distinct
            # aliases over one column (e.g. q39's isk1/isk2 -> inv_item_sk)
            # otherwise emit GROUP BY 1,2,3,4 instead of 1,3.
            if sql in seen_sql:
                continue
            seen_sql.add(sql)
            if c.address in select_index:
                idx = select_index[c.address]
                if idx not in seen:
                    seen.add(idx)
                    indices.append(idx)
            else:
                existing_idx = rendered_to_index.get(sql)
                if existing_idx is not None:
                    if existing_idx not in seen:
                        seen.add(existing_idx)
                        indices.append(existing_idx)
                else:
                    fallbacks.append(sql)
        rendered = [str(i) for i in sorted(indices)] + sorted(fallbacks)
        if not rendered:
            return constant_output_fallback
        return rendered

    def safe_quote(self, name: str) -> str:
        return safe_quote(name, self.QUOTE_CHARACTER)

    def quote(self, name: str) -> str:
        return f"{self.QUOTE_CHARACTER}{name}{self.QUOTE_CHARACTER}"

    def render_cte(self, cte: CTE | UnionCTE, auto_sort: bool = True) -> CompiledCTE:
        if isinstance(cte, UnionCTE):
            base_statement = f"\n{cte.operator}\n".join(
                [
                    self.render_cte(child, auto_sort=False).statement
                    for child in cte.internal_ctes
                ]
            )
            if cte.order_by:

                ordering = [self.render_order_item(i, cte) for i in cte.order_by.items]
                base_statement += "\nORDER BY " + ",".join(ordering)
            return CompiledCTE(name=cte.name, statement=base_statement)
        elif isinstance(cte, RecursiveCTE):
            base_statement = "\nUNION ALL\n".join(
                [self.render_cte(child, False).statement for child in cte.internal_ctes]
            )
            return CompiledCTE(name=cte.name, statement=base_statement)
        join_derived_addresses = {c.address for c in cte.join_derived_concepts}
        if self.UNNEST_MODE in (
            UnnestMode.CROSS_APPLY,
            UnnestMode.CROSS_JOIN,
            UnnestMode.CROSS_JOIN_ALIAS,
            UnnestMode.SNOWFLAKE,
        ):
            # for a cross apply, derivation happens in the join
            # so we only use the alias to select
            select_columns = {
                **{
                    c.address: self.render_concept_sql(c, cte)
                    for c in cte.output_columns
                    if c.address not in join_derived_addresses
                    and c.address not in cte.hidden_concepts
                },
                **{
                    c.address: f"{self.QUOTE_CHARACTER}{c.safe_address}{self.QUOTE_CHARACTER}"
                    for c in cte.join_derived_concepts
                    if c.address not in cte.hidden_concepts
                },
            }
        elif self.UNNEST_MODE in (UnnestMode.CROSS_JOIN_UNNEST, UnnestMode.PRESTO):
            select_columns = {
                **{
                    c.address: self.render_concept_sql(c, cte)
                    for c in cte.output_columns
                    if c.address not in join_derived_addresses
                    and c.address not in cte.hidden_concepts
                },
                **{
                    c.address: f"{UNNEST_NAME} as {self.QUOTE_CHARACTER}{c.safe_address}{self.QUOTE_CHARACTER}"
                    for c in cte.join_derived_concepts
                    if c.address not in cte.hidden_concepts
                },
            }
        else:
            # otherwse, assume we are unnesting directly in the select
            select_columns = {
                c.address: self.render_concept_sql(c, cte)
                for c in cte.output_columns
                if c.address not in cte.hidden_concepts
            }
        if auto_sort:
            render_columns = sorted(select_columns.values(), key=lambda x: x)
        else:
            render_columns = list(select_columns.values())
        lookups = {v: i for i, v in enumerate(render_columns)}
        select_concept_index = {k: lookups[v] + 1 for k, v in select_columns.items()}
        source: str | None = cte.base_name
        if not cte.render_from_clause:
            if len(cte.joins) > 0:
                if cte.join_derived_concepts and self.UNNEST_MODE in (
                    UnnestMode.CROSS_JOIN_ALIAS,
                    # UnnestMode.CROSS_JOIN_UNNEST,
                    UnnestMode.CROSS_JOIN,
                    UnnestMode.CROSS_APPLY,
                ):

                    source = f"{render_unnest(self.UNNEST_MODE, self.QUOTE_CHARACTER, cte.join_derived_concepts[0], self.render_expr, cte)}"
                elif cte.join_derived_concepts and self.UNNEST_MODE in (
                    UnnestMode.CROSS_JOIN_UNNEST,
                ):
                    source = f"{self.render_expr(cte.join_derived_concepts[0], cte)} as {self.QUOTE_CHARACTER}{UNNEST_NAME}{self.QUOTE_CHARACTER}"
                elif cte.join_derived_concepts and self.UNNEST_MODE in (
                    UnnestMode.PRESTO,
                ):
                    source = f"{self.render_expr(cte.join_derived_concepts[0], cte)} as t({self.QUOTE_CHARACTER}{UNNEST_NAME}{self.QUOTE_CHARACTER})"
                elif (
                    cte.join_derived_concepts
                    and self.UNNEST_MODE == UnnestMode.SNOWFLAKE
                ):
                    source = f"{render_unnest(self.UNNEST_MODE, self.QUOTE_CHARACTER, cte.join_derived_concepts[0], self.render_expr, cte)}"
                # direct - eg DUCK DB - can be directly selected inline
                elif (
                    cte.join_derived_concepts and self.UNNEST_MODE == UnnestMode.DIRECT
                ):
                    source = None
                else:
                    raise SyntaxError("CTE has joins but no from clause")
            else:
                source = None
        else:
            # Inlined-datasource base is rendered via the consistent
            # source_address / base_*_override path (set by inline) — the same
            # path a non-inlined leaf datasource uses for its own FROM.
            addr = cte.source_address
            if isinstance(addr, Address):
                source = self.render_source(addr)
            elif cte.quote_address:
                source = safe_quote(addr, self.QUOTE_CHARACTER)
            else:
                source = addr
            if cte.base_name != cte.base_alias:
                source = f"{source} as {self.QUOTE_CHARACTER}{cte.base_alias}{self.QUOTE_CHARACTER}"
        if not cte.render_from_clause:
            final_joins = []
        else:
            final_joins = cte.joins or []
        where: BoolExpr | None = None
        having: BoolExpr | None = None
        qualify: BoolExpr | None = None
        materialized = {x for x, v in cte.source_map.items() if v}
        if cte.condition:
            # Window predicates (rank/lag/... over) can't live in WHERE or HAVING;
            # they must be lowered to QUALIFY. Fast-path the common no-window case.
            if not contains_window(cte.condition, materialized=materialized) and (
                not cte.group_to_grain
                or is_scalar_condition(cte.condition, materialized=materialized)
            ):
                where = cte.condition
            else:
                components = decompose_condition(cte.condition)
                for x in components:
                    if contains_window(x, materialized=materialized):
                        qualify = qualify + x if qualify else x
                    elif cte.group_to_grain and not is_scalar_condition(
                        x, materialized=materialized
                    ):
                        having = having + x if having else x
                    else:
                        where = where + x if where else x

        if qualify is not None and not self.SUPPORTS_QUALIFY:
            raise InvalidSyntaxException(
                "Window functions are not allowed in a `having` clause for this "
                "dialect. Project the window as a column "
                "(`--rank() over (...) as r`) and filter on it instead."
            )

        rendered_where = self.render_expr(where, cte) if where else None
        rendered_qualify = self.render_expr(qualify, cte) if qualify else None
        if having is not None and self.SUPPORTS_ALIAS_IN_HAVING:
            # reference the SELECT alias rather than re-inlining the aggregate.
            # Only valid at the top of the HAVING tree — dialects resolve
            # aliases against the projection only at the outermost comparison
            # operands, not inside nested functions/aggregates/case/etc.
            rendered_having: str | None = self.render_expr(
                having, cte, materialized_addresses=set(select_columns.keys())
            )
        else:
            rendered_having = self.render_expr(having, cte) if having else None

        logger.debug(f"{LOGGER_PREFIX} {len(final_joins)} joins for cte {cte.name}")
        return CompiledCTE(
            name=cte.name,
            statement=self.SQL_TEMPLATE.render(
                select_columns=render_columns,
                base=f"{source}" if source else None,
                grain=cte.grain,
                limit=cte.limit,
                comment=cte.comment if CONFIG.show_comments else None,
                # some joins may not need to be rendered
                joins=[
                    j
                    for j in [
                        render_join(
                            join,
                            self.QUOTE_CHARACTER,
                            self.render_expr,
                            cte,
                            use_map=self.used_map,
                            unnest_mode=self.UNNEST_MODE,
                            null_wrapper=self.NULL_WRAPPER,
                        )
                        for join in final_joins
                    ]
                    if j
                ],
                where=rendered_where,
                having=rendered_having,
                qualify=rendered_qualify,
                order_by=(
                    [self.render_order_item(i, cte) for i in cte.order_by.items]
                    if cte.order_by
                    else None
                ),
                group_by=self.render_cte_group_by(cte, select_concept_index),
            ),
        )

    def generate_ctes(
        self,
        query: ProcessedQuery,
    ) -> List[CompiledCTE]:
        return [self.render_cte(cte) for cte in query.ctes[:-1]] + [
            # last CTE needs to respect the user output order
            self.render_cte(sort_select_output(query.ctes[-1], query), auto_sort=False)
        ]

    def create_show_output(
        self,
        environment: Environment,
        content: ShowCategory,
    ):
        if content == ShowCategory.CONCEPTS:
            output_columns = [
                environment.concepts[
                    DEFAULT_CONCEPTS["concept_address"].address
                ].reference,
                environment.concepts[
                    DEFAULT_CONCEPTS["concept_datatype"].address
                ].reference,
                environment.concepts[
                    DEFAULT_CONCEPTS["concept_description"].address
                ].reference,
            ]
            output_values = [
                {
                    DEFAULT_CONCEPTS["concept_address"].address: (
                        concept.name
                        if concept.namespace == DEFAULT_NAMESPACE
                        else concept.address
                    ),
                    DEFAULT_CONCEPTS["concept_datatype"].address: str(concept.datatype),
                    DEFAULT_CONCEPTS[
                        "concept_description"
                    ].address: concept.metadata.description
                    or "",
                }
                for _, concept in environment.concepts.items()
                if not concept.is_internal
            ]
        else:
            raise NotImplementedError(f"Show category {content} not implemented")
        return ProcessedShowStatement(
            output_columns=output_columns,
            output_values=[ProcessedStaticValueOutput(values=output_values)],
        )

    def generate_queries(
        self,
        environment: Environment,
        statements: Sequence[
            SelectStatement
            | MultiSelectStatement
            | PersistStatement
            | ShowStatement
            | ConceptDeclarationStatement
            | RowsetDerivationStatement
            | ImportStatement
            | RawSQLStatement
            | MergeStatementV2
            | CopyStatement
            | ValidateStatement
            | CreateStatement
            | PublishStatement
            | MockStatement
            | ChartStatement
        ],
        hooks: Optional[List[BaseHook]] = None,
    ) -> List[PROCESSED_STATEMENT_TYPES]:
        output: List[PROCESSED_STATEMENT_TYPES] = []
        for statement in statements:
            if isinstance(statement, PersistStatement):
                if hooks:
                    for hook in hooks:
                        hook.process_persist_info(statement)
                persist = process_persist(environment, statement, hooks=hooks)
                output.append(persist)
            elif isinstance(statement, ChartStatement):
                if hooks:
                    for hook in hooks:
                        for layer in statement.layers:
                            if layer.select is not None:
                                hook.process_select_info(layer.select)
                chart = process_chart(environment, statement, hooks=hooks)
                output.append(chart)
            elif isinstance(statement, CopyStatement):
                if hooks:
                    for hook in hooks:
                        if isinstance(statement.select, ChartStatement):
                            for layer in statement.select.layers:
                                if layer.select is not None:
                                    hook.process_select_info(layer.select)
                        else:
                            hook.process_select_info(statement.select)
                copy = process_copy(environment, statement, hooks=hooks)
                output.append(copy)
            elif isinstance(statement, SelectStatement):
                if hooks:
                    for hook in hooks:
                        hook.process_select_info(statement)
                output.append(
                    process_query(
                        environment,
                        statement,
                        hooks=hooks,
                        having_alias=self.SUPPORTS_ALIAS_IN_HAVING,
                    )
                )
            elif isinstance(statement, MultiSelectStatement):
                if hooks:
                    for hook in hooks:
                        hook.process_multiselect_info(statement)
                output.append(
                    process_query(
                        environment,
                        statement,
                        hooks=hooks,
                        having_alias=self.SUPPORTS_ALIAS_IN_HAVING,
                    )
                )
            elif isinstance(statement, RowsetDerivationStatement):
                if hooks:
                    for hook in hooks:
                        hook.process_rowset_info(statement)
            elif isinstance(statement, ShowStatement):

                if isinstance(
                    statement.content,
                    (SelectStatement, MultiSelectStatement, PersistStatement),
                ):
                    target = statement.content
                    output.append(
                        ProcessedShowStatement(
                            output_columns=[
                                environment.concepts[
                                    DEFAULT_CONCEPTS["query_text"].address
                                ].reference
                            ],
                            output_values=[
                                self.generate_queries(  # type: ignore
                                    environment, [target], hooks=hooks
                                )[-1]
                            ],
                        )
                    )
                elif isinstance(statement.content, ShowCategory):
                    output.append(
                        self.create_show_output(environment, statement.content)
                    )
                elif isinstance(statement.content, ValidateStatement):
                    output.append(
                        ProcessedShowStatement(
                            output_columns=[
                                environment.concepts[
                                    DEFAULT_CONCEPTS["label"].address
                                ].reference,
                                environment.concepts[
                                    DEFAULT_CONCEPTS["query_text"].address
                                ].reference,
                                environment.concepts[
                                    DEFAULT_CONCEPTS["expected"].address
                                ].reference,
                            ],
                            output_values=[
                                ProcessedValidateStatement(
                                    scope=statement.content.scope,
                                    targets=statement.content.targets,
                                )
                            ],
                        )
                    )
                else:
                    raise NotImplementedError(type(statement.content))
            elif isinstance(statement, RawSQLStatement):
                output.append(ProcessedRawSQLStatement(text=statement.text))
            elif isinstance(statement, ValidateStatement):
                output.append(
                    ProcessedValidateStatement(
                        scope=statement.scope,
                        targets=statement.targets,
                    )
                )
            elif isinstance(statement, MockStatement):
                output.append(
                    ProcessedMockStatement(
                        scope=statement.scope,
                        targets=statement.targets,
                    )
                )
            elif isinstance(statement, CreateStatement):
                output.append(process_create_statement(statement, environment))
            elif isinstance(statement, PublishStatement):
                output.append(
                    ProcessedPublishStatement(
                        scope=statement.scope,
                        targets=statement.targets,
                        action=statement.action,
                    )
                )
            elif isinstance(
                statement,
                (
                    ConceptDeclarationStatement,
                    MergeStatementV2,
                    ImportStatement,
                    RowsetDerivationStatement,
                    Datasource,
                    FunctionDeclaration,
                ),
            ):
                continue
            else:
                raise NotImplementedError(type(statement))
        return output

    def generate_partitioned_insert(
        self,
        query: ProcessedQueryPersist,
        recursive: bool,
        compiled_ctes: list[CompiledCTE],
    ) -> str:
        return self.SQL_TEMPLATE.render(
            recursive=recursive,
            output=f"INSERT OVERWRITE {self.safe_quote(query.output_to.address.location)}",
            full_select=compiled_ctes[-1].statement,
            ctes=compiled_ctes[:-1],
        )

    def compile_create_table_statement(
        self, target: CreateTableInfo, create_mode: CreateMode
    ) -> str:
        type_map = {}
        for c in target.columns:
            type_map[c.name] = self.render_expr(c.type)
        return self.CREATE_TABLE_SQL_TEMPLATE.render(
            create_mode=create_mode.value,
            name=self.safe_quote(target.name),
            columns=target.columns,
            type_map=type_map,
            partition_keys=target.partition_keys,
        )

    def compile_statement(
        self,
        query: PROCESSED_STATEMENT_TYPES,
    ) -> str:
        if isinstance(query, ProcessedShowStatement):
            return ";\n".join(
                [
                    f"{self.EXPLAIN_KEYWORD} {self.compile_statement(x)}"
                    for x in query.output_values
                    if isinstance(x, (ProcessedQuery, ProcessedCopyStatement))
                ]
            )
        elif isinstance(query, ProcessedRawSQLStatement):
            return query.text

        elif isinstance(query, ProcessedValidateStatement):
            return "--Trilogy validate statements do not have a generic SQL representation;\nselect 1;"
        elif isinstance(query, ProcessedMockStatement):
            return "--Trilogy mock statements do not have a generic SQL representation;\nselect 1;"
        elif isinstance(query, ProcessedPublishStatement):
            return "--Trilogy publish statements do not have a generic SQL representation;\nselect 1;"
        elif isinstance(query, ProcessedCreateStatement):

            text = []
            for target in query.targets:
                text.append(
                    self.compile_create_table_statement(target, query.create_mode)
                )
            return "\n".join(text)
        elif isinstance(query, ProcessedChartStatement):
            return ";\n".join(
                self.compile_statement(layer.query)
                for layer in query.layers
                if layer.query is not None
            )
        elif isinstance(query, ProcessedChartCopyStatement):
            return self.compile_statement(query.chart)

        recursive = any(isinstance(x, RecursiveCTE) for x in query.ctes)

        compiled_ctes = self.generate_ctes(query)
        output = None
        if isinstance(query, ProcessedQueryPersist):
            if query.persist_mode == PersistMode.OVERWRITE:
                create_table_info = datasource_to_create_table_info(query.datasource)
                output = f"{self.compile_create_table_statement(create_table_info, CreateMode.CREATE_OR_REPLACE)} INSERT INTO {self.safe_quote(query.output_to.address.location)} "
            elif query.persist_mode == PersistMode.APPEND:
                if query.partition_by:
                    return self.generate_partitioned_insert(
                        query, recursive, compiled_ctes
                    )
                else:
                    output = f"INSERT INTO {self.safe_quote(query.output_to.address.location)} "
            else:
                raise NotImplementedError(
                    f"Persist mode {query.persist_mode} not implemented"
                )

        final = self.SQL_TEMPLATE.render(
            recursive=recursive,
            output=output,
            full_select=compiled_ctes[-1].statement,
            ctes=compiled_ctes[:-1],
        )

        if CONFIG.strict_mode and BASE_INVALID in final:
            # Surface the embedded reason(s) (e.g. "Missing source CTE for x.y")
            # so the failure names the unsourceable reference instead of dumping
            # the whole query with a generic message.
            reasons = extract_invalid_reference_reasons(final)
            detail = "; ".join(reasons) if reasons else "an unresolved reference"
            raise ValueError(
                f"Could not render the query: {detail}. A planned reference has no "
                "backing source CTE -- typically an unsupported cross-rowset or "
                "membership shape the planner could not wire. Review the rowset/join "
                "structure (or file an issue if the query looks valid).\n\n"
                f"Full SQL with sentinel(s):\n{final}"
            )
        logger.info(f"{LOGGER_PREFIX} Compiled query: {final}")
        return final

    def compile_without_limit(self, query: ProcessedQuery) -> str:
        """Re-render the query SQL with its output LIMIT removed — structurally
        (clear the output CTE's limit and recompile), never by editing the SQL
        text. The output LIMIT lives on the final CTE (``query.ctes[-1]``, also
        ``query.base``); inner-CTE limits (e.g. a rowset's own limit) are
        deliberately preserved."""
        if not query.ctes or query.ctes[-1].limit is None:
            return self.compile_statement(replace(query, limit=None))
        new_last = replace(query.ctes[-1], limit=None)
        unlimited = replace(
            query,
            ctes=[*query.ctes[:-1], new_last],
            limit=None,
            base=new_last if query.base is query.ctes[-1] else query.base,
        )
        return self.compile_statement(unlimited)

    def summarize_result(
        self, query: ProcessedQuery, run_sql: Callable[[str], "ResultProtocol"]
    ) -> "tuple[list[dict], int] | None":
        """Per-column stats (non_null / nulls / distinct / min / max) plus the
        row count over the FULL result — the query with its LIMIT removed — so a
        consumer reads true cardinality, not the LIMIT-bounded prefix.

        Gated by ``SUPPORTS_RESULT_SUMMARY``; the base implementation is not
        provided. ``run_sql`` runs a raw SQL string and returns a result with
        ``keys()`` / ``fetchall()``."""
        raise NotImplementedError(
            f"{type(self).__name__} does not implement summarize_result"
        )

    def compile_statement_with_params(
        self,
        query: PROCESSED_STATEMENT_TYPES,
    ) -> tuple[str, dict[str, Any]]:
        """Return (sql, parameters) where sql contains :name placeholders and
        parameters maps name -> value (keys without the leading colon).

        Returns ({}, ) as the second element when rendering.parameters is False
        or the statement type carries no parameters.
        """
        import re

        sql = self.compile_statement(query)
        if not self.rendering.parameters or not isinstance(query, ProcessedQuery):
            return sql, {}
        param_keys = re.findall(r"(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)", sql)
        return sql, {
            k: query.parameters[k] for k in param_keys if k in query.parameters
        }
