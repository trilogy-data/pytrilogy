from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar

from trilogy.core.casts import (
    DATE_PATTERN,
    DATETIME_PATTERN,
    FLOAT_PATTERN,
    INSTANT_READ_TARGETS,
    INT64,
    NUMBER_PATTERN,
)
from trilogy.core.enums import (
    ComparisonOperator,
    FunctionType,
    JoinType,
    Modifier,
    UnnestMode,
    WindowType,
)
from trilogy.core.models.author import get_basic_type
from trilogy.core.models.core import DataType, MapWrapper, NumericType
from trilogy.core.models.execute import CTE, Join, UnionCTE
from trilogy.core.statements.execute import ColumnInfo, CreateTableInfo
from trilogy.dialect.base import (
    INLINE_SAFE_PARAM_DATATYPES,
    NAVIGATION_WINDOW_FUNCTION_MAP,
    BaseDialect,
    array_element_type,
    guard_divisor,
    in_domain,
    is_array_arg,
    navigation_window_factory,
    render_log,
)
from trilogy.dialect.common import (
    CONCAT_COALESCE_LOWER,
    bool_from_text,
    cast_kind,
    guarded_read,
    reads_text,
    render_window_substring,
    sunday_first_from_iso,
)


def date_part(expr: str, part: str) -> str:
    if part == "day_of_week":
        return sunday_first_from_iso(f"toDayOfWeek({expr})")
    if part == "week":
        return f"toISOWeek({expr})"
    return f"EXTRACT({part} FROM {expr})"


if TYPE_CHECKING:
    from trilogy.core.statements.execute import ProcessedQuery

OUTER_JOINS = (JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER, JoinType.FULL)


def pads_outer_rows(ctes: "list[CTE | UnionCTE]") -> bool:
    for cte in ctes:
        if isinstance(cte, UnionCTE):
            if pads_outer_rows(cte.internal_ctes):
                return True
        elif any(isinstance(j, Join) and j.join_type in OUTER_JOINS for j in cte.joins):
            return True
    return False


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


# ClickHouse types are NOT NULL unless wrapped in Nullable, which these refuse
UNWRAPPABLE_TYPES = (DataType.ARRAY, DataType.MAP, DataType.STRUCT)


FLOAT_TYPES = (DataType.FLOAT, DataType.DOUBLE, DataType.NUMBER)
INTEGER_TYPES = (DataType.INTEGER, DataType.BIGINT)


def data_type(datatype: Any) -> Any:
    return getattr(datatype, "data_type", datatype)


def round_half_away(value: str, digits: str, value_type: Any) -> str:
    """`round` of a float is half to even here (`round(2.5)` is 2) and half
    away from zero everywhere else; a decimal already rounds half away."""
    if data_type(value_type) not in FLOAT_TYPES:
        return f"round({value}, {digits})"
    if digits.strip() == "0":
        return f"(sign({value}) * floor(abs({value}) + 0.5))"
    scale = f"pow(10, {digits})"
    return f"(sign({value}) * floor(abs({value}) * {scale} + 0.5) / {scale})"


def read_text(value: str, target: str, target_type: Any) -> str:
    """A string as a number: read exactly at 18 digits, which truncates only
    digits past any half-away tie, then rounded to the target. As a date: the
    zero-padded day, only if `toDate32` gives it back, as it rolls
    '2020-02-30' into March and '2020-13-01' to 1970."""
    value = f"toString({value})"
    text = f"replaceRegexpAll({value}, '^[[:space:]]+|[[:space:]]+$', '')"
    parts = f"splitByRegexp('[-/ T]', {text})"
    day = (
        f"concat({parts}[1], '-', leftPad({parts}[2], 2, '0'), '-',"
        f" leftPad({parts}[3], 2, '0'))"
    )
    real_day = f"toString(toDate32OrNull({day})) = {day}"
    if data_type(target_type) == DataType.DATE:
        return guarded_read(
            f"match({value}, '{DATE_PATTERN}') AND {real_day}",
            f"CAST(toDate32OrNull({day}) AS Nullable(Date))",
        )
    if data_type(target_type) in INSTANT_READ_TARGETS:
        # best effort also reads epoch seconds, '20200105' and '24:00:00'
        return guarded_read(
            f"match({value}, '{DATETIME_PATTERN}') AND {real_day}",
            f"CAST(parseDateTime64BestEffortOrNull({text}, 6, 'UTC')"
            f" AS {target if target.startswith('Nullable(') else f'Nullable({target})'})",
        )
    if data_type(target_type) in FLOAT_TYPES:
        return guarded_read(
            f"match({value}, '{FLOAT_PATTERN}')", f"toFloat64OrNull({text})"
        )
    scale = target_type.scale if isinstance(target_type, NumericType) else 0
    if target.startswith("Nullable("):
        target = target[len("Nullable(") : -1]
    number = f"round(toDecimal128OrNull({text}, 18), {scale})"
    return guarded_read(
        f"match({value}, '{NUMBER_PATTERN}') AND {in_range(number, target_type)}",
        f"accurateCastOrNull({number}, '{target}')",
    )


def in_range(number: str, target_type: Any) -> str:
    """`accurateCastOrNull` raises past Int64 from a decimal, and a Decimal
    keeps more whole digits than its precision allows."""
    if isinstance(target_type, NumericType):
        whole = "1" + "0" * (target_type.precision - target_type.scale)
        return f"abs({number}) < toDecimal256('{whole}', 0)"
    low, high = INT64
    return f"{number} BETWEEN {low} AND {high}"


def render_cast(args: list[str], types: list) -> str:
    """A cast keeps NULL: CAST to a bare type raises on one. A fraction cast
    to an integer or a decimal of a smaller scale rounds half away, as `round`
    does; ClickHouse truncates it."""
    value, target = args
    target_type = data_type(types[-1]) if types else None
    if cast_kind(types) == (DataType.STRING, DataType.BOOL):
        return bool_from_text(value)
    if reads_text(types):
        return read_text(value, target, types[-1])
    if target_type not in UNWRAPPABLE_TYPES and not target.startswith("Nullable("):
        target = f"Nullable({target})"
    if (
        len(types) == 2
        and target_type in INTEGER_TYPES
        and data_type(types[0]) in (*FLOAT_TYPES, DataType.NUMERIC)
    ):
        value = round_half_away(value, "0", types[0])
    elif (
        len(types) == 2
        and isinstance(types[1], NumericType)
        and data_type(types[0]) in (*FLOAT_TYPES, DataType.NUMERIC)
    ):
        value = round_half_away(value, str(types[1].scale), types[0])
    return f"CAST({value} AS {target})"


def render_mod(args: list[str], types: list) -> str:
    """`modulo` of a decimal drops its fraction (`7.5 % 3` is 0); the
    truncated remainder `x - y * trunc(x / y)` keeps it."""
    x, y = args[0], guard_divisor(args[1])
    if any(data_type(t) == DataType.NUMERIC for t in types):
        return f"({x} - ({y}) * trunc(({x}) / ({y})))"
    return f"modulo({x}, {y})"


def present_extreme(name: str) -> Callable[[list[str], list], str]:
    """`greatest`/`least` refuse an integer beside a float; the float is the
    type they declare, so the integers are widened to it."""

    def render(args: list[str], types: list) -> str:
        bases = [data_type(t) for t in types]
        if any(b in FLOAT_TYPES for b in bases):
            args = [
                f"toFloat64({a})" if b in INTEGER_TYPES else a
                for a, b in zip(args, bases)
            ]
        return f"{name}({', '.join(args)})"

    return render


def date_trunc(args: list[str], types: list) -> str:
    # a day truncation is a DateTime here; it is typed DATE (`get_date_trunc_output`)
    rendered = f"date_trunc('{args[1]}', {args[0]})"
    return f"toDate({rendered})" if args[1].lower() == "day" else rendered


def date_diff(args: list[str], types: list) -> str:
    # `week` is whole 7-day spans of the day difference; ClickHouse counts
    # Monday boundaries instead
    if args[2].lower() == "week":
        return f"intDiv(dateDiff('day', {args[0]}, {args[1]}), 7)"
    return f"dateDiff('{args[2]}', {args[0]}, {args[1]})"


def null_safe_join_key(
    lval: str, rval: str, modifiers: list[Modifier], jointype: JoinType | None = None
) -> str:
    # ClickHouse finds no join key in the `a = b or (a is null and b is null)`
    # expansion and refuses the join; `<=>` is its null-safe equality
    if Modifier.NULLABLE in modifiers:
        return f"({lval} <=> {rval})"
    return f"{lval} = {rval}"


def nullable_navigation(name: str) -> Callable[[str, str, str, int | None], str]:
    """`lag`/`lead` past the partition's edge return the operand type's
    default (0, '') when the operand is NOT NULL; NULL everywhere else."""
    render = navigation_window_factory(name)

    def wrapped(concept: str, window: str, sort: str, offset: int | None = None):
        return render(f"toNullable({concept})", window, sort, offset)

    return wrapped


def array_to_string(args, types):
    # arrayStringConcat only takes a string array; map other element types
    if array_element_type(types) == DataType.STRING:
        return f"arrayStringConcat({args[0]}, {args[1]})"
    return f"arrayStringConcat(arrayMap(x -> toString(x), {args[0]}), {args[1]})"


# the sentinel group of a two-argument regexp_extract
AUTO_CAPTURE_GROUP = "-1"


def char_length(args: list[str], types: list) -> str:
    """`length` of a String counts bytes here; a string counts characters"""
    if types and get_basic_type(types[0]) == DataType.STRING:
        return f"lengthUTF8({args[0]})"
    return f"length({args[0]})"


def regexp_extract(args: list[str], types: list) -> str:
    # `extract` takes the first group when the pattern has one, else the match
    if len(args) < 3 or args[2] == AUTO_CAPTURE_GROUP:
        return f"extract({args[0]}, {args[1]})"
    return f"regexpExtract({args[0]}, {args[1]}, {args[2]})"


def _ch_log(args, types):
    base = str(args[1]).strip()
    if base in ("2", "2.0"):
        return f"log2({in_domain(args[0], strict=True)})"
    return render_log(args, types)


def _ch_hash(val, hash_type):
    name = hash_type.strip("'\"").lower()
    funcs = {"md5": "MD5", "sha1": "SHA1", "sha256": "SHA256", "sha512": "SHA512"}
    if name not in funcs:
        raise ValueError(f"Unsupported hash type for ClickHouse: {hash_type}")
    return f"lower(hex({funcs[name]}({val})))"


# ClickHouse uses backticks and is mostly forgiving on EXTRACT/CAST syntax,
# but several functions diverge meaningfully from the BaseDialect defaults.
FUNCTION_MAP = {
    FunctionType.BIT_AND: lambda x, types: f"bitAnd({x[0]}, {x[1]})",
    FunctionType.BIT_OR: lambda x, types: f"bitOr({x[0]}, {x[1]})",
    FunctionType.BIT_XOR: lambda x, types: f"bitXor({x[0]}, {x[1]})",
    FunctionType.BIT_SHIFT_LEFT: lambda x, types: f"bitShiftLeft({x[0]}, {x[1]})",
    FunctionType.BIT_SHIFT_RIGHT: lambda x, types: f"bitShiftRight({x[0]}, {x[1]})",
    FunctionType.ARRAY_REDUCE: lambda x, types: f"arrayFold(({x[1]}, {x[2]}) -> {x[3]}, {x[0]}, {x[4]})",
    # constants
    FunctionType.CURRENT_DATE: lambda x, types: "today()",
    FunctionType.CURRENT_DATETIME: lambda x, types: "now()",
    FunctionType.CURRENT_TIMESTAMP: lambda x, types: "now()",
    # math
    FunctionType.POWER: lambda x, types: f"pow({x[0]}, {x[1]})",
    FunctionType.RANDOM: lambda x, types: "randCanonical()",
    FunctionType.LOG: _ch_log,
    FunctionType.MOD: render_mod,
    FunctionType.ROUND: lambda x, types: round_half_away(
        x[0], x[1], types[0] if types else None
    ),
    FunctionType.GREATEST: present_extreme("greatest"),
    FunctionType.LEAST: present_extreme("least"),
    # string
    # the plain string functions count bytes; the UTF8 forms count characters
    FunctionType.LENGTH: char_length,
    FunctionType.SUBSTRING: lambda x, types: render_window_substring(
        "substringUTF8", "lengthUTF8({})", x
    ),
    FunctionType.STRPOS: lambda x, types: f"positionUTF8({x[0]}, {x[1]})",
    FunctionType.UPPER: lambda x, types: f"upperUTF8({x[0]})",
    FunctionType.LOWER: lambda x, types: f"lowerUTF8({x[0]})",
    FunctionType.CONTAINS: lambda x, types: (
        f"has({x[0]}, {x[1]})"
        if is_array_arg(types)
        else f"positionCaseInsensitiveUTF8({x[0]}, {x[1]}) > 0"
    ),
    FunctionType.REGEXP_CONTAINS: lambda x, types: f"match({x[0]}, {x[1]})",
    FunctionType.REGEXP_EXTRACT: regexp_extract,
    FunctionType.REGEXP_REPLACE: lambda x, types: f"replaceRegexpAll({x[0]}, {x[1]}, {x[2]})",
    FunctionType.REPLACE: lambda x, types: f"replaceAll({x[0]}, {x[1]}, {x[2]})",
    FunctionType.SPLIT: lambda x, types: f"splitByString({x[1]}, {x[0]})",
    FunctionType.HASH: lambda x, types: _ch_hash(x[0], x[1]),
    FunctionType.HEX: lambda x, types: f"hex({x[0]})",
    # native concat propagates NULL; wrap to match the null-skipping semantics.
    # concat_ws must drop NULL elements (not render them as '') so the
    # separator is skipped too — filter then assumeNotNull for Array(String).
    **CONCAT_COALESCE_LOWER,
    FunctionType.CONCAT_WS: lambda x, types: (
        "arrayStringConcat(arrayMap(v -> assumeNotNull(v), "
        f"arrayFilter(v -> isNotNull(v), [{', '.join(x[1:])}])), {x[0]})"
    ),
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
    FunctionType.DAY_OF_WEEK: lambda x, types: sunday_first_from_iso(
        f"toDayOfWeek({x[0]})"
    ),
    FunctionType.MONTH: lambda x, types: f"toMonth({x[0]})",
    FunctionType.QUARTER: lambda x, types: f"toQuarter({x[0]})",
    FunctionType.YEAR: lambda x, types: f"toYear({x[0]})",
    FunctionType.MONTH_NAME: lambda x, types: f"dateName('month', {x[0]})",
    FunctionType.DAY_NAME: lambda x, types: f"dateName('weekday', {x[0]})",
    FunctionType.FORMAT_TIME: lambda x, types: f"formatDateTime({x[0]}, {x[1]})",
    FunctionType.PARSE_TIME: lambda x, types: f"parseDateTime({x[0]}, {x[1]})",
    # CH versions ≥25 alias dateAdd/date_add to `plus` (binary +) with a 3-arg
    # mismatch error. INTERVAL syntax is the portable form.
    FunctionType.DATE_TRUNCATE: date_trunc,
    FunctionType.DATE_PART: lambda x, types: date_part(x[0], x[1]),
    FunctionType.DATE_ADD: lambda x, types: f"({x[0]} + INTERVAL ({x[2]}) {x[1]})",
    FunctionType.DATE_SUB: lambda x, types: f"({x[0]} - INTERVAL ({x[2]}) {x[1]})",
    FunctionType.DATE_DIFF: date_diff,
    FunctionType.WEEK: lambda x, types: f"toISOWeek({x[0]})",
    # casts
    FunctionType.CAST: lambda x, types: render_cast(x, types),
    FunctionType.DATE: lambda x, types: f"toDate({x[0]})",
    FunctionType.DATETIME: lambda x, types: f"toDateTime64({x[0]}, 3)",
    FunctionType.TIMESTAMP: lambda x, types: f"toDateTime64({x[0]}, 3)",
    FunctionType.DATE_LITERAL: lambda x, types: f"toDate('{x}')",
    FunctionType.DATETIME_LITERAL: lambda x, types: (
        f"parseDateTime64BestEffort('{x}', 3, 'UTC')"
        if x.tzinfo
        else f"toDateTime64('{x}', 3)"
    ),
    # arrays
    FunctionType.UNNEST: lambda x, types: f"arrayJoin({x[0]})",
    FunctionType.ARRAY_SUM: lambda x, types: f"arraySum({x[0]})",
    FunctionType.ARRAY_DISTINCT: lambda x, types: f"arrayDistinct({x[0]})",
    FunctionType.ARRAY_SORT: lambda x, types: f"arraySort({x[0]})",
    FunctionType.ARRAY_TO_STRING: lambda x, types: array_to_string(x, types),
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


# CAST target type names used in `cast(x as <type>)` and column DDL
DATATYPE_MAP: dict[DataType, str] = {
    DataType.STRING: "String",
    DataType.BYTES: "String",
    DataType.INTEGER: "Int64",
    DataType.BIGINT: "Int64",
    DataType.FLOAT: "Float64",
    DataType.DOUBLE: "Float64",
    DataType.NUMBER: "Float64",
    DataType.BOOL: "Bool",
    DataType.NUMERIC: "Decimal64(4)",
    DataType.DATE: "Date",
    DataType.DATETIME: "DateTime64(3)",
    DataType.TIMESTAMP: "DateTime64(3, 'UTC')",
    DataType.MAP: "Map",
    DataType.ARRAY: "Array",
}

# Reverse map for information_schema column type strings.
# CH returns concrete types like "String", "UInt32", "Nullable(Int64)", etc.
# Nullable wrappers and parametric types are stripped before lookup at the
# call site; this map only needs the base type names.
DB_COLUMN_TYPE_MAP: dict[str, DataType] = {
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
    "date32": DataType.DATE,
    "datetime64": DataType.DATETIME,
    "tuple": DataType.STRUCT,
}


class ClickhouseDialect(BaseDialect):
    FUNCTION_MAP: ClassVar[dict[FunctionType, Callable[..., str]]] = {
        **BaseDialect.FUNCTION_MAP,
        **FUNCTION_MAP,
    }
    DATATYPE_MAP: ClassVar[dict[DataType, str]] = {
        **BaseDialect.DATATYPE_MAP,
        **DATATYPE_MAP,
    }
    DB_COLUMN_TYPE_MAP: ClassVar[dict[str, DataType]] = {
        **BaseDialect.DB_COLUMN_TYPE_MAP,
        **DB_COLUMN_TYPE_MAP,
    }
    # a bound date is sent as a string, which ClickHouse does not coerce in
    # most functions (date_trunc, date arithmetic); render it as a typed literal
    INLINE_PARAM_DATATYPES: ClassVar[frozenset[DataType]] = (
        INLINE_SAFE_PARAM_DATATYPES
        | {DataType.DATE, DataType.DATETIME, DataType.TIMESTAMP}
    )
    NULL_WRAPPER = staticmethod(null_safe_join_key)
    NAVIGATION_WINDOW_FUNCTION_MAP: ClassVar[
        dict[WindowType, Callable[[str, str, str, int | None], str]]
    ] = {
        **NAVIGATION_WINDOW_FUNCTION_MAP,
        WindowType.LAG: nullable_navigation("lag"),
        WindowType.LEAD: nullable_navigation("lead"),
    }
    # `Decimal(p, s)`; DATATYPE_MAP's bare `Decimal64(4)` takes no precision
    NUMERIC_TYPE_NAME = "Decimal"
    QUOTE_CHARACTER = "`"
    # LIKE escapes with a backslash natively and takes no ESCAPE clause
    LIKE_ESCAPE_CLAUSE = False
    STRING_LITERAL_BACKSLASH_ESCAPES = True
    SUPPORTS_QUALIFY = True
    # CH doesn't accept arrayJoin as a FROM-clause table function; DIRECT mode
    # emits `SELECT arrayJoin(...) AS alias` with no FROM, which CH supports.
    UNNEST_MODE = UnnestMode.DIRECT
    SUPPORTS_AGGREGATE_GROUPING_MODES = True
    TABLE_NOT_FOUND_PATTERN = "Table .* doesn't exist"
    COLUMN_NOT_FOUND_PATTERN = "Unknown identifier"
    TRANSACTIONAL_DDL = False

    def render_column_type(self, column: ColumnInfo, target: CreateTableInfo) -> str:
        """A column is NOT NULL unless declared Nullable, and would store a
        NULL as 0 or ''."""
        rendered = super().render_column_type(column, target)
        if column.nullable and data_type(column.type) not in UNWRAPPABLE_TYPES:
            return f"Nullable({rendered})"
        return rendered

    def _render_membership_exists(
        self,
        left_sql: str,
        member_sql: str,
        from_clause: str,
        operator: ComparisonOperator,
        null_free: bool = False,
    ) -> str:
        """Identity membership without a correlated subquery: ClickHouse
        decorrelates `exists (... where m <=> x)` into a join that drops the
        NULL match, so a NULL probe matched nothing and `not in` kept it. A
        NULL probe's `IN` is NULL, so it is read as FALSE."""
        members = f"select {member_sql} from {from_clause}"
        if null_free:
            matched = f"coalesce({left_sql} IN ({members}), false)"
        else:
            matched = (
                f"(coalesce({left_sql} IN ({members} where {member_sql} is not null),"
                f" false) OR {left_sql} IS NULL"
                f" AND exists ({members} where {member_sql} is null))"
            )
        if operator == ComparisonOperator.NOT_IN:
            return f"not {matched}"
        return matched

    def render_settings(self, query: "ProcessedQuery") -> str:
        """An outer join pads with each type's default (0, '') unless
        join_use_nulls is set; the padded side is NULL everywhere else."""
        if pads_outer_rows(query.ctes):
            return "\nSETTINGS join_use_nulls = 1"
        return ""

    def normalize_db_type(self, db_type: str) -> DataType:
        # DateTime64(3, 'UTC'): a zone argument is what makes the column an instant
        base = super().normalize_db_type(db_type)
        if base == DataType.DATETIME and "," in db_type:
            return DataType.TIMESTAMP
        return base

    def render_empty_copy(self, target: str, staged: str) -> list[str]:
        """``CREATE TABLE ... AS other`` copies structure and engine, no rows."""
        target, staged = self.safe_quote(target), self.safe_quote(staged)
        return [f"CREATE TABLE {staged} AS {target}"]

    def render_table_swap(self, target: str, staged: str, replace: bool) -> list[str]:
        """``EXCHANGE TABLES`` is atomic but needs both sides to exist, so an
        empty target is created first when it may be missing. The old rows end
        up under the staging name, which the executor drops."""
        target, staged = self.safe_quote(target), self.safe_quote(staged)
        if not replace:
            return [f"RENAME TABLE {staged} TO {target}"]
        return [
            f"CREATE TABLE IF NOT EXISTS {target} AS {staged}",
            f"EXCHANGE TABLES {target} AND {staged}",
        ]

    def render_like_pattern(self, pattern, render: Callable[[Any], str]) -> str:
        """ClickHouse honours an escape only before `%`, `_` and `\\`, and
        keeps the backslash before any other character; drop it there."""
        if isinstance(pattern, str):
            return super().render_like_pattern(pattern, render)
        regex = self.render_string_literal(r"\\([^%_\\])")
        group = self.render_string_literal(r"\1")
        return f"replaceRegexpAll({render(pattern)}, {regex}, {group})"

    def render_string_literal(self, value: str) -> str:
        # ClickHouse reads a backslash in a string literal as an escape.
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

    def render_map_literal(
        self,
        e: "MapWrapper[Any, Any]",
        cte: "CTE | UnionCTE | None" = None,
        raise_invalid: bool = False,
    ) -> str:
        # CH uses map(k1, v1, k2, v2, ...). Avoids the `key:value` syntax that
        # SQLAlchemy's text() reads as bound parameters.
        parts: list[str] = []
        for k, v in e.items():
            parts.append(self.render_expr(k, cte=cte, raise_invalid=raise_invalid))
            parts.append(self.render_expr(v, cte=cte, raise_invalid=raise_invalid))
        return f"map({', '.join(parts)})"
