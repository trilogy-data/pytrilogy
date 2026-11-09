from collections.abc import Callable
from typing import Any, ClassVar

from trilogy.core.casts import (
    DATE_PATTERN,
    INSTANT_READ_TARGETS,
    INT64,
    NUMBER_PATTERN,
)
from trilogy.core.enums import (
    CreateMode,
    DatePart,
    FunctionType,
    JoinType,
    Modifier,
    NullSort,
)
from trilogy.core.models.core import DataType, NumericType
from trilogy.core.statements.execute import CreateTableInfo
from trilogy.dialect.base import (
    BaseDialect,
    TableColumn,
    render_present_extreme,
    render_unsupported,
)
from trilogy.dialect.common import (
    CONCAT_COALESCE_UPPER,
    bool_from_text,
    cast_kind,
    guarded_read,
    reads_text,
    render_window_substring,
    text_from_bool,
)


def date_truncate(expr: str, part: str) -> str:
    grain = DatePart(part)
    formats = {
        DatePart.YEAR: "%Y-01-01 00:00:00",
        DatePart.MONTH: "%Y-%m-01 00:00:00",
        DatePart.DAY: "%Y-%m-%d 00:00:00",
        DatePart.HOUR: "%Y-%m-%d %H:00:00",
        DatePart.MINUTE: "%Y-%m-%d %H:%i:00",
        DatePart.SECOND: "%Y-%m-%d %H:%i:%s",
    }
    if grain in formats:
        return f"TIMESTAMP(DATE_FORMAT({expr}, '{formats[grain]}'))"
    if grain == DatePart.WEEK:
        return f"TIMESTAMP(DATE_SUB(DATE({expr}), INTERVAL WEEKDAY({expr}) DAY))"
    if grain == DatePart.QUARTER:
        return (
            f"TIMESTAMP(DATE_ADD(MAKEDATE(YEAR({expr}), 1), "
            f"INTERVAL (QUARTER({expr}) - 1) QUARTER))"
        )
    raise NotImplementedError(f"Date truncation not implemented for grain {grain}")


def date_interval(function: str, args: list[str]) -> str:
    grain = DatePart(args[1]).value.upper()
    return f"{function}({args[0]}, INTERVAL ({args[2]}) {grain})"


TRUNCATED_FORMAT = {
    DatePart.HOUR: "%Y-%m-%d %H:00:00",
    DatePart.MINUTE: "%Y-%m-%d %H:%i:00",
    DatePart.SECOND: "%Y-%m-%d %H:%i:%s",
}


def date_diff(first: str, second: str, part: str) -> str:
    """The part boundaries crossed, as DuckDB counts them; TIMESTAMPDIFF
    counts whole elapsed units."""
    grain = DatePart(part)
    years = f"(YEAR({second}) - YEAR({first}))"
    if grain == DatePart.YEAR:
        return years
    if grain == DatePart.QUARTER:
        return f"({years} * 4 + QUARTER({second}) - QUARTER({first}))"
    if grain == DatePart.MONTH:
        return f"({years} * 12 + MONTH({second}) - MONTH({first}))"
    if grain == DatePart.DAY:
        return f"DATEDIFF({second}, {first})"
    if grain == DatePart.WEEK:
        return f"(DATEDIFF({second}, {first}) DIV 7)"
    if grain in TRUNCATED_FORMAT:
        fmt = TRUNCATED_FORMAT[grain]
        return (
            f"TIMESTAMPDIFF({grain.value.upper()}, DATE_FORMAT({first}, '{fmt}'),"
            f" DATE_FORMAT({second}, '{fmt}'))"
        )
    raise NotImplementedError(f"Date diff not implemented for grain {grain}")


# TEXT, INTEGER, BIGINT, BOOLEAN and TIMESTAMP are column types but not CAST
# targets; a bool is a TINYINT, so it casts to '1' and from 'true' as 0
CAST_TARGETS = {
    "TEXT": "CHAR",
    "INTEGER": "SIGNED",
    "BIGINT": "SIGNED",
    "TIMESTAMP": "DATETIME",
}


def read_text(value: str, target: str, types: list) -> str:
    """CAST reads a numeric prefix ('abc' AS SIGNED is 0); the reference's
    shape is required, and an integer is read through DECIMAL, which rounds
    half away where SIGNED truncates. There is no infinity, so a float reads
    numerals only."""
    text = f"REGEXP_REPLACE({value}, '^[[:space:]]+|[[:space:]]+$', '')"
    if cast_kind(types)[1] == DataType.DATE:
        day = f"SUBSTRING_INDEX(SUBSTRING_INDEX({text}, ' ', 1), 'T', 1)"
        read = f"STR_TO_DATE(REPLACE({day}, '/', '-'), '%Y-%c-%e')"
        return guarded_read(f"{value} REGEXP '{DATE_PATTERN}'", read)
    if cast_kind(types)[1] in INSTANT_READ_TARGETS:
        # CAST reads only a `+HH:MM` zone, into the session's zone; a zone is
        # left unread (NULL) here
        read = f"CAST(REPLACE({text}, '/', '-') AS DATETIME(6))"
        return guarded_read(f"{value} REGEXP '{DATE_PATTERN}'", read)
    matches = f"{value} REGEXP '{NUMBER_PATTERN}'"
    exact = f"CAST({text} AS DECIMAL(65, 30))"
    target_type = types[-1]
    # CAST saturates past the target's range
    if target.upper() in ("INTEGER", "BIGINT"):
        low, high = INT64
        matches += f" AND {exact} > {low}.5 AND {exact} < {high}.5"
        read = f"CAST({exact} AS SIGNED)"
    else:
        if isinstance(target_type, NumericType):
            whole = target_type.precision - target_type.scale
            matches += f" AND ABS(ROUND({exact}, {target_type.scale})) < 1e{whole}"
        read = f"CAST({text} AS {target})"
    return guarded_read(matches, read)


def render_cast(args: list[str], types: list) -> str:
    source, target = cast_kind(types)
    if reads_text(types):
        return read_text(args[0], args[1], types)
    if source == DataType.BOOL and target == DataType.STRING:
        return text_from_bool(args[0])
    if target == DataType.BOOL:
        if source == DataType.STRING:
            return bool_from_text(args[0])
        return args[0] if source == DataType.BOOL else f"({args[0]} <> 0)"
    return f"CAST({args[0]} AS {CAST_TARGETS.get(args[1].upper(), args[1])})"


def null_safe_join_key(
    lval: str, rval: str, modifiers: list[Modifier], jointype: JoinType | None = None
) -> str:
    # `<=>` is MySQL's null-safe equality. The base dialect's OR-expansion is
    # equivalent but never index-assisted, and every lowered FULL join emits one
    # of these per key.
    if Modifier.NULLABLE in modifiers:
        return f"({lval} <=> {rval})"
    return f"{lval} = {rval}"


FUNCTION_MAP = {
    FunctionType.GREATEST: render_present_extreme("GREATEST"),
    FunctionType.LEAST: render_present_extreme("LEAST"),
    # no fold over an array
    FunctionType.ARRAY_REDUCE: render_unsupported("array_reduce"),
    FunctionType.IS_NOT_DISTINCT: lambda x, types: f"({x[0]} <=> {x[1]})",
    FunctionType.POWER: lambda x, types: f"POWER({x[0]}, {x[1]})",
    FunctionType.RANDOM: lambda x, types: "RAND()",
    FunctionType.BOOL_OR: lambda x, types: f"MAX(CAST({x[0]} AS UNSIGNED))",
    FunctionType.BOOL_AND: lambda x, types: f"MIN(CAST({x[0]} AS UNSIGNED))",
    FunctionType.STRPOS: lambda x, types: f"LOCATE({x[1]}, {x[0]})",
    # LENGTH counts bytes; CHAR_LENGTH counts characters
    FunctionType.LENGTH: lambda x, types: f"CHAR_LENGTH({x[0]})",
    FunctionType.SUBSTRING: lambda x, types: render_window_substring(
        "SUBSTRING", "CHAR_LENGTH({})", x
    ),
    FunctionType.CONTAINS: lambda x, types: f"(LOCATE(LOWER({x[1]}), LOWER({x[0]})) > 0)",
    **CONCAT_COALESCE_UPPER,
    FunctionType.CONCAT_STRICT: lambda x, types: f"CONCAT({', '.join(x)})",
    FunctionType.CAST: render_cast,
    FunctionType.DATE_LITERAL: lambda x, types: f"DATE('{x}')",
    FunctionType.DATETIME_LITERAL: lambda x, types: f"TIMESTAMP('{x}')",
    FunctionType.DATE_TRUNCATE: lambda x, types: date_truncate(x[0], x[1]),
    FunctionType.DATE_ADD: lambda x, types: date_interval("DATE_ADD", x),
    FunctionType.DATE_SUB: lambda x, types: date_interval("DATE_SUB", x),
    FunctionType.DATE_PART: lambda x, types: (
        f"DAYOFWEEK({x[0]})"
        if DatePart(x[1]) == DatePart.DAY_OF_WEEK
        # mode 3 is the ISO week; EXTRACT(WEEK) counts Sunday weeks from 0
        else (
            f"WEEK({x[0]}, 3)"
            if DatePart(x[1]) == DatePart.WEEK
            else f"EXTRACT({DatePart(x[1]).value.upper()} FROM {x[0]})"
        )
    ),
    FunctionType.WEEK: lambda x, types: f"WEEK({x[0]}, 3)",
    FunctionType.DATE_DIFF: lambda x, types: date_diff(x[0], x[1], x[2]),
    FunctionType.DAY_OF_WEEK: lambda x, types: f"DAYOFWEEK({x[0]})",
    FunctionType.FORMAT_TIME: lambda x, types: f"DATE_FORMAT({x[0]}, {x[1]})",
    FunctionType.PARSE_TIME: lambda x, types: f"STR_TO_DATE({x[0]}, {x[1]})",
    FunctionType.CURRENT_DATETIME: lambda x, types: "CURRENT_TIMESTAMP()",
}

DATATYPE_MAP = {
    DataType.STRING: "TEXT",
    DataType.BYTES: "BLOB",
    DataType.INTEGER: "INTEGER",
    DataType.BIGINT: "BIGINT",
    # FLOAT is 4 bytes here; Trilogy's float is a double
    DataType.FLOAT: "DOUBLE",
    DataType.DOUBLE: "DOUBLE",
    DataType.BOOL: "BOOLEAN",
    DataType.NUMERIC: "DECIMAL",
    DataType.DATE: "DATE",
    DataType.DATETIME: "DATETIME",
    DataType.TIMESTAMP: "TIMESTAMP",
}


class MySQLDialect(BaseDialect):
    NULL_SORT = NullSort.SMALLEST
    NULLS_ORDER_SYNTAX = False
    ILIKE_AS_LOWER_LIKE = ("LOWER", "LIKE", "NOT ")
    STRING_LITERAL_BACKSLASH_ESCAPES = True
    FUNCTION_MAP: ClassVar[dict[FunctionType, Callable[..., str]]] = {
        **BaseDialect.FUNCTION_MAP,
        **FUNCTION_MAP,
    }
    DATATYPE_MAP: ClassVar[dict[DataType, str]] = {
        **BaseDialect.DATATYPE_MAP,
        **DATATYPE_MAP,
    }
    # TIMESTAMP is MySQL's instant type (stored UTC, read in the session zone),
    # so it is what a trilogy timestamp is written as and read back as.
    DB_COLUMN_TYPE_MAP: ClassVar[dict[str, DataType]] = {
        **BaseDialect.DB_COLUMN_TYPE_MAP,
        "timestamp": DataType.TIMESTAMP,
    }
    QUOTE_CHARACTER = "`"
    SUPPORTS_ALIAS_IN_HAVING = True
    SUPPORTS_FULL_JOIN = False
    SUPPORTS_ARRAYS = False
    NULL_WRAPPER = staticmethod(null_safe_join_key)
    TABLE_NOT_FOUND_PATTERN = "doesn't exist"
    COLUMN_NOT_FOUND_PATTERN = "unknown column"
    # DDL causes an implicit commit.
    TRANSACTIONAL_DDL = False

    def render_empty_copy(self, target: str, staged: str) -> list[str]:
        target, staged = self.safe_quote(target), self.safe_quote(staged)
        return [f"CREATE TABLE {staged} LIKE {target}"]

    def render_table_swap(self, target: str, staged: str, replace: bool) -> list[str]:
        """A multi-table ``RENAME TABLE`` is atomic; the old table is moved out
        under a retired name in the same step and dropped afterwards."""
        retired = self.safe_quote(f"{staged}_old")
        target, staged = self.safe_quote(target), self.safe_quote(staged)
        if not replace:
            return [f"RENAME TABLE {staged} TO {target}"]
        return [
            f"CREATE TABLE IF NOT EXISTS {target} LIKE {staged}",
            f"RENAME TABLE {target} TO {retired}, {staged} TO {target}",
            f"DROP TABLE {retired}",
        ]

    def partition_key_match(
        self, left: str, right: str, partition_by: list[str]
    ) -> str:
        """MySQL spells null-safe equality ``<=>``, so the base's explicit
        ``IS NULL`` arms collapse to one operator."""
        return " AND ".join(
            f"{left}.{self.quote(key)} <=> {right}.{self.quote(key)}"
            for key in partition_by
        )

    def render_partition_delete(
        self, target: str, staged: str, partition_by: list[str]
    ) -> str:
        """MySQL rejects the base's correlated ``EXISTS`` ("You can't specify
        target table for delete"), so the key match rides a multi-table DELETE
        join instead. The table name doubles as its own alias."""
        matches = self.partition_key_match(target, staged, partition_by)
        return f"DELETE {target} FROM {target} JOIN {staged} ON {matches}"

    def render_null_flag(self, rendered: str) -> str:
        return f"({rendered}) IS NULL"

    def render_string_literal(self, value: str) -> str:
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

    def compile_create_table_statements(
        self, target: CreateTableInfo, create_mode: CreateMode
    ) -> list[str]:
        statements = super().compile_create_table_statements(target, create_mode)
        if create_mode == CreateMode.CREATE_OR_REPLACE:
            return [
                f"DROP TABLE IF EXISTS {self.safe_quote(target.name)};",
                *[
                    s.replace("CREATE OR REPLACE TABLE", "CREATE TABLE", 1)
                    for s in statements
                ],
            ]
        return statements

    def get_table_schema(
        self, executor: Any, table_name: str, schema: str | None = None
    ) -> list[TableColumn]:
        schema_filter = (
            f"table_schema = '{schema}'" if schema else "table_schema = DATABASE()"
        )
        query = f"""
        SELECT column_name, data_type, is_nullable, column_comment
        FROM information_schema.columns
        WHERE table_name = '{table_name}' AND {schema_filter}
        ORDER BY ordinal_position
        """
        return self._columns_from_info_schema_rows(
            executor.execute_raw_sql(query).fetchall()
        )

    def get_table_primary_keys(
        self, executor: Any, table_name: str, schema: str | None = None
    ) -> list[str]:
        schema_filter = (
            f"table_schema = '{schema}'" if schema else "table_schema = DATABASE()"
        )
        query = f"""
        SELECT column_name
        FROM information_schema.key_column_usage
        WHERE table_name = '{table_name}'
          AND constraint_name = 'PRIMARY'
          AND {schema_filter}
        ORDER BY ordinal_position
        """
        return [row[0] for row in executor.execute_raw_sql(query).fetchall()]

    def list_tables(
        self, executor: Any, schema: str | None = None
    ) -> list[tuple[str, str]]:
        schema_filter = (
            f"table_schema = '{schema}'" if schema else "table_schema = DATABASE()"
        )
        query = f"""
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE {schema_filter}
        ORDER BY table_name
        """
        return executor.execute_raw_sql(query).fetchall()
