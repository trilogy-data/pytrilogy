import re
from collections.abc import Callable
from typing import Any, ClassVar

from trilogy.core.enums import FunctionType, NullSort, UnnestMode
from trilogy.core.models.core import DataType, MapWrapper
from trilogy.core.models.execute import CTE, UnionCTE
from trilogy.dialect.base import (
    CALENDAR_PARTS,
    INLINE_SAFE_PARAM_DATATYPES,
    BaseDialect,
    TableColumn,
    array_element_type,
    is_array_arg,
    offset_keeps_date,
)
from trilogy.dialect.common import CONCAT_COALESCE_UPPER, SQL_STANDARD_EXTRACT


def date_trunc(args: list[str], types: list) -> str:
    # date_trunc always yields a TIMESTAMP; day and coarser are typed DATE
    rendered = f"date_trunc('{args[1]}', {args[0]})"
    if args[1].lower() in CALENDAR_PARTS:
        return f"cast({rendered} as DATE)"
    if types and types[0] == DataType.DATETIME:
        return f"cast({rendered} as TIMESTAMP_NTZ)"
    return rendered


def date_offset(args: list[str], types: list, sign: str) -> str:
    rendered = f"timestampadd({args[1]}, {sign}({args[2]}), {args[0]})"
    if offset_keeps_date(args, types):
        return f"cast({rendered} as DATE)"
    return rendered


def date_diff(args: list[str], types: list) -> str:
    # timestampdiff counts whole units elapsed; truncating both ends counts
    # boundaries crossed, as every other dialect does
    part = args[2]
    return (
        f"timestampdiff({part}, date_trunc('{part}', {args[0]}),"
        f" date_trunc('{part}', {args[1]}))"
    )


def date_part(args: list[str], types: list) -> str:
    if args[1] == "day_of_week":
        return f"dayofweek({args[0]})"
    if args[1] == "week":
        return f"weekofyear({args[0]})"
    if args[1] == "second":
        return f"second({args[0]})"
    return f"EXTRACT({args[1]} FROM {args[0]})"


def contains(args: list[str], types: list) -> str:
    if is_array_arg(types):
        return f"array_contains({args[0]}, {args[1]})"
    return f"contains(lower({args[0]}), lower({args[1]}))"


def array_agg(args: list[str], types: list) -> str:
    # collect_list drops NULLs, and an empty group is an empty array, not NULL
    return f"nullif(collect_list({args[0]}), array())"


# the parser's marker for "group 1 when the pattern has one, else the match"
AUTO_CAPTURE_GROUP = "-1"


def regexp_extract(args: list[str], types: list) -> str:
    group = args[2]
    if str(group) == AUTO_CAPTURE_GROUP:
        group = "1" if re.compile(args[1]).groups else "0"
    return f"regexp_extract({args[0]}, {args[1]}, {group})"


def array_sum(args: list[str], types: list) -> str:
    # the fold's zero must share the element type; no present element is NULL
    element = array_element_type(types)
    zero = (
        DATATYPE_MAP.get(element, "DOUBLE")
        if isinstance(element, DataType)
        else "DOUBLE"
    )
    present = f"filter({args[0]}, e -> e is not null)"
    return (
        f"CASE WHEN size({present}) = 0 THEN NULL"
        f" ELSE aggregate({present}, cast(0 as {zero}), (acc, e) -> acc + e) END"
    )


# strftime directives as Java DateTimeFormatter letters
STRFTIME_TO_JAVA = {
    "Y": "yyyy",
    "y": "yy",
    "m": "MM",
    "d": "dd",
    "H": "HH",
    "I": "hh",
    "M": "mm",
    "S": "ss",
    "f": "SSSSSS",
    "p": "a",
    "B": "MMMM",
    "b": "MMM",
    "A": "EEEE",
    "a": "EEE",
    "j": "DDD",
    "z": "Z",
    "Z": "z",
}


def java_format(literal: str) -> str:
    """A strftime format literal as a Java pattern; text between directives is
    quoted, since Java reads every ASCII letter as a field."""
    if not (literal.startswith("'") and literal.endswith("'")):
        raise NotImplementedError(
            "Databricks formats need a literal strftime pattern to translate"
        )
    out: list[str] = []
    text = ""
    tokens = re.split(r"(%.)", literal[1:-1])
    for token in tokens:
        if len(token) == 2 and token[0] == "%":
            if token == "%%":
                text += "%"
                continue
            if token[1] not in STRFTIME_TO_JAVA:
                raise NotImplementedError(f"Unsupported format directive {token}")
            if text:
                out.append(_java_text(text))
                text = ""
            out.append(STRFTIME_TO_JAVA[token[1]])
        else:
            text += token
    if text:
        out.append(_java_text(text))
    return "'" + "".join(out) + "'"


def _java_text(text: str) -> str:
    if not re.search(r"[A-Za-z']", text):
        return text
    # inside the SQL literal the quote is backslash-escaped
    return "\\'" + text.replace("'", "\\'\\'") + "\\'"


FUNCTION_MAP: dict[FunctionType, Callable[..., str]] = {
    **SQL_STANDARD_EXTRACT,
    **CONCAT_COALESCE_UPPER,
    # EXTRACT(SECOND) carries the fraction
    FunctionType.SECOND: lambda x, types: f"second({x[0]})",
    # 1-based, and NULL out of range where `[]` raises under ANSI
    FunctionType.INDEX_ACCESS: lambda x, types: f"try_element_at({x[0]}, {x[1]})",
    FunctionType.MAP_ACCESS: lambda x, types: f"try_element_at({x[0]}, {x[1]})",
    FunctionType.DAY_OF_WEEK: lambda x, types: f"dayofweek({x[0]})",
    FunctionType.WEEK: lambda x, types: f"weekofyear({x[0]})",
    FunctionType.DATE_TRUNCATE: date_trunc,
    FunctionType.DATE_PART: date_part,
    FunctionType.DATE_ADD: lambda x, types: date_offset(x, types, ""),
    FunctionType.DATE_SUB: lambda x, types: date_offset(x, types, "-"),
    FunctionType.DATE_DIFF: date_diff,
    FunctionType.DATE: lambda x, types: f"cast({x[0]} as DATE)",
    FunctionType.DATETIME: lambda x, types: f"cast({x[0]} as TIMESTAMP_NTZ)",
    FunctionType.TIMESTAMP: lambda x, types: f"cast({x[0]} as TIMESTAMP)",
    FunctionType.DATE_LITERAL: lambda x, types: f"DATE '{x}'",
    FunctionType.DATETIME_LITERAL: lambda x, types: (
        f"TIMESTAMP '{x}'" if x.tzinfo else f"TIMESTAMP_NTZ '{x}'"
    ),
    FunctionType.CURRENT_DATETIME: lambda x, types: "cast(current_timestamp() as TIMESTAMP_NTZ)",
    FunctionType.DAY_NAME: lambda x, types: f"date_format({x[0]}, 'EEEE')",
    FunctionType.MONTH_NAME: lambda x, types: f"date_format({x[0]}, 'MMMM')",
    FunctionType.FORMAT_TIME: lambda x, types: f"date_format({x[0]}, {java_format(x[1])})",
    FunctionType.PARSE_TIME: lambda x, types: f"to_timestamp({x[0]}, {java_format(x[1])})",
    # string
    FunctionType.CONTAINS: contains,
    FunctionType.STRPOS: lambda x, types: f"instr({x[0]}, {x[1]})",
    FunctionType.REGEXP_CONTAINS: lambda x, types: f"regexp_like({x[0]}, {x[1]})",
    FunctionType.REGEXP_EXTRACT: regexp_extract,
    FunctionType.SPLIT: lambda x, types: f"split({x[0]}, concat('\\\\Q', {x[1]}, '\\\\E'))",
    FunctionType.LENGTH: lambda x, types: (
        f"size({x[0]})" if is_array_arg(types) else f"length({x[0]})"
    ),
    FunctionType.HASH: lambda x, types: _hash(x[0], x[1]),
    # math
    FunctionType.RANDOM: lambda x, types: "rand()",
    # bitwise: `^` is xor, shifts are functions
    FunctionType.BIT_SHIFT_LEFT: lambda x, types: f"shiftleft({x[0]}, {x[1]})",
    FunctionType.BIT_SHIFT_RIGHT: lambda x, types: f"shiftright({x[0]}, {x[1]})",
    # aggregate
    FunctionType.ARRAY_AGG: array_agg,
    FunctionType.ANY: lambda x, types: f"any_value({x[0]})",
    # arrays: an explode in the select list is a generator, one row per element
    FunctionType.UNNEST: lambda x, types: f"explode({x[0]})",
    FunctionType.ARRAY: lambda x, types: f"array({', '.join(x)})",
    FunctionType.GENERATE_ARRAY: lambda x, types: f"sequence({x[0]}, {x[1]}, {x[2]})",
    FunctionType.DATE_SPINE: lambda x, types: (
        f"explode(sequence({x[0]}, {x[1]}, INTERVAL 1 DAY))"
    ),
    FunctionType.ARRAY_SUM: array_sum,
    FunctionType.ARRAY_TRANSFORM: lambda x, types: f"transform({x[0]}, {x[1]} -> {x[2]})",
    FunctionType.ARRAY_FILTER: lambda x, types: f"filter({x[0]}, {x[1]} -> {x[2]})",
    FunctionType.ARRAY_REDUCE: lambda x, types: (
        f"aggregate({x[0]}, {x[4]}, ({x[1]}, {x[2]}) -> {x[3]})"
    ),
    FunctionType.ARRAY_TO_STRING: lambda x, types: f"array_join({x[0]}, {x[1]})",
    FunctionType.STRUCT: lambda x, types: (
        "named_struct("
        + ", ".join(f"'{name}', {value}" for value, name in zip(x[::2], x[1::2]))
        + ")"
    ),
}


def _hash(value: str, hash_type: str) -> str:
    kind = hash_type.strip("'").lower()
    if kind == "md5":
        return f"md5({value})"
    if kind == "sha1":
        return f"sha1({value})"
    if kind == "sha256":
        return f"sha2({value}, 256)"
    if kind == "sha512":
        return f"sha2({value}, 512)"
    raise ValueError(f"Unsupported hash type: {hash_type}")


DATATYPE_MAP: dict[DataType, str] = {
    DataType.STRING: "STRING",
    DataType.BYTES: "BINARY",
    DataType.INTEGER: "INT",
    DataType.BIGINT: "BIGINT",
    # Trilogy's float is a double; Spark's FLOAT is 4 bytes
    DataType.FLOAT: "DOUBLE",
    DataType.DOUBLE: "DOUBLE",
    DataType.BOOL: "BOOLEAN",
    DataType.NUMERIC: "DECIMAL",
    DataType.MAP: "MAP",
    DataType.DATE: "DATE",
    DataType.DATETIME: "TIMESTAMP_NTZ",
    DataType.TIMESTAMP: "TIMESTAMP",
}

DB_COLUMN_TYPE_MAP: dict[str, DataType] = {
    "boolean": DataType.BOOL,
    "byte": DataType.INTEGER,
    "short": DataType.INTEGER,
    "timestamp": DataType.TIMESTAMP,
    "timestamp_ntz": DataType.DATETIME,
    "binary": DataType.BYTES,
}


class DatabricksDialect(BaseDialect):
    """Databricks SQL (Spark SQL in ANSI mode) over Unity Catalog."""

    NULL_SORT = NullSort.SMALLEST
    FUNCTION_MAP: ClassVar[dict[FunctionType, Callable[..., str]]] = {
        **BaseDialect.FUNCTION_MAP,
        **FUNCTION_MAP,
    }
    DATATYPE_MAP: ClassVar[dict[DataType, str]] = {
        **BaseDialect.DATATYPE_MAP,
        **DATATYPE_MAP,
    }
    COMPLEX_DATATYPE_MAP: ClassVar[dict[DataType, Callable[[str], str]]] = {
        DataType.ARRAY: lambda x: f"ARRAY<{x}>",
    }
    DB_COLUMN_TYPE_MAP: ClassVar[dict[str, DataType]] = {
        **BaseDialect.DB_COLUMN_TYPE_MAP,
        **DB_COLUMN_TYPE_MAP,
    }
    QUOTE_CHARACTER = "`"
    STRING_LITERAL_BACKSLASH_ESCAPES = True
    UNNEST_MODE = UnnestMode.DIRECT
    SUPPORTS_AGGREGATE_GROUPING_MODES = True
    SUPPORTS_QUALIFY = True
    # Delta commits each statement on its own; there is no multi-statement
    # transaction to hold DDL.
    TRANSACTIONAL_DDL = False
    TABLE_NOT_FOUND_PATTERN = "TABLE_OR_VIEW_NOT_FOUND"
    COLUMN_NOT_FOUND_PATTERN = "UNRESOLVED_COLUMN"
    # the driver binds a list or dict as an empty value; render it inline
    INLINE_PARAM_DATATYPES: ClassVar[frozenset[DataType]] = (
        INLINE_SAFE_PARAM_DATATYPES | {DataType.ARRAY, DataType.MAP, DataType.STRUCT}
    )

    def render_string_literal(self, value: str) -> str:
        # Backslash escapes, and `'a''b'` is two adjacent literals that
        # concatenate to `ab`, so the quote is backslash-escaped too.
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

    def render_map_literal(
        self,
        e: MapWrapper[Any, Any],
        cte: CTE | UnionCTE | None = None,
        raise_invalid: bool = False,
    ) -> str:
        parts = [
            self.render_expr(x, cte=cte, raise_invalid=raise_invalid)
            for pair in e.items()
            for x in pair
        ]
        return f"map({', '.join(parts)})"

    def render_staging_drop(self, staged: str) -> str:
        # a bare DROP TABLE refuses to resolve to a temporary table
        return f"DROP TEMPORARY TABLE {staged}"

    def hash_column_value(self, column_name: str) -> str:
        return f"md5(CAST({self.safe_quote(column_name)} AS STRING))"

    def _table_scope(
        self, table_name: str, schema: str | None
    ) -> tuple[str, str, str | None]:
        """The information_schema to read (it is per catalog), the bare table
        name, and the schema; a `catalog.schema` qualifier reads that
        catalog's, a bare schema the session's current one."""
        catalog = None
        if schema and "." in schema:
            catalog, schema = schema.split(".", 1)
        elif not schema and table_name.count(".") == 2:
            catalog, schema, table_name = table_name.split(".")
        elif not schema and "." in table_name:
            schema, table_name = table_name.split(".", 1)
        info = (
            f"{self.quote(catalog)}.information_schema"
            if catalog
            else "information_schema"
        )
        return info, table_name, schema

    def _table_filter(self, alias: str, table_name: str, schema: str | None) -> str:
        clause = f"lower({alias}.table_name) = lower({self.render_string_literal(table_name)})"
        if schema:
            clause += f" AND lower({alias}.table_schema) = lower({self.render_string_literal(schema)})"
        return clause

    def get_table_schema(
        self, executor: Any, table_name: str, schema: str | None = None
    ) -> list[TableColumn]:
        info, table_name, schema = self._table_scope(table_name, schema)
        query = f"""
        SELECT c.column_name, c.data_type, c.is_nullable, c.comment
        FROM {info}.columns c
        WHERE {self._table_filter("c", table_name, schema)}
        ORDER BY c.ordinal_position"""
        return self._columns_from_info_schema_rows(
            executor.execute_raw_sql(query).fetchall()
        )

    def get_table_primary_keys(
        self, executor: Any, table_name: str, schema: str | None = None
    ) -> list[str]:
        """Unity Catalog's informational (unenforced) PRIMARY KEY constraint."""
        info, table_name, schema = self._table_scope(table_name, schema)
        query = f"""
        SELECT k.column_name
        FROM {info}.table_constraints t
        JOIN {info}.key_column_usage k
          ON k.constraint_catalog = t.constraint_catalog
         AND k.constraint_schema = t.constraint_schema
         AND k.constraint_name = t.constraint_name
        WHERE t.constraint_type = 'PRIMARY KEY'
          AND {self._table_filter("t", table_name, schema)}
        ORDER BY k.ordinal_position"""
        return [r[0] for r in executor.execute_raw_sql(query).fetchall()]
