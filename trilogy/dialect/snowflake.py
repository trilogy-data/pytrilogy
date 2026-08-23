from collections.abc import Callable
from typing import ClassVar

from trilogy.core.enums import FunctionType, UnnestMode
from trilogy.core.models.core import CONCRETE_TYPES, DataType
from trilogy.dialect.base import BaseDialect, TableColumn

FUNCTION_MAP = {
    FunctionType.MINUTE: lambda x, types: f"EXTRACT(MINUTE from {x[0]})",
    FunctionType.SECOND: lambda x, types: f"EXTRACT(SECOND from {x[0]})",
    FunctionType.HOUR: lambda x, types: f"EXTRACT(HOUR from {x[0]})",
    FunctionType.DAY_OF_WEEK: lambda x, types: f"EXTRACT(DAYOFWEEK from {x[0]})",
    FunctionType.DAY: lambda x, types: f"EXTRACT(DAY from {x[0]})",
    FunctionType.YEAR: lambda x, types: f"EXTRACT(YEAR from {x[0]})",
    FunctionType.MONTH: lambda x, types: f"EXTRACT(MONTH from {x[0]})",
    FunctionType.WEEK: lambda x, types: f"EXTRACT(WEEK from {x[0]})",
    FunctionType.QUARTER: lambda x, types: f"EXTRACT(QUARTER from {x[0]})",
    # math
    FunctionType.POWER: lambda x, types: f"POWER({x[0]}, {x[1]})",
    FunctionType.DIVIDE: lambda x, types: f"DIV0({x[0]},{x[1]})",
    FunctionType.UNNEST: lambda x, types: f"table(flatten({x[0]}))",
    FunctionType.ARRAY: lambda x, types: f"ARRAY_CONSTRUCT({', '.join(x)})",
    FunctionType.CURRENT_DATETIME: lambda x, types: "CURRENT_TIMESTAMP()",
    FunctionType.CURRENT_DATE: lambda x, types: "CURRENT_DATE()",
    FunctionType.CURRENT_TIMESTAMP: lambda x, types: "CURRENT_TIMESTAMP()",
    # Snowflake date functions use different argument order
    FunctionType.DATE_TRUNCATE: lambda x, types: f"DATE_TRUNC({x[1]}, {x[0]})",
    FunctionType.DATE_PART: lambda x, types: f"DATE_PART({x[1]}, {x[0]})",
    FunctionType.DATE_ADD: lambda x, types: f"DATEADD({x[1]}, {x[2]}, {x[0]})",
    FunctionType.DATE_SUB: lambda x, types: f"DATEADD({x[1]}, -{x[2]}, {x[0]})",
    FunctionType.DATE_DIFF: lambda x, types: f"DATEDIFF({x[2]}, {x[0]}, {x[1]})",
    # native CONCAT/CONCAT_WS propagate NULL; wrap to match the null-skipping
    # semantics (ARRAY_CONSTRUCT_COMPACT drops NULL elements)
    FunctionType.CONCAT: lambda x, types: (
        "CONCAT(" + ", ".join([f"COALESCE({a}, '')" for a in x]) + ")"
    ),
    FunctionType.CONCAT_WS: lambda x, types: (
        f"ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT({', '.join(x[1:])}), {x[0]})"
    ),
}


MAX_IDENTIFIER_LENGTH = 50


class SnowflakeDialect(BaseDialect):
    FUNCTION_MAP: ClassVar[dict[FunctionType, Callable[..., str]]] = {
        **BaseDialect.FUNCTION_MAP,
        **FUNCTION_MAP,
    }
    QUOTE_CHARACTER = '"'
    QUOTE_CTE_NAMES = True
    UNNEST_MODE = UnnestMode.SNOWFLAKE
    SUPPORTS_AGGREGATE_GROUPING_MODES = True
    SUPPORTS_QUALIFY = True
    TABLE_NOT_FOUND_PATTERN = "does not exist"
    COLUMN_NOT_FOUND_PATTERN = "invalid identifier"

    def render_string_literal(self, value: str) -> str:
        # Snowflake treats backslash as an escape character in string literals.
        return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"

    def render_array_member_source(
        self, array_sql: str, from_clause: str | None, member_type: CONCRETE_TYPES
    ) -> tuple[str, str]:
        """FLATTEN is a table function; the element lands in its `value` column.

        That column is a VARIANT — `value` for the string element `24128` is
        the JSON text `"24128"`, which matches no VARCHAR probe — so it is cast
        back to what the probe presents. Casting the member rather than
        stringifying both sides keeps 1 and '1' distinct."""
        alias = self.ARRAY_MEMBER_SOURCE_ALIAS
        if from_clause:
            source = f"{from_clause}, lateral flatten(input => {array_sql}) as {alias}"
        else:
            source = f"table(flatten(input => {array_sql})) as {alias}"
        return source, f"cast({alias}.value as {self.render_expr(member_type)})"

    def get_table_schema(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[TableColumn]:
        """Snowflake stores unquoted identifiers as UPPER and quoted as lowercase.
        Use UPPER() comparison to find tables regardless of how they were created.
        """
        table_name_upper = table_name.upper()

        column_query = f"""
        SELECT
            column_name,
            data_type,
            is_nullable,
            comment as column_comment
        FROM information_schema.columns
        WHERE UPPER(table_name) = '{table_name_upper}'
        """
        if schema:
            schema_upper = schema.upper()
            column_query += f" AND UPPER(table_schema) = '{schema_upper}'"
        column_query += " ORDER BY ordinal_position"

        rows = executor.execute_raw_sql(column_query).fetchall()
        return self._columns_from_info_schema_rows(rows)

    # Snowflake information_schema reports internal type names that differ from DDL tokens.
    # e.g. INTEGER/NUMBER → "NUMBER", VARCHAR/TEXT → "TEXT", TIMESTAMP_NTZ → "TIMESTAMP_NTZ".
    # Extends the shared base map; bare TIMESTAMP defaults to TIMESTAMP_NTZ semantics.
    DB_COLUMN_TYPE_MAP: ClassVar[dict[str, DataType]] = {
        **BaseDialect.DB_COLUMN_TYPE_MAP,
        "text": DataType.STRING,
        "number": DataType.INTEGER,
        "float": DataType.FLOAT,
        "boolean": DataType.BOOL,
        "date": DataType.DATE,
        "timestamp": DataType.DATETIME,
        "timestamp_ntz": DataType.DATETIME,
        "timestamp_ltz": DataType.TIMESTAMP,
        "timestamp_tz": DataType.TIMESTAMP,
        "array": DataType.ARRAY,
    }

    def get_table_primary_keys(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[str]:
        """Uses SHOW PRIMARY KEYS; note Snowflake PKs are not enforced."""
        table_name_upper = table_name.upper()

        # Use SHOW PRIMARY KEYS command (column_name is at index 4)
        if schema:
            schema_upper = schema.upper()
            pk_query = f"SHOW PRIMARY KEYS IN {schema_upper}.{table_name_upper}"
        else:
            pk_query = f"SHOW PRIMARY KEYS IN {table_name_upper}"

        rows = executor.execute_raw_sql(pk_query).fetchall()
        return [row[4] for row in rows] if rows else []
