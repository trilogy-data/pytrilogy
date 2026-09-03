from collections.abc import Callable
from typing import ClassVar

from trilogy.core.enums import FunctionType
from trilogy.core.statements.execute import (
    PROCESSED_STATEMENT_TYPES,
    ProcessedQuery,
    ProcessedQueryPersist,
)
from trilogy.dialect.base import BaseDialect, TableColumn
from trilogy.utility import string_to_hash

FUNCTION_MAP = {
    # CONCAT skips NULLs natively; `+` propagates (CONCAT_NULL_YIELDS_NULL ON)
    FunctionType.CONCAT: lambda args, types: f"CONCAT({', '.join(args)})",
    FunctionType.CONCAT_STRICT: lambda args, types: f"({' + '.join(args)})",
    FunctionType.CONCAT_WS: lambda args, types: f"CONCAT_WS({', '.join(args)})",
}

MAX_IDENTIFIER_LENGTH = 128


class SqlServerDialect(BaseDialect):
    FUNCTION_MAP: ClassVar[dict[FunctionType, Callable[..., str]]] = {
        **BaseDialect.FUNCTION_MAP,
        **FUNCTION_MAP,
    }
    QUOTE_CHARACTER = '"'
    LIMIT_STYLE = "TOP"
    RECURSIVE_KEYWORD = ""
    SUPPORTS_AGGREGATE_GROUPING_MODES = True
    SUPPORTS_ARRAYS = False
    # Msg 208: `Invalid object name 'dbo.orders'.`
    TABLE_NOT_FOUND_PATTERN = r"Invalid object name"
    # Msg 207: `Invalid column name 'updated_at'.` Msg 4104 (multi-part
    # identifier could not be bound) is deliberately excluded — the alias
    # trilogy emits always binds, so that would mean a generated-SQL bug.
    COLUMN_NOT_FOUND_PATTERN = r"Invalid column name"

    def staging_table_name(self, query) -> str:
        """T-SQL marks a session-local temp table with a leading ``#``; without
        it the staging table is a permanent one in the default schema."""
        return f"#{super().staging_table_name(query)}"

    def render_staging_create(self, target: str, staged: str) -> str:
        """T-SQL has no ``CREATE TABLE ... AS SELECT``; ``SELECT ... INTO``
        creates the table from the query's shape instead."""
        return f"SELECT * INTO {staged} FROM {target} WHERE 1=0"

    def get_table_schema(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[TableColumn]:
        """Defaults to 'dbo' schema if none specified."""
        if not schema:
            schema = "dbo"

        column_query = f"""
        SELECT
            column_name,
            data_type,
            is_nullable,
            '' as column_comment
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        AND table_schema = '{schema}'
        ORDER BY ordinal_position
        """

        rows = executor.execute_raw_sql(column_query).fetchall()
        return self._columns_from_info_schema_rows(rows)

    def get_table_primary_keys(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[str]:
        """Uses sys catalog views for more reliable constraint information."""
        if not schema:
            schema = "dbo"

        pk_query = f"""
        SELECT c.name
        FROM sys.indexes i
        INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        INNER JOIN sys.tables t ON i.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE i.is_primary_key = 1
        AND t.name = '{table_name}'
        AND s.name = '{schema}'
        ORDER BY ic.key_ordinal
        """

        rows = executor.execute_raw_sql(pk_query).fetchall()
        return [row[0] for row in rows]

    def compile_statement(self, query: PROCESSED_STATEMENT_TYPES) -> str:
        base = super().compile_statement(query)
        if isinstance(query, (ProcessedQuery, ProcessedQueryPersist)):
            for cte in query.ctes:
                if len(cte.name) > MAX_IDENTIFIER_LENGTH:
                    new_name = f"rhash_{string_to_hash(cte.name)}"
                    base = base.replace(cte.name, new_name)
        return base
