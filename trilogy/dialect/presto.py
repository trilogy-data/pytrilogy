from collections.abc import Callable
from typing import ClassVar

from trilogy.core.enums import FunctionType, GroupMode, UnnestMode
from trilogy.core.models.core import CONCRETE_TYPES, DataType
from trilogy.core.statements.execute import CreateTableInfo
from trilogy.dialect.base import BaseDialect

FUNCTION_MAP = {
    FunctionType.INDEX_ACCESS: lambda x, types: f"element_at({x[0]},{x[1]})",
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
    FunctionType.DIVIDE: lambda x, types: f"{x[0]}/{x[1]}",
    FunctionType.DATE_ADD: lambda x, types: f"date_add({x[1]},{x[2]}, {x[0]})",
    FunctionType.DATE_SUB: lambda x, types: f"date_add({x[1]},-{x[2]}, {x[0]})",
    FunctionType.CURRENT_DATE: lambda x, types: "CURRENT_DATE",
    FunctionType.CURRENT_DATETIME: lambda x, types: "CURRENT_TIMESTAMP",
    FunctionType.ARRAY: lambda x, types: f"ARRAY[{', '.join(x)}]",
    # regex
    FunctionType.REGEXP_CONTAINS: lambda x, types: f"REGEXP_LIKE({x[0]}, {x[1]})",
    # native concat propagates NULL; wrap to match the null-skipping semantics.
    # array_join omits NULL elements when no null_replacement is given.
    FunctionType.CONCAT: lambda x, types: (
        "concat(" + ", ".join([f"coalesce({a}, '')" for a in x]) + ")"
    ),
    FunctionType.CONCAT_WS: lambda x, types: (
        f"array_join(ARRAY[{', '.join(x[1:])}], {x[0]})"
    ),
}

MAX_IDENTIFIER_LENGTH = 50


class PrestoDialect(BaseDialect):
    FUNCTION_MAP: ClassVar[dict[FunctionType, Callable[..., str]]] = {
        **BaseDialect.FUNCTION_MAP,
        **FUNCTION_MAP,
    }
    QUOTE_CHARACTER = '"'

    DATATYPE_MAP: ClassVar[dict[DataType, str]] = {
        **BaseDialect.DATATYPE_MAP,
        DataType.NUMERIC: "DECIMAL",
        DataType.STRING: "VARCHAR",
    }
    # `line 1:15: Table 'hive.default.orders' does not exist`; the client also
    # surfaces the engine's error name, which is matched for both forms.
    TABLE_NOT_FOUND_PATTERN = (
        r"(Table|Schema) '.+' does not exist|TABLE_NOT_FOUND|SCHEMA_NOT_FOUND"
    )
    # `line 1:12: Column 'updated_at' cannot be resolved`
    COLUMN_NOT_FOUND_PATTERN = r"Column '.+' cannot be resolved|COLUMN_NOT_FOUND"
    UNNEST_MODE = UnnestMode.PRESTO
    GROUP_MODE = GroupMode.BY_INDEX
    SUPPORTS_AGGREGATE_GROUPING_MODES = True
    SUPPORTS_QUALIFY = True
    ALIAS_ORDER_REFERENCING_ALLOWED = (
        False  # some complex presto functions don't support aliasing
    )

    def get_table_primary_keys(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[str]:
        """Presto/Trino don't enforce PKs; rely on data-driven grain detection."""
        return []

    def render_array_member_source(
        self, array_sql: str, from_clause: str | None, member_type: CONCRETE_TYPES
    ) -> tuple[str, str]:
        """UNNEST is a relation here, and names its output column via an alias
        list rather than in a select list."""
        source = (
            f"unnest({array_sql}) as {self.ARRAY_MEMBER_SOURCE_ALIAS}"
            f"({self.ARRAY_MEMBER_COLUMN})"
        )
        if from_clause:
            source = f"{from_clause} cross join {source}"
        return source, self.ARRAY_MEMBER_COLUMN

    def render_partition_clause(self, target: CreateTableInfo) -> str:
        """Presto/Trino declare partitioning as a table property.

        The Hive connector requires partition columns to be the LAST columns in
        the table, in partition order — a column list that violates that is
        rejected at CREATE time, so it raises here rather than emitting DDL the
        engine will refuse. (Iceberg has no such rule, but the stricter check is
        the safe default: satisfying it is valid on both. Reordering the columns
        for the user is NOT safe — the persist INSERT is positional, so a table
        whose column order differs from the datasource's would be written to
        wrong.)
        """
        if not target.partition_keys:
            return ""
        names = [column.name for column in target.columns]
        if names[-len(target.partition_keys) :] != target.partition_keys:
            raise ValueError(
                f"{target.name} partitions by {', '.join(target.partition_keys)}, but"
                " Presto/Trino require partition columns to be the last columns of"
                f" the table, in partition order; got {', '.join(names)}."
            )
        keys = ", ".join(
            self.render_string_literal(key) for key in target.partition_keys
        )
        return f"WITH (partitioned_by = ARRAY[{keys}])"


class TrinoDialect(PrestoDialect):
    pass
