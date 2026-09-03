from collections.abc import Callable
from typing import ClassVar

from trilogy.core.enums import DatePart, FunctionType
from trilogy.dialect.base import BaseDialect


def date_diff(first: str, second: str, grain: DatePart) -> str:
    grain = DatePart(grain)
    if grain == DatePart.YEAR:
        return f"date_part('year', {second}) - date_part('year', {first})"
    elif grain == DatePart.MONTH:
        return f"12 * {date_diff(first, second, DatePart.YEAR)} + date_part('month', {second}) - date_part('month', {first})"
    elif grain == DatePart.DAY:
        return f"date_part('day', {second} - {first})"
    elif grain == DatePart.HOUR:
        return f"{date_diff(first, second, DatePart.DAY)} *24 + date_part('hour', {second} - {first})"
    elif grain == DatePart.MINUTE:
        return f"{date_diff(first, second, DatePart.HOUR)} *60 + date_part('minute', {second} - {first})"
    elif grain == DatePart.SECOND:
        return f"{date_diff(first, second, DatePart.MINUTE)} *60 + date_part('second', {second} - {first})"
    else:
        raise NotImplementedError(f"Date diff not implemented for grain {grain}")


FUNCTION_MAP = {
    FunctionType.SPLIT: lambda x, types: f"string_to_array({x[0]}, {x[1]})",
    FunctionType.DATE_TRUNCATE: lambda x, types: f"date_trunc('{x[1]}', {x[0]})",
    FunctionType.DATE_ADD: lambda x, types: f"({x[0]} + INTERVAL '{x[2]} {x[1]}')",
    FunctionType.DATE_PART: lambda x, types: f"date_part('{x[1]}', {x[0]})",
    FunctionType.DATE_DIFF: lambda x, types: date_diff(x[0], x[1], x[2]),
    FunctionType.IS_NULL: lambda x, types: f"{x[0]} IS NULL",
}

MAX_IDENTIFIER_LENGTH = 50


class PostgresDialect(BaseDialect):
    FUNCTION_MAP: ClassVar[dict[FunctionType, Callable[..., str]]] = {
        **BaseDialect.FUNCTION_MAP,
        **FUNCTION_MAP,
    }
    QUOTE_CHARACTER = '"'
    SUPPORTS_AGGREGATE_GROUPING_MODES = True
    # `relation "orders" does not exist` / `schema "analytics" does not exist`.
    # Anchored on the object kind: bare `does not exist` also covers a missing
    # type or function, which are query bugs rather than a missing source.
    TABLE_NOT_FOUND_PATTERN = r'(relation|schema) ".+" does not exist'
    # quoted when unqualified (`column "updated_at" does not exist`), unquoted
    # when qualified (`column base.updated_at does not exist`)
    COLUMN_NOT_FOUND_PATTERN = r"column .+ does not exist"

    def get_table_primary_keys(
        self, executor, table_name: str, schema: str | None = None
    ) -> list[str]:
        """Uses pg_catalog for more reliable constraint information than information_schema."""
        if schema:
            pk_query = f"""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = '{schema}.{table_name}'::regclass
            AND i.indisprimary
            ORDER BY a.attnum
            """
        else:
            pk_query = f"""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = '{table_name}'::regclass
            AND i.indisprimary
            ORDER BY a.attnum
            """

        rows = executor.execute_raw_sql(pk_query).fetchall()
        return [row[0] for row in rows]
