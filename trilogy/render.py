from trilogy.constants import Rendering
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.enums import Dialects


def get_dialect_generator(
    dialect: Dialects, rendering: Rendering | None = None
) -> BaseDialect:
    if dialect == Dialects.BIGQUERY:
        from trilogy.dialect.bigquery import BigqueryDialect

        return BigqueryDialect(rendering=rendering)
    elif dialect == Dialects.SQL_SERVER:
        from trilogy.dialect.sql_server import SqlServerDialect

        return SqlServerDialect(rendering=rendering)
    elif dialect == Dialects.DUCK_DB:
        from trilogy.dialect.duckdb import DuckDBDialect

        return DuckDBDialect(rendering=rendering)
    elif dialect == Dialects.PRESTO:
        from trilogy.dialect.presto import PrestoDialect

        return PrestoDialect(rendering=rendering)
    elif dialect == Dialects.TRINO:
        from trilogy.dialect.presto import TrinoDialect

        return TrinoDialect(rendering=rendering)
    elif dialect == Dialects.POSTGRES:
        from trilogy.dialect.postgres import PostgresDialect

        return PostgresDialect(rendering=rendering)
    elif dialect == Dialects.SNOWFLAKE:
        from trilogy.dialect.snowflake import SnowflakeDialect

        return SnowflakeDialect(rendering=rendering)
    elif dialect == Dialects.DATAFRAME:
        from trilogy.dialect.dataframe import DataframeDialect

        return DataframeDialect(rendering=rendering)
    else:
        raise ValueError(f"Unsupported dialect {dialect}")
