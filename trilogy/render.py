from trilogy.constants import Rendering
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.config import DialectConfig
from trilogy.dialect.enums import Dialects
from trilogy.staging import StagingConfig


def get_dialect_class(dialect: Dialects) -> type[BaseDialect]:
    """Imported on demand so a dialect's optional driver dependency is only
    required when that dialect is actually used."""
    if dialect == Dialects.BIGQUERY:
        from trilogy.dialect.bigquery import BigqueryDialect

        return BigqueryDialect
    elif dialect == Dialects.SQL_SERVER:
        from trilogy.dialect.sql_server import SqlServerDialect

        return SqlServerDialect
    elif dialect == Dialects.DUCK_DB:
        from trilogy.dialect.duckdb import DuckDBDialect

        return DuckDBDialect
    elif dialect == Dialects.SQLITE:
        from trilogy.dialect.sqlite import SQLiteDialect

        return SQLiteDialect
    elif dialect == Dialects.PRESTO:
        from trilogy.dialect.presto import PrestoDialect

        return PrestoDialect
    elif dialect == Dialects.TRINO:
        from trilogy.dialect.presto import TrinoDialect

        return TrinoDialect
    elif dialect == Dialects.POSTGRES:
        from trilogy.dialect.postgres import PostgresDialect

        return PostgresDialect
    elif dialect == Dialects.MYSQL:
        from trilogy.dialect.mysql import MySQLDialect

        return MySQLDialect
    elif dialect == Dialects.SNOWFLAKE:
        from trilogy.dialect.snowflake import SnowflakeDialect

        return SnowflakeDialect
    elif dialect == Dialects.DATAFRAME:
        from trilogy.dialect.dataframe import DataframeDialect

        return DataframeDialect
    elif dialect == Dialects.CLICKHOUSE:
        from trilogy.dialect.clickhouse import ClickhouseDialect

        return ClickhouseDialect
    raise ValueError(f"Unsupported dialect {dialect}")


def get_dialect_generator(
    dialect: Dialects,
    rendering: Rendering | None = None,
    config: DialectConfig | None = None,
    staging: StagingConfig | None = None,
    instance_id: str | None = None,
) -> BaseDialect:
    return get_dialect_class(dialect)(
        rendering=rendering,
        config=config,
        staging=staging,
        instance_id=instance_id,
    )
