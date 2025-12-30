from trilogy.constants import Rendering
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.config import DialectConfig
from trilogy.dialect.enums import Dialects


def get_dialect_generator(
    dialect: Dialects,
    rendering: Rendering | None = None,
    config: DialectConfig | None = None,
) -> BaseDialect:
    if dialect == Dialects.BIGQUERY:
        from trilogy.dialect.bigquery import BigqueryDialect

        return BigqueryDialect(rendering=rendering, config=config)
    elif dialect == Dialects.SQL_SERVER:
        from trilogy.dialect.sql_server import SqlServerDialect

        return SqlServerDialect(rendering=rendering, config=config)
    elif dialect == Dialects.DUCK_DB:
        from trilogy.dialect.duckdb import DuckDBDialect

        return DuckDBDialect(rendering=rendering, config=config)
    elif dialect == Dialects.PRESTO:
        from trilogy.dialect.presto import PrestoDialect

        return PrestoDialect(rendering=rendering, config=config)
    elif dialect == Dialects.TRINO:
        from trilogy.dialect.presto import TrinoDialect

        return TrinoDialect(rendering=rendering, config=config)
    elif dialect == Dialects.POSTGRES:
        from trilogy.dialect.postgres import PostgresDialect

        return PostgresDialect(rendering=rendering, config=config)
    elif dialect == Dialects.SNOWFLAKE:
        from trilogy.dialect.snowflake import SnowflakeDialect

        return SnowflakeDialect(rendering=rendering, config=config)
    elif dialect == Dialects.DATAFRAME:
        from trilogy.dialect.dataframe import DataframeDialect

        return DataframeDialect(rendering=rendering, config=config)
    else:
        raise ValueError(f"Unsupported dialect {dialect}")
