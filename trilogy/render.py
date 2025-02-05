from trilogy.dialect.enums import Dialects


def get_dialect_generator(dialect: Dialects):
    if dialect == Dialects.BIGQUERY:
        from trilogy.dialect.bigquery import BigqueryDialect

        return BigqueryDialect()
    elif dialect == Dialects.SQL_SERVER:
        from trilogy.dialect.sql_server import SqlServerDialect

        return SqlServerDialect()
    elif dialect == Dialects.DUCK_DB:
        from trilogy.dialect.duckdb import DuckDBDialect

        return DuckDBDialect()
    elif dialect == Dialects.PRESTO:
        from trilogy.dialect.presto import PrestoDialect

        return PrestoDialect()
    elif dialect == Dialects.TRINO:
        from trilogy.dialect.presto import TrinoDialect

        return TrinoDialect()
    elif dialect == Dialects.POSTGRES:
        from trilogy.dialect.postgres import PostgresDialect

        return PostgresDialect()
    elif dialect == Dialects.SNOWFLAKE:
        from trilogy.dialect.snowflake import SnowflakeDialect

        return SnowflakeDialect()
    elif dialect == Dialects.DATAFRAME:
        from trilogy.dialect.dataframe import DataframeDialect

        return DataframeDialect()
    else:
        raise ValueError(f"Unsupported dialect {dialect}")
