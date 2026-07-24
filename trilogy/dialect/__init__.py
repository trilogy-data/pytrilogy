from .config import (
    BigQueryConfig,
    DialectConfig,
    DuckDBConfig,
    PostgresConfig,
    PrestoConfig,
    SnowflakeConfig,
    SQLiteConfig,
    SQLServerConfig,
)

__all__ = [
    "BigQueryConfig",
    "BigqueryDialect",
    "DialectConfig",
    "DuckDBConfig",
    "DuckDBDialect",
    "PostgresConfig",
    "PostgresDialect",
    "PrestoConfig",
    "PrestoDialect",
    "SQLServerConfig",
    "SQLiteConfig",
    "SQLiteDialect",
    "SnowflakeConfig",
    "SnowflakeDialect",
    "SqlServerDialect",
]

_dialect_imports: dict[str, tuple[str, str]] = {
    "BigqueryDialect": (".bigquery", "BigqueryDialect"),
    "DuckDBDialect": (".duckdb", "DuckDBDialect"),
    "PostgresDialect": (".postgres", "PostgresDialect"),
    "PrestoDialect": (".presto", "PrestoDialect"),
    "SnowflakeDialect": (".snowflake", "SnowflakeDialect"),
    "SqlServerDialect": (".sql_server", "SqlServerDialect"),
    "SQLiteDialect": (".sqlite", "SQLiteDialect"),
}


def __getattr__(name: str):
    if name in _dialect_imports:
        module_path, attr = _dialect_imports[name]
        import importlib

        mod = importlib.import_module(module_path, package=__name__)
        return getattr(mod, attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
