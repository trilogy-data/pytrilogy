from dataclasses import dataclass
from pathlib import Path

from tomllib import loads

from trilogy.dialect import (
    BigQueryConfig,
    DialectConfig,
    DuckDBConfig,
    PostgresConfig,
    PrestoConfig,
    SnowflakeConfig,
    SQLServerConfig,
)
from trilogy.dialect.enums import Dialects

DEFAULT_PARALLELISM = 4


@dataclass
class RuntimeConfig:

    startup_trilogy: list[Path]
    startup_sql: list[Path]
    parallelism: int = DEFAULT_PARALLELISM
    engine_dialect: Dialects | None = None
    engine_config: DialectConfig | None = None


def load_config_file(path: Path) -> RuntimeConfig:
    with open(path, "r") as f:
        toml_content = f.read()
    config_data = loads(toml_content)

    engine_raw: dict = config_data.get("engine", {})
    engine_config_raw = engine_raw.get("config", {})
    engine = Dialects(engine_raw.get("dialect")) if engine_raw.get("dialect") else None
    engine_config: DialectConfig | None
    if engine:
        if engine == Dialects.DUCK_DB:
            engine_config = (
                DuckDBConfig(**engine_config_raw) if engine_config_raw else None
            )
        elif engine == Dialects.POSTGRES:
            engine_config = (
                PostgresConfig(**engine_config_raw) if engine_config_raw else None
            )
        elif engine == Dialects.PRESTO:
            engine_config = (
                PrestoConfig(**engine_config_raw) if engine_config_raw else None
            )
        elif engine == Dialects.SNOWFLAKE:
            engine_config = (
                SnowflakeConfig(**engine_config_raw) if engine_config_raw else None
            )
        elif engine == Dialects.SQL_SERVER:
            engine_config = (
                SQLServerConfig(**engine_config_raw) if engine_config_raw else None
            )
        elif engine == Dialects.BIGQUERY:
            engine_config = (
                BigQueryConfig(**engine_config_raw) if engine_config_raw else None
            )
        else:
            engine_config = None
    else:
        engine_config = None
    setup: dict = config_data.get("setup", {})
    return RuntimeConfig(
        startup_trilogy=[path.parent / p for p in setup.get("trilogy", [])],
        startup_sql=[path.parent / p for p in setup.get("sql", [])],
        parallelism=config_data.get("parallelism", DEFAULT_PARALLELISM),
        engine_dialect=engine,
        engine_config=engine_config,
    )
