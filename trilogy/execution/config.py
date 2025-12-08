from dataclasses import dataclass
from pathlib import Path

from tomllib import loads

from trilogy.dialect import (
    BigQueryConfig,
    DuckDBConfig,
    PostgresConfig,
    PrestoConfig,
    SnowflakeConfig,
    SQLServerConfig,
)
from trilogy.dialect.enums import DialectConfig, Dialects

DEFAULT_PARALLELLISM = 2


@dataclass
class RuntimeConfig:

    startup_trilogy: list[Path]
    startup_sql: list[Path]
    parallellism: int = DEFAULT_PARALLELLISM
    engine: Dialects | None = None
    engine_config: DialectConfig | None = None


def load_config_file(path: Path) -> RuntimeConfig:
    """Load configuration from a file."""
    # Placeholder implementation

    with open(path, "r") as f:
        toml_content = f.read()
    config_data = loads(toml_content)

    engine_raw = config_data.get("engine", None)
    engine_config_raw = config_data.get("engine_config", None)
    engine = Dialects(engine_raw) if engine_raw else None
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
    setup:dict = config_data.get("setup", {})
    return RuntimeConfig(
        startup_trilogy=[Path(p) for p in setup.get("trilogy", [])],
        startup_sql=[Path(p) for p in setup.get("sql", [])],
        parallellism=config_data.get("parallellism", DEFAULT_PARALLELLISM),
        engine=engine,
        engine_config=engine_config,
    )
