import os
from dataclasses import dataclass, field
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
from trilogy.constants import logger

DEFAULT_PARALLELISM = 4


def load_env_file(env_file_path: Path) -> dict[str, str] | None:
    """Load environment variables from a .env file."""
    env_vars: dict[str, str] = {}
    if not env_file_path.exists():
        logger.info(f"Environment file not found: {env_file_path}")
        return None
    with open(env_file_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            # Remove surrounding quotes if present
            if (value.startswith('"') and value.endswith('"')) or (
                value.startswith("'") and value.endswith("'")
            ):
                value = value[1:-1]
            env_vars[key] = value
    return env_vars


def apply_env_vars(env_vars: dict[str, str]) -> None:
    """Apply environment variables to os.environ."""
    for key, value in env_vars.items():
        os.environ[key] = value


@dataclass
class RuntimeConfig:

    startup_trilogy: list[Path]
    startup_sql: list[Path]
    parallelism: int = DEFAULT_PARALLELISM
    engine_dialect: Dialects | None = None
    engine_config: DialectConfig | None = None
    source_path: Path | None = None
    env_files: list[Path] = field(default_factory=list)


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

    # Parse env_file - can be a single string or list of strings
    env_raw = engine_raw.get("env_file", [])
    if isinstance(env_raw, str):
        env_files = [path.parent / env_raw]
    else:
        env_files = [path.parent / p for p in env_raw]

    return RuntimeConfig(
        startup_trilogy=[path.parent / p for p in setup.get("trilogy", [])],
        startup_sql=[path.parent / p for p in setup.get("sql", [])],
        parallelism=config_data.get("parallelism", DEFAULT_PARALLELISM),
        engine_dialect=engine,
        engine_config=engine_config,
        source_path=path,
        env_files=env_files,
    )
