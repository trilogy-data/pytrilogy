"""Common helper functions used across all CLI commands."""

import traceback
from dataclasses import dataclass
from io import StringIO
from pathlib import Path as PathlibPath
from typing import Any, Iterable, Union

from click.exceptions import Exit

from trilogy import Executor
from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.exceptions import ConfigurationException, ModelValidationError
from trilogy.core.models.environment import Environment
from trilogy.dialect.enums import Dialects
from trilogy.execution.config import RuntimeConfig, load_config_file
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.scripts.dependency import ScriptNode
from trilogy.scripts.display import (
    print_error,
    print_info,
    print_success,
)
from trilogy.scripts.environment import extra_to_kwargs, parse_env_params

# Configuration file name
TRILOGY_CONFIG_NAME = "trilogy.toml"


@dataclass
class CLIRuntimeParams:
    """Parameters provided via CLI for execution."""

    input: str
    dialect: Dialects | None = None
    parallelism: int | None = None
    param: tuple[str, ...] = ()
    conn_args: tuple[str, ...] = ()
    debug: bool = False
    config_path: PathlibPath | None = None
    execution_strategy: str = "eager_bfs"


def merge_runtime_config(
    cli_params: CLIRuntimeParams, file_config: RuntimeConfig
) -> tuple[Dialects, int]:
    """
    Merge CLI parameters with config file settings.
    CLI parameters take precedence over config file.

    Returns:
        tuple of (dialect, parallelism)

    Raises:
        Exit: If no dialect is specified in either CLI or config
    """
    # Resolve dialect: CLI argument takes precedence over config
    if cli_params.dialect:
        dialect = cli_params.dialect
    elif file_config.engine_dialect:
        dialect = file_config.engine_dialect
    else:
        print_error(
            "No dialect specified. Provide dialect as argument or set engine.dialect in config file."
        )
        raise Exit(1)

    # Resolve parallelism: CLI argument takes precedence over config
    parallelism = (
        cli_params.parallelism
        if cli_params.parallelism is not None
        else file_config.parallelism
    )

    return dialect, parallelism


def find_trilogy_config(start_path: PathlibPath | None = None) -> PathlibPath | None:
    """
    Search for trilogy.toml starting from the given path, walking up parent directories.

    Args:
        start_path: Starting directory for search. If None, uses current working directory.

    Returns:
        Path to trilogy.toml if found, None otherwise.
    """
    search_path = start_path if start_path else PathlibPath.cwd()
    if not search_path.is_dir():
        search_path = search_path.parent

    for parent in [search_path] + list(search_path.parents):
        candidate = parent / TRILOGY_CONFIG_NAME
        if candidate.exists():
            return candidate
    return None


def resolve_input(path: PathlibPath) -> list[PathlibPath]:
    # Directory
    if path.is_dir():
        pattern = "**/*.preql"
        return sorted(path.glob(pattern))
    # Single file
    if path.exists() and path.is_file():
        return [path]

    raise FileNotFoundError(f"Input path '{path}' does not exist.")


def get_runtime_config(
    path: PathlibPath, config_override: PathlibPath | None = None
) -> RuntimeConfig:
    config_path: PathlibPath | None = None

    if config_override:
        config_path = config_override
    else:
        config_path = find_trilogy_config(path)

    if not config_path:
        return RuntimeConfig(startup_trilogy=[], startup_sql=[])

    try:
        return load_config_file(config_path)
    except Exception as e:
        print_error(f"Failed to load configuration file {config_path}: {e}")
        handle_execution_exception(e)
        # This won't be reached due to handle_execution_exception raising Exit
        return RuntimeConfig(startup_trilogy=[], startup_sql=[])


def resolve_input_information(
    input: str, config_path_input: PathlibPath | None = None
) -> tuple[Iterable[PathlibPath | StringIO], PathlibPath, str, str, RuntimeConfig]:
    input_as_path = PathlibPath(input)
    files: Iterable[StringIO | PathlibPath]
    if input_as_path.exists():
        pathlib_path = input_as_path
        files = resolve_input(pathlib_path)

        if pathlib_path.is_dir():
            directory = pathlib_path
            input_type = "directory"
            config = get_runtime_config(pathlib_path, config_path_input)

        else:
            directory = pathlib_path.parent
            input_type = "file"
            config = get_runtime_config(pathlib_path, config_path_input)

        input_name = pathlib_path.name
    else:
        script = input
        files = [StringIO(script)]
        directory = PathlibPath.cwd()
        input_type = "query"
        input_name = "inline"
        config = RuntimeConfig(startup_trilogy=[], startup_sql=[])
    return files, directory, input_type, input_name, config


def validate_required_connection_params(
    conn_dict: dict[str, Any],
    required_keys: list[str],
    optional_keys: list[str],
    dialect_name: str,
) -> dict:
    missing = [key for key in required_keys if key not in conn_dict]
    extra = [
        key
        for key in conn_dict
        if key not in required_keys and key not in optional_keys
    ]
    if missing:
        raise ConfigurationException(
            f"Missing required {dialect_name} connection parameters: {', '.join(missing)}"
        )
    if extra:
        print(
            f"Warning: Extra {dialect_name} connection parameters provided: {', '.join(extra)}"
        )
    return {
        k: v for k, v in conn_dict.items() if k in required_keys or k in optional_keys
    }


def get_dialect_config(
    edialect: Dialects, conn_dict: dict[str, Any], runtime_config: RuntimeConfig
) -> Any:
    """Get dialect configuration based on dialect type."""
    conf: Union[Any, None] = None

    if edialect == Dialects.DUCK_DB:
        from trilogy.dialect.config import DuckDBConfig

        conn_dict = validate_required_connection_params(
            conn_dict, [], ["path"], "DuckDB"
        )
        conf = DuckDBConfig(**conn_dict)
    elif edialect == Dialects.SNOWFLAKE:
        from trilogy.dialect.config import SnowflakeConfig

        conn_dict = validate_required_connection_params(
            conn_dict, ["username", "password", "account"], [], "Snowflake"
        )
        conf = SnowflakeConfig(**conn_dict)
    elif edialect == Dialects.SQL_SERVER:
        from trilogy.dialect.config import SQLServerConfig

        conn_dict = validate_required_connection_params(
            conn_dict,
            ["host", "port", "username", "password", "database"],
            [],
            "SQL Server",
        )
        conf = SQLServerConfig(**conn_dict)
    elif edialect == Dialects.POSTGRES:
        from trilogy.dialect.config import PostgresConfig

        conn_dict = validate_required_connection_params(
            conn_dict,
            ["host", "port", "username", "password", "database"],
            [],
            "Postgres",
        )
        conf = PostgresConfig(**conn_dict)
    elif edialect == Dialects.BIGQUERY:
        from trilogy.dialect.config import BigQueryConfig

        conn_dict = validate_required_connection_params(
            conn_dict, [], ["project"], "BigQuery"
        )
        conf = BigQueryConfig(**conn_dict)
    elif edialect == Dialects.PRESTO:
        from trilogy.dialect.config import PrestoConfig

        conn_dict = validate_required_connection_params(
            conn_dict,
            ["host", "port", "username", "password", "catalog"],
            [],
            "Presto",
        )
        conf = PrestoConfig(**conn_dict)
    if conf and runtime_config.engine_config:
        conf = runtime_config.engine_config.merge_config(conf)
    return conf


def create_executor(
    param: tuple[str, ...],
    directory: PathlibPath,
    conn_args: Iterable[str],
    edialect: Dialects,
    debug: bool,
    config: RuntimeConfig,
) -> Executor:
    # Parse environment parameters from dedicated flag
    namespace = DEFAULT_NAMESPACE
    try:
        env_params = parse_env_params(param)
        from trilogy.scripts.display import show_environment_params

        show_environment_params(env_params)
    except ValueError as e:
        print_error(str(e))
        raise Exit(1) from e

    # Parse connection arguments from remaining args
    conn_dict = extra_to_kwargs(conn_args)

    # Configure dialect
    try:
        conf = get_dialect_config(edialect, conn_dict, runtime_config=config)
    except Exception as e:
        handle_execution_exception(e)

    # Create environment and set additional parameters if any exist
    environment = Environment(working_path=str(directory), namespace=namespace)
    if env_params:
        environment.set_parameters(**env_params)

    exec = Executor(
        dialect=edialect,
        engine=edialect.default_engine(conf=conf),
        environment=environment,
        hooks=[DebuggingHook()] if debug else [],
    )
    if config.startup_sql:
        for script in config.startup_sql:
            print_info(f"Executing startup SQL script: {script}")
            exec.execute_file(script)
    if config.startup_trilogy:
        for script in config.startup_trilogy:
            print_info(f"Executing startup Trilogy script: {script}")
            exec.execute_file(script)
    return exec


def create_executor_for_script(
    node: ScriptNode,
    param: tuple[str, ...],
    conn_args: Iterable[str],
    edialect: Dialects,
    debug: bool,
    config: RuntimeConfig,
) -> Executor:
    """
    Create an executor for a specific script node.

    Each script gets its own executor with its own environment,
    using the script's parent directory as the working path.
    """
    directory = node.path.parent
    return create_executor(param, directory, conn_args, edialect, debug, config)


def validate_datasources(
    exec: Executor, mock: bool = False, quiet: bool = False
) -> None:
    """Validate datasources with consistent error handling.

    Args:
        exec: The executor instance
        mock: If True, mock datasources before validation (for unit tests)
        quiet: If True, suppress informational messages (for parallel execution)

    Raises:
        Exit: If validation fails
    """
    datasources = exec.environment.datasources.keys()
    if not datasources:
        if not quiet:
            message = "unit" if mock else "integration"
            print_success(f"No datasources found to {message} test.")
        return

    if mock:
        exec.execute_text("mock datasources {};".format(", ".join(datasources)))

    try:
        exec.execute_text("validate datasources {};".format(", ".join(datasources)))
    except ModelValidationError as e:
        if not e.children:
            print_error(f"Datasource validation failed: {e.message}")
        for idx, child in enumerate(e.children or []):
            print_error(f"Error {idx + 1}: {child.message}")
        raise Exit(1) from e


def handle_execution_exception(e: Exception, debug: bool = False) -> None:
    print_error(f"Unexpected error: {e}")
    if debug:
        print_error(f"Full traceback:\n{traceback.format_exc()}")
    raise Exit(1) from e
