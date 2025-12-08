from sys import path
import traceback
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from io import StringIO
from pathlib import Path as PathlibPath
from typing import Any, Iterable, Union

from click import UNPROCESSED, Path, argument, group, option, pass_context
from click.exceptions import Exit

from trilogy import Executor, parse
from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.exceptions import (
    ConfigurationException,
    ModelValidationError,
)
from trilogy.core.models.environment import Environment
from trilogy.dialect.enums import Dialects
from trilogy.execution.config import RuntimeConfig, load_config_file
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.parsing.render import Renderer
from trilogy.scripts.dependency import (
    ETLDependencyStrategy,
    ScriptNode,
)
from trilogy.scripts.display import (
    RICH_AVAILABLE,
    create_progress_context,
    print_error,
    print_info,
    print_success,
    set_rich_mode,
    show_debug_mode,
    show_environment_params,
    show_execution_info,
    show_execution_start,
    show_execution_summary,
    show_formatting_result,
    show_parallel_execution_start,
    show_parallel_execution_summary,
    show_script_result,
    with_status,
)
from trilogy.scripts.environment import extra_to_kwargs, parse_env_params
from trilogy.scripts.parallel_execution import (
    EagerBFSStrategy,
    ParallelExecutor,
)
from trilogy.scripts.single_execution import (
    execute_queries_simple,
    execute_queries_with_progress,
)

set_rich_mode = set_rich_mode

# Default parallelism level
DEFAULT_PARALLELISM = 2


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


class RunMode(Enum):
    RUN = "run"
    INTEGRATION = "integration"
    UNIT = "unit"


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
    if config_override:
        config_path = config_override
    elif path.is_dir():
        config_path = path / "trilogy.toml"
    else:
        config_path = path.parent / "trilogy.toml"
    if config_path.exists():
        try:
            return load_config_file(config_path)
        except Exception as e:
            print_error(f"Failed to load configuration file {config_path}: {e}")
            handle_execution_exception(e)
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


def execute_script_for_run(
    exec: Executor, node: ScriptNode, quiet: bool = False
) -> None:
    """Execute a script for the 'run' command (parallel execution mode)."""
    with open(node.path, "r") as f:
        queries = exec.parse_text(f.read())
    for query in queries:
        exec.execute_query(query)


def execute_script_for_integration(
    exec: Executor, node: ScriptNode, quiet: bool = False
) -> None:
    """Execute a script for the 'integration' command (parse + validate)."""
    with open(node.path, "r") as f:
        exec.parse_text(f.read())
    validate_datasources(exec, mock=False, quiet=quiet)


def execute_script_for_unit(
    exec: Executor, node: ScriptNode, quiet: bool = False
) -> None:
    """Execute a script for the 'unit' command (parse + mock validate)."""
    with open(node.path, "r") as f:
        exec.parse_text(f.read())
    validate_datasources(exec, mock=True, quiet=quiet)


def get_execution_strategy(strategy_name: str):
    """Get execution strategy by name."""
    strategies = {
        "eager_bfs": EagerBFSStrategy,
    }
    if strategy_name not in strategies:
        raise ValueError(
            f"Unknown execution strategy: {strategy_name}. "
            f"Available: {', '.join(strategies.keys())}"
        )
    return strategies[strategy_name]()


def handle_execution_exception(e: Exception, debug: bool = False) -> None:
    print_error(f"Unexpected error: {e}")
    if debug:
        print_error(f"Full traceback:\n{traceback.format_exc()}")
    raise Exit(1) from e


def run_single_script_execution(
    files: list[StringIO | PathlibPath],
    directory: PathlibPath,
    input_type: str,
    input_name: str,
    edialect: Dialects,
    param: tuple[str, ...],
    conn_args: Iterable[str],
    debug: bool,
    execution_mode: str,
    config: RuntimeConfig,
) -> None:
    """
    Run single script execution with polished multi-statement progress display.

    Args:
        text: List of script contents
        directory: Working directory
        input_type: Type of input (file, query, etc.)
        input_name: Name of the input
        edialect: Dialect to use
        param: Environment parameters
        conn_args: Connection arguments
        debug: Debug mode flag
        execution_mode: One of 'run', 'integration', or 'unit'
    """
    show_execution_info(input_type, input_name, edialect.value, debug)

    exec = create_executor(param, directory, conn_args, edialect, debug, config)
    base = files[0]
    if isinstance(base, StringIO):
        text = [base.getvalue()]
    else:
        with open(base, "r") as raw:
            text = [raw.read()]

    if execution_mode == "run":
        # Parse all scripts and collect queries
        queries = []
        try:
            for script in text:
                queries += exec.parse_text(script)
        except Exception as e:
            handle_execution_exception(e, debug=debug)

        start = datetime.now()
        show_execution_start(len(queries))

        # Execute with progress tracking for multiple statements
        if len(queries) > 1 and RICH_AVAILABLE:
            progress = create_progress_context()
        else:
            progress = None

        try:
            if progress:
                exception = execute_queries_with_progress(exec, queries)
            else:
                exception = execute_queries_simple(exec, queries)

            total_duration = datetime.now() - start
            show_execution_summary(len(queries), total_duration, exception is None)

            if exception:
                raise Exit(1) from exception
        except Exit:
            raise
        except Exception as e:
            handle_execution_exception(e, debug=debug)

    elif execution_mode == "integration":
        for script in text:
            exec.parse_text(script)
        validate_datasources(exec, mock=False, quiet=False)
        print_success("Integration tests passed successfully!")

    elif execution_mode == "unit":
        for script in text:
            exec.parse_text(script)
        validate_datasources(exec, mock=True, quiet=False)
        print_success("Unit tests passed successfully!")


def run_parallel_execution(
    cli_params: CLIRuntimeParams,
    execution_fn,
    execution_mode: str = "run",
) -> None:
    """
    Run parallel execution for directory inputs, or single-script execution
    with polished progress display for single files/inline queries.

    Args:
        cli_params: CLI runtime parameters containing all execution settings
        execution_fn: Function to execute each script (exec, node, quiet) -> None
        execution_mode: One of 'run', 'integration', or 'unit'
    """
    # Check if input is a directory (parallel execution)
    pathlib_input = PathlibPath(cli_params.input)
    files_iter, directory, input_type, input_name, config = resolve_input_information(
        cli_params.input, cli_params.config_path
    )
    files = list(files_iter)

    # Merge CLI params with config file
    edialect, parallelism = merge_runtime_config(cli_params, config)
    if not pathlib_input.exists() or len(files) == 1:
        # Inline query - use polished single-script execution

        run_single_script_execution(
            files=files,
            directory=directory,
            input_type=input_type,
            input_name=input_name,
            edialect=edialect,
            param=cli_params.param,
            conn_args=cli_params.conn_args,
            debug=cli_params.debug,
            execution_mode=execution_mode,
            config=config,
        )
        return
    # Multiple files - use parallel execution
    show_execution_info(input_type, input_name, edialect.value, cli_params.debug)

    # Get execution strategy
    strategy = get_execution_strategy(cli_params.execution_strategy)

    # Set up parallel executor
    parallel_exec = ParallelExecutor(
        max_workers=parallelism,
        dependency_strategy=ETLDependencyStrategy(),
        execution_strategy=strategy,
    )

    # Get execution plan for display
    if pathlib_input.is_dir():
        execution_plan = parallel_exec.get_folder_execution_plan(pathlib_input)
    elif pathlib_input.is_file():
        execution_plan = parallel_exec.get_execution_plan([pathlib_input])
    else:
        raise FileNotFoundError(f"Input path '{pathlib_input}' does not exist.")

    num_edges = execution_plan.number_of_edges()
    num_nodes = execution_plan.number_of_nodes()

    show_parallel_execution_start(
        num_nodes, num_edges, parallelism, cli_params.execution_strategy
    )

    # Factory to create executor for each script
    def executor_factory(node: ScriptNode) -> Executor:
        return create_executor_for_script(
            node,
            cli_params.param,
            cli_params.conn_args,
            edialect,
            cli_params.debug,
            config,
        )

    # Wrap execution_fn to pass quiet=True for parallel execution
    def quiet_execution_fn(exec: Executor, node: ScriptNode) -> None:
        execution_fn(exec, node, quiet=True)

    # Run parallel execution
    summary = parallel_exec.execute(
        root=pathlib_input,
        executor_factory=executor_factory,
        execution_fn=quiet_execution_fn,
        on_script_complete=show_script_result,
    )

    show_parallel_execution_summary(summary)

    if not summary.all_succeeded:
        print_error("Some scripts failed during execution.")
        raise Exit(1)

    print_success("All scripts executed successfully!")


@group()
@option("--debug", default=False, help="Enable debug mode")
@pass_context
def cli(ctx, debug: bool):
    """Trilogy CLI - A beautiful query execution tool."""
    ctx.ensure_object(dict)
    ctx.obj["DEBUG"] = debug

    if debug:
        show_debug_mode()


@cli.command("fmt")
@argument("input", type=Path(exists=True))
@pass_context
def fmt(ctx, input):
    """Format a Trilogy script file."""
    with with_status("Formatting script"):
        start = datetime.now()
        try:
            with open(input, "r") as f:
                script = f.read()
            _, queries = parse(script)
            r = Renderer()
            with open(input, "w") as f:
                f.write("\n".join([r.to_string(x) for x in queries]))
            duration = datetime.now() - start

            print_success("Script formatted successfully")
            show_formatting_result(input, len(queries), duration)

        except Exception as e:
            handle_execution_exception(e, debug=ctx.obj["DEBUG"])


@cli.command(
    "integration",
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)
@argument("input", type=Path())
@argument("dialect", type=str, required=False)
@option("--param", multiple=True, help="Environment parameters as key=value pairs")
@option(
    "--parallelism",
    "-p",
    default=None,
    help="Maximum parallel workers for directory execution",
)
@option(
    "--config", type=Path(exists=True), help="Path to trilogy.toml configuration file"
)
@argument("conn_args", nargs=-1, type=UNPROCESSED)
@pass_context
def integration(
    ctx, input, dialect: str | None, param, parallelism: int | None, config, conn_args
):
    """Run integration tests on Trilogy scripts."""
    cli_params = CLIRuntimeParams(
        input=input,
        dialect=Dialects(dialect) if dialect else None,
        parallelism=parallelism,
        param=param,
        conn_args=conn_args,
        debug=ctx.obj["DEBUG"],
        config_path=PathlibPath(config) if config else None,
        execution_strategy="eager_bfs",
    )

    try:
        run_parallel_execution(
            cli_params=cli_params,
            execution_fn=execute_script_for_integration,
            execution_mode="integration",
        )
    except Exit:
        raise
    except Exception as e:
        handle_execution_exception(e, debug=cli_params.debug)


@cli.command(
    "unit",
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)
@argument("input", type=Path())
@option("--param", multiple=True, help="Environment parameters as key=value pairs")
@option(
    "--parallelism",
    "-p",
    default=None,
    help="Maximum parallel workers for directory execution",
)
@option(
    "--config", type=Path(exists=True), help="Path to trilogy.toml configuration file"
)
@pass_context
def unit(
    ctx,
    input,
    param,
    parallelism: int | None,
    config,
):
    """Run unit tests on Trilogy scripts with mocked datasources."""
    # Build CLI runtime params (unit tests always use DuckDB)
    cli_params = CLIRuntimeParams(
        input=input,
        dialect=Dialects.DUCK_DB,
        parallelism=parallelism,
        param=param,
        conn_args=(),
        debug=ctx.obj["DEBUG"],
        config_path=PathlibPath(config) if config else None,
        execution_strategy="eager_bfs",
    )

    try:
        run_parallel_execution(
            cli_params=cli_params,
            execution_fn=execute_script_for_unit,
            execution_mode="unit",
        )
    except Exit:
        raise
    except Exception as e:
        handle_execution_exception(e, debug=cli_params.debug)


@cli.command(
    "run",
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)
@argument("input", type=Path())
@argument("dialect", type=str, required=False)
@option("--param", multiple=True, help="Environment parameters as key=value pairs")
@option(
    "--parallelism",
    "-p",
    default=None,
    help="Maximum parallel workers for directory execution",
)
@option(
    "--config", type=Path(exists=True), help="Path to trilogy.toml configuration file"
)
@argument("conn_args", nargs=-1, type=UNPROCESSED)
@pass_context
def run(
    ctx, input, dialect: str | None, param, parallelism: int | None, config, conn_args
):
    """Execute a Trilogy script or query."""
    cli_params = CLIRuntimeParams(
        input=input,
        dialect=Dialects(dialect) if dialect else None,
        parallelism=parallelism,
        param=param,
        conn_args=conn_args,
        debug=ctx.obj["DEBUG"],
        config_path=PathlibPath(config) if config else None,
        execution_strategy="eager_bfs",
    )

    try:
        run_parallel_execution(
            cli_params=cli_params,
            execution_fn=execute_script_for_run,
            execution_mode="run",
        )
    except Exit:
        raise
    except Exception as e:
        handle_execution_exception(e, debug=cli_params.debug)


if __name__ == "__main__":
    cli()
