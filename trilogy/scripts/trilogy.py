import traceback
from datetime import datetime
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
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.parsing.render import Renderer
from trilogy.scripts.dependency import (
    FolderDepthStrategy,
    ScriptNode,
)
from trilogy.scripts.display import (
    RICH_AVAILABLE,
    ResultSet,
    create_progress_context,
    print_error,
    print_results_table,
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
from trilogy.scripts.execution import (
    execute_queries_simple,
    execute_queries_with_progress,
)
from trilogy.scripts.parallel import (
    EagerBFSStrategy,
    ParallelExecutor,
)

set_rich_mode = set_rich_mode

# Default parallelism level
DEFAULT_PARALLELISM = 5


def resolve_input(path: PathlibPath) -> list[PathlibPath]:

    # Directory
    if path.is_dir():
        pattern = "**/*.preql"
        return sorted(path.glob(pattern))
    # Single file
    if path.exists() and path.is_file():
        return [path]

    raise FileNotFoundError(f"Input path '{path}' does not exist.")


def resolve_input_information(input: str) -> tuple[list[str], PathlibPath, str, str]:
    text: list[str] = []
    if PathlibPath(input).exists():
        pathlib_path = PathlibPath(input)
        files = resolve_input(pathlib_path)

        for file in files:
            with open(file, "r") as f:
                text.append(f.read())
        if pathlib_path.is_dir():
            directory = pathlib_path
            input_type = "directory"
        else:
            directory = pathlib_path.parent
            input_type = "file"
        input_name = pathlib_path.name
    else:
        script = input

        directory = PathlibPath.cwd()
        input_type = "query"
        input_name = "inline"
        text.append(script)
    return text, directory, input_type, input_name


def resolve_input_files(input: str) -> tuple[list[PathlibPath], PathlibPath, str, str]:
    """
    Resolve input to a list of file paths (for parallel execution).

    """
    if PathlibPath(input).exists():
        pathlib_path = PathlibPath(input)
        files = resolve_input(pathlib_path)

        if pathlib_path.is_dir():
            directory = pathlib_path
            input_type = "directory"
        else:
            directory = pathlib_path.parent
            input_type = "file"
        input_name = pathlib_path.name
        return files, directory, input_type, input_name
    else:
        # Inline query - not applicable for parallel execution
        raise FileNotFoundError(
            f"Directory {input} does not exist for parallel execution."
        )


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


def get_dialect_config(edialect: Dialects, conn_dict: dict[str, Any]) -> Any:
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

    return conf


def create_executor(
    param: tuple[str, ...],
    directory: PathlibPath,
    conn_args: Iterable[str],
    edialect: Dialects,
    debug: bool,
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
        conf = get_dialect_config(edialect, conn_dict)
    except Exception as e:
        print_error(f"Failed to configure dialect: {e}")
        print_error(f"Full traceback:\n{traceback.format_exc()}")
        raise Exit(1) from e

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
    return exec


def create_executor_for_script(
    node: ScriptNode,
    param: tuple[str, ...],
    conn_args: Iterable[str],
    edialect: Dialects,
    debug: bool,
) -> Executor:
    """
    Create an executor for a specific script node.

    Each script gets its own executor with its own environment,
    using the script's parent directory as the working path.
    """
    directory = node.path.parent
    return create_executor(param, directory, conn_args, edialect, debug)


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
    queries = exec.parse_text(node.content)
    for query in queries:
        results = exec.execute_query(query)
        if results and not quiet:
            data = results.fetchall()
            if not quiet:
                print_results_table(ResultSet(rows=data, columns=results.keys()))


def execute_script_for_integration(
    exec: Executor, node: ScriptNode, quiet: bool = False
) -> None:
    """Execute a script for the 'integration' command (parse + validate)."""
    exec.parse_text(node.content)
    validate_datasources(exec, mock=False, quiet=quiet)


def execute_script_for_unit(
    exec: Executor, node: ScriptNode, quiet: bool = False
) -> None:
    """Execute a script for the 'unit' command (parse + mock validate)."""
    exec.parse_text(node.content)
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


def handle_execution_exception(e: Exception) -> None:
    print_error(f"Unexpected error during execution: {e}")
    print_error(f"Full traceback:\n{traceback.format_exc()}")
    raise Exit(1) from e


def run_single_script_execution(
    text: list[str],
    directory: PathlibPath,
    input_type: str,
    input_name: str,
    edialect: Dialects,
    param: tuple[str, ...],
    conn_args: Iterable[str],
    debug: bool,
    execution_mode: str,
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

    exec = create_executor(param, directory, conn_args, edialect, debug)

    if execution_mode == "run":
        # Parse all scripts and collect queries
        queries = []
        try:
            for script in text:
                queries += exec.parse_text(script)
        except Exception as e:
            print_error(f"Failed to parse script: {e}")
            print_error(f"Full traceback:\n{traceback.format_exc()}")
            raise Exit(1) from e

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
            handle_execution_exception(e)

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
    input: str,
    edialect: Dialects,
    param: tuple[str, ...],
    conn_args: Iterable[str],
    debug: bool,
    parallelism: int,
    execution_fn,
    execution_strategy: str = "eager_bfs",
    execution_mode: str = "run",
) -> None:
    """
    Run parallel execution for directory inputs, or single-script execution
    with polished progress display for single files/inline queries.

    Args:
        input: Input path (file or directory)
        edialect: Dialect to use
        param: Environment parameters
        conn_args: Connection arguments
        debug: Debug mode flag
        parallelism: Maximum parallel workers
        execution_fn: Function to execute each script (exec, node, quiet) -> None
        execution_strategy: Name of execution strategy ("eager_bfs" or "level")
        execution_mode: One of 'run', 'integration', or 'unit'
    """
    # Check if input is a directory (parallel execution)
    pathlib_input = PathlibPath(input)

    if not pathlib_input.exists():
        # Inline query - use polished single-script execution
        text, directory, input_type, input_name = resolve_input_information(input)
        run_single_script_execution(
            text=text,
            directory=directory,
            input_type=input_type,
            input_name=input_name,
            edialect=edialect,
            param=param,
            conn_args=conn_args,
            debug=debug,
            execution_mode=execution_mode,
        )
        return

    files, directory, input_type, input_name = resolve_input_files(input)

    if len(files) <= 1:
        # Single file - use more verbose output
        text, directory, input_type, input_name = resolve_input_information(input)
        run_single_script_execution(
            text=text,
            directory=directory,
            input_type=input_type,
            input_name=input_name,
            edialect=edialect,
            param=param,
            conn_args=conn_args,
            debug=debug,
            execution_mode=execution_mode,
        )
        return

    # Multiple files - use parallel execution
    show_execution_info(input_type, input_name, edialect.value, debug)

    # Get execution strategy
    strategy = get_execution_strategy(execution_strategy)

    # Set up parallel executor
    parallel_exec = ParallelExecutor(
        max_workers=parallelism,
        dependency_strategy=FolderDepthStrategy(),
        execution_strategy=strategy,
    )

    # Get execution plan for display
    execution_plan = parallel_exec.get_execution_plan(files)
    num_edges = execution_plan.number_of_edges()

    show_parallel_execution_start(
        len(files), num_edges, parallelism, execution_strategy
    )

    # Factory to create executor for each script
    def executor_factory(node: ScriptNode) -> Executor:
        return create_executor_for_script(node, param, conn_args, edialect, debug)

    # Wrap execution_fn to pass quiet=True for parallel execution
    def quiet_execution_fn(exec: Executor, node: ScriptNode) -> None:
        execution_fn(exec, node, quiet=True)

    # Run parallel execution
    summary = parallel_exec.execute(
        files=files,
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
            print_error(f"Failed to format script: {e}")
            print_error(f"Full traceback:\n{traceback.format_exc()}")
            raise Exit(1)


@cli.command(
    "integration",
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)
@argument("input", type=Path())
@argument("dialect", type=str)
@option("--param", multiple=True, help="Environment parameters as key=value pairs")
@option(
    "--parallelism",
    "-p",
    default=DEFAULT_PARALLELISM,
    help="Maximum parallel workers for directory execution",
)
@argument("conn_args", nargs=-1, type=UNPROCESSED)
@pass_context
def integration(ctx, input, dialect: str, param, parallelism: int, conn_args):
    """Run integration tests on Trilogy scripts."""
    edialect = Dialects(dialect)
    strategy = "eager_bfs"
    debug = ctx.obj["DEBUG"]

    try:
        run_parallel_execution(
            input=input,
            edialect=edialect,
            param=param,
            conn_args=conn_args,
            debug=debug,
            parallelism=parallelism,
            execution_fn=execute_script_for_integration,
            execution_strategy=strategy,
            execution_mode="integration",
        )
    except Exit:
        raise
    except Exception as e:
        handle_execution_exception(e)


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
    default=DEFAULT_PARALLELISM,
    help="Maximum parallel workers for directory execution",
)
@pass_context
def unit(
    ctx,
    input,
    param,
    parallelism: int,
):
    """Run unit tests on Trilogy scripts with mocked datasources."""
    edialect = Dialects.DUCK_DB
    debug = ctx.obj["DEBUG"]
    strategy = "eager_bfs"
    try:
        run_parallel_execution(
            input=input,
            edialect=edialect,
            param=param,
            conn_args=(),
            debug=debug,
            parallelism=parallelism,
            execution_fn=execute_script_for_unit,
            execution_strategy=strategy,
            execution_mode="unit",
        )
    except Exit:
        raise
    except Exception as e:
        handle_execution_exception(e)


@cli.command(
    "run",
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)
@argument("input", type=Path())
@argument("dialect", type=str)
@option("--param", multiple=True, help="Environment parameters as key=value pairs")
@option(
    "--parallelism",
    "-p",
    default=DEFAULT_PARALLELISM,
    help="Maximum parallel workers for directory execution",
)
@argument("conn_args", nargs=-1, type=UNPROCESSED)
@pass_context
def run(ctx, input, dialect: str, param, parallelism: int, conn_args):
    """Execute a Trilogy script or query."""
    edialect = Dialects(dialect)
    debug = ctx.obj["DEBUG"]
    strategy = "eager_bfs"
    try:
        run_parallel_execution(
            input=input,
            edialect=edialect,
            param=param,
            conn_args=conn_args,
            debug=debug,
            parallelism=parallelism,
            execution_fn=execute_script_for_run,
            execution_strategy=strategy,
            execution_mode="run",
        )
    except Exit:
        raise
    except Exception as e:
        print_error(f"Execution failed: {e}")
        print_error(f"Full traceback:\n{traceback.format_exc()}")
        raise Exit(1) from e


if __name__ == "__main__":
    cli()
