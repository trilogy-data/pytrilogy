import traceback
from datetime import datetime
from pathlib import Path as PathlibPath
from typing import Any, Iterable, Union

from click import UNPROCESSED, Path, argument, group, option, pass_context
from click.exceptions import Exit

from trilogy import Executor, parse
from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.exceptions import ConfigurationException
from trilogy.core.models.environment import Environment
from trilogy.core.statements.execute import PROCESSED_STATEMENT_TYPES
from trilogy.dialect.enums import Dialects
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.parsing.render import Renderer
from trilogy.scripts.display import (
    FETCH_LIMIT,
    ResultSet,
    create_progress_context,
    print_error,
    print_info,
    print_results_table,
    print_success,
    set_rich_mode,
    show_debug_mode,
    show_environment_params,
    show_execution_info,
    show_execution_start,
    show_execution_summary,
    show_formatting_result,
    show_statement_result,
    show_statement_type,
    with_status,
)

set_rich_mode = set_rich_mode


def smart_convert(value: str):
    """Convert string to appropriate Python type."""
    if not value:
        return value

    # Handle booleans
    if value.lower() in ("true", "false"):
        return value.lower() == "true"

    # Try numeric conversion
    try:
        if "." not in value and "e" not in value.lower():
            return int(value)
        return float(value)
    except ValueError:
        return value


def pairwise(t: Iterable[Any]) -> Iterable[tuple[Any, Any]]:
    it = iter(t)
    return zip(it, it)


def extra_to_kwargs(arg_list: Iterable[str]) -> dict[str, Any]:
    pairs = pairwise(arg_list)
    final = {}
    for k, v in pairs:
        k = k.lstrip("--")
        final[k] = smart_convert(v)
    return final


def parse_env_params(env_param_list: tuple[str]) -> dict[str, str]:
    """Parse environment parameters from key=value format."""
    env_params = {}
    for param in env_param_list:
        if "=" not in param:
            raise ValueError(
                f"Environment parameter must be in key=value format: {param}"
            )
        key, value = param.split("=", 1)  # Split on first = only
        env_params[key] = smart_convert(value)
    return env_params


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


def get_statement_type(statement: PROCESSED_STATEMENT_TYPES) -> str:
    """Get the type/class name of a statement."""
    return type(statement).__name__


def execute_single_statement(
    exec: Executor,
    query: PROCESSED_STATEMENT_TYPES,
    idx: int,
    total_queries: int,
    use_progress=False,
) -> tuple[bool, ResultSet | None, Any, Union[Exception, None]]:
    """Execute a single statement and handle results/errors consistently."""
    # Log the statement type before execution
    statement_type = get_statement_type(query)
    if not use_progress:  # Only show type when not using progress bar
        show_statement_type(idx, total_queries, statement_type)

    start_time = datetime.now()

    try:
        raw_results = exec.execute_statement(query)
        results = (
            ResultSet(
                rows=raw_results.fetchmany(FETCH_LIMIT + 1), columns=raw_results.keys()
            )
            if raw_results
            else None
        )
        duration = datetime.now() - start_time

        if not use_progress:
            show_statement_result(idx, total_queries, duration, bool(results))

        return True, results, duration, None

    except Exception as e:
        duration = datetime.now() - start_time

        if not use_progress:
            show_statement_result(idx, total_queries, duration, False, str(e), type(e))

        return False, None, duration, e


def execute_queries_with_progress(
    exec: Executor, queries: list[PROCESSED_STATEMENT_TYPES]
) -> Exception | None:
    """Execute queries with Rich progress bar. Returns True if all succeeded, False if any failed."""
    progress = create_progress_context(len(queries))
    results_to_print = []
    exception = None

    with progress:
        task = progress.add_task("Executing statements...", total=len(queries))

        for idx, query in enumerate(queries):
            statement_type = get_statement_type(query)
            progress.update(
                task, description=f"Statement {idx+1}/{len(queries)} ({statement_type})"
            )

            success, results, duration, error = execute_single_statement(
                exec, query, idx, len(queries), use_progress=True
            )

            if not success:
                exception = error

            # Store results for printing after progress is done
            results_to_print.append(
                (idx, len(queries), duration, success, results, error)
            )
            progress.advance(task)
            if exception:
                break

    # Print all results after progress bar is finished
    for idx, total_queries, duration, success, results, error in results_to_print:
        if error:
            show_statement_result(
                idx, total_queries, duration, False, str(error), type(error)
            )
            print_error(f"Full traceback:\n{traceback.format_exc()}")
        else:
            show_statement_result(idx, total_queries, duration, bool(results))
            if results and not error:
                print_results_table(results)

    return exception


def execute_queries_simple(
    exec: Executor, queries: list[PROCESSED_STATEMENT_TYPES]
) -> Exception | None:
    """Execute queries with simple output. Returns True if all succeeded, False if any failed."""
    exception = None

    for idx, query in enumerate(queries):
        if len(queries) > 1:
            print_info(f"Executing statement {idx+1} of {len(queries)}...")

        success, results, duration, error = execute_single_statement(
            exec, query, idx, len(queries), use_progress=False
        )

        if not success:
            exception = error

        if results and not error:
            print_results_table(results)

    return exception


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
    "run",
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)
@argument("input", type=Path())
@argument("dialect", type=str)
@option("--param", multiple=True, help="Environment parameters as key=value pairs")
@argument("conn_args", nargs=-1, type=UNPROCESSED)
@pass_context
def run(ctx, input, dialect: str, param, conn_args):
    """Execute a Trilogy script or query."""

    # Setup input handling
    if PathlibPath(input).exists():
        inputp = PathlibPath(input)
        with open(input, "r") as f:
            script = f.read()
        namespace = DEFAULT_NAMESPACE
        directory = inputp.parent
        input_type = "file"
        input_name = inputp.name
    else:
        script = input
        namespace = DEFAULT_NAMESPACE
        directory = PathlibPath.cwd()
        input_type = "query"
        input_name = "inline"

    edialect = Dialects(dialect)
    debug = ctx.obj["DEBUG"]

    # Show execution info
    show_execution_info(input_type, input_name, dialect, debug)

    # Parse environment parameters from dedicated flag
    try:
        env_params = parse_env_params(param)
        show_environment_params(env_params)
    except ValueError as e:
        print_error(str(e))
        raise Exit(1) from e

    # Parse connection arguments from remaining args
    conn_dict = extra_to_kwargs(conn_args)

    # Configure dialect
    conf: Union[Any, None] = None
    try:
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
        else:
            conf = None
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

    # Parse and execute
    try:
        queries = exec.parse_text(script)
    except Exception as e:
        print_error(f"Failed to parse script: {e}")
        print_error(f"Full traceback:\n{traceback.format_exc()}")
        raise Exit(1) from e

    start = datetime.now()
    show_execution_start(len(queries))

    # Execute with progress tracking for multiple statements or simple execution
    progress = create_progress_context(len(queries))

    try:
        if progress:
            exception = execute_queries_with_progress(exec, queries)
        else:
            exception = execute_queries_simple(exec, queries)

        total_duration = datetime.now() - start
        show_execution_summary(len(queries), total_duration, exception is None)

        # Exit with code 1 if any queries failed
        if exception:
            raise Exit(1) from exception
    except Exit:
        raise
    except Exception as e:
        print_error(f"Unexpected error during execution: {e}")
        print_error(f"Full traceback:\n{traceback.format_exc()}")
        raise Exit(1) from e


if __name__ == "__main__":
    cli()
