import traceback
from datetime import datetime
from pathlib import Path as PathlibPath
from typing import Any, Union

from click import UNPROCESSED, Path, argument, group, option, pass_context
from click.exceptions import Exit

from trilogy import Executor, parse
from trilogy.constants import DEFAULT_NAMESPACE
from trilogy.core.exceptions import ConfigurationException, ModelValidationError, DatasourceModelValidationError
from trilogy.core.models.environment import Environment
from trilogy.dialect.enums import Dialects
from trilogy.hooks.query_debugger import DebuggingHook
from trilogy.parsing.render import Renderer
from trilogy.scripts.display import (
    RICH_AVAILABLE,
    create_progress_context,
    print_error,
    print_success,
    set_rich_mode,
    show_debug_mode,
    show_environment_params,
    show_execution_info,
    show_execution_start,
    show_execution_summary,
    show_formatting_result,
    with_status,
)
from trilogy.scripts.environment import extra_to_kwargs, parse_env_params
from trilogy.scripts.execution import (
    execute_queries_simple,
    execute_queries_with_progress,
)

set_rich_mode = set_rich_mode


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


def create_executor(
    param: tuple[str],
    directory: PathlibPath,
    conn_args: dict[str, Any],
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
    return exec


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
@argument("conn_args", nargs=-1, type=UNPROCESSED)
@pass_context
def integration(ctx, input, dialect: str, param, conn_args):
    text, directory, input_type, input_name = resolve_input_information(input)
    edialect = Dialects(dialect)
    debug = ctx.obj["DEBUG"]
    exec = create_executor(param, directory, conn_args, edialect, debug)
    for script in text:
        exec.parse_text(script)

    datasources = exec.environment.datasources.keys()
    if not datasources:
        print_success("No datasources found")
    else:
        try:
            exec.execute_text("validate datasources {};".format(", ".join(datasources)))
        except ModelValidationError as e:
            if not e.children:
                print_error(f"Datasource validation failed: {e.message}")
            for idx, child in enumerate(e.children or []):
                print_error(f"Error {idx + 1}: {child.message}")
            raise Exit(1) from e
        except Exception as e:
            raise Exit(1) from e
    print_success("Integration tests passed successfully!")


@cli.command(
    "unit",
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)
@argument("input", type=Path())
@option("--param", multiple=True, help="Environment parameters as key=value pairs")
@pass_context
def unit(ctx, input, param):
    text, directory, input_type, input_name = resolve_input_information(input)
    edialect = Dialects.DUCK_DB
    debug = ctx.obj["DEBUG"]
    exec = create_executor(param, directory, [], edialect, debug)
    for script in text:
        exec.parse_text(script)

    datasources = exec.environment.datasources.keys()
    if not datasources:
        print_success("No datasources found")
    else:
        exec.execute_text("mock datasources {};".format(", ".join(datasources)))
        try:
            exec.execute_text("validate datasources {};".format(", ".join(datasources)))
        except ModelValidationError as e:
            if not e.children:
                print_error(f"Datasource validation failed: {e.message}")
            for idx, child in enumerate(e.children or []):
                print_error(f"Error {idx + 1}: {child.message}")
            raise Exit(1) from e
        except Exception as e:
            raise Exit(1) from e
    print_success(f"Unit tests passed successfully!")



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

    text, directory, input_type, input_name = resolve_input_information(input)
    edialect = Dialects(dialect)
    debug = ctx.obj["DEBUG"]

    # Show execution info
    show_execution_info(input_type, input_name, dialect, debug)

    exec = create_executor(param, directory, conn_args, edialect, debug)
    # Parse and execute
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

    # Execute with progress tracking for multiple statements or simple execution
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
