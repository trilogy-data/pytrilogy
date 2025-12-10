"""Testing commands (integration and unit) for Trilogy CLI."""

from pathlib import Path as PathlibPath

from click import UNPROCESSED, Path, argument, option, pass_context
from click.exceptions import Exit

from trilogy import Executor
from trilogy.dialect.enums import Dialects
from trilogy.scripts.common import (
    CLIRuntimeParams,
    handle_execution_exception,
    validate_datasources,
)
from trilogy.scripts.dependency import ScriptNode
from trilogy.scripts.parallel_execution import run_parallel_execution


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
