"""Run command for Trilogy CLI."""

from pathlib import Path as PathlibPath

from click import UNPROCESSED, Path, argument, option, pass_context
from click.exceptions import Exit

from trilogy import Executor
from trilogy.dialect.enums import Dialects
from trilogy.scripts.click_utils import validate_dialect
from trilogy.scripts.common import (
    CLIRuntimeParams,
    ExecutionStats,
    execute_script_with_stats,
    handle_execution_exception,
)
from trilogy.scripts.dependency import ScriptNode
from trilogy.scripts.parallel_execution import ExecutionMode, run_parallel_execution


def execute_script_for_run(
    exec: Executor, node: ScriptNode, quiet: bool = False
) -> ExecutionStats:
    """Execute a script for the 'run' command (parallel execution mode)."""
    return execute_script_with_stats(exec, node.path, run_statements=True)


def _normalize_import(value: str) -> str:
    """Convert a path-ish --import value into a trilogy import module name.

    Accepts bare module names (``flight``), filenames (``flight.preql``), and
    relative paths (``root/flight.preql``) and returns the dotted module name
    trilogy expects (``flight``, ``root.flight``).
    """
    stripped = value.strip()
    if stripped.endswith(".preql"):
        stripped = stripped[: -len(".preql")]
    stripped = stripped.replace("\\", "/").strip("/")
    while stripped.startswith("./"):
        stripped = stripped[2:]
    return stripped.replace("/", ".")


@argument("input", type=Path(), default=".")
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
@option(
    "--env",
    "-e",
    multiple=True,
    help="Set env vars as KEY=VALUE or pass an env file path",
)
@option(
    "--import",
    "imports",
    multiple=True,
    help=(
        "Prepend 'import <module>;' to an inline query. Accepts bare module "
        "names (flight), filenames (flight.preql), or relative paths "
        "(root/flight.preql). Repeatable."
    ),
)
@argument("conn_args", nargs=-1, type=UNPROCESSED)
@pass_context
def run(
    ctx,
    input,
    dialect: str | None,
    param,
    parallelism: int | None,
    config,
    env,
    imports: tuple[str, ...],
    conn_args,
):
    """Execute a Trilogy script or query."""
    validate_dialect(dialect, "run")

    if imports:
        pathlib_input = PathlibPath(input)
        if pathlib_input.exists():
            from trilogy.scripts.display import print_error

            print_error(
                "--import only applies to inline queries, not file/directory inputs."
            )
            raise Exit(2)
        prefix = "".join(f"import {_normalize_import(v)};\n" for v in imports)
        input = prefix + input

    cli_params = CLIRuntimeParams(
        input=input,
        dialect=Dialects(dialect) if dialect else None,
        parallelism=parallelism,
        param=param,
        conn_args=conn_args,
        debug=ctx.obj["DEBUG"],
        debug_file=ctx.obj.get("DEBUG_FILE"),
        config_path=PathlibPath(config) if config else None,
        execution_strategy="eager_bfs",
        env=env,
    )

    try:
        run_parallel_execution(
            cli_params=cli_params,
            execution_fn=execute_script_for_run,
            execution_mode=ExecutionMode.RUN,
        )
    except Exit:
        raise
    except Exception as e:
        handle_execution_exception(e, debug=cli_params.debug)
