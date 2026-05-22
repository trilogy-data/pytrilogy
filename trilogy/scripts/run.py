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


def _format_import(value: str) -> str:
    """Render a --import value as an ``import ...;`` statement line.

    ``module:alias`` namespaces the import so its concepts are reached as
    ``alias.*``, matching file-based ``import ... as ...``. A bare value
    imports without a namespace prefix.
    """
    spec, sep, alias = value.partition(":")
    # A lone leading drive letter (Windows path) is not an alias separator.
    if sep and len(spec) == 1 and spec.isalpha():
        spec, alias = value, ""
    module = _normalize_import(spec)
    alias = alias.strip()
    return f"import {module} as {alias};\n" if alias else f"import {module};\n"


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
        "Prepend an import to an inline query. Accepts bare module names "
        "(flight), filenames (flight.preql), or relative paths "
        "(root/flight.preql). Append ':alias' to namespace the import so its "
        "concepts are reached as alias.* (e.g. raw/item:item), matching "
        "file-based 'import ... as ...'. Repeatable."
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

    is_inline = not PathlibPath(input).exists()

    if imports:
        if not is_inline:
            from trilogy.scripts.display import print_error

            print_error(
                "--import only applies to inline queries, not file/directory inputs."
            )
            raise Exit(2)
        input = "".join(_format_import(v) for v in imports) + input

    if is_inline:
        # Inline queries may omit the trailing terminator; the parser needs it.
        stripped = input.rstrip()
        if stripped and not stripped.endswith(";"):
            input = stripped + ";"

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
