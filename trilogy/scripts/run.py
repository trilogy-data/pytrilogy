"""Run command for Trilogy CLI."""

import sys
from pathlib import Path as PathlibPath

import click
from click import UNPROCESSED, Path, argument, option, pass_context
from click.exceptions import Exit

from trilogy import Executor
from trilogy.dialect.enums import Dialects
from trilogy.scripts.click_utils import misplaced_group_value_hint, validate_dialect
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


def _looks_like_missing_path(value: str) -> bool:
    """True if ``value`` looks like a file path (extension or separator).

    Inline SQL legitimately contains ``/`` (division) and ``\\`` (rare escape),
    so any whitespace or statement terminator means we treat it as inline.
    """
    if any(c.isspace() for c in value) or ";" in value:
        return False
    return (
        value.endswith(".preql")
        or value.endswith(".sql")
        or "/" in value
        or "\\" in value
    )


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
    type=int,
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
        "Prepend an import to an inline query. Use the SAME dotted form as "
        "in-file imports: 'raw.unified_sales:s' becomes 'import "
        "raw.unified_sales as s;'. Slash/.preql path forms ('raw/item.preql') "
        "still work and convert to dotted, but prefer the dotted form so the "
        "CLI and file syntax match. Repeatable."
    ),
)
@option(
    "--displayed-rows",
    "displayed_rows",
    type=int,
    default=25,
    show_default=True,
    help=(
        "Cap on result rows displayed per statement. The query still runs in "
        "full — this caps the dump only. When the result exceeds the cap, the "
        "table is middle-truncated (first half + ellipsis + last half) so you "
        "see both the head and the tail. The footer reports displayed-vs-total "
        "rows. Use `--all-rows` to disable the cap."
    ),
)
@option(
    "--all-rows",
    is_flag=True,
    default=False,
    help="Show every result row, overriding --displayed-rows. Useful for piping.",
)
@option(
    "--scope",
    "scope",
    is_flag=True,
    default=False,
    help=(
        "Print a 'Derived value scopes' block after each result: the effective "
        "input row filters, grouping/partitioning, and post-computation filters of "
        "every aggregate and window value the statement computes. JSON output "
        "always includes this data as `derived_value_scopes`."
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
    displayed_rows: int,
    all_rows: bool,
    scope: bool,
    conn_args,
):
    """Execute a Trilogy script or query."""
    validate_dialect(dialect, "run")

    # `-` reads the query body from stdin (the conventional CLI sentinel).
    # Empty stdin is allowed — combined with `--import`, this lets a caller
    # validate that the imports parse without running any query body.
    if input == "-":
        input = sys.stdin.read()

    # Empty input (from `-` with empty stdin) is always inline — without this
    # guard, Path("").exists() returns True on most platforms (it resolves to
    # the cwd), causing the --import check below to reject the call.
    is_inline = not input or not PathlibPath(input).exists()
    # If the input clearly looks like a file path (.preql/.sql extension or
    # contains a path separator) but does not exist, fail explicitly instead of
    # falling through to inline-query mode and reporting a confusing "No
    # dialect specified" error from the parser.
    if is_inline and _looks_like_missing_path(input):
        from trilogy.scripts.display import print_error

        print_error(f"Input '{input}' does not exist.")
        raise Exit(2)

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

    if dialect:
        try:
            dialect_enum = Dialects(dialect)
        except ValueError as exc:
            valid = ", ".join(d.value for d in Dialects)
            msg = f"'{dialect}' is not a valid dialect. Choose one of: {valid}."
            hint = misplaced_group_value_hint(dialect, ctx, "run")
            if hint:
                msg = f"{msg}\n  {hint}"
            raise click.UsageError(msg) from exc
    else:
        dialect_enum = None
    cli_params = CLIRuntimeParams(
        input=input,
        dialect=dialect_enum,
        parallelism=parallelism,
        param=param,
        conn_args=conn_args,
        debug=ctx.obj["DEBUG"],
        debug_file=ctx.obj.get("DEBUG_FILE"),
        config_path=PathlibPath(config) if config else None,
        execution_strategy="eager_bfs",
        env=env,
        row_limit=None if all_rows else displayed_rows,
        show_scopes=scope,
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
