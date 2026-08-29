"""Run command for Trilogy CLI."""

import sys
from functools import partial
from pathlib import Path as PathlibPath

import click
from click import UNPROCESSED, Path, argument, option, pass_context
from click.exceptions import Exit

from trilogy import Executor
from trilogy.dialect.enums import Dialects
from trilogy.scripts.click_utils import (
    dry_run_option,
    misplaced_group_value_hint,
    report_options,
    state_file_option,
    validate_dialect,
)
from trilogy.scripts.common import (
    CLIRuntimeParams,
    ExecutionStats,
    execute_script_with_stats,
    handle_execution_exception,
)
from trilogy.scripts.dependency import ScriptNode
from trilogy.scripts.parallel_execution import ExecutionMode, run_parallel_execution


def execute_script_for_run(
    exec: Executor, node: ScriptNode, quiet: bool = False, dry_run: bool = False
) -> ExecutionStats:
    """Execute a script for the 'run' command (parallel execution mode)."""
    return execute_script_with_stats(exec, node.path, dry_run=dry_run)


def _looks_like_missing_path(value: str) -> bool:
    """True if ``value`` looks like a file path (extension or separator).

    Inline SQL legitimately contains ``/`` (division) and ``\\`` (rare escape),
    so any whitespace or statement terminator means we treat it as inline.
    """
    if any(c.isspace() for c in value) or ";" in value:
        return False
    return value.endswith((".preql", ".sql")) or "/" in value or "\\" in value


def _normalize_run_input(input: str, imports: tuple[str, ...]) -> str:
    """Resolve the ``run`` input to what the executor should parse.

    Raises ``Exit(2)`` for a missing path or ``--import`` used with a file
    input. Empty input (``-`` with empty stdin) is always inline — without that
    guard, ``Path("").exists()`` returns True on most platforms (it resolves to
    the cwd) and the ``--import`` check below would reject the call.
    """
    from trilogy.scripts.display import print_error

    is_inline = not input or not PathlibPath(input).exists()
    # A path-looking input that does not exist fails explicitly rather than
    # falling through to inline mode, where the parser reports a confusing
    # "No dialect specified".
    if is_inline and _looks_like_missing_path(input):
        print_error(f"Input '{input}' does not exist.")
        raise Exit(2)

    if imports:
        if not is_inline:
            print_error(
                "--import only applies to inline queries, not file/directory inputs."
            )
            raise Exit(2)
        input = "".join(_format_import(v) for v in imports) + input

    if is_inline:
        # Inline queries may omit the trailing terminator; the parser needs it.
        stripped = input.rstrip()
        if stripped and not stripped.endswith(";"):
            return stripped + ";"
    return input


def _normalize_import(value: str) -> str:
    """Convert a path-ish --import value into a trilogy import module name.

    Accepts bare module names (``flight``), filenames (``flight.preql``), and
    relative paths (``root/flight.preql``) and returns the dotted module name
    trilogy expects (``flight``, ``root.flight``).
    """
    stripped = value.strip()
    stripped = stripped.removesuffix(".preql")
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
    "--timeout",
    "timeout",
    type=click.FloatRange(min=0, min_open=True),
    default=None,
    help=(
        "Seconds a single statement may run before it is cancelled. Off by "
        "default. The driver is asked to abort the query, so the warehouse stops "
        "working rather than the CLI merely giving up on it; dialects whose "
        "driver cannot cancel a statement reject the flag instead of ignoring it."
    ),
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
        "every aggregate and window value the statement computes. In JSON mode "
        "this full block is emitted as `agg_window_rows_used` only when "
        "TRILOGY_AGENT_SCOPE_DIAGNOSTICS is set; the distilled `warnings` list is "
        "on by default (disable with TRILOGY_AGENT_SCOPE_WARNINGS=0)."
    ),
)
@option(
    "--environment",
    default=None,
    help="Build into this deployment environment (overrides the activated one)",
)
@dry_run_option(
    "Compile every statement and print the SQL it would issue, without "
    "executing any of it. Nothing is queried, persisted or created."
)
@report_options
@state_file_option
@argument("conn_args", nargs=-1, type=UNPROCESSED)
@pass_context
def run(
    ctx,
    input,
    dialect: str | None,
    param,
    parallelism: int | None,
    timeout: float | None,
    config,
    env,
    imports: tuple[str, ...],
    displayed_rows: int,
    all_rows: bool,
    scope: bool,
    environment: str | None,
    dry_run: bool,
    report_file: str | None,
    run_id: str | None,
    state_input: str | None,
    state_file: str | None,
    state_partition: tuple[str, ...],
    state_max_partitions: str | None,
    conn_args,
):
    """Execute a Trilogy script or query."""
    validate_dialect(dialect, "run")

    # `-` reads the query body from stdin (the conventional CLI sentinel).
    # Empty stdin is allowed — combined with `--import`, this lets a caller
    # validate that the imports parse without running any query body.
    if input == "-":
        input = sys.stdin.read()

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

    from trilogy.execution.report import report_run

    try:
        # Input validation lives INSIDE report_run so a --report-file consumer
        # gets the guaranteed fallback summary even for config errors (missing
        # path, misused --import) that die before the file loop.
        with report_run(
            "run",
            report_file,
            run_id,
            target=str(input)[:200],
            dialect=dialect,
            parallelism=parallelism,
            config_path=str(config) if config else None,
            # Stamped on run_start so a report consumer can tell an invocation
            # that wrote nothing on purpose from one that did the work.
            dry_run=dry_run or None,
        ):
            cli_params = CLIRuntimeParams(
                input=_normalize_run_input(input, imports),
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
                timeout=timeout,
                dry_run=dry_run,
            )
            from trilogy.execution.envs import env_activation_scope
            from trilogy.scripts.env_commands import (
                announce_activation,
                resolve_activation,
            )
            from trilogy.scripts.state import (
                maybe_write_state_snapshot,
                state_input_scope,
            )

            activation = resolve_activation(
                environment, str(input), cli_params.config_path
            )
            announce_activation(activation)
            with env_activation_scope(activation):
                try:
                    with state_input_scope(state_input, cli_params):
                        run_parallel_execution(
                            cli_params=cli_params,
                            execution_fn=partial(
                                execute_script_for_run, dry_run=dry_run
                            ),
                            execution_mode=ExecutionMode.RUN,
                        )
                    # A dry run built nothing, so the env's recorded model
                    # fingerprint must not claim it did -- the same guard
                    # refresh applies.
                    if activation and not dry_run:
                        from trilogy.scripts.env_commands import (
                            record_env_fingerprint,
                        )

                        record_env_fingerprint(
                            activation, str(input), param, cli_params.config_path
                        )
                finally:
                    # Snapshot regardless of outcome; never alters the exit code.
                    maybe_write_state_snapshot(
                        cli_params, state_file, state_partition, state_max_partitions
                    )
    except Exit:
        raise
    except Exception as e:
        handle_execution_exception(e, debug=ctx.obj["DEBUG"])
