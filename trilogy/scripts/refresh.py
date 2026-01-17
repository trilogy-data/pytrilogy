"""Refresh command for Trilogy CLI - refreshes stale assets."""

from pathlib import Path as PathlibPath

from click import UNPROCESSED, Path, argument, option, pass_context
from click.exceptions import Exit

from trilogy import Executor
from trilogy.core.statements.execute import ProcessedValidateStatement
from trilogy.dialect.enums import Dialects
from trilogy.execution.state.state_store import (
    DatasourceWatermark,
    refresh_stale_assets,
)
from trilogy.scripts.common import (
    CLIRuntimeParams,
    ExecutionStats,
    RefreshParams,
    count_statement_stats,
    handle_execution_exception,
)
from trilogy.scripts.dependency import ScriptNode
from trilogy.scripts.parallel_execution import ExecutionMode, run_parallel_execution


def _format_watermarks(watermarks: dict[str, DatasourceWatermark]) -> None:
    """Print watermark information for all datasources."""
    from trilogy.scripts.display import print_info

    print_info("Watermarks:")
    for ds_id, watermark in sorted(watermarks.items()):
        if not watermark.keys:
            print_info(f"  {ds_id}: (no watermarks)")
            continue
        for key_name, update_key in watermark.keys.items():
            print_info(
                f"  {ds_id}.{key_name}: {update_key.value} ({update_key.type.value})"
            )


def execute_script_for_refresh(
    exec: Executor,
    node: ScriptNode,
    quiet: bool = False,
    print_watermarks: bool = False,
    force_sources: frozenset[str] = frozenset(),
) -> ExecutionStats:
    """Execute a script for the 'refresh' command - parse and refresh stale assets."""
    from trilogy.scripts.display import print_info, print_success, print_warning

    validation = []
    with open(node.path, "r") as f:
        statements = exec.parse_text(f.read())

    for x in statements:
        if isinstance(x, ProcessedValidateStatement):
            validation.append(x)

    stats = count_statement_stats([])

    def on_stale_found(stale_count: int, root_assets: int, all_assets: int) -> None:
        if stale_count == 0 and not quiet:
            print_info(
                f"No stale assets found in {node.path.name} ({root_assets}/{all_assets} root assets)"
            )
        elif not quiet:
            print_warning(f"Found {stale_count} stale asset(s) in {node.path.name}")

    def on_refresh(asset_id: str, reason: str) -> None:
        if not quiet:
            print_info(f"  Refreshing {asset_id}: {reason}")

    def on_watermarks(watermarks: dict[str, DatasourceWatermark]) -> None:
        if print_watermarks:
            _format_watermarks(watermarks)

    result = refresh_stale_assets(
        exec,
        on_stale_found=on_stale_found,
        on_refresh=on_refresh,
        on_watermarks=on_watermarks,
        force_sources=set(force_sources) if force_sources else None,
    )
    stats.update_count = result.refreshed_count

    for x in validation:
        exec.execute_statement(x)
        stats = count_statement_stats([x])

    if result.had_stale and not quiet:
        print_success(
            f"Refreshed {result.refreshed_count} asset(s) in {node.path.name}"
        )

    return stats


def make_refresh_execution_fn(print_watermarks: bool, force_sources: frozenset[str]):
    """Create a refresh execution function with the given parameters."""

    def wrapped_execute(
        exec: Executor, node: ScriptNode, quiet: bool = False
    ) -> ExecutionStats:
        return execute_script_for_refresh(
            exec, node, quiet, print_watermarks, force_sources
        )

    return wrapped_execute


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
    "--print-watermarks",
    is_flag=True,
    default=False,
    help="Print watermark values for all datasources before refreshing",
)
@option(
    "--env",
    "-e",
    multiple=True,
    help="Set environment variables as KEY=VALUE pairs",
)
@option(
    "--force",
    "-f",
    multiple=True,
    help="Force rebuild of specific datasources by name (skip staleness detection)",
)
@argument("conn_args", nargs=-1, type=UNPROCESSED)
@pass_context
def refresh(
    ctx,
    input,
    dialect: str | None,
    param,
    parallelism: int | None,
    config,
    print_watermarks,
    env,
    force,
    conn_args,
):
    """Refresh stale assets in Trilogy scripts.

    Parses each script, identifies datasources marked as 'root' (source of truth),
    compares watermarks to find stale derived assets, and refreshes them.

    Returns 0 if any assets were refreshed, 2 if all assets were up to date,
    and 1 on error.
    """
    refresh_params = RefreshParams(
        print_watermarks=print_watermarks,
        force_sources=frozenset(force),
    )

    cli_params = CLIRuntimeParams(
        input=input,
        dialect=Dialects(dialect) if dialect else None,
        parallelism=parallelism,
        param=param,
        conn_args=conn_args,
        debug=ctx.obj["DEBUG"],
        config_path=PathlibPath(config) if config else None,
        execution_strategy="eager_bfs",
        env=env,
        refresh_params=refresh_params,
    )

    execution_fn = make_refresh_execution_fn(
        refresh_params.print_watermarks, refresh_params.force_sources
    )

    try:
        summary = run_parallel_execution(
            cli_params=cli_params,
            execution_fn=execution_fn,
            execution_mode=ExecutionMode.REFRESH,
        )
        if summary.successful == 0 and summary.skipped > 0:
            # if everything was up to date, exit with code 2
            raise Exit(2)
    except Exit:
        raise
    except Exception as e:
        handle_execution_exception(e, debug=cli_params.debug)
