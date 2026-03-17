"""Refresh command for Trilogy CLI - refreshes stale assets."""

from pathlib import Path as PathlibPath

import click
from click import UNPROCESSED, Path, argument, option, pass_context
from click.exceptions import Exit

from trilogy import Executor
from trilogy.core.statements.execute import ProcessedValidateStatement
from trilogy.dialect.enums import Dialects
from trilogy.execution.state import (
    DatasourceWatermark,
    StaleAsset,
    refresh_stale_assets,
)
from trilogy.scripts.common import (
    CLIRuntimeParams,
    ExecutionStats,
    RefreshParams,
    RefreshQuery,
    count_statement_stats,
    handle_execution_exception,
)
from trilogy.scripts.dependency import ScriptNode
from trilogy.scripts.parallel_execution import ExecutionMode, run_parallel_execution


def _prompt_approval(
    stale_assets: list[StaleAsset],
    watermarks: dict[str, DatasourceWatermark],
) -> bool:
    """Show refresh plan and prompt user for approval."""
    import click

    from trilogy.scripts.display import show_refresh_plan

    show_refresh_plan(stale_assets, watermarks)
    return click.confirm("\nProceed with refresh?", default=True)


def execute_script_for_refresh(
    exec: Executor,
    node: ScriptNode,
    quiet: bool = False,
    print_watermarks: bool = False,
    force_sources: frozenset[str] = frozenset(),
    interactive: bool = False,
    dry_run: bool = False,
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
            label = "Would refresh" if dry_run else "Found"
            print_warning(f"{label} {stale_count} stale asset(s) in {node.path.name}")

    def on_refresh(asset_id: str, reason: str) -> None:
        if not quiet:
            label = "Would refresh" if dry_run else "Refreshing"
            print_info(f"  {label} {asset_id}: {reason}")

    def on_watermarks(watermarks: dict[str, DatasourceWatermark]) -> None:
        if print_watermarks:
            from trilogy.scripts.display import show_watermarks

            show_watermarks(watermarks)

    def on_refresh_query(ds_id: str, sql: str) -> None:
        stats.refresh_queries.append(RefreshQuery(datasource_id=ds_id, sql=sql))
        if dry_run and not quiet:
            print_info(f"\n-- {ds_id}\n{sql}")

    result = refresh_stale_assets(
        exec,
        on_stale_found=on_stale_found,
        on_refresh=on_refresh,
        on_watermarks=on_watermarks,
        on_approval=_prompt_approval if interactive else None,
        force_sources=set(force_sources) if force_sources else None,
        on_refresh_query=on_refresh_query,
        dry_run=dry_run,
    )
    stats.update_count = result.refreshed_count

    if not dry_run:
        for x in validation:
            exec.execute_statement(x)
            stats = count_statement_stats([x])

    if result.had_stale and not quiet:
        if dry_run:
            print_info(
                f"Dry run: {result.refreshed_count} asset(s) would be refreshed in {node.path.name}"
            )
        elif result.refreshed_count > 0:
            print_success(
                f"Refreshed {result.refreshed_count} asset(s) in {node.path.name}"
            )
        else:
            print_info(f"Refresh skipped by user for {node.path.name}")

    return stats


def make_refresh_execution_fn(
    print_watermarks: bool,
    force_sources: frozenset[str],
    interactive: bool,
    dry_run: bool = False,
):
    """Create a refresh execution function with the given parameters."""

    def wrapped_execute(
        exec: Executor, node: ScriptNode, quiet: bool = False
    ) -> ExecutionStats:
        return execute_script_for_refresh(
            exec, node, quiet, print_watermarks, force_sources, interactive, dry_run
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
    help="Set env vars as KEY=VALUE or pass an env file path",
)
@option(
    "--force",
    "-f",
    multiple=True,
    help="Force rebuild of specific datasources by name (skip staleness detection)",
)
@option(
    "--interactive",
    "-i",
    is_flag=True,
    default=False,
    help="Show refresh plan and prompt for approval before applying changes",
)
@option(
    "--dry-run",
    "-n",
    is_flag=True,
    default=False,
    help="Show SQL that would be executed without running it",
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
    interactive,
    dry_run,
    conn_args,
):
    """Refresh stale assets in Trilogy scripts.

    Parses each script, identifies datasources marked as 'root' (source of truth),
    compares watermarks to find stale derived assets, and refreshes them.

    Returns 0 if any assets were refreshed, 2 if all assets were up to date,
    and 1 on error.
    """
    if dialect and dialect.startswith("-"):
        raise click.UsageError(
            f"'{dialect}' looks like a flag, not a dialect. "
            "Global flags like --debug must come before the subcommand.\n"
            "  Try: trilogy --debug refresh ..."
        )
    refresh_params = RefreshParams(
        print_watermarks=print_watermarks,
        force_sources=frozenset(force),
        interactive=interactive,
        dry_run=dry_run,
    )

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
        refresh_params=refresh_params,
    )

    execution_fn = make_refresh_execution_fn(
        refresh_params.print_watermarks,
        refresh_params.force_sources,
        refresh_params.interactive,
        refresh_params.dry_run,
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
