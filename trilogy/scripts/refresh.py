"""Refresh command for Trilogy CLI - refreshes stale assets."""

from pathlib import Path as PathlibPath

from click import UNPROCESSED, Path, argument, option, pass_context
from click.exceptions import Exit

from trilogy import Executor
from trilogy.core.statements.execute import ProcessedValidateStatement
from trilogy.dialect.enums import Dialects
from trilogy.execution.state.state_store import BaseStateStore
from trilogy.scripts.common import (
    CLIRuntimeParams,
    ExecutionStats,
    count_statement_stats,
    handle_execution_exception,
)
from trilogy.scripts.dependency import ScriptNode
from trilogy.scripts.parallel_execution import run_parallel_execution


def execute_script_for_refresh(
    exec: Executor, node: ScriptNode, quiet: bool = False
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

    state_store = BaseStateStore()
    stale_assets = state_store.get_stale_assets(exec.environment, exec)

    if not stale_assets:
        if not quiet:
            print_info(f"No stale assets found in {node.path.name}")
        return stats

    if not quiet:
        print_warning(f"Found {len(stale_assets)} stale asset(s) in {node.path.name}")

    for asset in stale_assets:
        if not quiet:
            print_info(f"  Refreshing {asset.datasource_id}: {asset.reason}")
        datasource = exec.environment.datasources[asset.datasource_id]
        exec.update_datasource(datasource)
        stats.persist_count += 1
    for x in validation:
        exec.execute_statement(x)
        stats = count_statement_stats([x])
    if not quiet:
        print_success(f"Refreshed {len(stale_assets)} asset(s) in {node.path.name}")

    return stats


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
def refresh(
    ctx, input, dialect: str | None, param, parallelism: int | None, config, conn_args
):
    """Refresh stale assets in Trilogy scripts.

    Parses each script, identifies datasources marked as 'root' (source of truth),
    compares watermarks to find stale derived assets, and refreshes them.
    """
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
            execution_fn=execute_script_for_refresh,
            execution_mode="refresh",
        )
    except Exit:
        raise
    except Exception as e:
        handle_execution_exception(e, debug=cli_params.debug)
