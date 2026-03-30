"""Refresh command for Trilogy CLI - refreshes stale assets."""

from collections import defaultdict
from pathlib import Path as PathlibPath

import click
import networkx as nx
from click import UNPROCESSED, Path, argument, option, pass_context
from click.exceptions import Exit

from trilogy import Executor
from trilogy.core.statements.execute import ProcessedValidateStatement
from trilogy.dialect.enums import Dialects
from trilogy.execution.state import (
    DatasourceWatermark,
    RefreshPlan,
    RefreshResult,
    StaleAsset,
    create_refresh_plan,
    execute_refresh_plan,
)
from trilogy.scripts.click_utils import validate_dialect
from trilogy.scripts.common import (
    CLIRuntimeParams,
    ExecutionStats,
    RefreshParams,
    RefreshQuery,
    count_statement_stats,
    handle_execution_exception,
)
from trilogy.scripts.dependency import (
    DependencyResolver,
    ScriptNode,
    normalize_path_variants,
    resolve_script_with_errors,
    resolve_with_errors,
)
from trilogy.scripts.parallel_execution import ExecutionMode, run_parallel_execution


def _display_path(path: PathlibPath, root: PathlibPath) -> PathlibPath:
    resolved_path = path.resolve()
    resolved_root = root.resolve()
    try:
        return resolved_path.relative_to(resolved_root)
    except ValueError:
        return resolved_path


def _merge_watermarks(
    watermarks: list[dict[str, DatasourceWatermark]],
) -> dict[str, DatasourceWatermark]:
    merged: dict[str, DatasourceWatermark] = {}
    for watermark_set in watermarks:
        for datasource_id, watermark in watermark_set.items():
            merged.setdefault(datasource_id, watermark)
    return merged


def _resolve_directory_refresh_metadata(
    folder: PathlibPath,
) -> tuple[list[PathlibPath], dict[str, PathlibPath], dict[str, list[PathlibPath]]]:
    graph = DependencyResolver().build_folder_graph(folder)
    execution_order = list(
        nx.lexicographical_topological_sort(graph, key=lambda node: str(node.path))
    )
    order = [node.path.resolve() for node in execution_order]

    result = resolve_with_errors(folder.resolve())
    files_info = result.get("files_info", {})

    declaration_map: dict[str, PathlibPath] = {}
    updater_map: dict[str, list[PathlibPath]] = defaultdict(list)
    for raw_path, info in files_info.items():
        file_path = normalize_path_variants(raw_path).resolve()
        for datasource_name in info.get("datasources", []):
            declaration_map.setdefault(datasource_name, file_path)
        for datasource_name in info.get("persists", []):
            updater_map[datasource_name].append(file_path)

    return order, declaration_map, dict(updater_map)


def _resolve_script_refresh_metadata(
    path: PathlibPath,
) -> tuple[list[PathlibPath], dict[str, PathlibPath], dict[str, list[PathlibPath]]]:
    result = resolve_script_with_errors(path.resolve())
    order = [
        normalize_path_variants(raw_path).resolve()
        for raw_path in result.get("order", [])
    ]
    declaration_map = {
        datasource_id: normalize_path_variants(raw_path).resolve()
        for datasource_id, raw_path in result.get("datasource_declarations", {}).items()
    }
    updater_map = {
        datasource_id: [
            normalize_path_variants(raw_path).resolve() for raw_path in raw_paths
        ]
        for datasource_id, raw_paths in result.get("datasource_updaters", {}).items()
    }
    return order, declaration_map, updater_map


def _build_grouped_refresh_assets(
    logical_assets: list[tuple[PathlibPath, list[StaleAsset]]],
    display_root: PathlibPath,
    execution_order: list[PathlibPath],
    declaration_map: dict[str, PathlibPath],
    updater_map: dict[str, list[PathlibPath]],
):
    from trilogy.scripts.display import (
        LogicalRefreshAssetDisplay,
        PhysicalRefreshAssetDisplayGroup,
    )

    order_index = {path.resolve(): idx for idx, path in enumerate(execution_order)}
    grouped: dict[PathlibPath, list[LogicalRefreshAssetDisplay]] = defaultdict(list)

    def select_physical_path(
        datasource_id: str, logical_path: PathlibPath
    ) -> PathlibPath:
        updaters = updater_map.get(datasource_id, [])
        if updaters:
            return min(
                (path.resolve() for path in updaters),
                key=lambda candidate: (
                    order_index.get(candidate, len(order_index)),
                    str(candidate),
                ),
            )
        if datasource_id in declaration_map:
            return declaration_map[datasource_id].resolve()
        return logical_path.resolve()

    for logical_path, assets in logical_assets:
        for asset in assets:
            physical_path = select_physical_path(asset.datasource_id, logical_path)
            grouped[physical_path].append(
                LogicalRefreshAssetDisplay(
                    datasource_id=asset.datasource_id,
                    reason=asset.reason,
                    logical_path=_display_path(logical_path, display_root),
                )
            )

    ordered_paths = [
        path for path in execution_order if path.resolve() in grouped
    ] + sorted(
        (path for path in grouped if path not in execution_order),
        key=str,
    )

    return [
        PhysicalRefreshAssetDisplayGroup(
            physical_path=_display_path(path, display_root),
            logical_assets=grouped[path.resolve()],
        )
        for path in ordered_paths
    ]


def _group_assets_for_script(path: PathlibPath, assets: list[StaleAsset]):
    if not assets:
        return []
    try:
        execution_order, declaration_map, updater_map = (
            _resolve_script_refresh_metadata(path)
        )
    except Exception:
        return []

    return _build_grouped_refresh_assets(
        logical_assets=[(path, assets)],
        display_root=path.parent,
        execution_order=execution_order,
        declaration_map=declaration_map,
        updater_map=updater_map,
    )


def _preview_directory_refresh(cli_params: CLIRuntimeParams) -> bool:
    from trilogy.execution.config import apply_env_vars, load_env_file
    from trilogy.scripts.common import (
        create_executor_for_script,
        merge_runtime_config,
        resolve_input_information,
    )
    from trilogy.scripts.display import print_error, show_refresh_plan
    from trilogy.scripts.environment import parse_env_vars

    input_path = PathlibPath(cli_params.input)
    if not input_path.is_dir():
        return True

    files_iter, _, _, _, config = resolve_input_information(
        cli_params.input, cli_params.config_path
    )
    files = [file for file in files_iter if isinstance(file, PathlibPath)]

    for env_file in config.env_files:
        env_vars = load_env_file(env_file)
        if env_vars:
            apply_env_vars(env_vars)

    if cli_params.env:
        try:
            cli_env_vars = parse_env_vars(cli_params.env)
        except ValueError as e:
            print_error(str(e))
            raise Exit(1) from e
        apply_env_vars(cli_env_vars)

    edialect, _ = merge_runtime_config(cli_params, config)
    plans_by_node: list[tuple[ScriptNode, RefreshPlan]] = []
    for file_path in files:
        node = ScriptNode(path=file_path)
        executor = create_executor_for_script(
            node,
            cli_params.param,
            cli_params.conn_args,
            edialect,
            cli_params.debug,
            config,
            cli_params.debug_file,
        )
        try:
            with open(node.path, "r") as handle:
                try:
                    executor.parse_text(handle.read(), root=node.path)
                except Exception as e:
                    print_error(f"Error parsing {node.path}: {e}")
                    raise Exit(1) from e
            plans_by_node.append(
                (
                    node,
                    create_refresh_plan(
                        executor,
                        force_sources=(
                            set(cli_params.refresh_params.force_sources)
                            if cli_params.refresh_params
                            and cli_params.refresh_params.force_sources
                            else None
                        ),
                    ),
                )
            )
        finally:
            executor.close()

    refresh_assets = [
        asset for _, plan in plans_by_node for asset in plan.refresh_assets
    ]
    if not refresh_assets:
        return True

    execution_order, declaration_map, updater_map = _resolve_directory_refresh_metadata(
        input_path
    )
    grouped_assets = _build_grouped_refresh_assets(
        logical_assets=[
            (node.path, plan.refresh_assets)
            for node, plan in plans_by_node
            if plan.refresh_assets
        ],
        display_root=input_path,
        execution_order=execution_order,
        declaration_map=declaration_map,
        updater_map=updater_map,
    )
    show_refresh_plan(
        refresh_assets,
        _merge_watermarks([plan.watermarks for _, plan in plans_by_node]),
        grouped_assets=grouped_assets,
    )
    return click.confirm("\nProceed with refresh?", default=True)


def _prompt_approval(
    stale_assets: list[StaleAsset],
    watermarks: dict[str, DatasourceWatermark],
    grouped_assets=None,
) -> bool:
    """Show refresh plan and prompt user for approval."""

    from trilogy.scripts.display import show_refresh_plan

    show_refresh_plan(stale_assets, watermarks, grouped_assets=grouped_assets)
    return click.confirm("\nProceed with refresh?", default=True)


def _run_refresh_plan(
    exec: Executor,
    plan: RefreshPlan,
    stats: ExecutionStats,
    quiet: bool,
    dry_run: bool,
) -> RefreshResult:
    from trilogy.scripts.display import print_info

    def on_refresh(asset_id: str, reason: str) -> None:
        if quiet:
            return
        label = "Would refresh" if dry_run else "Refreshing"
        print_info(f"  {label} {asset_id}: {reason}")

    def on_refresh_query(ds_id: str, sql: str) -> None:
        stats.refresh_queries.append(RefreshQuery(datasource_id=ds_id, sql=sql))
        if dry_run and not quiet:
            print_info(f"\n-- {ds_id}\n{sql}")

    return execute_refresh_plan(
        exec,
        plan,
        on_refresh=on_refresh,
        on_refresh_query=on_refresh_query,
        dry_run=dry_run,
    )


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
        statements = exec.parse_text(f.read(), root=node.path)

    for x in statements:
        if isinstance(x, ProcessedValidateStatement):
            validation.append(x)

    stats = count_statement_stats([])
    plan = create_refresh_plan(
        exec,
        force_sources=set(force_sources) if force_sources else None,
    )

    if plan.stale_count == 0 and not quiet:
        print_info(
            f"No stale assets found in {node.path.name} ({plan.root_assets}/{plan.all_assets} root assets)"
        )
    elif not quiet:
        label = "Would refresh" if dry_run else "Found"
        print_warning(f"{label} {plan.stale_count} stale asset(s) in {node.path.name}")

    if print_watermarks:
        from trilogy.scripts.display import show_watermarks

        show_watermarks(plan.watermarks, plan.concept_max_watermarks)

    if interactive and plan.refresh_assets:
        grouped_assets = _group_assets_for_script(node.path, plan.refresh_assets)
        if not _prompt_approval(
            plan.refresh_assets,
            plan.watermarks,
            grouped_assets=grouped_assets,
        ):
            result = RefreshResult(
                stale_count=plan.stale_count,
                refreshed_count=0,
                root_assets=plan.root_assets,
                all_assets=plan.all_assets,
            )
        else:
            result = _run_refresh_plan(exec, plan, stats, quiet, dry_run)
    else:
        result = _run_refresh_plan(exec, plan, stats, quiet, dry_run)

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
    validate_dialect(dialect, "refresh")
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

    try:
        input_path = PathlibPath(input)
        execution_interactive = refresh_params.interactive
        if refresh_params.interactive and input_path.is_dir():
            approved = _preview_directory_refresh(cli_params)
            if not approved:
                raise Exit(2)
            execution_interactive = False
            cli_params = CLIRuntimeParams(
                input=cli_params.input,
                dialect=cli_params.dialect,
                parallelism=cli_params.parallelism,
                param=cli_params.param,
                conn_args=cli_params.conn_args,
                debug=cli_params.debug,
                debug_file=cli_params.debug_file,
                config_path=cli_params.config_path,
                execution_strategy=cli_params.execution_strategy,
                env=cli_params.env,
                refresh_params=RefreshParams(
                    print_watermarks=refresh_params.print_watermarks,
                    force_sources=refresh_params.force_sources,
                    interactive=False,
                    dry_run=refresh_params.dry_run,
                ),
            )

        execution_fn = make_refresh_execution_fn(
            refresh_params.print_watermarks,
            refresh_params.force_sources,
            execution_interactive,
            refresh_params.dry_run,
        )
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
