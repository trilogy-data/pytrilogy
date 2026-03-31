"""Refresh command for Trilogy CLI - refreshes stale assets."""

from collections import defaultdict
from io import StringIO
from pathlib import Path

import click
import networkx as nx
from click import UNPROCESSED, argument, option, pass_context
from click import Path as ClickPath
from click.exceptions import Exit

from trilogy import Executor
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
    handle_execution_exception,
)
from trilogy.scripts.dependency import (
    DependencyResolver,
    ManagedRefreshNode,
    ScriptNode,
)
from trilogy.scripts.parallel_execution import ExecutionMode, run_parallel_execution


def _merge_watermarks(
    watermarks: list[dict[str, DatasourceWatermark]],
) -> dict[str, DatasourceWatermark]:
    merged: dict[str, DatasourceWatermark] = {}
    for watermark_set in watermarks:
        for datasource_id, watermark in watermark_set.items():
            merged.setdefault(datasource_id, watermark)
    return merged


def _preview_directory_refresh(
    cli_params: CLIRuntimeParams, input_path: Path, interactive: bool = False
) -> tuple[bool, nx.DiGraph | None]:
    from trilogy.execution.config import apply_env_vars, load_env_file
    from trilogy.scripts.common import (
        create_executor_for_script,
        merge_runtime_config,
        resolve_input_information,
    )
    from trilogy.scripts.display import (
        print_error,
        print_info,
        probe_progress,
        show_asset_status_summary,
    )
    from trilogy.scripts.environment import parse_env_vars

    files_iter, _, _, _, config = resolve_input_information(
        str(input_path), cli_params.config_path
    )
    files = list(files_iter)

    # Load environment variables
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
    force_sources = (
        set(cli_params.refresh_params.force_sources)
        if cli_params.refresh_params and cli_params.refresh_params.force_sources
        else None
    )

    address_map: dict[str, str] = {}
    ds_to_scripts: dict[str, list[ScriptNode]] = defaultdict(list)
    ds_is_root: dict[str, bool] = {}

    script_files = [fp for fp in files if not isinstance(fp, StringIO)]
    print_info(f"Scanning {len(script_files)} file(s)...")

    # Phase 1: parse all scripts to collect datasource metadata — no DB queries
    for file_path in script_files:
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
            for ds_id, ds in executor.environment.datasources.items():
                address_map.setdefault(ds_id, ds.safe_address)
                ds_to_scripts[ds_id].append(node)
                ds_is_root.setdefault(ds_id, ds.is_root)
        finally:
            executor.close()

    # Build topo order so we can assign each physical address to its furthest-upstream owner
    resolver = DependencyResolver()
    script_graph = resolver.build_folder_graph(input_path)
    execution_order = list(nx.topological_sort(script_graph))
    script_to_order = {node: i for i, node in enumerate(execution_order)}

    # Group unique ds_ids by physical address
    addr_to_ds_ids: dict[str, list[str]] = defaultdict(list)
    seen_ds_ids: set[str] = set()
    for ds_id in address_map:
        if ds_id not in seen_ds_ids:
            addr_to_ds_ids[address_map[ds_id]].append(ds_id)
            seen_ds_ids.add(ds_id)

    # For each physical address, the owner is the script with the lowest topo index
    addr_to_owner: dict[str, ScriptNode] = {}
    for addr, ds_ids in addr_to_ds_ids.items():
        best_node: ScriptNode | None = None
        best_idx = 999999
        for ds_id in ds_ids:
            for node in ds_to_scripts[ds_id]:
                idx = script_to_order.get(node, 999999)
                if idx < best_idx:
                    best_idx = idx
                    best_node = node
        if best_node:
            addr_to_owner[addr] = best_node

    # Addresses that contain at least one non-root datasource — these are the
    # assets we actually refresh. Root-only addresses (Python scripts, remote
    # files) are probed for watermarks inside their importers' executor calls.
    probe_addrs: set[str] = {
        addr
        for addr, ds_ids in addr_to_ds_ids.items()
        if any(not ds_is_root.get(ds_id, False) for ds_id in ds_ids)
    }

    owner_to_addrs: dict[ScriptNode, set[str]] = defaultdict(set)
    for addr, owner in addr_to_owner.items():
        if addr in probe_addrs:
            owner_to_addrs[owner].add(addr)

    total_physical = len(probe_addrs)
    from trilogy.scripts.display import show_managed_asset_list

    show_managed_asset_list(sorted(probe_addrs))
    print_info(f"Probing {total_physical} managed asset(s)...")

    # Phase 2: probe each physical asset exactly once via its owner script
    plans_by_node: list[tuple[ScriptNode, RefreshPlan]] = []
    with probe_progress(total_physical) as _progress:
        for owner_node in sorted(
            owner_to_addrs, key=lambda n: script_to_order.get(n, 999999)
        ):
            owned_addrs = owner_to_addrs[owner_node]
            # Skip non-root ds_ids whose address is owned by a different script;
            # roots are always probed so concept_max_watermarks stays accurate.
            skip_ids: set[str] = {
                ds_id
                for ds_id, addr in address_map.items()
                if addr not in owned_addrs
                and not ds_is_root.get(ds_id, False)
                and owner_node in ds_to_scripts.get(ds_id, [])
            }
            executor = create_executor_for_script(
                owner_node,
                cli_params.param,
                cli_params.conn_args,
                edialect,
                cli_params.debug,
                config,
                cli_params.debug_file,
            )
            try:
                with open(owner_node.path, "r") as handle:
                    executor.parse_text(handle.read(), root=owner_node.path)
                plans_by_node.append(
                    (
                        owner_node,
                        create_refresh_plan(
                            executor,
                            force_sources=force_sources,
                            skip_datasources=skip_ids,
                        ),
                    )
                )
            finally:
                executor.close()
            for _ in owned_addrs & probe_addrs:
                _progress.advance()

    refresh_assets = [
        asset for _, plan in plans_by_node for asset in plan.refresh_assets
    ]
    if not refresh_assets:
        print_info("All assets are up to date.")
        return True, None

    # Group stale assets by managed address
    physical_groups: dict[str, list[tuple[StaleAsset, ScriptNode]]] = defaultdict(list)
    for node, plan in plans_by_node:
        for asset in plan.refresh_assets:
            addr = address_map.get(asset.datasource_id, asset.datasource_id)
            physical_groups[addr].append((asset, node))

    # Build Managed Nodes and assign owners
    physical_nodes: dict[str, ManagedRefreshNode] = {}
    for addr, entries in physical_groups.items():
        # Pick the furthest upstream script (lowest topological index)
        # In a tie, we could look for PERSIST presence, but topo order handles most cases.
        owner_entry = min(entries, key=lambda x: script_to_order.get(x[1], 999999))
        owner_script = owner_entry[1]

        # Deduplicate assets for this address (same ds_id + reason can be merged)
        seen_assets: dict[str, StaleAsset] = {}
        for asset, _ in entries:
            # If exact same asset already seen, skip
            key = f"{asset.datasource_id}:{asset.reason}"
            if key not in seen_assets:
                seen_assets[key] = asset

        physical_nodes[addr] = ManagedRefreshNode(
            address=addr, owner_script=owner_script, assets=list(seen_assets.values())
        )

    print_info(
        f"Found {len(physical_nodes)} stale asset(s) of {total_physical} managed"
    )

    phys_graph = nx.DiGraph()
    for pnode in physical_nodes.values():
        phys_graph.add_node(pnode)

    for addr1, node1 in physical_nodes.items():
        for addr2, node2 in physical_nodes.items():
            if addr1 == addr2:
                continue
            if (
                node1.owner_script in script_graph
                and node2.owner_script in script_graph
                and nx.has_path(script_graph, node1.owner_script, node2.owner_script)
            ):
                phys_graph.add_edge(node1, node2)

    merged_root_watermarks = _merge_watermarks(
        [plan.root_watermarks for _, plan in plans_by_node]
    )
    merged_concept_max: dict = {}
    for _, plan in plans_by_node:
        for k, v in plan.concept_max_watermarks.items():
            merged_concept_max.setdefault(k, v)

    from trilogy.scripts.display import show_root_probe_breakdown

    show_root_probe_breakdown(merged_root_watermarks, merged_concept_max)
    show_asset_status_summary(
        _merge_watermarks([plan.watermarks for _, plan in plans_by_node]),
        address_map,
        refresh_assets,
    )

    if interactive and not click.confirm("\nProceed with refresh?", default=True):
        return False, None

    return True, phys_graph


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


def execute_managed_node_for_refresh(
    executor: Executor,
    node: ManagedRefreshNode,
    quiet: bool,
    print_watermarks: bool,
    interactive: bool,
    dry_run: bool,
) -> ExecutionStats:
    stats = ExecutionStats()

    # Create a minimal refresh plan for this specific physical address
    # Use empty watermarks since we're already at the execution stage and don't need them
    plan = RefreshPlan(
        stale_assets=node.assets,
        forced_assets=[],
        watermarks={},
        concept_max_watermarks={},
        root_assets=len(node.assets),
        all_assets=len(node.assets),
    )

    result = _run_refresh_plan(executor, plan, stats, quiet, dry_run)
    stats.update_count = result.refreshed_count
    if not quiet and result.refreshed_count > 0:
        from trilogy.scripts.display import print_success

        label = "Would refresh" if dry_run else "Refreshed"
        print_success(f"{label} {result.refreshed_count} asset(s) for {node.address}")
    return stats


def make_managed_refresh_fn(
    print_watermarks: bool,
    interactive: bool,
    dry_run: bool = False,
):
    """Create a refresh execution function for physical nodes."""

    def wrapped_execute(
        exec: Executor, node: ManagedRefreshNode, quiet: bool = False
    ) -> ExecutionStats:
        return execute_managed_node_for_refresh(
            exec, node, quiet, print_watermarks, interactive, dry_run
        )

    return wrapped_execute


@argument("input", type=ClickPath(), default=".")
@argument("dialect", type=str, required=False)
@option("--param", multiple=True, help="Environment parameters as key=value pairs")
@option(
    "--parallelism",
    "-p",
    default=None,
    help="Maximum parallel workers for directory execution",
)
@option(
    "--config",
    type=ClickPath(exists=True),
    help="Path to trilogy.toml configuration file",
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
        config_path=Path(config) if config else None,
        execution_strategy="eager_bfs",
        env=env,
        refresh_params=refresh_params,
    )

    try:
        input_path = Path(input)
        from trilogy.scripts.common import (
            merge_runtime_config,
            resolve_input_information,
        )
        from trilogy.scripts.display import show_execution_info

        _, _, input_type, input_name, runtime_config = resolve_input_information(
            input, cli_params.config_path
        )
        edialect, _ = merge_runtime_config(cli_params, runtime_config)
        config_path_str = (
            str(runtime_config.source_path) if runtime_config.source_path else None
        )
        show_execution_info(
            input_type,
            input_name,
            edialect.value,
            cli_params.debug,
            config_path_str,
            cli_params.debug_file,
        )
        if input_path.is_dir():
            approved, phys_graph = _preview_directory_refresh(
                cli_params, input_path, interactive=refresh_params.interactive
            )
            if not approved:
                raise Exit(2)

            # If we have a physical graph, use it for deduplicated execution
            if phys_graph:
                execution_fn = make_managed_refresh_fn(
                    refresh_params.print_watermarks,
                    False,  # interactive always False after preview
                    refresh_params.dry_run,
                )

                def physical_executor_factory(node: ManagedRefreshNode) -> Executor:
                    from trilogy.scripts.common import create_executor_for_script

                    executor = create_executor_for_script(
                        node.owner_script,
                        cli_params.param,
                        cli_params.conn_args,
                        Dialects(dialect) if dialect else edialect,
                        cli_params.debug,
                        runtime_config,
                        cli_params.debug_file,
                    )
                    with open(node.owner_script.path, "r") as handle:
                        executor.parse_text(handle.read(), root=node.owner_script.path)
                    return executor

                summary = run_parallel_execution(
                    cli_params=cli_params,
                    execution_fn=execution_fn,  # type: ignore
                    execution_mode=ExecutionMode.REFRESH,
                    graph=phys_graph,
                    executor_factory_override=physical_executor_factory,
                )
            else:
                # No stale assets found
                raise Exit(2)
        else:
            # Single file refresh — run_parallel_execution routes to run_single_script_execution
            summary = run_parallel_execution(
                cli_params=cli_params,
                execution_mode=ExecutionMode.REFRESH,
            )

        if summary.successful == 0 and summary.skipped > 0:
            # if everything was up to date, exit with code 2
            raise Exit(2)
    except Exit:
        raise
    except Exception as e:
        handle_execution_exception(e, debug=cli_params.debug)
