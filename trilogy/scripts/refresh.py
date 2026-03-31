"""Refresh command for Trilogy CLI - refreshes stale assets."""

from collections import defaultdict
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

import click
import networkx as nx
from click import UNPROCESSED, argument, option, pass_context
from click import Path as ClickPath
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
)
from trilogy.scripts.parallel_execution import ExecutionMode, run_parallel_execution


@dataclass(frozen=True)
class PhysicalRefreshNode:
    """Represents a physical data address to be refreshed."""

    address: str
    owner_script: ScriptNode
    assets: list[StaleAsset]

    def __hash__(self):
        return hash(self.address)

    def __eq__(self, other):
        if not isinstance(other, PhysicalRefreshNode):
            return False
        return self.address == other.address

    def __repr__(self):
        return f"PhysicalRefreshNode({self.address})"


def _display_path(path: Path, root: Path) -> Path:
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


def _build_grouped_refresh_assets(
    logical_assets: list[tuple[Path, list[StaleAsset]]],
    display_root: Path,
    address_map: dict[str, str],
):
    """Group stale assets by their physical data address, deduplicating datasources."""
    from trilogy.scripts.display import (
        PhysicalDataGroup,
        StaleDataSourceEntry,
    )

    # address -> datasource_id -> (reason, set of file paths)
    grouped: dict[str, dict[str, tuple[str, list[Path]]]] = defaultdict(dict)

    for logical_path, assets in logical_assets:
        for asset in assets:
            addr = address_map.get(asset.datasource_id, asset.datasource_id)
            rel_path = _display_path(logical_path, display_root)
            ds_map = grouped[addr]
            if asset.datasource_id not in ds_map:
                ds_map[asset.datasource_id] = (asset.reason, [rel_path])
            else:
                files = ds_map[asset.datasource_id][1]
                if rel_path not in files:
                    files.append(rel_path)

    final_groups = []
    for addr, ds_map in sorted(grouped.items()):
        # Detect if all datasources share the same reason
        all_reasons = {reason for reason, _ in ds_map.values()}
        common_reason = None
        if len(all_reasons) == 1:
            common_reason = next(iter(all_reasons))

        final_groups.append(
            PhysicalDataGroup(
                data_address=addr,
                common_reason=common_reason,
                datasources=[
                    StaleDataSourceEntry(
                        datasource_id=ds_id,
                        reason=reason if common_reason is None else None,
                        referenced_in=files,
                    )
                    for ds_id, (reason, files) in ds_map.items()
                ],
            )
        )
    return final_groups


def _group_assets_for_script(
    path: Path,
    assets: list[StaleAsset],
    address_map: dict[str, str],
):
    if not assets:
        return []
    return _build_grouped_refresh_assets(
        logical_assets=[(path, assets)],
        display_root=path.parent,
        address_map=address_map,
    )


def _preview_directory_refresh(
    cli_params: CLIRuntimeParams, input_path: Path
) -> tuple[bool, nx.DiGraph | None]:
    from trilogy.execution.config import apply_env_vars, load_env_file
    from trilogy.scripts.common import (
        create_executor_for_script,
        merge_runtime_config,
        resolve_input_information,
    )
    from trilogy.scripts.display import print_error, show_refresh_plan
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
    plans_by_node: list[tuple[ScriptNode, RefreshPlan]] = []
    address_map: dict[str, str] = {}

    # Track which scripts define/persist which datasources
    ds_to_scripts: dict[str, list[ScriptNode]] = defaultdict(list)

    for file_path in files:
        if isinstance(file_path, StringIO):
            continue
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
        return True, None

    # Group stale assets by physical address
    # addr -> list of (asset, script_node)
    physical_groups: dict[str, list[tuple[StaleAsset, ScriptNode]]] = defaultdict(list)
    for node, plan in plans_by_node:
        for asset in plan.refresh_assets:
            addr = address_map.get(asset.datasource_id, asset.datasource_id)
            physical_groups[addr].append((asset, node))

    # Determine execution order of original scripts
    resolver = DependencyResolver()
    script_graph = resolver.build_folder_graph(input_path)
    execution_order = list(nx.topological_sort(script_graph))
    script_to_order = {node: i for i, node in enumerate(execution_order)}

    # Build Physical Nodes and assign owners
    physical_nodes: dict[str, PhysicalRefreshNode] = {}
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

        physical_nodes[addr] = PhysicalRefreshNode(
            address=addr, owner_script=owner_script, assets=list(seen_assets.values())
        )

    # Build Physical dependency graph
    # (Simplified: if any logical ds in Node B depends on logical ds in Node A)
    # For now, let's derive it from the script graph dependencies?
    # Or more directly from concept/datasource deps.
    # Actually, if script B depends on script A, and both have physical nodes,
    # then PhysNode B likely depends on PhysNode A.
    phys_graph = nx.DiGraph()
    for pnode in physical_nodes.values():
        phys_graph.add_node(pnode)

    # Connect physical nodes if their owner scripts have dependencies
    # This is a safe heuristic for now.
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

    grouped_assets = _build_grouped_refresh_assets(
        logical_assets=[
            (node.path, plan.refresh_assets)
            for node, plan in plans_by_node
            if plan.refresh_assets
        ],
        display_root=input_path,
        address_map=address_map,
    )

    show_refresh_plan(
        refresh_assets,
        _merge_watermarks([plan.watermarks for _, plan in plans_by_node]),
        grouped_assets=grouped_assets,
    )

    if click.confirm("\nProceed with refresh?", default=True):
        return True, phys_graph
    return False, None


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
        addr_map = {
            ds_id: ds.safe_address for ds_id, ds in exec.environment.datasources.items()
        }
        grouped_assets = _group_assets_for_script(
            node.path, plan.refresh_assets, addr_map
        )
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


def execute_physical_node_for_refresh(
    executor: Executor,
    node: PhysicalRefreshNode,
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


def make_physical_refresh_fn(
    print_watermarks: bool,
    interactive: bool,
    dry_run: bool = False,
):
    """Create a refresh execution function for physical nodes."""

    def wrapped_execute(
        exec: Executor, node: PhysicalRefreshNode, quiet: bool = False
    ) -> ExecutionStats:
        return execute_physical_node_for_refresh(
            exec, node, quiet, print_watermarks, interactive, dry_run
        )

    return wrapped_execute


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

        _, _, _, _, runtime_config = resolve_input_information(
            input, cli_params.config_path
        )
        edialect, _ = merge_runtime_config(cli_params, runtime_config)
        if input_path.is_dir():
            approved, phys_graph = _preview_directory_refresh(cli_params, input_path)
            if not approved:
                raise Exit(2)

            # If we have a physical graph, use it for deduplicated execution
            if phys_graph:
                execution_fn = make_physical_refresh_fn(
                    refresh_params.print_watermarks,
                    False,  # interactive always False after preview
                    refresh_params.dry_run,
                )

                def physical_executor_factory(node: PhysicalRefreshNode) -> Executor:
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
            # Single file refresh
            execution_fn = make_refresh_execution_fn(
                refresh_params.print_watermarks,
                refresh_params.force_sources,
                refresh_params.interactive,
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
