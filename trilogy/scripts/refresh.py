"""Refresh command for Trilogy CLI - refreshes stale assets."""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO
from pathlib import Path

import click
from click import UNPROCESSED, argument, option, pass_context
from click import Path as ClickPath
from click.exceptions import Exit

from trilogy import Executor
from trilogy.core import graph as nx
from trilogy.dialect.enums import Dialects
from trilogy.execution.config import RuntimeConfig
from trilogy.execution.state import (
    BaseStateStore,
    DatasourceWatermark,
    RefreshKind,
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
    parse_force_sources,
    validate_force_sources,
)
from trilogy.scripts.dependency import (
    DependencyResolver,
    ManagedRefreshNode,
    ScriptNode,
)
from trilogy.scripts.parallel_execution import (
    ExecutionMode,
    ParallelExecutionSummary,
    run_parallel_execution,
)
from trilogy.utility import safe_open


def _collect_root_watermarks(
    owner_node: ScriptNode,
    owned_root_addrs: set[str],
    address_map: dict[str, str],
    needed_concepts: set[str],
    cli_params: CLIRuntimeParams,
    edialect: Dialects,
    config: RuntimeConfig,
) -> dict[str, DatasourceWatermark]:
    """Probe owned root datasources and return their watermarks.

    needed_concepts must be pre-computed from all non-root datasources across all scripts.
    """
    if not needed_concepts:
        return {}
    from trilogy.execution.state import get_concept_max_watermarks
    from trilogy.scripts.common import create_executor_for_script

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
        with safe_open(owner_node.path) as handle:
            executor.parse_text(handle.read(), root=owner_node.path)
        watermarks: dict[str, DatasourceWatermark] = {}
        for ds in executor.environment.datasources.values():
            if not ds.is_root:
                continue
            if address_map.get(ds.identifier) not in owned_root_addrs:
                continue
            target_refs = [
                ref for ref in ds.output_concepts if ref.address in needed_concepts
            ]
            if target_refs:
                wm = get_concept_max_watermarks(ds, target_refs, executor)
                if wm.keys:
                    watermarks[ds.identifier] = wm
        return watermarks
    finally:
        executor.close()


def _probe_owner_node(
    owner_node: ScriptNode,
    address_map: dict[str, str],
    addr_to_owner: dict[str, ScriptNode],
    force_sources: set[str] | None,
    cli_params: CLIRuntimeParams,
    edialect: Dialects,
    config: RuntimeConfig,
    initial_watermarks: dict[str, DatasourceWatermark] | None = None,
) -> tuple[ScriptNode, RefreshPlan]:
    from trilogy.scripts.common import create_executor_for_script

    skip_ids: set[str] = {
        ds_id
        for ds_id, addr in address_map.items()
        if addr_to_owner.get(addr) != owner_node
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
        with safe_open(owner_node.path) as handle:
            executor.parse_text(handle.read(), root=owner_node.path)
        plan = create_refresh_plan(
            executor,
            force_sources=force_sources,
            skip_datasources=skip_ids,
            initial_watermarks=initial_watermarks,
        )
        return owner_node, plan
    finally:
        executor.close()


def _merge_watermarks(
    watermarks: list[dict[str, DatasourceWatermark]],
) -> dict[str, DatasourceWatermark]:
    merged: dict[str, DatasourceWatermark] = {}
    for watermark_set in watermarks:
        for datasource_id, watermark in watermark_set.items():
            merged.setdefault(datasource_id, watermark)
    return merged


def _validate_probe_coverage(
    probe_addrs: set[str],
    address_map: dict[str, str],
    plans_by_node: list[tuple[ScriptNode, RefreshPlan]],
    expected_root_names_by_addr: dict[str, set[str]],
    all_root_watermarks: dict[str, DatasourceWatermark],
    refreshable_root_addrs: set[str] | None = None,
) -> None:
    from trilogy.scripts.display import print_error

    merged_plan_watermarks = _merge_watermarks(
        [plan.watermarks for _, plan in plans_by_node]
    )
    # Refreshable roots are managed via probe + script, not watermark — exclude
    # them from the watermark-coverage requirement.
    refreshable_root_addrs = refreshable_root_addrs or set()
    watermark_required = probe_addrs - refreshable_root_addrs
    covered_probe_addrs = {
        address
        for ds_id, address in address_map.items()
        if address in watermark_required and ds_id in merged_plan_watermarks
    }
    missing_probe_addrs = sorted(watermark_required - covered_probe_addrs)
    if missing_probe_addrs:
        print_error(
            "Refresh probe validation failed: some managed assets were never "
            f"watermarked: {', '.join(missing_probe_addrs)}"
        )
        raise Exit(1)

    provided_root_names_by_addr: dict[str, set[str]] = defaultdict(set)
    for ds_id, watermark in all_root_watermarks.items():
        addr = address_map.get(ds_id)
        if addr:
            provided_root_names_by_addr[addr].update(watermark.keys)

    missing_root_details = {
        addr: sorted(expected_names - provided_root_names_by_addr.get(addr, set()))
        for addr, expected_names in expected_root_names_by_addr.items()
        if expected_names - provided_root_names_by_addr.get(addr, set())
    }
    if missing_root_details:
        detail_str = "; ".join(
            f"{addr}: {', '.join(names)}"
            for addr, names in sorted(missing_root_details.items())
        )
        print_error(
            "Refresh probe validation failed: some root watermark concepts were "
            f"planned but never collected: {detail_str}"
        )
        raise Exit(1)


def _managed_dependency_edges(
    physical_nodes: dict[str, ManagedRefreshNode],
    script_graph: nx.DiGraph,
    addr_line: dict[str, int],
) -> list[tuple[str, str]]:
    """Edges between managed addresses, derived from owner-script dependencies.

    Cross-script: addr1 -> addr2 when addr1's owner script must run before addr2's
    (a path exists in the script graph).

    Same-script: order by definition line so a later asset may depend on an earlier
    one — addr1 -> addr2 when addr1 is defined above addr2. This is a total order
    (tie-broken by address), so it stays acyclic. Without it, two assets in one
    script would get bidirectional edges (nx.has_path(G, n, n) is trivially True
    both ways), forming a cycle that leaves the executor with no in-degree-0 node
    to schedule — deadlocking the refresh.
    """
    edges: list[tuple[str, str]] = []
    for addr1, node1 in physical_nodes.items():
        owner1_key = str(node1.owner_script.path)
        for addr2, node2 in physical_nodes.items():
            if addr1 == addr2:
                continue
            owner2_key = str(node2.owner_script.path)
            if owner1_key == owner2_key:
                # line is the primary key; address breaks ties (and covers
                # missing line info) so exactly one direction is ever added.
                if (addr_line.get(addr1, 0), addr1) < (addr_line.get(addr2, 0), addr2):
                    edges.append((addr1, addr2))
                continue
            if (
                owner1_key in script_graph
                and owner2_key in script_graph
                and nx.has_path(script_graph, owner1_key, owner2_key)
            ):
                edges.append((addr1, addr2))
    return edges


def _preview_directory_refresh(
    cli_params: CLIRuntimeParams, input_path: Path, interactive: bool = False
) -> tuple[bool, nx.DiGraph | None]:
    from trilogy.execution.config import apply_env_vars, load_env_file
    from trilogy.scripts.common import (
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

    input_path = input_path.resolve()
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
    available_datasources: set[str] = set()
    # (physical address, owner script path) -> earliest definition line in that script
    addr_line_by_script: dict[tuple[str, str], int] = {}
    ds_to_scripts: dict[str, list[ScriptNode]] = defaultdict(list)
    ds_is_root: dict[str, bool] = {}
    ds_is_refreshable_root: dict[str, bool] = {}
    # root physical address -> set of concept addresses (as seen in that executor's namespace)
    # that are needed by at least one non-root datasource in any script
    root_addr_to_needed_concepts: dict[str, set[str]] = defaultdict(set)
    root_probe_candidates: dict[str, dict[ScriptNode, set[str]]] = defaultdict(dict)
    root_probe_name_candidates: dict[str, dict[ScriptNode, set[str]]] = defaultdict(
        dict
    )

    script_files = [fp.resolve() for fp in files if not isinstance(fp, StringIO)]
    print_info(f"Scanning {len(script_files)} file(s)...")

    # Phase 1: parse all scripts to collect datasource metadata — no DB queries.
    # Within each executor context, directly match non-root freshness/incremental refs
    # against root output concepts — no cross-script namespace reconciliation needed.
    # We use lightweight Environment parsing (no executor/engine/startup scripts)
    # and skip files that contribute no datasources.
    from trilogy.core.models.environment import Environment as Env
    from trilogy.parsing.parse_engine_v2 import parse_text as lightweight_parse

    for file_path in script_files:
        node = ScriptNode(path=file_path)
        with safe_open(file_path) as handle:
            raw_text = handle.read()
        env = Env(working_path=str(file_path.parent))
        try:
            env, _ = lightweight_parse(raw_text, environment=env, root=node.path)
        except Exception as e:
            print_error(f"Error parsing {node.path}: {e}")
            raise Exit(1) from e
        if not env.datasources:
            print_info(f"Skipping {file_path.name} (no datasources)")
            continue
        available_datasources.update(env.datasources)
        needed_in_script: set[str] = set()
        for ds_id, ds in env.datasources.items():
            address_map.setdefault(ds_id, ds.safe_address)
            line_no = ds.metadata.line_no
            if line_no is not None:
                line_key = (ds.safe_address, str(node.path))
                prev = addr_line_by_script.get(line_key)
                if prev is None or line_no < prev:
                    addr_line_by_script[line_key] = line_no
            ds_to_scripts[ds_id].append(node)
            ds_is_root.setdefault(ds_id, ds.is_root)
            ds_is_refreshable_root.setdefault(
                ds_id,
                bool(ds.is_root and ds.refresh_script and ds.freshness_probe),
            )
            if not ds.is_root:
                for ref in ds.freshness_by:
                    needed_in_script.add(env.concepts[ref.address].address)
                for ref in ds.incremental_by:
                    needed_in_script.add(env.concepts[ref.address].address)
        if needed_in_script:
            for ds in env.datasources.values():
                if ds.is_root:
                    matching = {
                        ref.address
                        for ref in ds.output_concepts
                        if ref.address in needed_in_script
                    }
                    if matching:
                        root_addr_to_needed_concepts[ds.safe_address].update(matching)
                        root_probe_candidates.setdefault(ds.safe_address, {})
                        root_probe_name_candidates.setdefault(ds.safe_address, {})
                        root_probe_candidates[ds.safe_address].setdefault(
                            node, set()
                        ).update(matching)
                        root_probe_name_candidates[ds.safe_address].setdefault(
                            node, set()
                        ).update(env.concepts[ref].name for ref in matching)

    validate_force_sources(force_sources, available_datasources)

    # Build topo order so we can assign each physical address to its furthest-upstream owner.
    # script_graph is string-keyed (path strings); map back to ScriptNode for callers.
    resolver = DependencyResolver()
    script_graph = resolver.build_folder_graph(input_path)
    execution_order = list(nx.topological_sort(script_graph))
    script_to_order = {
        ScriptNode(path=Path(key)): i for i, key in enumerate(execution_order)
    }

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
    # Refreshable roots (is_root + refresh_script + freshness_probe) are also
    # included: they're black-box managed by trilogy via their refresh script.
    # NOTE: cross-script cascade is partial — if a refreshable root R in
    # script S1 has a downstream non-root D in script S2 that probed fresh
    # against R's pre-refresh watermark in Phase 2b, D will NOT be picked up
    # after R refreshes. A complete fix requires re-running Phase 2a/2b after
    # script-kind nodes complete; for now within-script and pre-stale dependents
    # are handled correctly via phys_graph ordering.
    probe_addrs: set[str] = {
        addr
        for addr, ds_ids in addr_to_ds_ids.items()
        if any(
            not ds_is_root.get(ds_id, False) or ds_is_refreshable_root.get(ds_id, False)
            for ds_id in ds_ids
        )
    }

    owner_to_addrs: dict[ScriptNode, set[str]] = defaultdict(set)
    for addr, owner in addr_to_owner.items():
        if addr in probe_addrs:
            owner_to_addrs[owner].add(addr)

    total_physical = len(probe_addrs)
    from trilogy.scripts.display import show_managed_asset_list

    show_managed_asset_list(sorted(probe_addrs))

    _, parallelism = merge_runtime_config(cli_params, config)
    ordered_nodes = sorted(owner_to_addrs, key=lambda n: script_to_order.get(n, 999999))

    # Phase 2a: collect root watermarks once per physical root address (parallel).
    # Only probe root addresses that were directly matched to a needed concept within
    # an executor context — no cross-script namespace reconciliation needed.
    root_probe_plan: dict[ScriptNode, dict[str, set[str]]] = defaultdict(dict)
    expected_root_names_by_addr: dict[str, set[str]] = {}
    for addr, matches_by_node in root_probe_candidates.items():
        probe_node = min(matches_by_node, key=lambda n: script_to_order.get(n, 999999))
        root_probe_plan[probe_node][addr] = matches_by_node[probe_node]
        expected_root_names_by_addr[addr] = root_probe_name_candidates[addr][probe_node]

    from trilogy.scripts.display import root_probe_progress, show_root_concepts

    show_root_concepts(root_addr_to_needed_concepts)
    print_info(f"Probing {total_physical} managed asset(s)...")

    all_root_watermarks: dict[str, DatasourceWatermark] = {}
    if root_probe_plan:
        with root_probe_progress(len(root_addr_to_needed_concepts)) as _root_progress:
            with ThreadPoolExecutor(max_workers=parallelism) as pool:
                root_futures = {
                    pool.submit(
                        _collect_root_watermarks,
                        node,
                        set(root_probe_plan[node]),
                        address_map,
                        set().union(*root_probe_plan[node].values()),
                        cli_params,
                        edialect,
                        config,
                    ): node
                    for node in root_probe_plan
                }
                _root_progress.register_futures(root_futures)
                for root_future in as_completed(root_futures):
                    all_root_watermarks.update(root_future.result())
                    node = root_futures[root_future]
                    for _ in root_probe_plan[node]:
                        _root_progress.advance()

    # Phase 2b: probe each managed asset exactly once via its owner script (parallel)
    # Root watermarks are pre-injected so each root is only queried once across all scripts.
    initial_watermarks = all_root_watermarks or None
    plans_by_node: list[tuple[ScriptNode, RefreshPlan]] = []
    with probe_progress(total_physical) as _progress:
        with ThreadPoolExecutor(max_workers=parallelism) as pool:
            futures = {
                pool.submit(
                    _probe_owner_node,
                    node,
                    address_map,
                    addr_to_owner,
                    force_sources,
                    cli_params,
                    edialect,
                    config,
                    initial_watermarks,
                ): node
                for node in ordered_nodes
            }
            _progress.register_futures(futures)
            for future in as_completed(futures):
                owner_node, plan = future.result()
                plans_by_node.append((owner_node, plan))
                for _ in owner_to_addrs[owner_node] & probe_addrs:
                    _progress.advance()

    refreshable_root_addrs = {
        address_map[ds_id]
        for ds_id, is_refreshable in ds_is_refreshable_root.items()
        if is_refreshable and ds_id in address_map
    }
    _validate_probe_coverage(
        probe_addrs,
        address_map,
        plans_by_node,
        expected_root_names_by_addr,
        all_root_watermarks,
        refreshable_root_addrs=refreshable_root_addrs,
    )

    refresh_assets = [
        asset for _, plan in plans_by_node for asset in plan.refresh_assets
    ]
    has_refreshable_root_stale = any(
        a.kind == RefreshKind.SCRIPT for a in refresh_assets
    )

    if not refresh_assets:
        print_info("All assets are up to date.")
        return True, None

    # Group pre-classified stale assets by managed address.
    physical_groups: dict[str, list[tuple[StaleAsset, ScriptNode]]] = defaultdict(list)
    for node, plan in plans_by_node:
        for asset in plan.refresh_assets:
            addr = address_map.get(asset.datasource_id, asset.datasource_id)
            physical_groups[addr].append((asset, node))

    # Build a managed node for EVERY probe_addr (not just stale-at-preview).
    # Addresses without pre-classified stale assets get an empty `assets` list;
    # their staleness is re-evaluated at execute time. This is what closes the
    # cross-script cascade gap — when an upstream refreshable-root script bumps
    # its data, downstream addresses that probed fresh against the pre-refresh
    # watermark will be re-checked against the post-refresh state.
    physical_nodes: dict[str, ManagedRefreshNode] = {}
    addr_ds_ids: dict[str, list[str]] = defaultdict(list)
    for ds_id, addr in address_map.items():
        if addr in probe_addrs:
            addr_ds_ids[addr].append(ds_id)

    for addr in probe_addrs:
        owner_script = addr_to_owner.get(addr)
        if owner_script is None:
            continue

        seen_assets: dict[str, StaleAsset] = {}
        for asset, _ in physical_groups.get(addr, []):
            key = f"{asset.datasource_id}:{asset.reason}"
            if key not in seen_assets:
                seen_assets[key] = asset

        physical_nodes[addr] = ManagedRefreshNode(
            address=addr,
            owner_script=owner_script,
            assets=list(seen_assets.values()),
            datasource_ids=tuple(addr_ds_ids[addr]),
        )

    stale_known = sum(1 for n in physical_nodes.values() if n.assets)

    # phys_graph is string-keyed by physical address; the rich ManagedRefreshNode
    # objects are carried in graph.graph['node_map'] so downstream execution
    # (run_parallel_execution) can recover them.
    phys_graph = nx.DiGraph()
    phys_graph.graph["node_map"] = physical_nodes
    for pnode in physical_nodes.values():
        phys_graph.add_node(pnode.address)

    addr_line = {
        addr: line
        for addr, n in physical_nodes.items()
        if (line := addr_line_by_script.get((addr, str(n.owner_script.path))))
        is not None
    }
    phys_graph.add_edges_from(
        _managed_dependency_edges(physical_nodes, script_graph, addr_line)
    )

    # "Unknown" nodes: descendants of any refreshable-root-stale node, since the
    # script's effect on downstream watermarks can't be predicted at preview.
    unknown_addrs: set[str] = set()
    if has_refreshable_root_stale:
        script_stale_nodes = [
            n
            for n in physical_nodes.values()
            if any(a.kind == RefreshKind.SCRIPT for a in n.assets)
        ]
        for sn in script_stale_nodes:
            for desc_addr in nx.descendants(phys_graph, sn.address):
                if not physical_nodes[desc_addr].assets:  # not already stale
                    unknown_addrs.add(desc_addr)

    if unknown_addrs:
        print_info(
            f"Found {stale_known} stale, {len(unknown_addrs)} unknown "
            f"(downstream of refreshable-root scripts; will be evaluated on demand) "
            f"of {total_physical} managed"
        )
    else:
        print_info(f"Found {stale_known} stale asset(s) of {total_physical} managed")

    # Trim the graph for execution: drop addresses that are neither known-stale
    # nor in the unknown set. They have no chance of needing a refresh, so we
    # don't pay the per-node staleness check on them.
    relevant: set[str] = {
        addr for addr, n in physical_nodes.items() if n.assets
    } | unknown_addrs
    for addr in list(phys_graph.nodes):
        if addr not in relevant:
            phys_graph.remove_node(addr)

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
    cascade: bool = True,
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
        cascade=cascade,
    )


def execute_managed_node_for_refresh(
    executor: Executor,
    node: ManagedRefreshNode,
    quiet: bool,
    print_watermarks: bool,
    interactive: bool,
    dry_run: bool,
) -> ExecutionStats:
    """Refresh one managed physical address.

    Re-evaluates staleness against the live DB at execute time (not from the
    preview snapshot). When upstream refreshable-root scripts have already run
    in this same orchestrator pass, their data mutations are visible here, and
    cascade dependents that probed fresh at preview will now probe stale.
    """
    stats = ExecutionStats()
    store = BaseStateStore()

    # Resolve which datasources at this address to evaluate. Prefer the explicit
    # list set at preview time; if absent (legacy nodes), fall back to scanning
    # the executor environment.
    target_ds_ids: list[str]
    if node.datasource_ids:
        target_ds_ids = [
            ds_id
            for ds_id in node.datasource_ids
            if ds_id in executor.environment.datasources
        ]
    else:
        target_ds_ids = [
            ds.identifier
            for ds in executor.environment.datasources.values()
            if ds.safe_address == node.address
        ]

    # Honor pre-classified forced rebuilds — their `reason` is "forced rebuild"
    # and is_stale would skip them otherwise.
    forced_ids = {a.datasource_id for a in node.assets if a.reason == "forced rebuild"}

    assets_to_refresh: list[StaleAsset] = []
    for ds_id in target_ds_ids:
        forced = ds_id in forced_ids
        asset = store.is_stale(executor.environment, executor, ds_id, force=forced)
        if asset is not None:
            assets_to_refresh.append(asset)

    if not assets_to_refresh:
        return stats

    plan = RefreshPlan(
        stale_assets=assets_to_refresh,
        forced_assets=[],
        watermarks=dict(store.watermarks),
        concept_max_watermarks=dict(store.concept_max_watermarks),
        root_assets=len(assets_to_refresh),
        all_assets=len(target_ds_ids),
    )

    # cascade=False: the orchestrator handles cross-managed-node cascade via
    # phys_graph topo order. Per-node cascade would double-refresh dependents
    # that are already separate managed nodes.
    result = _run_refresh_plan(executor, plan, stats, quiet, dry_run, cascade=False)
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


def run_refresh_command(cli_params: CLIRuntimeParams) -> ParallelExecutionSummary:
    input_path = Path(cli_params.input)
    from trilogy.scripts.common import merge_runtime_config, resolve_input_information
    from trilogy.scripts.display import show_execution_info

    _, _, input_type, input_name, runtime_config = resolve_input_information(
        cli_params.input, cli_params.config_path
    )
    edialect, _ = merge_runtime_config(cli_params, runtime_config)
    config_path_str = (
        str(runtime_config.source_path) if runtime_config.source_path else None
    )
    refresh_params = cli_params.refresh_params or RefreshParams()

    if input_path.is_dir():
        show_execution_info(
            input_type,
            input_name,
            edialect.value,
            cli_params.debug,
            config_path_str,
            cli_params.debug_file,
        )
        approved, phys_graph = _preview_directory_refresh(
            cli_params, input_path, interactive=refresh_params.interactive
        )
        if not approved:
            raise Exit(2)
        if not phys_graph:
            raise Exit(2)

        execution_fn = make_managed_refresh_fn(
            refresh_params.print_watermarks,
            False,
            refresh_params.dry_run,
        )

        def physical_executor_factory(node: ManagedRefreshNode) -> Executor:
            from trilogy.scripts.common import create_executor_for_script

            executor = create_executor_for_script(
                node.owner_script,
                cli_params.param,
                cli_params.conn_args,
                cli_params.dialect or edialect,
                cli_params.debug,
                runtime_config,
                cli_params.debug_file,
            )
            with safe_open(node.owner_script.path) as handle:
                executor.parse_text(handle.read(), root=node.owner_script.path)
            return executor

        return run_parallel_execution(
            cli_params=cli_params,
            execution_fn=execution_fn,  # type: ignore[arg-type]
            execution_mode=ExecutionMode.REFRESH,
            graph=phys_graph,
            executor_factory_override=physical_executor_factory,
        )

    return run_parallel_execution(
        cli_params=cli_params,
        execution_mode=ExecutionMode.REFRESH,
    )


@argument("input", type=ClickPath(), default=".")
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
        force_sources=parse_force_sources(force),
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
        summary = run_refresh_command(cli_params)
        if summary.successful == 0 and summary.skipped > 0:
            raise Exit(2)
        return
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
        if input_path.is_dir():
            show_execution_info(
                input_type,
                input_name,
                edialect.value,
                cli_params.debug,
                config_path_str,
                cli_params.debug_file,
            )
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
                    with safe_open(node.owner_script.path) as handle:
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
