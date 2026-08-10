"""Why did the network pick THAT cover? Per-request solution breakdown.

    .venv/Scripts/python.exe local_scripts/s32_solution_detail.py <query.preql> [terminal-substring]

Prints, for every ROOT source request in the query (optionally only those whose
terminal list contains the substring): the terminals, every non-dominated
alternative with its per-source assignment / join keys / fan-out attribution,
and the legacy planner's choice scored the same way.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from trilogy import Dialects, Environment
from trilogy.core.models.build import BuildUnionDatasource
from trilogy.core.processing.nodes import SelectNode, StrategyNode
from trilogy.core.processing.v4_helper import source_planning
from trilogy.core.processing.v4_helper.network_search import (
    SourceNetwork,
    SourceSolution,
    _solution_for,
    build_source_network,
    search_sources,
)

TPCDS_ROOT = (
    Path(__file__).resolve().parents[1] / "tests" / "modeling" / "tpc_ds_duckdb"
)

FILTER = ""


def _node_datasources(node: StrategyNode | None) -> tuple[str, ...]:
    if node is None:
        return ()
    found: set[str] = set()
    stack: list[StrategyNode] = [node]
    seen: set[int] = set()
    while stack:
        current = stack.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, SelectNode) and current.datasource is not None:
            found.add("ds~" + current.datasource.identifier)
        stack.extend(current.parents)
    return tuple(sorted(found))


def _legacy_as_candidates(
    network: SourceNetwork, legacy: tuple[str, ...]
) -> frozenset[str] | None:
    remaining = set(legacy)
    chosen: set[str] = set()
    for node, candidate in sorted(network.candidates.items()):
        datasource = candidate.datasource
        if not isinstance(datasource, BuildUnionDatasource):
            continue
        children = {"ds~" + child.identifier for child in datasource.children}
        if children and children <= remaining:
            chosen.add(node)
            remaining -= children
    for node in sorted(remaining):
        if node not in network.candidates:
            return None
        chosen.add(node)
    return frozenset(chosen)


def describe(network: SourceNetwork, solution: SourceSolution, label: str) -> None:
    print(f"  {label}: {solution.cost.axes()}")
    joined_on = {
        node: frozenset(
            key
            for (left, right), keys in solution.join_keys.items()
            if node in (left, right)
            for key in keys
        )
        for node in solution.sources
    }
    for node in solution.sources:
        candidate = network.candidates[node]
        assigned = solution.assignments.get(node, frozenset())
        judged = assigned | joined_on[node]
        grain = ",".join(sorted(candidate.grain)) or "*"
        fan = network.fans_out(node, judged)
        print(f"    {node}  grain=({grain}) fanout={fan}")
        print(f"      provides : {sorted(assigned)}")
        print(
            f"      joined_on: {sorted(joined_on[node])[:8]}"
            f"{' ...' if len(joined_on[node]) > 8 else ''}"
            f" (n={len(joined_on[node])})"
        )


def shadow_plan_source(request: source_planning.SourceRequest):
    legacy_node = _ORIGINAL(request)
    terminals = source_planning._search_concepts_for_bridge(request)
    network = build_source_network(
        terminals, request.environment, request.graph, request.conditions
    )
    if FILTER and not any(FILTER in address for address in network.terminals):
        return legacy_node
    result = search_sources(network)
    print("\n" + "=" * 100)
    print(f"terminals: {', '.join(network.terminals)}")
    if result.solution is None:
        print(f"  NO SOLUTION unreachable={sorted(result.unreachable)}")
        return legacy_node
    for index, alternative in enumerate(result.alternatives[:6]):
        describe(network, alternative, f"alt{index}")
    legacy = _node_datasources(legacy_node)
    legacy_sources = _legacy_as_candidates(network, legacy)
    if legacy_sources is None:
        print(f"  legacy uses a disqualified source: {legacy}")
    else:
        describe(
            network,
            _solution_for(network, legacy_sources, list(network.terminals)),
            "LEGACY",
        )
    return legacy_node


_ORIGINAL = source_planning.plan_source


def main(argv: list[str]) -> int:
    global FILTER

    source_planning.plan_source = shadow_plan_source
    from trilogy.core.processing.v4_node_generators import root as v4_root

    v4_root.plan_source = shadow_plan_source  # type: ignore[attr-defined]

    path = Path(argv[0]) if Path(argv[0]).exists() else TPCDS_ROOT / argv[0]
    FILTER = argv[1] if len(argv) > 1 else ""
    env = Environment(working_path=path.parent)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    executor.parse_text(path.read_text())
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
