"""Shadow the legacy attempt-ladder source planner against the v4-native
network search and report where they choose different datasource sets.

    .venv/Scripts/python.exe local_scripts/s32_network_shadow.py [query ...]

With no args, sweeps the TPC-DS corpus. Output is a per-request table plus a
summary; divergences are the work list for the cutover (each is either a
missing carry-over rule or a genuine win).
"""

from __future__ import annotations

import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from trilogy import Dialects, Environment
from trilogy.core.models.build import BuildUnionDatasource
from trilogy.core.processing.nodes import SelectNode, StrategyNode
from trilogy.core.processing.v4_helper import source_planning
from trilogy.core.processing.v4_helper.network_search import (
    SourceNetwork,
    _solution_for,
    build_source_network,
    search_sources,
)

AXIS_NAMES = (
    "partial_terminals, completions, fanout_sources, sources, connectors,"
    " derived_joins"
)

TPCDS_ROOT = (
    Path(__file__).resolve().parents[1] / "tests" / "modeling" / "tpc_ds_duckdb"
)


@dataclass
class Divergence:
    query: str
    terminals: tuple[str, ...]
    legacy: tuple[str, ...]
    network: tuple[str, ...]
    note: str
    verdict: str = ""
    legacy_cost: str = ""
    network_cost: str = ""


RECORDS: list[Divergence] = []
STATS: Counter[str] = Counter()
CURRENT_QUERY = ""


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


def _expand_unions(network: SourceNetwork, sources: tuple[str, ...]) -> tuple[str, ...]:
    """The legacy planner names a union family's children; the network names the
    union. Compare the underlying physical scans."""
    out: set[str] = set()
    for node in sources:
        datasource = network.candidates[node].datasource
        if isinstance(datasource, BuildUnionDatasource):
            out.update("ds~" + child.identifier for child in datasource.children)
        else:
            out.add(node)
    return tuple(sorted(out))


def _legacy_as_candidates(
    network: SourceNetwork, legacy: tuple[str, ...]
) -> frozenset[str] | None:
    """Re-express the legacy planner's physical scans as network candidates,
    collapsing a fully-present union family onto its union candidate so the two
    plans are scored on the same terms."""
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


def _classify(
    network: SourceNetwork, legacy: tuple[str, ...], result
) -> tuple[str, str, str]:
    """Score both plans under one cost model. A divergence is then either a win
    (legacy is dominated), a search bug (the legacy plan dominates), or a real
    tie the cost model cannot yet separate."""
    if result.solution is None:
        return "network-declines", "", ""
    targets = list(network.terminals)
    legacy_sources = _legacy_as_candidates(network, legacy)
    if legacy_sources is None:
        return "legacy-uses-disqualified-source", "", str(result.solution.cost.axes())
    uncovered = [
        address
        for address in targets
        if not any(network.candidates[n].binds(address) for n in legacy_sources)
    ]
    network_cost = str(result.solution.cost.axes())
    if uncovered:
        return f"legacy-uncovered {uncovered}", "", network_cost
    legacy_solution = _solution_for(network, legacy_sources, targets)
    legacy_cost = str(legacy_solution.cost.axes())
    if result.solution.cost.dominates(legacy_solution.cost):
        verdict = "network-dominates"
    elif legacy_solution.cost.dominates(result.solution.cost):
        verdict = "LEGACY-DOMINATES(search bug)"
    else:
        verdict = "incomparable(needs axis)"
    return verdict, legacy_cost, network_cost


def shadow_plan_source(request: source_planning.SourceRequest):
    legacy_node = _ORIGINAL(request)
    STATS["requests"] += 1
    try:
        terminals = source_planning._search_concepts_for_bridge(request)
        network = build_source_network(
            terminals, request.environment, request.graph, request.conditions
        )
        result = search_sources(network)
    except Exception as exc:
        STATS["network_error"] += 1
        RECORDS.append(
            Divergence(
                CURRENT_QUERY,
                tuple(sorted(c.address for c in request.outputs)),
                _node_datasources(legacy_node),
                (),
                f"ERROR {type(exc).__name__}: {exc}",
            )
        )
        return legacy_node

    legacy = _node_datasources(legacy_node)
    network_sources = (
        _expand_unions(network, result.solution.sources)
        if result.solution is not None
        else ()
    )
    if not legacy and not network_sources:
        # both planners decline: the request is satisfied elsewhere (a derived
        # concept another group supplies), not a divergence
        STATS["both_decline"] += 1
        return legacy_node
    if result.solution is None:
        STATS["network_no_solution"] += 1
        note = "no solution" + (
            f" (unreachable {sorted(result.unreachable)})" if result.unreachable else ""
        )
    elif set(legacy) == set(network_sources):
        STATS["match"] += 1
        if result.ambiguous:
            STATS["match_but_ambiguous"] += 1
        return legacy_node
    else:
        STATS["diverge"] += 1
        note = "ambiguous" if result.ambiguous else ""
        if result.truncated:
            note = (note + " truncated").strip()
    unbound = tuple(
        address
        for address in network.terminals
        if result.solution is not None
        and not any(
            network.candidates[node].binds(address) for node in result.solution.sources
        )
    )
    if unbound:
        note = (note + f" UNBOUND {list(unbound)}").strip()
    verdict, legacy_cost, network_cost = _classify(network, legacy, result)
    STATS[f"verdict:{verdict.split('(')[0].split(' ')[0]}"] += 1
    RECORDS.append(
        Divergence(
            CURRENT_QUERY,
            network.terminals,
            legacy,
            network_sources,
            note,
            verdict,
            legacy_cost,
            network_cost,
        )
    )
    return legacy_node


_ORIGINAL = source_planning.plan_source


def run_query(path: Path) -> None:
    global CURRENT_QUERY
    CURRENT_QUERY = path.name
    env = Environment(working_path=path.parent)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    try:
        executor.parse_text(path.read_text())
    except Exception as exc:
        STATS["query_error"] += 1
        print(f"  !! {path.name}: {type(exc).__name__}: {exc}")


def main(argv: list[str]) -> int:

    source_planning.plan_source = shadow_plan_source
    # dispatch.py imported the symbol directly; repoint that binding too
    from trilogy.core.processing.v4_node_generators import root as v4_root

    v4_root.plan_source = shadow_plan_source  # type: ignore[attr-defined]

    if argv:
        queries = [Path(a) if Path(a).exists() else TPCDS_ROOT / a for a in argv]
    else:
        queries = sorted(TPCDS_ROOT.glob("query*.preql"))
    for path in queries:
        print(f"== {path.name}")
        run_query(path)

    print("\n=== divergences ===")
    print(f"cost axes: {AXIS_NAMES}\n")
    for record in RECORDS:
        print(f"{record.query}: {record.verdict} {record.note}")
        print(f"  terminals: {', '.join(record.terminals) or '(none)'}")
        print(
            f"  legacy   : {', '.join(record.legacy) or '(none)'}  {record.legacy_cost}"
        )
        print(
            f"  network  : {', '.join(record.network) or '(none)'}  {record.network_cost}"
        )
    print("\n=== summary ===")
    for key, value in sorted(STATS.items()):
        print(f"{key:24} {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
