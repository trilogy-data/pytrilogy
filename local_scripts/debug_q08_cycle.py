"""Probe q08's group-graph cycle and test two hypotheses for fixing it.

The reported cycle is:
    grp:unnest:d0:∅  <->  grp:basic:d1:customer.address.id

Hypothesis A (concept-level): the cycle is caused by the "every d1 → every d0"
constraint pass in concept_graph.py (lines 348-353). It applies no ancestor
check, so a d1 concept gets a constraint edge pointing back into its OWN
lineage ancestor — e.g. `final_zips` (d1, basic) gets a constraint edge to
`zips_pre` (d0, unnest), even though zips_pre is already a lineage parent of
final_zips (via zips → _virt_filter_zips → final_zips). The backfill pass on
line 372 already has this guard; the main pass doesn't.

Hypothesis B (grouping-level): BASIC's grain-subset merge folds nodes with
unrelated lineage parents into one bucket. `zips` (parent: unnest), `final_zips`
(parents: zips + p_cust_zip-derived), and `_virt_func_substring` (parent:
p_cust_zip filter) all share customer.address.id ⊇ ∅, so they merge into one
basic:d1:customer.address.id bucket. The merged bucket's grain isn't actually
zips's grain — `zips` is unnest output, not customer-grain. Keying basics by
`(grain, parent_group_set)` instead would split them.

This script applies both fixes in isolation and prints the resulting group
edges so we can see which paths each fix opens up.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import networkx as nx  # noqa: E402
from discovery_v4 import TPCDS_DIR, _find_select, _materialize_for_query  # noqa: E402

from trilogy import Environment  # noqa: E402
from trilogy.core.processing.concept_strategies_v4 import History  # noqa: E402
from trilogy.core.processing.v4_helper.concept_graph import (  # noqa: E402
    build_concept_graph,
)
from trilogy.core.processing.v4_helper.group_graph import (
    build_group_graph,
)  # noqa: E402


def build_for_q08():
    path = TPCDS_DIR / "query08.preql"
    env = Environment(working_path=TPCDS_DIR)
    _, queries = env.parse(path.read_text())
    select = _find_select(queries)
    history = History(base_environment=env)
    build_stmt, build_env, conditions = _materialize_for_query(env, select, history)
    cg = build_concept_graph(
        mandatory_list=list(build_stmt.output_components),
        environment=build_env,
        conditions=[conditions] if conditions else [],
    )
    return build_stmt, conditions, cg


def strip_spurious_constraints(cg: nx.DiGraph) -> int:
    """Apply hypothesis A: drop constraint edges where dst is already a lineage
    ancestor of src. Returns the number of edges removed."""
    lineage_only = nx.DiGraph()
    lineage_only.add_nodes_from(cg.nodes)
    for u, v, ed in cg.edges(data=True):
        if ed.get("kind") == "lineage":
            lineage_only.add_edge(u, v)
    removed = 0
    for u, v, ed in list(cg.edges(data=True)):
        if ed.get("kind") != "constraint":
            continue
        if v in nx.ancestors(lineage_only, u):
            print(f"    drop constraint {u} -> {v}  (dst is lineage ancestor of src)")
            cg.remove_edge(u, v)
            removed += 1
    return removed


def report_group_graph(label: str, cg, build_stmt, conditions) -> None:
    gg, attrs = build_group_graph(
        cg,
        [conditions] if conditions else [],
        mandatory_list=list(build_stmt.output_components),
    )
    print(
        f"\n--- {label}: group_graph ({gg.number_of_nodes()} nodes, {gg.number_of_edges()} edges) ---"
    )
    for n in gg.nodes:
        if n == "__final__":
            continue
        a = attrs[n]
        print(f"  {n}  members={a.members}")
        if a.output_concepts:
            print(f"    outputs={a.output_concepts}")
    print("  -- group edges --")
    for u, v, ed in gg.edges(data=True):
        if ed.get("kind") == "lineage":
            print(f"    {u}  ->  {v}")
    lineage_edges = [
        (u, v) for u, v, ed in gg.edges(data=True) if ed.get("kind") == "lineage"
    ]
    sub = nx.DiGraph()
    sub.add_nodes_from(gg.nodes)
    sub.add_edges_from(lineage_edges)
    try:
        cycle = nx.find_cycle(sub)
        print(f"  CYCLE: {cycle}")
    except nx.NetworkXNoCycle:
        print("  no cycle in lineage edges")


def drop_constraints_to_non_d0(cg: nx.DiGraph) -> int:
    """After re-labeling, any pre-existing constraint edge whose dst is no
    longer d0 was justified only by the old (mislabeled) d1→d0 rule. Drop it."""
    removed = 0
    for u, v, ed in list(cg.edges(data=True)):
        if ed.get("kind") != "constraint":
            continue
        if cg.nodes[v].get("depth_label") != "d0":
            print(f"    drop constraint {u} -> {v}  (dst no longer d0)")
            cg.remove_edge(u, v)
            removed += 1
    return removed


def propagate_d1_upstream(cg: nx.DiGraph) -> list[str]:
    """Hypothesis C: a concept whose lineage descendant is d1 must itself be
    d1 (it has to be computed in the pre-condition phase). The current
    classify_depth only tags the leaf condition addresses + row-shape
    barriers; barrierness wins over phase even when the barrier feeds a d1.

    Walk the lineage subgraph in reverse-topo and promote any node with a
    d1 descendant to d1, keeping the original derivation/grain intact.
    Returns the list of promoted nodes."""
    lineage = nx.DiGraph()
    lineage.add_nodes_from(cg.nodes(data=True))
    for u, v, ed in cg.edges(data=True):
        if ed.get("kind") == "lineage":
            lineage.add_edge(u, v)
    promoted: list[str] = []
    for node in reversed(list(nx.topological_sort(lineage))):
        if cg.nodes[node].get("depth_label") == "d1":
            continue
        for succ in lineage.successors(node):
            if cg.nodes[succ].get("depth_label") == "d1":
                cg.nodes[node]["depth_label"] = "d1"
                promoted.append(node)
                break
    return promoted


def main() -> None:
    print("=== baseline ===")
    build_stmt, conditions, cg = build_for_q08()
    report_group_graph("baseline", cg, build_stmt, conditions)

    print("\n=== hypothesis A: strip constraint edges to lineage ancestors ===")
    _, conditions2, cg2 = build_for_q08()
    n_removed = strip_spurious_constraints(cg2)
    print(f"  removed {n_removed} constraint edge(s)")
    report_group_graph("hypothesisA", cg2, build_stmt, conditions2)

    print("\n=== hypothesis C: propagate d1 upstream through lineage ===")
    _, conditions3, cg3 = build_for_q08()
    promoted = propagate_d1_upstream(cg3)
    print(f"  promoted {len(promoted)} node(s) to d1: {promoted}")
    n_dropped = drop_constraints_to_non_d0(cg3)
    print(f"  dropped {n_dropped} stale constraint edge(s)")
    report_group_graph("hypothesisC", cg3, build_stmt, conditions3)


if __name__ == "__main__":
    main()
