"""Stage 3: walk the group graph in topological order, hand each group's
already-built parents to its v4 generator, and stash the resulting node.

No source-concepts callback: parents are explicit, derived from the group
graph's lineage edges. Generators that haven't been ported to the v4 flat
style fall back inside `v4_node_generators.dispatch.build_node`."""

import networkx as nx

from trilogy.constants import logger
from trilogy.core.enums import BooleanOperator
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import (
    BuildConcept,
    BuildConditional,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import History, StrategyNode
from trilogy.core.processing.v4_node_generators import build_node

from .constants import FINAL_NODE_ID


def combine_clauses(clauses: list[BuildWhereClause]) -> BuildWhereClause | None:
    """AND a chain of clauses together; returns None if `clauses` is empty."""
    if not clauses:
        return None
    combined = clauses[0].conditional
    for extra in clauses[1:]:
        combined = BuildConditional(
            left=combined, right=extra.conditional, operator=BooleanOperator.AND
        )
    return BuildWhereClause(conditional=combined)


def _members_of(group_graph: nx.DiGraph, gid: str) -> set[str]:
    gd = group_graph.nodes[gid]
    return set(gd.get("primary_members", ())) | set(gd.get("secondary_members", ()))


def _active_clauses_at(group_graph: nx.DiGraph, gid: str) -> list[BuildWhereClause]:
    """Every clause injected at `gid` or any ancestor — those filters are in
    effect when we materialize this group's StrategyNode."""
    active: list[BuildWhereClause] = []
    ancestors = nx.ancestors(group_graph, gid) | {gid}
    for anc in ancestors:
        if anc == FINAL_NODE_ID:
            continue
        for clause in group_graph.nodes[anc].get("condition_objects", []) or []:
            if clause not in active:
                active.append(clause)
    return active


def _parent_nodes_for(
    group_graph: nx.DiGraph,
    built: dict[str, StrategyNode],
    gid: str,
) -> list[StrategyNode]:
    """Look up the already-built StrategyNodes for `gid`'s lineage
    predecessors. Topological order guarantees they exist (or that the
    generator was skipped, in which case we just skip that parent)."""
    parents: list[StrategyNode] = []
    for pgid in group_graph.predecessors(gid):
        if pgid == FINAL_NODE_ID:
            continue
        node = built.get(pgid)
        if node is not None:
            parents.append(node.copy())
    return parents


def _topological_order(group_graph: nx.DiGraph) -> list[str]:
    lineage_edges = [
        (u, v)
        for u, v, d in group_graph.edges(data=True)
        if d.get("kind") == "lineage"
    ]
    lineage_only = group_graph.edge_subgraph(lineage_edges).copy()
    for n in group_graph.nodes:
        lineage_only.add_node(n)
    return list(nx.topological_sort(lineage_only))


def _pick_result(
    group_graph: nx.DiGraph,
    built: dict[str, StrategyNode],
    mandatory_list: list[BuildConcept],
) -> StrategyNode:
    """Pick the most-downstream built group as the result; with multiple
    leaves, prefer the one whose primary members cover the most mandatory
    concepts."""
    mandatory_addrs = {c.address for c in mandatory_list}
    leaves = [
        gid
        for gid in built
        if not any(s in built for s in group_graph.successors(gid))
    ]
    if not leaves:
        leaves = list(built)
    leaves.sort(
        key=lambda gid: len(mandatory_addrs & _members_of(group_graph, gid)),
        reverse=True,
    )
    return built[leaves[0]]


def build_strategy_node(
    group_graph: nx.DiGraph,
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    g: ReferenceGraph,
    history: History,
) -> StrategyNode | None:
    """Walk groups in topological order, dispatching each to its v4 generator
    with explicit parent nodes. Returns the most-downstream built node, or
    None if nothing built."""
    built: dict[str, StrategyNode] = {}

    for gid in _topological_order(group_graph):
        if gid == FINAL_NODE_ID:
            continue
        data = group_graph.nodes[gid]
        derivation = data.get("derivation")
        # Outputs = primaries + secondaries. Secondary members are the
        # concepts a group can also expose for free: grain components on
        # aggregates/windows (which appear in the GROUP BY and need to be
        # in the SELECT), and lineage parents/roots for basics (so downstream
        # groups can reach them via this node).
        primary_addrs = data.get("primary_members", ())
        secondary_addrs = data.get("secondary_members", ())
        outputs = [
            environment.concepts[a]
            for a in (*primary_addrs, *secondary_addrs)
            if a in environment.concepts
        ]
        if not outputs:
            continue
        injected = combine_clauses(_active_clauses_at(group_graph, gid))
        parents = _parent_nodes_for(group_graph, built, gid)
        node = build_node(
            derivation=derivation,
            outputs=outputs,
            parents=parents,
            environment=environment,
            conditions=injected,
            history=history,
            g=g,
        )
        logger.info(
            f"[v4] built {gid} derivation={derivation} "
            f"outputs={[o.address for o in outputs]} "
            f"parents={[type(p).__name__ for p in parents]} "
            f"-> {type(node).__name__ if node else None}"
        )
        if node is not None:
            built[gid] = node

    if not built:
        return None
    return _pick_result(group_graph, built, mandatory_list)
