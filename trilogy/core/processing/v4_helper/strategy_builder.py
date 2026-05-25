"""Stage 3: walk the group graph in topological order, dispatch each group
to its derivation factory, and stitch the per-group `StrategyNode`s together
via a graph-aware `source_concepts` callback."""

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
from trilogy.core.processing import concept_strategies_v3 as v3
from trilogy.core.processing.nodes import History, StrategyNode

from .constants import FINAL_NODE_ID
from .factory_dispatch import build_node_for_group


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


def _make_source_callback(
    group_graph: nx.DiGraph,
    built: dict[str, StrategyNode],
    current_gid: str,
):
    """Return a `source_concepts`-shaped callback that the per-derivation
    factories will invoke when they need to fetch parent concepts. We prefer
    already-built groups (graph predecessors first, then anything else built);
    if no built group covers the request we delegate to v3."""
    candidates = list(group_graph.predecessors(current_gid)) + [
        gid for gid in built if gid != current_gid
    ]
    seen: set[str] = set()
    ordered: list[str] = []
    for gid in candidates:
        if gid in seen or gid == FINAL_NODE_ID:
            continue
        seen.add(gid)
        ordered.append(gid)

    def cb(
        mandatory_list: list[BuildConcept],
        history: History,
        environment: BuildEnvironment,
        depth: int,
        g: ReferenceGraph,
        accept_partial: bool = False,
        conditions: BuildWhereClause | None = None,
    ) -> StrategyNode | None:
        requested = {c.address for c in mandatory_list}
        for pgid in ordered:
            if pgid not in built:
                continue
            if requested <= _members_of(group_graph, pgid):
                return built[pgid].copy()
        return v3.search_concepts(
            mandatory_list=mandatory_list,
            history=history,
            environment=environment,
            depth=depth,
            g=g,
            accept_partial=accept_partial,
            conditions=conditions,
        )

    return cb


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
    """Walk groups in topological order, dispatching each to its derivation
    factory. Returns the most-downstream built node, or None if nothing built."""
    built: dict[str, StrategyNode] = {}

    for gid in _topological_order(group_graph):
        if gid == FINAL_NODE_ID:
            continue
        data = group_graph.nodes[gid]
        derivation = data.get("derivation")
        primary_addrs = data.get("primary_members", ())
        primaries = [
            environment.concepts[a]
            for a in primary_addrs
            if a in environment.concepts
        ]
        if not primaries:
            continue
        injected = combine_clauses(_active_clauses_at(group_graph, gid))
        cb = _make_source_callback(group_graph, built, gid)
        node = build_node_for_group(
            derivation=derivation,
            primaries=primaries,
            environment=environment,
            g=g,
            history=history,
            source_concepts=cb,
            conditions=injected,
        )
        logger.info(
            f"[v4] built {gid} derivation={derivation} "
            f"primaries={[p.address for p in primaries]} "
            f"-> {type(node).__name__ if node else None}"
        )
        if node is not None:
            built[gid] = node

    if not built:
        return None
    return _pick_result(group_graph, built, mandatory_list)
