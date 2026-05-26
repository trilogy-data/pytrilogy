"""Stage 3: walk the group graph in topological order, hand each group's
already-built parents to its v4 generator, and stash the resulting node.

No source-concepts callback: parents are explicit, derived from the group
graph's lineage edges. Generators that haven't been ported to the v4 flat
style fall back inside `v4_node_generators.dispatch.build_node`."""

from collections import defaultdict

import networkx as nx

from trilogy.constants import logger
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import combine_condition_atoms
from trilogy.core.processing.nodes import (
    GroupNode,
    History,
    MergeNode,
    StrategyNode,
)
from trilogy.core.processing.v4_node_generators import build_node

from .constants import FINAL_NODE_ID


def _wrap_atoms(atoms: list[BoolExpr]) -> BuildWhereClause | None:
    """AND-combine a list of condition atoms into a single BuildWhereClause."""
    if not atoms:
        return None
    combined = combine_condition_atoms(atoms)
    if combined is None:
        return None
    return BuildWhereClause(conditional=combined)


def _members_of(group_graph: nx.DiGraph, gid: str) -> set[str]:
    gd = group_graph.nodes[gid]
    return set(gd.get("primary_members", ())) | set(gd.get("secondary_members", ()))


def _atoms_at(group_graph: nx.DiGraph, gid: str) -> list[BoolExpr]:
    """Atoms injected AT `gid` only. These become the WHERE for this node."""
    return list(group_graph.nodes[gid].get("condition_atoms", []) or [])


def _accumulated_atoms_above(
    group_graph: nx.DiGraph, gid: str
) -> list[BoolExpr]:
    """Atoms applied at any STRICT ancestor of `gid`. Threaded into the node
    as `preexisting_conditions` so nullable inference (and any later
    optimizer) knows which rows the parent already filtered, without
    re-emitting the same WHERE on this CTE."""
    accumulated: list[BoolExpr] = []
    for anc in nx.ancestors(group_graph, gid):
        if anc == FINAL_NODE_ID:
            continue
        for atom in group_graph.nodes[anc].get("condition_atoms", []) or []:
            if atom not in accumulated:
                accumulated.append(atom)
    return accumulated


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


def _satisfiable_outputs(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
) -> list[BuildConcept]:
    """Drop outputs that no parent can actually supply. The group graph's
    secondary-members pass attaches every root to every basic on the
    optimistic theory "I can reach them"; but when a basic's direct parents
    are aggregates/windows, the roots were collapsed away and aren't in any
    parent's output. Without this filter those concepts end up in
    `output_concepts` with no source map entry, producing
    `INVALID_REFERENCE_BUG_<...>` markers in the rendered SQL."""
    if not parents:
        return outputs
    available: set[str] = set()
    for parent in parents:
        for output in parent.output_concepts:
            available.add(output.address)
    keep: list[BuildConcept] = []
    for concept in outputs:
        if concept.address in available:
            keep.append(concept)
            continue
        if concept.lineage is not None:
            args = {a.address for a in concept.lineage.concept_arguments}
            if args <= available:
                keep.append(concept)
    return keep


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


def _cover_groups_for_mandatory(
    group_graph: nx.DiGraph,
    built: dict[str, StrategyNode],
    mandatory_list: list[BuildConcept],
) -> dict[str, list[BuildConcept]]:
    """For each mandatory concept, pick the most-downstream built group that
    actually exposes it (more built ancestors = further downstream). Returns
    `{gid: [concepts that group provides]}` preserving discovery order so
    the MergeNode renders with a stable join layout."""
    per_group: dict[str, list[BuildConcept]] = defaultdict(list)
    for concept in mandatory_list:
        addr = concept.address
        candidates = [
            gid
            for gid, node in built.items()
            if any(o.address == addr for o in node.output_concepts)
        ]
        if not candidates:
            continue
        candidates.sort(
            key=lambda gid: sum(
                1 for a in nx.ancestors(group_graph, gid) if a in built
            ),
            reverse=True,
        )
        per_group[candidates[0]].append(concept)
    return per_group


def _wrap_for_grain(
    parent_node: StrategyNode,
    needed_concepts: list[BuildConcept],
    environment: BuildEnvironment,
) -> list[StrategyNode]:
    """When a parent feeds a merge edge, its grain may be wider than the
    natural grain of the concepts the merge actually wants — joining the
    parent's wider-grain rows into a per-key aggregate blows up cardinality.

    For each natural-grain bucket among `needed_concepts`, emit a GroupNode
    that aggregates `parent_node` to that grain and exposes only those
    concepts plus the grain keys. Buckets whose grain already matches the
    parent's grain pass through unchanged.

    Originally root-only; generalized because intermediate aggregates can
    also sit at a wider grain than a specific concept of theirs requires
    (e.g. a `sum(...) by (a, b)` whose downstream merge only needs grain
    `{a}` for one column)."""
    if not needed_concepts:
        return [parent_node]

    parent_grain_components = (
        frozenset(parent_node.grain.components) if parent_node.grain else frozenset()
    )

    # Each concept's natural grain is the key it functionally depends on
    # (e.g. text_id is a property of customer.id, so its grain is
    # {customer.id}). `BuildGrain.from_concepts([c])` is the wrong helper
    # here — that asks "what grain do these concepts collectively require"
    # which can include the concept itself as a self-key.
    by_grain: dict[frozenset[str], list[BuildConcept]] = defaultdict(list)
    for concept in needed_concepts:
        grain_components = (
            frozenset(concept.grain.components) if concept.grain else frozenset()
        )
        by_grain[grain_components].append(concept)

    wraps: list[StrategyNode] = []
    for grain_comps, concepts in by_grain.items():
        if grain_comps == parent_grain_components or not grain_comps:
            wraps.append(parent_node)
            continue
        grain_concepts = [
            environment.concepts[a]
            for a in grain_comps
            if a in environment.concepts
        ]
        # Dedup by address, keep concept order stable.
        outputs_by_addr: dict[str, BuildConcept] = {}
        for c in concepts + grain_concepts:
            outputs_by_addr.setdefault(c.address, c)
        outputs = list(outputs_by_addr.values())
        wraps.append(
            GroupNode(
                output_concepts=outputs,
                input_concepts=outputs,
                environment=environment,
                parents=[parent_node],
            )
        )
    return wraps


def _assemble_final_node(
    group_graph: nx.DiGraph,
    built: dict[str, StrategyNode],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
) -> StrategyNode | None:
    """Build the FINAL output node: merge the minimum set of built groups
    that together cover `mandatory_list`. When a single group already covers
    every mandatory concept, return it as-is. Otherwise wrap the contributing
    groups in a MergeNode whose auto-join logic links them on shared output
    concepts — this is what the FINAL sink in the group graph was reserved
    for, instead of just picking one leaf and dropping the rest.

    For ROOT contributions specifically, project the root scan down to the
    needed concepts' natural grain via `_wrap_root_for_grain` so the merge
    join doesn't blow up cardinality (e.g. `text_id` at customer grain
    instead of one row per store_return)."""
    if not built:
        return None
    per_group = _cover_groups_for_mandatory(group_graph, built, mandatory_list)
    if not per_group:
        return next(iter(built.values()))
    contributing = list(per_group.keys())
    if len(contributing) == 1:
        return built[contributing[0]]

    # Only root scans get the grain projection: their grain is the row-level
    # source-table grain (often much wider than what a downstream merge
    # wants), and a SELECT DISTINCT-style projection is always safe.
    # Wrapping intermediate aggregates is *not* safe — adding a GroupNode
    # over a `sum(x)` node would re-aggregate the partial sums (OK for SUM,
    # wrong for AVG/STDDEV), and intermediate aggregates often don't even
    # expose the requested grain key (their GROUP BY is their grain, not the
    # downstream's). Q17 surfaced both pathologies when this was generalized.
    parents: list[StrategyNode] = []
    for gid in contributing:
        node = built[gid]
        is_root = group_graph.nodes[gid].get("derivation") == "root"
        if is_root:
            parents.extend(_wrap_for_grain(node, per_group[gid], environment))
        else:
            parents.append(node)

    available: set[str] = set()
    for p in parents:
        for o in p.output_concepts:
            available.add(o.address)
    outputs = [c for c in mandatory_list if c.address in available]
    return MergeNode(
        input_concepts=outputs,
        output_concepts=outputs,
        environment=environment,
        parents=parents,
    )


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
        injected = _wrap_atoms(_atoms_at(group_graph, gid))
        preexisting = _wrap_atoms(_accumulated_atoms_above(group_graph, gid))
        parents = _parent_nodes_for(group_graph, built, gid)
        outputs = _satisfiable_outputs(outputs, parents)
        if not outputs:
            continue
        node = build_node(
            derivation=derivation,
            outputs=outputs,
            parents=parents,
            environment=environment,
            conditions=injected,
            preexisting_conditions=preexisting,
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
    return _assemble_final_node(group_graph, built, mandatory_list, environment)
