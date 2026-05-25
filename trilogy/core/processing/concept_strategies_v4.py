
from dataclasses import dataclass, field
from typing import List, Optional

from trilogy.constants import logger
from trilogy.core.enums import Derivation, Granularity
from trilogy.core.env_processor import generate_graph
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.author import (
    UndefinedConcept,
)
from trilogy.core.models.build import (
    BuildConcept,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import condition_implies
from trilogy.core.processing.constants import ROOT_DERIVATIONS, SKIPPED_DERIVATIONS
from trilogy.core.processing.discovery_node_factory import generate_node
from trilogy.core.processing.discovery_utility import (
    LOGGER_PREFIX,
    depth_to_prefix,
    get_loop_iteration_targets,
    group_if_required_v2,
)
from trilogy.core.processing.discovery_validation import (
    ValidationResult,
    _is_scalar_only,
    validate_stack,
)
from trilogy.core.processing.node_generators.basic_node import gen_basic_node
from trilogy.core.processing.node_generators.group_node import gen_group_node
from trilogy.core.processing.node_generators.window_node import gen_window_node 
from trilogy.core.processing.node_generators.node_merge_node import gen_merge_node
from trilogy.core.processing import concept_strategies_v3 as v3
from trilogy.core.processing.nodes import (
    History,
    MergeNode,
    StrategyNode,
)
from trilogy.utility import unique
import networkx as nx


# Derivations that change row shape — a filter cannot be safely pushed below
# one of these; it must be applied above the barrier instead.
ROW_SHAPE_BARRIER_DERIVATIONS: set[Derivation] = {
    Derivation.AGGREGATE,
    Derivation.WINDOW,
    Derivation.UNNEST,
    Derivation.GROUP_TO,
    Derivation.UNION,
    Derivation.RECURSIVE,
    Derivation.ROWSET,
}

FINAL_NODE_ID = "__final__"


@dataclass
class BuildInfo:
    """Result bundle for discovery: the raw concept graph, the grouped graph,
    and the materialized StrategyNode produced by walking the group graph."""

    concept_graph: nx.DiGraph = field(default_factory=nx.DiGraph)
    group_graph: nx.DiGraph = field(default_factory=nx.DiGraph)
    strategy_node: StrategyNode | None = None

    def copy(self) -> "BuildInfo":
        return BuildInfo(
            concept_graph=self.concept_graph.copy(),
            group_graph=self.group_graph.copy(),
            strategy_node=self.strategy_node.copy() if self.strategy_node else None,
        )


def _classify_depth(concept: BuildConcept, condition_addresses: set[str]) -> str:
    if concept.address in condition_addresses:
        return "d1"
    if concept.derivation in ROW_SHAPE_BARRIER_DERIVATIONS:
        return "d0"
    return "d*"


def _add_concept_to_graph(
    concept: BuildConcept,
    environment: BuildEnvironment,
    graph: nx.DiGraph,
    condition_addresses: set[str],
) -> None:
    """Recurse up a concept's lineage, adding nodes/edges for each ancestor."""
    if concept.address in graph:
        return
    graph.add_node(
        concept.address,
        derivation=concept.derivation.value,
        purpose=concept.purpose.value,
        granularity=concept.granularity.value,
        depth_label=_classify_depth(concept, condition_addresses),
        grain_components=frozenset(concept.grain.components) if concept.grain else frozenset(),
    )
    if concept.lineage is None:
        return
    for parent in concept.lineage.concept_arguments:
        resolved = environment.concepts.get(parent.address, parent)
        _add_concept_to_graph(resolved, environment, graph, condition_addresses)
        graph.add_edge(resolved.address, concept.address, kind="lineage")


def _build_concept_graph(
    mandatory_list: List[BuildConcept],
    environment: BuildEnvironment,
    conditions: list[BuildWhereClause],
) -> nx.DiGraph:
    graph: nx.DiGraph = nx.DiGraph()
    condition_addresses = {
        c.address for clause in conditions for c in clause.concept_arguments
    }
    for concept in mandatory_list:
        _add_concept_to_graph(concept, environment, graph, condition_addresses)
    for clause in conditions:
        for concept in clause.concept_arguments:
            resolved = environment.concepts.get(concept.address, concept)
            _add_concept_to_graph(resolved, environment, graph, condition_addresses)

    d1_nodes = [n for n, d in graph.nodes(data=True) if d.get("depth_label") == "d1"]
    d0_nodes = [n for n, d in graph.nodes(data=True) if d.get("depth_label") == "d0"]
    for src in d1_nodes:
        for dst in d0_nodes:
            graph.add_edge(src, dst, kind="constraint")
    return graph


def _build_group_graph(
    concept_graph: nx.DiGraph,
    conditions: list[BuildWhereClause],
) -> nx.DiGraph:
    """Collapse compatible concepts into groups and append a single 'final' sink.

    Non-basic concepts group when they share `depth_label`, `derivation`, and
    grain — same row shape, same role in the plan, deliverable in one scan.
    Basics group purely by grain compatibility (subset/equal): any pair of
    basics whose grains are nested can be co-projected, so connected
    components in the subset relation define the basic groups.
    """
    # Derivations whose row shape is defined by a grain/by/partition list —
    # those grain components can be "pulled into" the group for free since they
    # are part of the GROUP BY (or PARTITION BY) clause already.
    grouping_derivations = {
        Derivation.AGGREGATE.value,
        Derivation.WINDOW.value,
        Derivation.GROUP_TO.value,
    }

    def _lineage_parents(node: str) -> frozenset[str]:
        return frozenset(
            u
            for u, _, d in concept_graph.in_edges(node, data=True)
            if d.get("kind") == "lineage"
        )

    root_addresses: set[str] = {
        n
        for n, d in concept_graph.nodes(data=True)
        if d.get("derivation") == Derivation.ROOT.value
    }

    primary_group: dict[str, str] = {}
    group_data: dict[str, dict] = {}

    # Basics defer to a second pass: their groupability is a structural
    # property (grain compatibility), not a per-node identity, so we union-find
    # them once the non-basic groups exist.
    deferred_basics: list[tuple[str, str, frozenset[str]]] = []

    for node, data in concept_graph.nodes(data=True):
        depth_label = data.get("depth_label", "d*")
        derivation = data.get("derivation", "")
        grain = frozenset(data.get("grain_components", ()))
        # Roots can always be co-sourced (joined across datasources in a single
        # base scan), so collapse every root concept into one group regardless
        # of depth_label or grain.
        if derivation == Derivation.ROOT.value:
            group_id = "grp:root"
            group_depth = "root"
            group_grain: frozenset = frozenset()
        elif derivation == Derivation.BASIC.value:
            deferred_basics.append((node, depth_label, grain))
            continue
        else:
            grain_key = "|".join(sorted(grain)) or "∅"
            group_id = f"grp:{depth_label}:{derivation}:{grain_key}"
            group_depth = depth_label
            group_grain = grain
        primary_group[node] = group_id
        bucket = group_data.setdefault(
            group_id,
            {
                "depth_label": group_depth,
                "derivation": derivation,
                "grain_components": group_grain,
                "primary_members": [],
                "secondary_members": [],
                "member_depths": {},
            },
        )
        bucket["primary_members"].append(node)
        bucket["member_depths"][node] = depth_label

    # Basic-grouping pass: union basics whose grains are subset/equal. Two
    # basics A,B share a scan iff grain(A) ⊆ grain(B) or grain(B) ⊆ grain(A);
    # by transitivity, the connected components in that relation are the
    # groups. We don't read derivation/purpose/parents — grain alone is the
    # structural property that decides co-projectability.
    parent_uf: dict[int, int] = {i: i for i in range(len(deferred_basics))}

    def _find(x: int) -> int:
        while parent_uf[x] != x:
            parent_uf[x] = parent_uf[parent_uf[x]]
            x = parent_uf[x]
        return x

    def _union(a: int, b: int) -> None:
        ra, rb = _find(a), _find(b)
        if ra != rb:
            parent_uf[rb] = ra

    for i in range(len(deferred_basics)):
        gi = deferred_basics[i][2]
        for j in range(i + 1, len(deferred_basics)):
            gj = deferred_basics[j][2]
            if gi <= gj or gj <= gi:
                _union(i, j)

    components: dict[int, list[int]] = {}
    for i in range(len(deferred_basics)):
        components.setdefault(_find(i), []).append(i)

    for ridx, members in components.items():
        merged_grain: frozenset[str] = frozenset().union(
            *(deferred_basics[i][2] for i in members)
        )
        depths = {deferred_basics[i][1] for i in members}
        # Group inherits the strictest placement label of its members: a d1
        # member pins the whole group above d0 barriers.
        group_depth = "d1" if "d1" in depths else next(iter(depths))
        grain_key = "|".join(sorted(merged_grain)) or "∅"
        group_id = f"grp:basic:{grain_key}"
        bucket = group_data.setdefault(
            group_id,
            {
                "depth_label": group_depth,
                "derivation": Derivation.BASIC.value,
                "grain_components": merged_grain,
                "primary_members": [],
                "secondary_members": [],
                "member_depths": {},
            },
        )
        for i in members:
            node, depth_label, _ = deferred_basics[i]
            primary_group[node] = group_id
            bucket["primary_members"].append(node)
            bucket["member_depths"][node] = depth_label

    def _attach_secondary(group_id: str, address: str) -> None:
        gd = group_data[group_id]
        if address in gd["primary_members"] or address in gd["secondary_members"]:
            return
        if address not in concept_graph.nodes:
            return
        gd["secondary_members"].append(address)
        gd["member_depths"][address] = concept_graph.nodes[address].get(
            "depth_label", "d*"
        )

    # Secondary-membership pass: each group lists every concept it can also
    # expose (grain columns from a GROUP BY/PARTITION BY; roots and lineage
    # parents that co-project alongside a basic). Secondary membership is for
    # display/availability only — it does NOT suppress upstream lineage edges,
    # since the downstream group still needs the upstream scan to feed it.
    for gid, gd in group_data.items():
        if gd["derivation"] in grouping_derivations:
            for grain_addr in gd["grain_components"]:
                _attach_secondary(gid, grain_addr)
        elif gd["derivation"] == Derivation.BASIC.value:
            for root_addr in root_addresses:
                _attach_secondary(gid, root_addr)
            for member in list(gd["primary_members"]):
                for parent in _lineage_parents(member):
                    _attach_secondary(gid, parent)

    group_graph: nx.DiGraph = nx.DiGraph()
    for gid, gd in group_data.items():
        members = tuple(gd["primary_members"]) + tuple(gd["secondary_members"])
        group_graph.add_node(
            gid,
            depth_label=gd["depth_label"],
            derivation=gd["derivation"],
            grain_components=gd["grain_components"],
            members=members,
            primary_members=tuple(gd["primary_members"]),
            secondary_members=tuple(gd["secondary_members"]),
            member_depths=dict(gd["member_depths"]),
            conditions=[],
            condition_objects=[],
        )

    for u, v, edata in concept_graph.edges(data=True):
        if edata.get("kind") != "lineage":
            continue
        gu, gv = primary_group[u], primary_group[v]
        if gu == gv:
            continue
        group_graph.add_edge(gu, gv, kind="lineage")

    # Condition injection: place each clause on the furthest-upstream group
    # that already exposes every input the clause references. Validate that
    # no d0 barrier sits upstream of the chosen group (a filter cannot be
    # pushed past a row-shape change).
    d0_group_ids = {
        gid for gid, gd in group_data.items() if gd["depth_label"] == "d0"
    }
    group_members: dict[str, set[str]] = {
        gid: set(gd["primary_members"]) | set(gd["secondary_members"])
        for gid, gd in group_data.items()
    }
    condition_group_ids: set[str] = set()
    for clause in conditions:
        inputs = {c.address for c in clause.concept_arguments}
        candidates = [gid for gid, mems in group_members.items() if inputs <= mems]
        if not candidates:
            continue
        cand_set = set(candidates)
        upstream_most = [
            gid
            for gid in candidates
            if not (cand_set & nx.ancestors(group_graph, gid))
        ]
        chosen = upstream_most[0] if upstream_most else candidates[0]
        chosen_ancestors = nx.ancestors(group_graph, chosen)
        offending = d0_group_ids & chosen_ancestors
        if offending:
            raise ValueError(
                f"Condition {clause} would be injected at {chosen}, which is "
                f"downstream of d0 barrier(s) {sorted(offending)}; conditions "
                f"cannot be pushed past row-shape changes."
            )
        group_graph.nodes[chosen]["conditions"].append(str(clause))
        group_graph.nodes[chosen]["condition_objects"].append(clause)
        condition_group_ids.add(chosen)

    # Phase coloring: any edge originating from a condition group (or any
    # descendant) is "post_condition" (filter already applied); everything
    # earlier is "pre_condition".
    downstream: set[str] = set(condition_group_ids)
    for cgid in condition_group_ids:
        downstream |= nx.descendants(group_graph, cgid)
    for u, v in list(group_graph.edges()):
        group_graph.edges[u, v]["phase"] = (
            "post_condition" if u in downstream else "pre_condition"
        )

    non_condition_members = tuple(
        n for n, d in concept_graph.nodes(data=True) if d.get("depth_label") != "d1"
    )
    group_graph.add_node(
        FINAL_NODE_ID,
        depth_label="final",
        derivation="final",
        grain_components=frozenset(),
        members=non_condition_members,
        conditions=[str(c) for c in conditions],
    )
    for gid in group_data:
        phase = "post_condition" if gid in downstream else "pre_condition"
        group_graph.add_edge(gid, FINAL_NODE_ID, kind="merge", phase=phase)

    return group_graph


def _combine_clauses(clauses: list[BuildWhereClause]) -> BuildWhereClause | None:
    if not clauses:
        return None
    from trilogy.core.enums import BooleanOperator
    from trilogy.core.models.build import BuildConditional

    combined = clauses[0].conditional
    for extra in clauses[1:]:
        combined = BuildConditional(
            left=combined, right=extra.conditional, operator=BooleanOperator.AND
        )
    return BuildWhereClause(conditional=combined)


def _build_strategy_node(
    group_graph: nx.DiGraph,
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    g: ReferenceGraph,
    history: History,
) -> StrategyNode | None:
    """Walk the group graph in topological order, dispatching to existing
    per-derivation factories (gen_select_node for root, gen_group_node for
    aggregate, gen_basic_node for basic) and wiring parent groups through a
    graph-aware `source_concepts` callback."""

    built: dict[str, StrategyNode] = {}

    lineage_edges = [
        (u, v) for u, v, d in group_graph.edges(data=True) if d.get("kind") == "lineage"
    ]
    lineage_only = group_graph.edge_subgraph(lineage_edges).copy()
    for n in group_graph.nodes:
        lineage_only.add_node(n)
    order = list(nx.topological_sort(lineage_only))

    def _members_of(gid: str) -> set[str]:
        gd = group_graph.nodes[gid]
        return set(gd.get("primary_members", ())) | set(gd.get("secondary_members", ()))

    def _make_source_callback(current_gid: str):
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
                if requested <= _members_of(pgid):
                    return built[pgid].copy()
            # Fallback: defer to v3 when the request includes concepts the
            # group graph didn't surface (e.g. implicit grain components
            # like order_id for an aggregate over a property of order_id).
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

    def _active_clauses(gid: str) -> list[BuildWhereClause]:
        # A group inherits every condition injected at or above it in the
        # group graph, so downstream sources are filtered accordingly.
        active: list[BuildWhereClause] = []
        ancestors = nx.ancestors(group_graph, gid) | {gid}
        for anc in ancestors:
            if anc == FINAL_NODE_ID:
                continue
            for clause in group_graph.nodes[anc].get("condition_objects", []) or []:
                if clause not in active:
                    active.append(clause)
        return active

    for gid in order:
        if gid == FINAL_NODE_ID:
            continue
        data = group_graph.nodes[gid]
        derivation = data.get("derivation")
        primary_addrs = data.get("primary_members", ())
        primaries = [
            environment.concepts[a] for a in primary_addrs if a in environment.concepts
        ]
        if not primaries:
            continue
        injected = _combine_clauses(_active_clauses(gid))
        cb = _make_source_callback(gid)
        node: StrategyNode | None = None
        try:
            if derivation == Derivation.ROOT.value:
                # Try single-datasource select first; otherwise delegate to
                # the v3 search to plan cross-datasource joins (root-level
                # join discovery is still v3's job in this prototype).
                node = history.gen_select_node(
                    primaries, environment, g, depth=0, conditions=injected
                )
                if node is None:
                    node = v3.search_concepts(
                        mandatory_list=primaries,
                        history=history,
                        environment=environment,
                        depth=1,
                        g=g,
                        conditions=injected,
                    )
            elif derivation == Derivation.AGGREGATE.value:
                node = gen_group_node(
                    primaries[0],
                    primaries[1:],
                    environment=environment,
                    g=g,
                    depth=1,
                    source_concepts=cb,
                    history=history,
                    conditions=injected,
                )
            elif derivation == Derivation.WINDOW.value:
                node = gen_window_node(
                    primaries[0],
                    primaries[1:],
                    environment=environment,
                    g=g,
                    depth=1,
                    source_concepts=cb,
                    history=history,
                    conditions=injected,
                    is_window=True,
                )
            elif derivation == Derivation.BASIC.value:
                node = gen_basic_node(
                    primaries[0],
                    primaries[1:],
                    environment=environment,
                    g=g,
                    depth=1,
                    source_concepts=cb,
                    history=history,
                    conditions=injected,
                )
            else:
                logger.info(
                    f"[v4] no walker handler yet for derivation {derivation}; "
                    f"skipping group {gid}"
                )
        except Exception as exc:
            logger.info(f"[v4] factory failed for group {gid}: {exc}")
            node = None
        logger.info(
            f"[v4] built {gid} derivation={derivation} "
            f"primaries={[p.address for p in primaries]} "
            f"-> {type(node).__name__ if node else None}"
        )
        if node is not None:
            built[gid] = node

    if not built:
        return None
    # Pick the most-downstream built group as the result; if multiple leaves,
    # prefer the one whose primary members cover the most mandatory concepts.
    mandatory_addrs = {c.address for c in mandatory_list}
    leaves = [
        gid
        for gid in built
        if not any(s in built for s in group_graph.successors(gid))
    ]
    if not leaves:
        leaves = list(built)
    leaves.sort(
        key=lambda gid: len(mandatory_addrs & _members_of(gid)), reverse=True
    )
    return built[leaves[0]]


def _search_concepts(
    mandatory_list: List[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    history: History,
    conditions: list[BuildWhereClause],
    accept_partial: bool = False,
) -> BuildInfo:
    concept_graph = _build_concept_graph(mandatory_list, environment, conditions)
    group_graph = _build_group_graph(concept_graph, conditions)
    strategy_node = _build_strategy_node(
        group_graph, mandatory_list, environment, g, history
    )
    return BuildInfo(
        concept_graph=concept_graph,
        group_graph=group_graph,
        strategy_node=strategy_node,
    )



def search_concepts(
    mandatory_list: List[BuildConcept],
    history: History,
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    accept_partial: bool = False,
    conditions: list[BuildWhereClause] | None = None,
) -> BuildInfo:
    conditions = conditions or []
    hist = history.get_history(
        search=mandatory_list, accept_partial=accept_partial, conditions=conditions
    )
    if hist is not False:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Returning search node from history ({'exists' if hist is not None else 'does not exist'}) for {[c.address for c in mandatory_list]} with accept_partial {accept_partial}"
        )
        assert not isinstance(hist, bool)
        return hist

    result = _search_concepts(
        mandatory_list,
        environment,
        depth=depth,
        g=g,
        accept_partial=accept_partial,
        history=history,
        conditions=conditions,
    )
    # a node may be mutated after be cached; always store a copy
    history.search_to_history(
        mandatory_list,
        accept_partial,
        result.copy() if result else None,
        conditions=conditions,
    )
    return result
