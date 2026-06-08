"""v4 root datasource planning.

This replaces the old ROOT hand-off to v3 discovery for the case where a set
of sourced concepts needs connector concepts added before datasource components
can be merged. The connector search still reuses the graph Steiner helper for
now, but component sourcing and final assembly stay in the v4 root planner.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import cast

from trilogy.core import graph as nx
from trilogy.core.enums import Granularity
from trilogy.core.graph_models import (
    ReferenceGraph,
    concept_to_node,
    prune_sources_for_conditions,
)
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildUnionDatasource,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import merge_conditions
from trilogy.core.processing.node_generators.node_merge_node import (
    AMBIGUITY_CHECK_LIMIT,
    detect_ambiguity_and_raise,
    determine_induced_minimal_nodes,
    extract_concept,
)
from trilogy.core.processing.node_generators.select_helpers.datasource_injection import (
    get_union_sources,
)
from trilogy.core.processing.node_generators.select_helpers.datasource_nodes import (
    create_select_node_candidate,
    finalize_select_node,
)
from trilogy.core.processing.nodes import History, MergeNode, SelectNode, StrategyNode
from trilogy.core.processing.v4_helper.source_policy import (
    STRICT_SOURCE_POLICY,
    SourceAttempt,
    SourcePolicy,
)
from trilogy.utility import unique


@dataclass(frozen=True)
class SourceRequest:
    outputs: list[BuildConcept]
    environment: BuildEnvironment
    graph: ReferenceGraph
    history: History
    conditions: BuildWhereClause | None = None
    depth: int = 0
    source_policy: SourcePolicy = STRICT_SOURCE_POLICY


@dataclass(frozen=True)
class BridgePlan:
    concepts: list[BuildConcept]
    graph: ReferenceGraph


def _concept_node_address(node: str) -> str:
    return node.split("~", maxsplit=1)[1].split("@", maxsplit=1)[0]


def _concept_node_grain_addresses(node: str) -> set[str]:
    marker = "@Grain<"
    if marker not in node:
        return set()
    grain = node.split(marker, maxsplit=1)[1].rsplit(">", maxsplit=1)[0]
    return {address for address in grain.split(",") if address}


def _concepts_in_graph(
    graph: ReferenceGraph, environment: BuildEnvironment
) -> list[BuildConcept]:
    return [
        extract_concept(_concept_node_address(node), environment)
        for node in graph.nodes
        if node.startswith("c~")
    ]


def _condition_row_concepts(
    conditions: BuildWhereClause | None,
) -> list[BuildConcept]:
    if conditions is None:
        return []
    return unique(list(conditions.row_arguments), "address")


def _requested_concepts(request: SourceRequest) -> list[BuildConcept]:
    return unique(
        request.outputs + _condition_row_concepts(request.conditions),
        "address",
    )


def _concepts_with_grain_keys(
    concepts: list[BuildConcept],
    environment: BuildEnvironment,
) -> list[BuildConcept]:
    expanded: list[BuildConcept] = []
    for concept in concepts:
        expanded.append(concept)
        expanded.extend(
            environment.concepts[address]
            for address in concept.grain.components
            if address in environment.concepts
        )
    return unique(expanded, "address")


def _direct_source(
    request: SourceRequest, attempt: SourceAttempt
) -> StrategyNode | None:
    outputs = unique(
        request.outputs + _condition_row_concepts(request.conditions),
        "address",
    )
    node = request.history.gen_select_node(
        outputs,
        request.environment,
        request.graph,
        request.depth,
        fail_if_not_found=False,
        conditions=request.conditions,
        accept_partial=attempt.accepts_partial,
    )
    if node is None or {c.address for c in outputs} == {
        c.address for c in request.outputs
    }:
        return node
    return SelectNode(
        output_concepts=request.outputs,
        input_concepts=node.output_concepts,
        environment=request.environment,
        parents=[node],
    )


def _search_concepts_for_bridge(request: SourceRequest) -> list[BuildConcept]:
    return _concepts_with_grain_keys(_requested_concepts(request), request.environment)


def _bridge_plan(request: SourceRequest, attempt: SourceAttempt) -> BridgePlan | None:
    search_concepts = _search_concepts_for_bridge(request)
    requested = {c.address for c in _requested_concepts(request)}
    condition_options = (request.conditions, None) if request.conditions else (None,)
    for filter_downstream in (True, False):
        for search_conditions in condition_options:
            graph = _resolve_bridge_graph(
                search_concepts,
                request,
                attempt=attempt,
                filter_downstream=filter_downstream,
                search_conditions=search_conditions,
            )
            if graph is None:
                continue
            bridged = unique(_concepts_in_graph(graph, request.environment), "address")
            # The bridge is only worth preferring over `_direct_source` when it
            # ADDED connector concepts (a superset of what was requested) or a
            # union. A bridged set that is a strict *subset* of `requested` is
            # not a connector — it just means single-row/abstract concepts were
            # excluded from the bridge search (`_resolve_bridge_graph`). Treating
            # that subset as a connector would route the remaining concepts
            # through a single partial datasource, dropping the complete-key
            # merge `_direct_source` would otherwise build (refresh of a
            # count-by-X persist whose output also carries a `data_through`).
            if ({c.address for c in bridged} - requested) or _has_union_datasource(
                graph
            ):
                return BridgePlan(concepts=bridged, graph=graph)
    return None


def _has_union_datasource(graph: ReferenceGraph) -> bool:
    return any(
        node in graph and isinstance(datasource, BuildUnionDatasource)
        for node, datasource in graph.datasources.items()
    )


def _resolve_bridge_graph(
    concepts: list[BuildConcept],
    request: SourceRequest,
    *,
    attempt: SourceAttempt,
    filter_downstream: bool,
    search_conditions: BuildWhereClause | None,
) -> ReferenceGraph | None:
    search_graph = request.graph.copy()
    _inject_union_datasources(search_graph, concepts, request.environment)
    prune_sources_for_conditions(
        search_graph,
        attempt.criteria,
        conditions=search_conditions,
    )
    # Single-row / abstract-grain concepts (a `<*>` watermark, a constant) join
    # by cross product, never by a key, so they must not drive the connectivity
    # search — otherwise the Steiner walk invents a spurious intermediate join
    # key to attach them and two equal candidates raise a false ambiguity
    # (refresh of a count-by-X persist with a shared `data_through`). They fall
    # through to `_direct_source`, which sources constants as a cross-join.
    bridge_concepts = [
        concept for concept in concepts if concept.granularity != Granularity.SINGLE_ROW
    ]
    node_list = sorted(
        concept_to_node(concept.with_default_grain())
        for concept in bridge_concepts
        if "__preql_internal" not in concept.address
    )
    synonyms: dict[str, str] = {}
    for concept in bridge_concepts:
        for pseudonym in concept.pseudonyms:
            synonyms[pseudonym] = concept.address

    found: list[ReferenceGraph] = []
    reduced_concept_sets: list[set[str]] = []
    requested_addresses = {concept.address for concept in bridge_concepts}
    for _ in range(AMBIGUITY_CHECK_LIMIT):
        try:
            graph = determine_induced_minimal_nodes(
                search_graph,
                node_list,
                filter_downstream=filter_downstream,
                accept_partial=attempt.accepts_partial,
                environment=request.environment,
                synonyms=synonyms,
            )
        except nx.exception.NetworkXNoPath:
            break
        if graph is None or not graph.nodes or not nx.is_weakly_connected(graph):
            break
        bridge_graph = cast(ReferenceGraph, graph)
        all_graph_concepts = _concepts_in_graph(bridge_graph, request.environment)
        new = [
            concept
            for concept in all_graph_concepts
            if concept.address not in requested_addresses
        ]
        found.append(bridge_graph)
        reduced_concept_sets.append(
            {concept.address for concept in new if concept.address not in synonyms}
        )
        if not new:
            break
        for concept in new:
            node = concept_to_node(concept)
            if node in search_graph:
                search_graph.remove_node(node)
    if not found:
        return None
    detect_ambiguity_and_raise(
        bridge_concepts, reduced_concept_sets, request.environment
    )
    return found[0]


def _inject_union_datasources(
    graph: ReferenceGraph,
    concepts: list[BuildConcept],
    environment: BuildEnvironment,
) -> None:
    datasources = [
        datasource
        for datasource in environment.datasources.values()
        if isinstance(datasource, BuildDatasource)
    ]
    union_edges: list[tuple[str, str]] = []
    for datasource_group in get_union_sources(datasources, concepts):
        node_address = "ds~" + "-".join(
            [datasource.name for datasource in datasource_group]
        )
        if node_address in graph.datasources:
            continue
        merged_condition = merge_conditions(
            [
                datasource.non_partial_for.conditional
                for datasource in datasource_group
                if datasource.non_partial_for is not None
            ]
        )
        non_partial_for = (
            BuildWhereClause(conditional=merged_condition)
            if merged_condition is not None
            else None
        )
        graph.datasources[node_address] = BuildUnionDatasource(
            children=datasource_group,
            non_partial_for=non_partial_for,
        )
        common_outputs = set(datasource_group[0].output_concepts)
        for datasource in datasource_group[1:]:
            common_outputs &= set(datasource.output_concepts)
        for concept in common_outputs:
            concept_node = concept_to_node(concept)
            union_edges.append((node_address, concept_node))
            union_edges.append((concept_node, node_address))
    graph.add_edges_from(union_edges)


def _datasource_nodes_for_bridge(
    request: SourceRequest,
    plan: BridgePlan,
    attempt: SourceAttempt,
) -> list[StrategyNode] | None:
    parents: list[StrategyNode] = []
    bridge_addresses = {concept.address for concept in plan.concepts}
    for ds_node in sorted(node for node in plan.graph.datasources):
        concept_nodes = _local_concept_nodes_for_datasource(
            plan.graph,
            ds_node,
            bridge_addresses,
            request.environment,
        )
        concept_nodes.extend(
            _original_datasource_concept_nodes(
                request.graph,
                plan.graph,
                ds_node,
                bridge_addresses,
                request.environment,
            )
        )
        concept_nodes.extend(
            _datasource_grain_concept_nodes(
                plan.graph,
                ds_node,
                concept_nodes,
                request.environment,
            )
        )
        concept_nodes = sorted(set(concept_nodes))
        if not concept_nodes:
            continue
        candidate = create_select_node_candidate(
            ds_node,
            concept_nodes,
            accept_partial=attempt.accepts_partial,
            g=plan.graph,
            environment=request.environment,
            depth=request.depth + 1,
            conditions=None,
        )
        parents.append(
            finalize_select_node(
                candidate,
                environment=request.environment,
                depth=request.depth + 1,
            )
        )
    parents.extend(_derived_connector_nodes(request, plan, parents))
    if not parents:
        return None
    return parents


def _derived_connector_nodes(
    request: SourceRequest,
    plan: BridgePlan,
    datasource_parents: list[StrategyNode],
) -> list[StrategyNode]:
    """Materialize bridge concepts whose source is a *derived* connector.

    The bridge can route through a merged derivation (e.g. a recursive
    `recurse_edge` whose output was `merge`d into a dimension key) that is not a
    real datasource — its real lineage lives in `alias_origin_lookup`, keyed by
    the concept's address or any pseudonym, while `environment.concepts` holds a
    demoted lineage-less ROOT. Such a connector is dropped by the `ds~`-only
    loop above, leaving the concept it provides unsourced (INVALID_REFERENCE).
    Plan each needed connector's true origin and hand it back as a parent;
    `_merge_component_sources` joins it on the pseudonym equivalence.
    """
    covered = {
        c.address for parent in datasource_parents for c in parent.usable_outputs
    }
    if {c.address for c in plan.concepts} <= covered:
        return []
    # Imported lazily: `concept_strategies_v4` imports this module's package.
    from trilogy.core.processing.concept_strategies_v4 import (
        V4History,
        search_concepts,
    )

    env = request.environment
    history = cast(V4History, request.history)
    planned: set[str] = set()
    connectors: list[StrategyNode] = []
    for concept in plan.concepts:
        for alias in (concept.address, *concept.pseudonyms):
            origin = env.alias_origin_lookup.get(alias)
            # Skip non-derived origins, anything a datasource already sources,
            # and any connector currently mid-plan. The last is the re-entry
            # guard: planning a connector recurses to source its own inputs,
            # whose bridge re-routes through the same connector — without the
            # guard that re-injects forever.
            if (
                origin is None
                or origin.lineage is None
                or origin.address in planned
                or origin.address in covered
                or origin.address in history.connectors_in_progress
            ):
                continue
            # Carry the connector's grain keys it still owes the bridge (e.g. a
            # recursion keyed by `id` must emit `id`, not group it away) so the
            # merge has the columns the consumer reads, not just the join key.
            mandatory = unique(
                [origin]
                + [
                    env.concepts[address]
                    for address in origin.grain.components
                    if address in env.concepts and address not in covered
                ],
                "address",
            )
            history.connectors_in_progress.add(origin.address)
            try:
                info = search_concepts(
                    mandatory_list=mandatory,
                    history=history,
                    environment=env,
                    depth=request.depth + 1,
                    g=request.graph,
                    conditions=[],
                    source_policy=request.source_policy,
                )
            finally:
                history.connectors_in_progress.discard(origin.address)
            if info.strategy_node is not None:
                planned.add(origin.address)
                connectors.append(info.strategy_node)
    return connectors


def _datasource_grain_concept_nodes(
    graph: ReferenceGraph,
    ds_node: str,
    concept_nodes: list[str],
    environment: BuildEnvironment,
) -> list[str]:
    selected_addresses = {
        _concept_node_address(node) for node in concept_nodes if node.startswith("c~")
    }
    datasource = graph.datasources.get(ds_node)
    if datasource is None:
        return []
    grain_addresses: set[str] = set()
    for node in concept_nodes:
        if node.startswith("c~") and isinstance(datasource, BuildUnionDatasource):
            grain_addresses.update(_concept_node_grain_addresses(node))
    for address in selected_addresses:
        concept = environment.concepts.get(address)
        if concept is not None:
            grain_addresses.update(concept.grain.components)
    if not grain_addresses or ds_node not in graph:
        return []
    nodes = [
        neighbor
        for neighbor in graph.neighbors(ds_node)
        if neighbor.startswith("c~")
        and _concept_node_address(neighbor) in grain_addresses
    ]
    node_addresses = {_concept_node_address(node) for node in nodes}
    for address in sorted(grain_addresses - node_addresses):
        concept = environment.concepts.get(address)
        if concept is None or not _datasource_can_output(datasource, address):
            continue
        nodes.append(concept_to_node(concept))
    return nodes


def _datasource_can_output(
    datasource: BuildDatasource | BuildUnionDatasource,
    address: str,
) -> bool:
    if isinstance(datasource, BuildDatasource):
        return any(concept.address == address for concept in datasource.output_concepts)
    return all(
        any(concept.address == address for concept in child.output_concepts)
        for child in datasource.children
    )


def _original_datasource_concept_nodes(
    source_graph: ReferenceGraph,
    bridge_graph: ReferenceGraph,
    ds_node: str,
    bridge_addresses: set[str],
    environment: BuildEnvironment,
) -> list[str]:
    concept_nodes: list[str] = []
    if ds_node not in source_graph:
        return concept_nodes
    for neighbor in source_graph.neighbors(ds_node):
        if not neighbor.startswith("c~"):
            continue
        address = _concept_node_address(neighbor)
        if address not in bridge_addresses or address not in environment.concepts:
            continue
        if neighbor not in bridge_graph:
            bridge_graph.add_node(neighbor)
            if neighbor in source_graph.concepts:
                bridge_graph.concepts[neighbor] = source_graph.concepts[neighbor]
        if not bridge_graph.has_edge(ds_node, neighbor):
            bridge_graph.add_edge(ds_node, neighbor)
        concept_nodes.append(neighbor)
    return concept_nodes


def _local_concept_nodes_for_datasource(
    graph: ReferenceGraph,
    ds_node: str,
    bridge_addresses: set[str],
    environment: BuildEnvironment,
) -> list[str]:
    queue: deque[str] = deque([ds_node])
    seen: set[str] = {ds_node}
    concepts: dict[str, str] = {}
    while queue:
        node = queue.popleft()
        for neighbor in graph.neighbors(node):
            if neighbor in seen:
                continue
            if neighbor.startswith("ds~"):
                continue
            seen.add(neighbor)
            if not neighbor.startswith("c~"):
                continue
            address = _concept_node_address(neighbor)
            if (
                address in environment.canonical_concepts
                and address in bridge_addresses
            ):
                concepts.setdefault(address, neighbor)
            queue.append(neighbor)
    return sorted(concepts.values())


def _merge_component_sources(
    request: SourceRequest,
    parents: list[StrategyNode],
    output_concepts: list[BuildConcept] | None = None,
) -> StrategyNode | None:
    if not parents:
        return None
    outputs = output_concepts or request.outputs
    inputs = unique(
        [concept for parent in parents for concept in parent.usable_outputs],
        "address",
    )
    if len(parents) == 1:
        parent = parents[0]
        if request.conditions is None and {
            c.address for c in parent.output_concepts
        } == {c.address for c in outputs}:
            return parent
        return SelectNode(
            output_concepts=outputs,
            input_concepts=inputs,
            environment=request.environment,
            parents=[parent],
            conditions=(
                request.conditions.conditional
                if request.conditions is not None
                else None
            ),
        )
    return MergeNode(
        input_concepts=inputs,
        output_concepts=outputs,
        environment=request.environment,
        parents=parents,
        depth=request.depth,
        conditions=(
            request.conditions.conditional if request.conditions is not None else None
        ),
    )


def _bridge_parents_cover(parents: list[StrategyNode], required: set[str]) -> bool:
    """Every requested concept is actually emitted by some bridge datasource.

    The graph Steiner solver can route a requested root concept through a
    *derived* concept's lineage (a window/aggregate output that shares a key
    with it), yielding a bridge whose datasource set never sources the concept
    — e.g. `country` (on `users`) "reached" from `posts` via a window keyed by
    `user_id`. `_merge_component_sources` would then promise `country` off a
    posts-only scan and render INVALID_REFERENCE. Reject such a bridge so
    `_direct_source` (which merges both datasources) is used instead."""
    available = {c.address for parent in parents for c in parent.usable_outputs}
    return required <= available


def plan_source(request: SourceRequest) -> StrategyNode | None:
    """Source ROOT-level concepts through one v4 path.

    First try the ordinary datasource component planner. If the requested roots
    are split but the graph can prove connector concepts, source each expanded
    component directly and merge them under a v4 node.
    """
    requested_addresses = {c.address for c in _requested_concepts(request)}
    for attempt in request.source_policy.attempts:
        bridge_plan = _bridge_plan(request, attempt)
        if bridge_plan is not None:
            parents = _datasource_nodes_for_bridge(request, bridge_plan, attempt)
            if parents is not None and (
                attempt.accepts_partial
                or _bridge_parents_cover(parents, requested_addresses)
            ):
                return _merge_component_sources(request, parents, bridge_plan.concepts)
        direct = _direct_source(request, attempt)
        if direct is not None:
            return direct
    if request.conditions is not None:
        outputs = unique(
            request.outputs + _condition_row_concepts(request.conditions),
            "address",
        )
        unfiltered = plan_source(
            SourceRequest(
                outputs=outputs,
                environment=request.environment,
                graph=request.graph,
                history=request.history,
                conditions=None,
                depth=request.depth,
                source_policy=request.source_policy,
            )
        )
        if unfiltered is not None:
            return SelectNode(
                output_concepts=request.outputs,
                input_concepts=unfiltered.output_concepts,
                environment=request.environment,
                parents=[unfiltered],
                conditions=request.conditions.conditional,
            )
    return None
