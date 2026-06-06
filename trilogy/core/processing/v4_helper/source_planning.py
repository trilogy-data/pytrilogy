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


def _direct_source(
    request: SourceRequest, attempt: SourceAttempt
) -> StrategyNode | None:
    return request.history.gen_select_node(
        request.outputs,
        request.environment,
        request.graph,
        request.depth,
        fail_if_not_found=False,
        conditions=request.conditions,
        accept_partial=attempt.accepts_partial,
    )


def _search_concepts_for_bridge(request: SourceRequest) -> list[BuildConcept]:
    return unique(
        request.outputs + _condition_row_concepts(request.conditions),
        "address",
    )


def _bridge_plan(request: SourceRequest, attempt: SourceAttempt) -> BridgePlan | None:
    search_concepts = _search_concepts_for_bridge(request)
    requested = {c.address for c in search_concepts}
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
            if {c.address for c in bridged} != requested:
                return BridgePlan(concepts=bridged, graph=graph)
    return None


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
    node_list = sorted(
        concept_to_node(concept.with_default_grain())
        for concept in concepts
        if "__preql_internal" not in concept.address
    )
    synonyms: dict[str, str] = {}
    for concept in concepts:
        for pseudonym in concept.pseudonyms:
            synonyms[pseudonym] = concept.address

    found: list[ReferenceGraph] = []
    reduced_concept_sets: list[set[str]] = []
    requested_addresses = {concept.address for concept in concepts}
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
    detect_ambiguity_and_raise(concepts, reduced_concept_sets, request.environment)
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
    if not parents:
        return None
    return parents


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
    request: SourceRequest, parents: list[StrategyNode]
) -> StrategyNode | None:
    if not parents:
        return None
    inputs = unique(
        [concept for parent in parents for concept in parent.usable_outputs],
        "address",
    )
    if len(parents) == 1:
        parent = parents[0]
        if request.conditions is None and {
            c.address for c in parent.output_concepts
        } == {c.address for c in request.outputs}:
            return parent
        return SelectNode(
            output_concepts=request.outputs,
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
        output_concepts=request.outputs,
        environment=request.environment,
        parents=parents,
        depth=request.depth,
        conditions=(
            request.conditions.conditional if request.conditions is not None else None
        ),
    )


def plan_source(request: SourceRequest) -> StrategyNode | None:
    """Source ROOT-level concepts through one v4 path.

    First try the ordinary datasource component planner. If the requested roots
    are split but the graph can prove connector concepts, source each expanded
    component directly and merge them under a v4 node.
    """
    for attempt in request.source_policy.attempts:
        direct = _direct_source(request, attempt)
        if direct is not None:
            return direct

    for attempt in request.source_policy.attempts:
        bridge_plan = _bridge_plan(request, attempt)
        if bridge_plan is None:
            continue
        parents = _datasource_nodes_for_bridge(request, bridge_plan, attempt)
        if parents is None:
            continue
        return _merge_component_sources(request, parents)
    return None
