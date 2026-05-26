"""Stage 1: walk every mandatory concept (and condition input) back to its
roots and produce a DAG of concept-level lineage + d1→d0 constraint edges.

For each concept added, an upstream-fetcher (dispatched on
`concept.derivation`) decides what additional concepts must be in the
graph for this concept to actually be sourceable. The default fetcher
returns `lineage.concept_arguments` — the computational dependencies that
also get `kind="lineage"` edges. Specialized fetchers (AGGREGATE, FILTER,
WINDOW, SUBSELECT) add row-identity concepts that aren't visible from the
lineage walk alone: property keys, grain components, partition keys. They
don't get lineage edges — they're structural deps, not parents — but the
grouping pass picks them up and places them in the appropriate group.

This mirrors the per-derivation `resolve_*_parent_concepts` helpers used
by the v3 generators (`gen_group_node`, `gen_filter_node`,
`gen_window_node`, `gen_subselect_node`).
"""

from typing import Callable, List

import networkx as nx

from trilogy.core.enums import Derivation, Purpose
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildFilterItem,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.common import (
    _walk_aggregate_grain_inputs,
)

from .constants import LABEL_DEPTH, ROW_SHAPE_BARRIER_DERIVATIONS


UpstreamFetcher = Callable[[BuildConcept, BuildEnvironment], List[BuildConcept]]


def classify_depth(concept: BuildConcept, condition_addresses: set[str]) -> str:
    """Tag a concept by its placement role in the plan."""
    if concept.address in condition_addresses:
        return "d1"
    if concept.derivation in ROW_SHAPE_BARRIER_DERIVATIONS:
        return "d0"
    return "d*"


def _lineage_args(
    concept: BuildConcept, environment: BuildEnvironment
) -> List[BuildConcept]:
    """The default — concepts the lineage's expression directly consumes."""
    if concept.lineage is None:
        return []
    return [
        environment.concepts.get(p.address, p)
        for p in concept.lineage.concept_arguments
    ]


def _upstream_default(
    concept: BuildConcept, environment: BuildEnvironment
) -> List[BuildConcept]:
    return _lineage_args(concept, environment)


def _upstream_aggregate(
    concept: BuildConcept, environment: BuildEnvironment
) -> List[BuildConcept]:
    """AGGREGATE: lineage args plus row-identity concepts of each function
    arg (property keys, rowset grain). Stops at inner aggregate boundaries
    via `_walk_aggregate_grain_inputs`."""
    base = list(_lineage_args(concept, environment))
    if isinstance(concept.lineage, BuildAggregateWrapper):
        for arg in concept.lineage.function.arguments:
            if isinstance(arg, BuildConcept):
                base.extend(_walk_aggregate_grain_inputs(arg, environment))
    return base


def _upstream_filter(
    concept: BuildConcept, environment: BuildEnvironment
) -> List[BuildConcept]:
    """FILTER: lineage args plus property keys of the filtered concept
    (matches `resolve_filter_parent_concepts`). A filter over a property
    needs the property's keys to keep the row stream identifiable."""
    base = list(_lineage_args(concept, environment))
    if isinstance(concept.lineage, BuildFilterItem):
        direct_parent = concept.lineage.content
        if (
            isinstance(direct_parent, BuildConcept)
            and direct_parent.purpose in (Purpose.PROPERTY, Purpose.METRIC)
            and direct_parent.keys
        ):
            base += [
                environment.concepts[k]
                for k in direct_parent.keys
                if k in environment.concepts
            ]
    return base


def _grain_and_keys(
    concept: BuildConcept, environment: BuildEnvironment
) -> List[BuildConcept]:
    extras: list[BuildConcept] = []
    if concept.grain:
        for g in concept.grain.components:
            if g in environment.concepts:
                extras.append(environment.concepts[g])
    if concept.keys:
        for k in concept.keys:
            if k in environment.concepts:
                extras.append(environment.concepts[k])
    return extras


def _upstream_window(
    concept: BuildConcept, environment: BuildEnvironment
) -> List[BuildConcept]:
    """WINDOW: lineage args plus grain components and keys (matches
    `resolve_window_parent_concepts`)."""
    return list(_lineage_args(concept, environment)) + _grain_and_keys(
        concept, environment
    )


def _upstream_subselect(
    concept: BuildConcept, environment: BuildEnvironment
) -> List[BuildConcept]:
    """SUBSELECT: lineage args plus grain components (matches
    `resolve_subselect_parent_concepts`)."""
    base = list(_lineage_args(concept, environment))
    if concept.grain:
        for g in concept.grain.components:
            if g in environment.concepts:
                base.append(environment.concepts[g])
    return base


_UPSTREAM: dict[Derivation, UpstreamFetcher] = {
    Derivation.AGGREGATE: _upstream_aggregate,
    Derivation.FILTER: _upstream_filter,
    Derivation.WINDOW: _upstream_window,
    Derivation.SUBSELECT: _upstream_subselect,
}


def _add_concept(
    concept: BuildConcept,
    environment: BuildEnvironment,
    graph: nx.DiGraph,
    condition_addresses: set[str],
) -> None:
    if concept.address in graph:
        return
    graph.add_node(
        concept.address,
        derivation=concept.derivation.value,
        purpose=concept.purpose.value,
        granularity=concept.granularity.value,
        depth_label=classify_depth(concept, condition_addresses),
        grain_components=(
            frozenset(concept.grain.components) if concept.grain else frozenset()
        ),
    )

    # Per-derivation upstream fetcher (see `_UPSTREAM`): walks both the
    # lineage args (computational parents) and the row-identity deps the
    # lineage walk alone misses (property keys for filters and aggregates,
    # grain components and partition keys for windows and subselects).
    # Lineage edges below come only from `concept_arguments`; structural
    # extras land in the graph without edges and are placed by the
    # grouping pass.
    fetcher = _UPSTREAM.get(concept.derivation, _upstream_default)
    for upstream in fetcher(concept, environment):
        _add_concept(upstream, environment, graph, condition_addresses)
    if concept.lineage is None:
        return
    for parent in concept.lineage.concept_arguments:
        resolved = environment.concepts.get(parent.address, parent)
        graph.add_edge(resolved.address, concept.address, kind="lineage")


def build_concept_graph(
    mandatory_list: List[BuildConcept],
    environment: BuildEnvironment,
    conditions: list[BuildWhereClause],
) -> nx.DiGraph:
    """Build the concept-level DAG. Constraint edges (d1→d0) record the
    invariant that filter inputs must be available above any row-shape barrier
    that consumes their filtered output."""
    graph: nx.DiGraph = nx.DiGraph()
    condition_addresses = {
        c.address for clause in conditions for c in clause.concept_arguments
    }
    for concept in mandatory_list:
        _add_concept(concept, environment, graph, condition_addresses)
    for clause in conditions:
        for concept in clause.concept_arguments:
            resolved = environment.concepts.get(concept.address, concept)
            _add_concept(resolved, environment, graph, condition_addresses)

    # Add a constraint edge from every d1 to every d0 — except when a
    # lineage edge already connects them. NetworkX merges duplicate edges
    # (last write wins), so overwriting would silently demote the lineage
    # relationship to "constraint only" and break downstream group-graph
    # construction (the agg group would lose its by-clause inputs because
    # those happen to be d1 conditions).
    d1_nodes = [n for n, d in graph.nodes(data=True) if d.get(LABEL_DEPTH) == "d1"]
    d0_nodes = [n for n, d in graph.nodes(data=True) if d.get(LABEL_DEPTH) == "d0"]
    for src in d1_nodes:
        for dst in d0_nodes:
            if graph.has_edge(src, dst):
                graph.edges[src, dst]["is_constraint"] = True
            else:
                graph.add_edge(src, dst, kind="constraint", is_constraint=True)
    return graph
