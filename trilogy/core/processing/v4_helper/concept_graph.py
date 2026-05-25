"""Stage 1: walk every mandatory concept (and condition input) back to its
roots and produce a DAG of concept-level lineage + d1→d0 constraint edges."""

from typing import List

import networkx as nx

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment

from .constants import ROW_SHAPE_BARRIER_DERIVATIONS


def classify_depth(concept: BuildConcept, condition_addresses: set[str]) -> str:
    """Tag a concept by its placement role in the plan."""
    if concept.address in condition_addresses:
        return "d1"
    if concept.derivation in ROW_SHAPE_BARRIER_DERIVATIONS:
        return "d0"
    return "d*"


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
    if concept.lineage is None:
        return
    for parent in concept.lineage.concept_arguments:
        resolved = environment.concepts.get(parent.address, parent)
        _add_concept(resolved, environment, graph, condition_addresses)
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

    d1_nodes = [n for n, d in graph.nodes(data=True) if d.get("depth_label") == "d1"]
    d0_nodes = [n for n, d in graph.nodes(data=True) if d.get("depth_label") == "d0"]
    for src in d1_nodes:
        for dst in d0_nodes:
            graph.add_edge(src, dst, kind="constraint")
    return graph
