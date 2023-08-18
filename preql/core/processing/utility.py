from typing import List, Optional, Tuple, Dict, TypedDict, Set
import networkx as nx
from preql.core.graph_models import ReferenceGraph
from preql.core.models import Datasource, JoinType, BaseJoin, Concept
from preql.core.enums import Purpose
from enum import Enum


class NodeType(Enum):
    CONCEPT = 1
    NODE = 2


class PathInfo(TypedDict):
    paths: Dict[str, List[str]]
    datasource: Datasource


def path_to_joins(input: List[str], g: ReferenceGraph) -> List[BaseJoin]:
    """Build joins and ensure any required CTEs are also created/tracked"""
    out = []
    zipped = parse_path_to_matches(input)
    for row in zipped:
        left_ds, right_ds, raw_concepts = row
        concepts = [g.nodes[concept]["concept"] for concept in raw_concepts]
        left_value = g.nodes[left_ds]["datasource"]
        if not right_ds:
            continue
        right_value = g.nodes[right_ds]["datasource"]
        out.append(
            BaseJoin(
                left_datasource=left_value,
                right_datasource=right_value,
                join_type=JoinType.LEFT_OUTER,
                concepts=concepts,
            )
        )
    return out


def parse_path_to_matches(
    input: List[str],
) -> List[Tuple[Optional[str], Optional[str], List[str]]]:
    """Parse a networkx path to a set of join relations"""
    left_ds = None
    right_ds = None
    concept = None
    output: List[Tuple[Optional[str], Optional[str], List[str]]] = []
    while input:
        ds = None
        next = input.pop(0)
        if next.startswith("ds~"):
            ds = next
        elif next.startswith("c~"):
            concept = next
        if ds and not left_ds:
            left_ds = ds
            continue
        elif ds and concept:
            right_ds = ds
            output.append((left_ds, right_ds, [concept]))
            left_ds = right_ds
            concept = None
    if left_ds and concept and not right_ds:
        output.append((left_ds, None, [concept]))
    return output


def calculate_graph_relevance(
    g: nx.DiGraph, subset_nodes: set[str], concepts: set[Concept]
) -> int:
    """Calculate the relevance of each node in a graph"""
    relevance = 0
    for node in g.nodes:
        if node not in subset_nodes:
            continue
        if not g.nodes[node]["type"] == NodeType.CONCEPT:
            continue
        concept = [x for x in concepts if x.address == node].pop()
        if concept.purpose == Purpose.CONSTANT:
            continue
        if concept.grain and len(concept.grain.components) > 0:
            relevance += 1
    return relevance


def get_disconnected_components(
    concept_map: Dict[str, Set[Concept]]
) -> Tuple[int, List]:
    """Find if any of the datasources are not linked"""
    import networkx as nx

    graph = nx.Graph()
    all_concepts = set()
    for datasource, concepts in concept_map.items():
        graph.add_node(datasource, type=NodeType.NODE)
        for concept in concepts:
            # TODO: determine if this is the right way to handle things
            # if concept.derivation in (PurposeLineage.FILTER, PurposeLineage.WINDOW):
            #     if isinstance(concept.lineage, FilterItem):
            #         graph.add_node(concept.lineage.content.address, type=NodeType.CONCEPT)
            #         graph.add_edge(datasource, concept.lineage.content.address)
            #     if isinstance(concept.lineage, WindowItem):
            #         graph.add_node(concept.lineage.content.address, type=NodeType.CONCEPT)
            #         graph.add_edge(datasource, concept.lineage.content.address)
            graph.add_node(concept.address, type=NodeType.CONCEPT)
            graph.add_edge(datasource, concept.address)
            all_concepts.add(concept)
    sub_graphs = list(nx.connected_components(graph))
    sub_graphs = [
        x for x in sub_graphs if calculate_graph_relevance(graph, x, all_concepts) > 0
    ]
    return len(sub_graphs), sub_graphs
