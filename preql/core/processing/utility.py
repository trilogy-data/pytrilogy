from typing import List, Optional, Tuple, Dict, TypedDict, Set
import networkx as nx
from preql.core.graph_models import ReferenceGraph
from preql.core.models import Datasource, JoinType, BaseJoin, Concept, QueryDatasource
from preql.core.enums import Purpose
from enum import Enum
from preql.utility import unique
from collections import defaultdict


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
    """Calculate the relevance of each node in a graph
    Relevance is used to prune irrelevant nodes from the graph
    """
    relevance = 0
    for node in g.nodes:
        if node not in subset_nodes:
            continue
        if not g.nodes[node]["type"] == NodeType.CONCEPT:
            continue
        concept = [x for x in concepts if x.address == node].pop()
        if concept.purpose == Purpose.CONSTANT:
            continue
        # if it's an aggregate up to an arbitrary grain, it can be joined in later
        # and can be ignored in subgraph
        if concept.purpose == Purpose.METRIC:
            if not concept.grain:
                continue
            if len(concept.grain.components) == 0:
                continue
        if concept.grain and len(concept.grain.components) > 0:
            relevance += 1
            continue
        # Added 2023-10-18 since we seemed to be strangely dropping things
        relevance += 1

    return relevance


def get_node_joins(
    datasources: List[QueryDatasource],
    # concepts:List[Concept],
) -> List[BaseJoin]:
    """Find if any of the datasources are not linked"""
    import networkx as nx

    graph = nx.Graph()
    concepts: List[Concept] = []
    for datasource in datasources:
        graph.add_node(datasource.identifier, type=NodeType.NODE)
        for concept in datasource.output_concepts:
            concepts.append(concept)
            graph.add_node(concept.address, type=NodeType.CONCEPT)
            graph.add_edge(datasource.identifier, concept.address)

    # add edges for every constant to every datasource
    for datasource in datasources:
        for concept in datasource.output_concepts:
            if concept.purpose == Purpose.CONSTANT:
                for node in graph.nodes:
                    if graph.nodes[node]["type"] == NodeType.NODE:
                        graph.add_edge(node, concept.address)

    joins: defaultdict[str, set] = defaultdict(set)
    identifier_map = {x.identifier: x for x in datasources}

    node_list = sorted(
        [x for x in graph.nodes if graph.nodes[x]["type"] == NodeType.NODE],
        key=lambda x: len(identifier_map[x].partial_concepts),
    )
    for left in node_list:
        for cnode in graph.neighbors(left):
            if graph.nodes[cnode]["type"] == NodeType.CONCEPT:
                for right in graph.neighbors(cnode):
                    # skip concepts
                    if graph.nodes[right]["type"] == NodeType.CONCEPT:
                        continue
                    if left == right:
                        continue
                    identifier = [left, right]
                    joins["-".join(identifier)].add(cnode)

    final_joins_pre: List[BaseJoin] = []

    for key, join_concepts in joins.items():
        left, right = key.split("-")
        local_concepts: List[Concept] = unique(
            [c for c in concepts if c.address in join_concepts], "address"
        )
        if all([c.purpose == Purpose.CONSTANT for c in local_concepts]):
            join_type = JoinType.FULL
            local_concepts = []
        else:
            join_type = JoinType.LEFT_OUTER
        final_joins_pre.append(
            BaseJoin(
                left_datasource=identifier_map[left],
                right_datasource=identifier_map[right],
                join_type=join_type,
                concepts=local_concepts,
            )
        )
    final_joins: List[BaseJoin] = []
    available_aliases: set[str] = set()
    while final_joins_pre:
        new_final_joins_pre: List[BaseJoin] = []
        for join in final_joins_pre:
            if not available_aliases:
                final_joins.append(join)
                available_aliases.add(join.left_datasource.identifier)
                available_aliases.add(join.right_datasource.identifier)
            elif join.left_datasource.identifier in available_aliases:
                # we don't need to join twice
                # so whatever join we found first, works
                if join.right_datasource.identifier in available_aliases:
                    continue
                final_joins.append(join)
                available_aliases.add(join.left_datasource.identifier)
                available_aliases.add(join.right_datasource.identifier)
            else:
                new_final_joins_pre.append(join)
        if len(new_final_joins_pre) == len(final_joins_pre):
            remaining = [
                join.left_datasource.identifier for join in new_final_joins_pre
            ]
            remaining_right = [
                join.right_datasource.identifier for join in new_final_joins_pre
            ]
            raise SyntaxError(
                f"did not find any new joins, available {available_aliases} remaining is {remaining + remaining_right} "
            )
        final_joins_pre = new_final_joins_pre

    # this is extra validation
    if len(datasources) > 1:
        for x in datasources:
            found = False
            for join in final_joins:
                if (
                    join.left_datasource.identifier == x.identifier
                    or join.right_datasource.identifier == x.identifier
                ):
                    found = True
            if not found:
                raise SyntaxError(
                    f"Could not find join for {x.identifier}, all {[z.identifier for z in datasources]}"
                )
    return final_joins


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
