from __future__ import annotations

from types import SimpleNamespace
from typing import Iterable, TypeVar

import networkx as nx

NetworkXError = nx.NetworkXError
NetworkXNoPath = nx.NetworkXNoPath
NetworkXUnfeasible = nx.NetworkXUnfeasible
NodeNotFound = nx.NodeNotFound

exception = nx.exception


class _CopyableMixin:
    def _copy_from(self, other) -> None:
        self.clear()
        self.graph = other.graph.copy()
        self.add_nodes_from((node, attrs.copy()) for node, attrs in other.nodes.items())
        self.add_edges_from(
            (left, right, attrs.copy()) for left, right, attrs in other.edges(data=True)
        )


class Graph(_CopyableMixin, nx.Graph):
    pass


class DiGraph(_CopyableMixin, nx.DiGraph):
    pass


GraphT = TypeVar("GraphT", Graph, DiGraph)


approximation = SimpleNamespace(
    steinertree=SimpleNamespace(
        steiner_tree=nx.algorithms.approximation.steinertree.steiner_tree
    )
)


def neighbors(graph: GraphT, node: str):
    return graph.neighbors(node)


def all_neighbors(graph: GraphT, node: str):
    return nx.all_neighbors(graph, node)


def connected_components(graph: GraphT):
    return nx.connected_components(graph)


def isolates(graph: GraphT):
    return nx.isolates(graph)


def topological_sort(graph: GraphT):
    return nx.topological_sort(graph)


def shortest_path(graph: GraphT, source: str, target: str):
    return nx.shortest_path(graph, source, target)


def shortest_path_length(graph: GraphT, source: str, target: str):
    return nx.shortest_path_length(graph, source, target)


def ego_graph(graph: GraphT, center: str, radius: int) -> GraphT:
    return nx.ego_graph(graph, center, radius)


def multi_source_dijkstra_path(
    graph: GraphT,
    sources: Iterable[str],
    weight: str = "weight",
):
    return nx.multi_source_dijkstra_path(graph, sources, weight=weight)


def is_weakly_connected(graph: GraphT) -> bool:
    return nx.is_weakly_connected(graph)


def to_undirected(graph: GraphT) -> nx.Graph:
    return graph.to_undirected()


def subgraph(graph: GraphT, nodes: Iterable[str]) -> GraphT:
    return graph.subgraph(nodes).copy()
