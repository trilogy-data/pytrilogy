from __future__ import annotations

import os
from types import SimpleNamespace
from typing import Iterable, Iterator, Mapping, MutableMapping, Self, TypeVar, cast

try:
    from _preql_import_resolver import PyGraphCore as _RustGraphCore
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "Rust graph bindings are required but were not found. "
        "Build/install the local extension with `maturin develop` in the project venv."
    ) from exc

_COMPARE_MODE = os.getenv("TRILOGY_GRAPH_COMPARE") == "1"
if _COMPARE_MODE:  # pragma: no cover
    import networkx as _shadow_nx
else:  # pragma: no cover
    _shadow_nx = None


class NetworkXError(RuntimeError):
    pass


class NetworkXNoPath(NetworkXError):
    pass


class NetworkXUnfeasible(NetworkXError):
    pass


class NodeNotFound(NetworkXError):
    pass


exception = SimpleNamespace(
    NetworkXError=NetworkXError,
    NetworkXNoPath=NetworkXNoPath,
    NetworkXUnfeasible=NetworkXUnfeasible,
    NodeNotFound=NodeNotFound,
)


def _coerce_node(node: object) -> str:
    if not isinstance(node, str):
        raise TypeError(f"Rust graph nodes must be strings, got {type(node).__name__}")
    return node


def _edge_key(graph: "_GraphBase", left: str, right: str) -> tuple[str, str]:
    if graph.directed or left <= right:
        return (left, right)
    return (right, left)


def _canonical_edge_set(
    graph: "_GraphBase",
    edges: Iterable[tuple[str, str]],
) -> set[tuple[str, str]]:
    return {_edge_key(graph, left, right) for left, right in edges}


def _copy_edge_attrs(
    graph: "_GraphBase",
) -> dict[tuple[str, str], dict[str, object]]:
    return {edge: attrs.copy() for edge, attrs in graph._edge_attrs.items()}


def _weight_triples(
    graph: "_GraphBase",
    weight: str,
) -> list[tuple[str, str, float]]:
    output: list[tuple[str, str, float]] = []
    for left, right in graph.edges:
        attrs = graph.edges[left, right]
        raw = attrs.get(weight, 1.0)
        output.append(
            (
                left,
                right,
                float(raw) if isinstance(raw, (int, float, str)) else 1.0,
            )
        )
    return output


def _raise_missing(node: str) -> None:
    raise NodeNotFound(f"Node not found: {node}")


GraphT = TypeVar("GraphT", bound="_GraphBase")


class _NodeView(Mapping[str, MutableMapping[str, object]]):
    def __init__(self, graph: "_GraphBase") -> None:
        self._graph = graph

    def __iter__(self) -> Iterator[str]:
        return iter(self._graph._ordered_nodes())

    def __len__(self) -> int:
        return len(self._graph._ordered_nodes())

    def __contains__(self, node: object) -> bool:
        present = isinstance(node, str) and self._graph._core.has_node(node)
        self._graph._assert_shadow("nodes.__contains__", present, node)
        return present

    def __getitem__(self, node: str) -> MutableMapping[str, object]:
        if node not in self:
            _raise_missing(node)
        attrs = self._graph._node_attrs.setdefault(node, {})
        shadow_attrs = self._graph._shadow.nodes[node] if self._graph._shadow else None
        return _AttrProxy(attrs, shadow_attrs)

    def __call__(self) -> list[str]:
        return list(self)


class _EdgeView:
    def __init__(self, graph: "_GraphBase") -> None:
        self._graph = graph

    def __iter__(self) -> Iterator[tuple[str, str]]:
        return iter(self._graph._ordered_edges())

    def __len__(self) -> int:
        return len(self._graph._ordered_edges())

    def __contains__(self, edge: object) -> bool:
        if not isinstance(edge, tuple) or len(edge) != 2:
            return False
        left, right = edge
        if not isinstance(left, str) or not isinstance(right, str):
            return False
        present = self._graph._core.has_edge(left, right)
        self._graph._assert_shadow("edges.__contains__", present, edge)
        return present

    def __getitem__(self, edge: tuple[str, str]) -> MutableMapping[str, object]:
        left, right = edge
        if not self._graph._core.has_edge(left, right):
            raise KeyError(edge)
        attrs = self._graph._edge_attrs.setdefault(
            _edge_key(self._graph, left, right), {}
        )
        shadow_attrs = (
            self._graph._shadow.edges[left, right] if self._graph._shadow else None
        )
        return _AttrProxy(attrs, shadow_attrs)

    def __call__(self) -> list[tuple[str, str]]:
        return list(self)


class _GraphBase:
    directed = False

    def __init__(
        self, incoming_graph_data: object | None = None, **attr: object
    ) -> None:
        self._core = _RustGraphCore(self.directed)
        self._node_attrs: dict[str, dict[str, object]] = {}
        self._edge_attrs: dict[tuple[str, str], dict[str, object]] = {}
        self.graph: dict[str, object] = dict(attr)
        self._shadow = self._make_shadow_graph()

        if incoming_graph_data is None:
            return
        if isinstance(incoming_graph_data, _GraphBase):
            self._copy_from(incoming_graph_data)
            return
        for edge in cast(Iterable[tuple[object, object]], incoming_graph_data):
            if not isinstance(edge, tuple) or len(edge) != 2:
                raise TypeError("Expected iterable of 2-tuples for graph edges")
            self.add_edge(edge[0], edge[1])

    def _copy_from(self, other: "_GraphBase") -> None:
        self._core = other._core.clone_graph()
        self._node_attrs = {
            node: attrs.copy() for node, attrs in other._node_attrs.items()
        }
        self._edge_attrs = _copy_edge_attrs(other)
        self.graph = other.graph.copy()
        self._shadow = other._shadow.copy() if other._shadow is not None else None

    def _make_shadow_graph(self):
        if _shadow_nx is None:
            return None
        return _shadow_nx.DiGraph() if self.directed else _shadow_nx.Graph()

    def _shadow_expected(self, op: str, value: object | None = None):
        if self._shadow is None:
            return None
        if op == "nodes":
            return list(self._shadow.nodes)
        if op == "edges":
            return list(self._shadow.edges)
        if op == "node_in":
            return bool(value in self._shadow)
        if op == "edge_in":
            return bool(value in self._shadow.edges)
        if op == "has_edge" and isinstance(value, tuple):
            return self._shadow.has_edge(value[0], value[1])
        if op == "neighbors" and isinstance(value, str):
            return list(self._shadow.neighbors(value))
        if op == "successors" and isinstance(value, str):
            return list(self._shadow.successors(value))
        if op == "predecessors" and isinstance(value, str):
            return list(self._shadow.predecessors(value))
        if op == "in_degree" and isinstance(value, str):
            return (
                self._shadow.in_degree(value)
                if self.directed
                else self._shadow.degree(value)
            )
        if op == "out_degree" and isinstance(value, str):
            return (
                self._shadow.out_degree(value)
                if self.directed
                else self._shadow.degree(value)
            )
        return None

    def _assert_equal(self, op: str, actual: object, expected: object) -> None:
        if actual != expected:
            raise AssertionError(
                f"Graph parity mismatch for {op}: expected {expected!r}, got {actual!r}"
            )

    def _assert_shadow(
        self, op: str, actual: object, value: object | None = None
    ) -> None:
        expected = self._shadow_expected(
            {
                "nodes.__contains__": "node_in",
                "edges.__contains__": "edge_in",
                "has_edge": "has_edge",
                "neighbors": "neighbors",
                "successors": "successors",
                "predecessors": "predecessors",
                "in_degree": "in_degree",
                "out_degree": "out_degree",
            }.get(op, ""),
            value,
        )
        if expected is not None:
            self._assert_equal(op, actual, expected)

    def _assert_graph_parity(self, op: str) -> None:
        if self._shadow is None:
            return
        self._assert_equal(f"{op}.nodes", self._core.nodes(), list(self._shadow.nodes))
        self._assert_equal(f"{op}.edges", self._core.edges(), list(self._shadow.edges))

    def _ordered_nodes(self) -> list[str]:
        nodes = self._core.nodes()
        if self._shadow is not None:
            self._assert_equal("nodes()", nodes, list(self._shadow.nodes))
        return nodes

    def _ordered_edges(self) -> list[tuple[str, str]]:
        edges = self._core.edges()
        if self._shadow is not None:
            self._assert_equal("edges()", edges, list(self._shadow.edges))
        return edges

    def __contains__(self, node: object) -> bool:
        present = isinstance(node, str) and self._core.has_node(node)
        self._assert_shadow("nodes.__contains__", present, node)
        return present

    def __iter__(self) -> Iterator[str]:
        return iter(self.nodes)

    def __len__(self) -> int:
        return len(self._core.nodes())

    def is_directed(self) -> bool:
        return self.directed

    def is_multigraph(self) -> bool:
        return False

    def number_of_nodes(self) -> int:
        return len(self)

    def number_of_edges(self) -> int:
        return len(self.edges)

    @property
    def nodes(self) -> _NodeView:
        return _NodeView(self)

    @property
    def edges(self) -> _EdgeView:
        return _EdgeView(self)

    def add_node(self, node_for_adding: object, **attr: object) -> None:
        node = _coerce_node(node_for_adding)
        self._core.add_node(node)
        attrs = self._node_attrs.setdefault(node, {})
        attrs.update(attr)
        if self._shadow is not None:
            self._shadow.add_node(node, **attr)
        self._assert_graph_parity("add_node")

    def add_edge(self, u_of_edge: object, v_of_edge: object, **attr: object) -> None:
        left = _coerce_node(u_of_edge)
        right = _coerce_node(v_of_edge)
        self.add_node(left)
        self.add_node(right)
        self._core.add_edge(left, right)
        edge_attrs = self._edge_attrs.setdefault(_edge_key(self, left, right), {})
        edge_attrs.update(attr)
        if self._shadow is not None:
            self._shadow.add_edge(left, right, **attr)
        self._assert_graph_parity("add_edge")

    def has_edge(self, left: str, right: str) -> bool:
        present = self._core.has_edge(left, right)
        self._assert_shadow("has_edge", present, (left, right))
        return present

    def copy(self) -> Self:
        new = self.__class__()
        new._copy_from(self)
        return new

    def remove_node(self, node: str) -> None:
        if node not in self:
            _raise_missing(node)
        self._core.remove_node(node)
        self._node_attrs.pop(node, None)
        for edge in list(self._edge_attrs):
            if node in edge:
                self._edge_attrs.pop(edge, None)
        if self._shadow is not None:
            self._shadow.remove_node(node)
        self._assert_graph_parity("remove_node")

    def remove_nodes_from(self, nodes: Iterable[str]) -> None:
        normalized: list[str] = []
        for node in nodes:
            if not isinstance(node, str) or not self._core.has_node(node):
                continue
            normalized.append(node)
            self._node_attrs.pop(node, None)
            for edge in list(self._edge_attrs):
                if node in edge:
                    self._edge_attrs.pop(edge, None)
        self._core.remove_nodes(normalized)
        if self._shadow is not None:
            self._shadow.remove_nodes_from(normalized)
        self._assert_graph_parity("remove_nodes_from")

    def remove_edges_from(self, edges: Iterable[tuple[str, str]]) -> None:
        normalized: list[tuple[str, str]] = []
        for edge in edges:
            if len(edge) != 2:
                continue
            left = _coerce_node(edge[0])
            right = _coerce_node(edge[1])
            if self._core.has_edge(left, right):
                normalized.append((left, right))
                self._edge_attrs.pop(_edge_key(self, left, right), None)
        self._core.remove_edges(normalized)
        if self._shadow is not None:
            self._shadow.remove_edges_from(normalized)
        self._assert_graph_parity("remove_edges_from")

    def neighbors(self, node: str) -> Iterator[str]:
        if node not in self:
            _raise_missing(node)
        neighbors = self._core.neighbors(node)
        self._assert_shadow("neighbors", neighbors, node)
        return iter(neighbors)

    def successors(self, node: str) -> Iterator[str]:
        if node not in self:
            _raise_missing(node)
        successors = self._core.successors(node)
        self._assert_shadow("successors", successors, node)
        return iter(successors)

    def predecessors(self, node: str) -> Iterator[str]:
        if node not in self:
            _raise_missing(node)
        predecessors = self._core.predecessors(node)
        self._assert_shadow("predecessors", predecessors, node)
        return iter(predecessors)

    def in_degree(self, node: str) -> int:
        if node not in self:
            _raise_missing(node)
        degree = self._core.in_degree(node)
        self._assert_shadow("in_degree", degree, node)
        return degree

    def out_degree(self, node: str) -> int:
        if node not in self:
            _raise_missing(node)
        degree = self._core.out_degree(node)
        self._assert_shadow("out_degree", degree, node)
        return degree

    def subgraph(self, nodes: Iterable[str]) -> Self:
        keep_set: set[str] = set()
        for node in nodes:
            coerced = _coerce_node(node)
            if coerced not in self:
                continue
            keep_set.add(coerced)
        new = self.__class__()
        for node in self._ordered_nodes():
            if node not in keep_set:
                continue
            new.add_node(node, **self._node_attrs.get(node, {}).copy())
        for left, right in self._ordered_edges():
            if left in keep_set and right in keep_set:
                new.add_edge(
                    left,
                    right,
                    **self._edge_attrs.get(_edge_key(self, left, right), {}).copy(),
                )
        new.graph = self.graph.copy()
        if self._shadow is not None:
            expected_shadow = self._shadow.subgraph(keep_set).copy()
            new._assert_equal(
                "subgraph.nodes", list(new._core.nodes()), list(expected_shadow.nodes)
            )
            new._assert_equal(
                "subgraph.edges", list(new._core.edges()), list(expected_shadow.edges)
            )
            new._shadow = expected_shadow
        return new

    def to_undirected(self) -> "Graph":
        if not self.directed:
            return cast("Graph", self.copy())
        new = Graph()
        new._core = self._core.to_undirected_graph()
        new._node_attrs = {
            node: attrs.copy() for node, attrs in self._node_attrs.items()
        }
        new._edge_attrs = {
            _edge_key(new, left, right): attrs.copy()
            for (left, right), attrs in self._edge_attrs.items()
        }
        new.graph = self.graph.copy()
        if self._shadow is not None:
            new._shadow = self._shadow.to_undirected()
            new._assert_graph_parity("to_undirected")
        return new


class Graph(_GraphBase):
    directed = False


class DiGraph(_GraphBase):
    directed = True


class _ApproximationSteinerTree:
    @staticmethod
    def steiner_tree(
        graph: GraphT,
        nodes: Iterable[str],
        weight: str = "weight",
    ) -> GraphT:
        terminals = [_coerce_node(node) for node in nodes]
        try:
            tree_nodes = graph._core.steiner_tree_nodes(
                terminals, _weight_triples(graph, weight)
            )
        except ValueError as exc:
            message = str(exc)
            if "Node not found" in message:
                raise NodeNotFound(message) from exc
            raise NetworkXNoPath(message) from exc
        return graph.subgraph(tree_nodes)


approximation = SimpleNamespace(
    steinertree=SimpleNamespace(steiner_tree=_ApproximationSteinerTree.steiner_tree)
)


def neighbors(graph: _GraphBase, node: str) -> Iterator[str]:
    return graph.neighbors(node)


def all_neighbors(graph: _GraphBase, node: str) -> Iterator[str]:
    if node not in graph:
        _raise_missing(node)
    return iter(graph._core.all_neighbors(node))


def connected_components(graph: _GraphBase) -> Iterator[set[str]]:
    components = [set(component) for component in graph._core.connected_components()]
    if graph._shadow is not None:
        expected = [
            set(component)
            for component in _shadow_nx.connected_components(graph._shadow)
        ]
        if components != expected:
            raise AssertionError(
                f"Graph parity mismatch for connected_components: expected {expected!r}, got {components!r}"
            )
    for component in components:
        yield component


def isolates(graph: _GraphBase) -> Iterator[str]:
    for node in graph.nodes:
        if graph.in_degree(node) == 0 and graph.out_degree(node) == 0:
            yield node


def topological_sort(graph: _GraphBase) -> Iterator[str]:
    try:
        order = graph._core.topological_sort()
    except ValueError as exc:
        raise NetworkXUnfeasible(str(exc)) from exc
    if graph._shadow is not None:
        expected = list(_shadow_nx.topological_sort(graph._shadow))
        if order != expected:
            raise AssertionError(
                f"Graph parity mismatch for topological_sort: expected {expected!r}, got {order!r}"
            )
    return iter(order)


def shortest_path(graph: _GraphBase, source: str, target: str) -> list[str]:
    if source not in graph:
        _raise_missing(source)
    if target not in graph:
        _raise_missing(target)
    path = graph._core.shortest_path(source, target)
    if path is None:
        raise NetworkXNoPath(f"No path between {source} and {target}")
    if graph._shadow is not None:
        expected = _shadow_nx.shortest_path(graph._shadow, source, target)
        if path != expected:
            raise AssertionError(
                f"Graph parity mismatch for shortest_path: expected {expected!r}, got {path!r}"
            )
    return path


def shortest_path_length(graph: _GraphBase, source: str, target: str) -> int:
    if source not in graph:
        _raise_missing(source)
    if target not in graph:
        _raise_missing(target)
    length = graph._core.shortest_path_length(source, target)
    if length is None:
        raise NetworkXNoPath(f"No path between {source} and {target}")
    return length


def ego_graph(graph: GraphT, center: str, radius: int) -> GraphT:
    if center not in graph:
        _raise_missing(center)
    ego = graph.subgraph(graph._core.ego_graph_nodes(center, radius))
    if graph._shadow is not None:
        expected = _shadow_nx.ego_graph(graph._shadow, center, radius)
        if list(ego.nodes) != list(expected.nodes) or list(ego.edges) != list(
            expected.edges
        ):
            raise AssertionError(
                "Graph parity mismatch for ego_graph: "
                f"expected nodes={list(expected.nodes)!r} edges={list(expected.edges)!r}, "
                f"got nodes={list(ego.nodes)!r} edges={list(ego.edges)!r}"
            )
    return ego


def multi_source_dijkstra_path(
    graph: _GraphBase,
    sources: Iterable[str],
    weight: str = "weight",
) -> dict[str, list[str]]:
    source_nodes = [_coerce_node(node) for node in sources]
    try:
        pairs = graph._core.multi_source_dijkstra_path(
            source_nodes,
            _weight_triples(graph, weight),
        )
    except ValueError as exc:
        message = str(exc)
        if "Node not found" in message:
            raise NodeNotFound(message) from exc
        raise NetworkXError(message) from exc
    result = dict(pairs)
    if graph._shadow is not None:
        expected = _shadow_nx.multi_source_dijkstra_path(
            graph._shadow, source_nodes, weight=weight
        )
        if result != expected:
            raise AssertionError(
                f"Graph parity mismatch for multi_source_dijkstra_path: expected {expected!r}, got {result!r}"
            )
    return result


class _AttrProxy(MutableMapping[str, object]):
    def __init__(
        self,
        attrs: dict[str, object],
        shadow_attrs: MutableMapping[str, object] | None,
    ) -> None:
        self._attrs = attrs
        self._shadow_attrs = shadow_attrs

    def __getitem__(self, key: str) -> object:
        return self._attrs[key]

    def __setitem__(self, key: str, value: object) -> None:
        self._attrs[key] = value
        if self._shadow_attrs is not None:
            self._shadow_attrs[key] = value

    def __delitem__(self, key: str) -> None:
        del self._attrs[key]
        if self._shadow_attrs is not None and key in self._shadow_attrs:
            del self._shadow_attrs[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._attrs)

    def __len__(self) -> int:
        return len(self._attrs)


def is_weakly_connected(graph: _GraphBase) -> bool:
    return graph._core.is_weakly_connected()


def to_undirected(graph: _GraphBase) -> Graph:
    return graph.to_undirected()


def subgraph(graph: GraphT, nodes: Iterable[str]) -> GraphT:
    return graph.subgraph(nodes)
