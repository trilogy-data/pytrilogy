from __future__ import annotations

from types import SimpleNamespace
from typing import Iterable, Iterator, Mapping, MutableMapping, Self, TypeVar, cast

try:
    from _preql_import_resolver import PyGraphCore as _RustGraphCore
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "Rust graph bindings are required but were not found. "
        "Build/install the local extension with `maturin develop` in the project venv."
    ) from exc


class NetworkXError(RuntimeError):
    pass


class NetworkXNoPath(NetworkXError):
    pass


class NetworkXUnfeasible(NetworkXError):
    pass


class NodeNotFound(NetworkXError):
    pass


class NetworkXNoCycle(NetworkXError):
    pass


exception = SimpleNamespace(
    NetworkXError=NetworkXError,
    NetworkXNoPath=NetworkXNoPath,
    NetworkXUnfeasible=NetworkXUnfeasible,
    NodeNotFound=NodeNotFound,
    NetworkXNoCycle=NetworkXNoCycle,
)


def _edge_key(graph: "_GraphBase", left: str, right: str) -> tuple[str, str]:
    if graph.directed or left <= right:
        return (left, right)
    return (right, left)


def _copy_edge_attrs(
    graph: "_GraphBase",
) -> dict[tuple[str, str], dict[str, object]]:
    return {edge: attrs.copy() for edge, attrs in graph._edge_attrs.items()}


def _weight_triples(
    graph: "_GraphBase",
    weight: str,
) -> list[tuple[str, str, float]]:
    edges = graph._cached_edges
    if edges is None:
        edges = graph._core.edges()
        graph._cached_edges = edges
    edge_attrs = graph._edge_attrs
    # Fast path: when no edges carry attrs (the common case — only
    # node_merge_node sets weights, and only for BASIC non-ATTR_ACCESS
    # targets), every edge resolves to the default weight of 1.0.
    if not edge_attrs:
        return [(left, right, 1.0) for left, right in edges]
    # Slow path — hoist attribute lookups, inline _edge_key, and skip the
    # empty-dict allocation for edges without attrs.
    directed = graph.directed
    numeric_types = (int, float, str)
    output: list[tuple[str, str, float]] = []
    append = output.append
    for left, right in edges:
        key = (left, right) if directed or left <= right else (right, left)
        attrs = edge_attrs.get(key)
        if attrs is None:
            w = 1.0
        else:
            raw = attrs.get(weight, 1.0)
            w = float(raw) if isinstance(raw, numeric_types) else 1.0
        append((left, right, w))
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
        if not isinstance(node, str):
            raise TypeError(f"Graph nodes must be strings, got {type(node).__name__}")
        return self._graph._core.has_node(node)

    def __getitem__(self, node: str) -> MutableMapping[str, object]:
        if node not in self:
            _raise_missing(node)
        return self._graph._node_attrs.setdefault(node, {})

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
            raise TypeError("Graph edges must be 2-tuples of strings")
        left, right = edge
        if not isinstance(left, str) or not isinstance(right, str):
            raise TypeError("Graph edges must contain string nodes")
        return self._graph._core.has_edge(left, right)

    def __getitem__(self, edge: tuple[str, str]) -> MutableMapping[str, object]:
        left, right = edge
        key = _edge_key(self._graph, left, right)
        attrs = self._graph._edge_attrs.get(key)
        if attrs is None:
            if not self._graph._core.has_edge(left, right):
                raise KeyError(edge)
            attrs = self._graph._edge_attrs.setdefault(key, {})
        return attrs

    def __call__(self) -> list[tuple[str, str]]:
        return list(self)


class _NeighborView(Mapping[str, MutableMapping[str, object]]):
    def __init__(self, graph: "_GraphBase", node: str, reverse: bool = False) -> None:
        self._graph = graph
        self._node = node
        self._reverse = reverse and graph.directed

    def _neighbors(self) -> list[str]:
        if self._reverse:
            return list(self._graph._core.predecessors(self._node))
        return list(self._graph._core.successors(self._node))

    def __iter__(self) -> Iterator[str]:
        return iter(self._neighbors())

    def __len__(self) -> int:
        return len(self._neighbors())

    def __getitem__(self, neighbor: str) -> MutableMapping[str, object]:
        if neighbor not in self._neighbors():
            raise KeyError(neighbor)
        left, right = (
            (neighbor, self._node) if self._reverse else (self._node, neighbor)
        )
        return self._graph._edge_attrs.setdefault(
            _edge_key(self._graph, left, right), {}
        )


class _AdjacencyView(Mapping[str, _NeighborView]):
    def __init__(self, graph: "_GraphBase", reverse: bool = False) -> None:
        self._graph = graph
        self._reverse = reverse

    def __iter__(self) -> Iterator[str]:
        return iter(self._graph._ordered_nodes())

    def __len__(self) -> int:
        return len(self._graph)

    def __getitem__(self, node: str) -> _NeighborView:
        if not self._graph._core.has_node(node):
            raise KeyError(node)
        return _NeighborView(self._graph, node, reverse=self._reverse)


class _GraphBase:
    directed = False

    def __init__(
        self, incoming_graph_data: object | None = None, **attr: object
    ) -> None:
        self._core = _RustGraphCore(self.directed)
        self._node_attrs: dict[str, dict[str, object]] = {}
        self._edge_attrs: dict[tuple[str, str], dict[str, object]] = {}
        self._cached_nodes: list[str] | None = None
        self._cached_edges: list[tuple[str, str]] | None = None
        self.graph: dict[str, object] = dict(attr)

        if incoming_graph_data is None:
            return
        if isinstance(incoming_graph_data, _GraphBase):
            self._copy_from(incoming_graph_data)
            return
        for edge in cast(Iterable[tuple[object, object]], incoming_graph_data):
            if not isinstance(edge, tuple) or len(edge) != 2:
                raise TypeError("Expected iterable of 2-tuples for graph edges")
            left, right = edge
            if not isinstance(left, str) or not isinstance(right, str):
                raise TypeError("Expected graph edges to contain string nodes")
            self.add_edge(left, right)

    def _copy_from(self, other: "_GraphBase") -> None:
        self._core = other._core.clone_graph()
        self._node_attrs = {
            node: attrs.copy() for node, attrs in other._node_attrs.items()
        }
        self._edge_attrs = _copy_edge_attrs(other)
        self._cached_nodes = (
            list(other._cached_nodes) if other._cached_nodes is not None else None
        )
        self._cached_edges = (
            list(other._cached_edges) if other._cached_edges is not None else None
        )
        self.graph = other.graph.copy()

    def _ordered_nodes(self) -> list[str]:
        nodes = self._cached_nodes
        if nodes is None:
            nodes = self._core.nodes()
            self._cached_nodes = nodes
        return nodes

    def _ordered_edges(self) -> list[tuple[str, str]]:
        edges = self._cached_edges
        if edges is None:
            edges = self._core.edges()
            self._cached_edges = edges
        return edges

    def _invalidate_structure_cache(self) -> None:
        self._cached_nodes = None
        self._cached_edges = None

    def __contains__(self, node: object) -> bool:
        if not isinstance(node, str):
            raise TypeError(f"Graph nodes must be strings, got {type(node).__name__}")
        return self._core.has_node(node)

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

    @property
    def adj(self) -> _AdjacencyView:
        return _AdjacencyView(self)

    @property
    def _adj(self) -> _AdjacencyView:
        return self.adj

    @property
    def succ(self) -> _AdjacencyView:
        return _AdjacencyView(self)

    @property
    def _succ(self) -> _AdjacencyView:
        return self.succ

    @property
    def pred(self) -> _AdjacencyView:
        return _AdjacencyView(self, reverse=True)

    @property
    def _pred(self) -> _AdjacencyView:
        return self.pred

    def __getitem__(self, node: str) -> _NeighborView:
        return self.adj[node]

    def add_node(self, node_for_adding: str, **attr: object) -> None:
        self._core.add_node(node_for_adding)
        self._invalidate_structure_cache()
        if attr:
            attrs = self._node_attrs.setdefault(node_for_adding, {})
            attrs.update(attr)

    def add_edge(self, u_of_edge: str, v_of_edge: str, **attr: object) -> None:
        # ``_core.add_edge`` creates endpoint nodes on demand, so no separate
        # existence check / insert is needed here.
        self._core.add_edge(u_of_edge, v_of_edge)
        self._invalidate_structure_cache()
        if attr:
            edge_attrs = self._edge_attrs.setdefault(
                _edge_key(self, u_of_edge, v_of_edge), {}
            )
            edge_attrs.update(attr)

    def add_nodes_from(self, nodes: Iterable[str]) -> None:
        """Batch ``add_node``: one Python->Rust crossing instead of one per
        node. Attribute-free; callers needing node attrs use ``add_node``."""
        node_list = list(nodes)
        if not node_list:
            return
        self._core.add_nodes(node_list)
        self._invalidate_structure_cache()

    def add_edges_from(self, edges: Iterable[tuple[str, str]]) -> None:
        """Batch ``add_edge``: one Python->Rust crossing instead of one per
        edge. Edges are 2-tuples; the Rust core adds endpoint nodes as needed.
        Attribute-free; callers needing edge attrs use ``add_edge``."""
        edge_list = [(u, v) for u, v in edges]
        if not edge_list:
            return
        self._core.add_edges(edge_list)
        self._invalidate_structure_cache()

    def has_edge(self, left: str, right: str) -> bool:
        return self._core.has_edge(left, right)

    def in_edges(self, node: str | None = None) -> list[tuple[str, str]]:
        if node is None:
            return self._ordered_edges()
        if node not in self:
            _raise_missing(node)
        return [(pred, node) for pred in self._core.predecessors(node)]

    def out_edges(self, node: str | None = None) -> list[tuple[str, str]]:
        if node is None:
            return self._ordered_edges()
        if node not in self:
            _raise_missing(node)
        return [(node, succ) for succ in self._core.successors(node)]

    def remove_edge(self, u: str, v: str) -> None:
        if not self._core.has_edge(u, v):
            raise NetworkXError(f"The edge {u}-{v} is not in the graph.")
        self._core.remove_edges([(u, v)])
        self._invalidate_structure_cache()
        self._edge_attrs.pop(_edge_key(self, u, v), None)

    def copy(self) -> Self:
        new = self.__class__()
        new._copy_from(self)
        return new

    def remove_node(self, node: str) -> None:
        if node not in self:
            _raise_missing(node)
        self._core.remove_node(node)
        self._invalidate_structure_cache()
        self._node_attrs.pop(node, None)
        for edge in list(self._edge_attrs):
            if node in edge:
                self._edge_attrs.pop(edge, None)

    def remove_nodes_from(self, nodes: Iterable[str]) -> None:
        normalized: list[str] = []
        for node in nodes:
            if not isinstance(node, str):
                raise TypeError(
                    f"Graph nodes must be strings, got {type(node).__name__}"
                )
            if not self._core.has_node(node):
                continue
            normalized.append(node)
        if not normalized:
            return
        removed = set(normalized)
        for node in normalized:
            self._node_attrs.pop(node, None)
        self._edge_attrs = {
            edge: attrs
            for edge, attrs in self._edge_attrs.items()
            if edge[0] not in removed and edge[1] not in removed
        }
        self._core.remove_nodes(normalized)
        self._invalidate_structure_cache()

    def remove_edges_from(self, edges: Iterable[tuple[str, str]]) -> None:
        normalized: list[tuple[str, str]] = []
        for edge in edges:
            if len(edge) != 2:
                raise TypeError("Graph edges must be 2-tuples of strings")
            left, right = edge
            if not isinstance(left, str) or not isinstance(right, str):
                raise TypeError("Graph edges must contain string nodes")
            if self._core.has_edge(left, right):
                normalized.append((left, right))
                self._edge_attrs.pop(_edge_key(self, left, right), None)
        self._core.remove_edges(normalized)
        self._invalidate_structure_cache()

    def neighbors(self, node: str) -> Iterator[str]:
        if node not in self:
            _raise_missing(node)
        return iter(self._core.neighbors(node))

    def successors(self, node: str) -> Iterator[str]:
        if node not in self:
            _raise_missing(node)
        return iter(self._core.successors(node))

    def predecessors(self, node: str) -> Iterator[str]:
        if node not in self:
            _raise_missing(node)
        return iter(self._core.predecessors(node))

    def in_degree(self, node: str) -> int:
        if node not in self:
            _raise_missing(node)
        return self._core.in_degree(node)

    def out_degree(self, node: str) -> int:
        if node not in self:
            _raise_missing(node)
        return self._core.out_degree(node)

    def subgraph(self, nodes: Iterable[str]) -> Self:
        keep_set: set[str] = set()
        for node in nodes:
            if node not in self:
                continue
            keep_set.add(node)
        ordered_keep = [node for node in self._ordered_nodes() if node in keep_set]
        new = self.__class__()
        new._core = self._core.induced_subgraph(ordered_keep)
        new._cached_nodes = list(ordered_keep)
        new._cached_edges = None
        new._node_attrs = {
            node: self._node_attrs.get(node, {}).copy() for node in ordered_keep
        }
        new._edge_attrs = {
            _edge_key(new, left, right): attrs.copy()
            for (left, right), attrs in self._edge_attrs.items()
            if left in keep_set and right in keep_set
        }
        new.graph = self.graph.copy()
        return new

    def to_undirected(self) -> "Graph":
        if not self.directed:
            return cast("Graph", self.copy())
        new = Graph()
        new._core = self._core.to_undirected_graph()
        new._cached_nodes = list(self._ordered_nodes())
        new._cached_edges = None
        new._node_attrs = {
            node: attrs.copy() for node, attrs in self._node_attrs.items()
        }
        new._edge_attrs = {
            _edge_key(new, left, right): attrs.copy()
            for (left, right), attrs in self._edge_attrs.items()
        }
        new.graph = self.graph.copy()
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
        try:
            tree_nodes = graph._core.steiner_tree_nodes(
                list(nodes), _weight_triples(graph, weight)
            )
        except ValueError as exc:
            message = str(exc)
            if "Node not found" in message:
                raise NodeNotFound(message) from exc
            return graph.subgraph([])
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
    for component in graph._core.connected_components():
        yield set(component)


def isolates(graph: _GraphBase) -> Iterator[str]:
    for node in graph.nodes:
        if graph.in_degree(node) == 0 and graph.out_degree(node) == 0:
            yield node


def topological_sort(graph: _GraphBase) -> Iterator[str]:
    try:
        order = graph._core.topological_sort()
    except ValueError as exc:
        raise NetworkXUnfeasible(str(exc)) from exc
    return iter(order)


def is_directed_acyclic_graph(graph: _GraphBase) -> bool:
    if not graph.directed:
        return False
    try:
        topological_sort(graph)
    except NetworkXUnfeasible:
        return False
    return True


def descendants(graph: _GraphBase, source: str) -> set[str]:
    if source not in graph:
        _raise_missing(source)
    seen: set[str] = set()
    stack = list(graph._core.successors(source))
    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        stack.extend(graph._core.successors(node))
    seen.discard(source)
    return seen


def ancestors(graph: _GraphBase, source: str) -> set[str]:
    if source not in graph:
        _raise_missing(source)
    seen: set[str] = set()
    stack = list(graph._core.predecessors(source))
    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        stack.extend(graph._core.predecessors(node))
    seen.discard(source)
    return seen


def find_cycle(graph: _GraphBase, source: str | None = None) -> list[tuple[str, str]]:
    """First directed cycle as a list of edges (NetworkX semantics); raises
    NetworkXNoCycle if the graph is acyclic. DFS that stops at the first back
    edge — only reached on the error path after topological_sort fails."""
    GRAY, BLACK = 1, 2
    color: dict[str, int] = {}
    roots = [source] if source is not None else graph._ordered_nodes()
    for root in roots:
        if color.get(root):
            continue
        stack: list[tuple[str, Iterator[str]]] = [
            (root, iter(graph._core.successors(root)))
        ]
        color[root] = GRAY
        path = [root]
        while stack:
            node, successors = stack[-1]
            nxt = next(successors, None)
            if nxt is None:
                color[node] = BLACK
                stack.pop()
                path.pop()
                continue
            state = color.get(nxt)
            if state == GRAY:
                cycle = path[path.index(nxt) :] + [nxt]
                return [(cycle[i], cycle[i + 1]) for i in range(len(cycle) - 1)]
            if state is None:
                color[nxt] = GRAY
                stack.append((nxt, iter(graph._core.successors(nxt))))
                path.append(nxt)
    raise NetworkXNoCycle("No cycle found.")


def _collect_cycles_from(
    start: str,
    node: str,
    path: list[str],
    on_path: set[str],
    succ: Mapping[str, list[str]],
    rank: Mapping[str, int],
    out: list[list[str]],
) -> None:
    for nxt in succ[node]:
        if nxt == start:
            out.append(list(path))
        elif rank[nxt] > rank[start] and nxt not in on_path:
            path.append(nxt)
            on_path.add(nxt)
            _collect_cycles_from(start, nxt, path, on_path, succ, rank, out)
            path.pop()
            on_path.discard(nxt)


def simple_cycles(graph: _GraphBase) -> Iterator[list[str]]:
    # Each elementary cycle is enumerated exactly once, at its lowest-ranked
    # node: the search from ``start`` only descends into higher-ranked nodes.
    nodes = graph._ordered_nodes()
    rank = {node: index for index, node in enumerate(nodes)}
    succ = {node: list(graph._core.successors(node)) for node in nodes}
    out: list[list[str]] = []
    for start in nodes:
        _collect_cycles_from(start, start, [start], {start}, succ, rank, out)
    return iter(out)


def shortest_path(graph: _GraphBase, source: str, target: str) -> list[str]:
    if source not in graph:
        _raise_missing(source)
    if target not in graph:
        _raise_missing(target)
    path = graph._core.shortest_path(source, target)
    if path is None:
        raise NetworkXNoPath(f"No path between {source} and {target}")
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


def has_path(graph: _GraphBase, source: str, target: str) -> bool:
    try:
        shortest_path(graph, source, target)
    except (NetworkXNoPath, NodeNotFound):
        return False
    return True


def ego_graph(graph: GraphT, center: str, radius: int) -> GraphT:
    if center not in graph:
        _raise_missing(center)
    return graph.subgraph(graph._core.ego_graph_nodes(center, radius))


def multi_source_dijkstra_path(
    graph: _GraphBase,
    sources: Iterable[str],
    weight: str = "weight",
) -> dict[str, list[str]]:
    source_nodes = list(sources)
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
    return dict(pairs)


def is_weakly_connected(graph: _GraphBase) -> bool:
    return graph._core.is_weakly_connected()


def to_undirected(graph: _GraphBase) -> Graph:
    return graph.to_undirected()


def subgraph(graph: GraphT, nodes: Iterable[str]) -> GraphT:
    return graph.subgraph(nodes)
