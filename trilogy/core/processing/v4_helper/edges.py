"""Edge metadata for the concept / group graphs.

The graphs themselves stay plain ``nx.DiGraph`` topology (string nodes, no
attributes); every edge's metadata lives in a side map keyed by the ``(u, v)``
tuple, exactly like per-node state lives in ``dict[str, ConceptAttrs]`` /
``dict[str, GroupAttrs]``. Keeping metadata off the graph keeps the planner
portable to a string-only topology engine that carries no typed edge attributes.

All edge access goes through the typed helpers here, so there is no stringly
``graph.edges[u, v]["kind"]`` left for mypy to miss.
"""

from __future__ import annotations

from dataclasses import dataclass

from trilogy.core import graph as nx

from .constants import DEPENDENCY_EDGE_KINDS, FINAL_NODE_ID, EdgeKind, EdgePhase

Edge = tuple[str, str]


@dataclass
class EdgeAttrs:
    """Strongly-typed per-edge state, held in the ``EdgeMap`` side dict."""

    kind: EdgeKind
    phase: EdgePhase | None = None
    # When set, this lineage edge is one ALTERNATIVE among several feeding the
    # same pseudonym-hub node; exactly one survives `resolve_alternatives`,
    # which then strips the tag. All edges sharing a tag are mutually exclusive
    # candidates (e.g. `local.a` reachable via `uA.a` OR `uB.a`). Downstream
    # (group graph onward) never sees a tagged edge: resolution runs first.
    alt_group: str | None = None
    # An EXISTENCE edge into a concept whose own lineage reads the set
    # (`x ? k in <set>`): the concept is computed from it. A WHERE membership's
    # edge only orders the set before the rows it filters.
    in_lineage: bool = False


# A graph's companion edge-metadata map, keyed by ``(source, target)``.
EdgeMap = dict[Edge, EdgeAttrs]


def add_edge(
    graph: nx.DiGraph,
    edges: EdgeMap,
    u: str,
    v: str,
    kind: EdgeKind,
    alt_group: str | None = None,
    in_lineage: bool = False,
) -> None:
    """Add a typed edge to both the topology graph and its metadata map."""
    graph.add_edge(u, v)
    edges[(u, v)] = EdgeAttrs(kind=kind, alt_group=alt_group, in_lineage=in_lineage)


def remove_edge(graph: nx.DiGraph, edges: EdgeMap, u: str, v: str) -> None:
    """Remove an edge from both the topology graph and its metadata map."""
    graph.remove_edge(u, v)
    edges.pop((u, v), None)


def edge_kind(edges: EdgeMap, u: str, v: str) -> EdgeKind | None:
    attrs = edges.get((u, v))
    return attrs.kind if attrs is not None else None


def only_a_set(group_graph: nx.DiGraph, group_edges: EdgeMap, gid: str) -> bool:
    """A group read only as a set: it holds the rows its own condition kept."""
    if gid not in group_graph:
        return False
    kinds = {
        edge_kind(group_edges, gid, succ)
        for succ in group_graph.successors(gid)
        if succ != FINAL_NODE_ID
    }
    return kinds == {EdgeKind.EXISTENCE}


def lineage_predecessors(graph: nx.DiGraph, edges: EdgeMap, node: str) -> list[str]:
    return [
        pred
        for pred in graph.predecessors(node)
        if edge_kind(edges, pred, node) == EdgeKind.LINEAGE
    ]


def lineage_successors(graph: nx.DiGraph, edges: EdgeMap, node: str) -> list[str]:
    return [
        succ
        for succ in graph.successors(node)
        if edge_kind(edges, node, succ) == EdgeKind.LINEAGE
    ]


def existence_predecessors(graph: nx.DiGraph, edges: EdgeMap, node: str) -> list[str]:
    """The nodes feeding `node` a subselect, not rows."""
    return [
        pred
        for pred in graph.predecessors(node)
        if edge_kind(edges, pred, node) == EdgeKind.EXISTENCE
    ]


def lineage_set_predecessors(graph: nx.DiGraph, edges: EdgeMap, node: str) -> list[str]:
    """The sets `node`'s own lineage reads (`EdgeAttrs.in_lineage`)."""
    return [
        pred
        for pred in graph.predecessors(node)
        if (attrs := edges.get((pred, node))) is not None and attrs.in_lineage
    ]


def feeds_existence(graph: nx.DiGraph, edges: EdgeMap, node: str) -> bool:
    return any(
        edge_kind(edges, node, succ) == EdgeKind.EXISTENCE
        for succ in graph.successors(node)
    )


def edges_of_kind(edges: EdgeMap, *kinds: EdgeKind) -> list[Edge]:
    wanted = set(kinds)
    return [edge for edge, a in edges.items() if a.kind in wanted]


def lineage_edges(edges: EdgeMap) -> list[Edge]:
    return edges_of_kind(edges, EdgeKind.LINEAGE)


def dependency_edges(edges: EdgeMap) -> list[Edge]:
    return [edge for edge, a in edges.items() if a.kind in DEPENDENCY_EDGE_KINDS]


def _subgraph(graph: nx.DiGraph, keep: list[Edge]) -> nx.DiGraph:
    """A topology-only subgraph carrying every node but only ``keep`` edges."""
    sub: nx.DiGraph = nx.DiGraph()
    sub.add_nodes_from(graph.nodes)
    sub.add_edges_from(keep)
    return sub


def dependency_subgraph(graph: nx.DiGraph, edges: EdgeMap) -> nx.DiGraph:
    return _subgraph(graph, dependency_edges(edges))


def lineage_subgraph(graph: nx.DiGraph, edges: EdgeMap) -> nx.DiGraph:
    return _subgraph(graph, lineage_edges(edges))


def subgraph_of_kinds(
    graph: nx.DiGraph, edges: EdgeMap, *kinds: EdgeKind
) -> nx.DiGraph:
    return _subgraph(graph, edges_of_kind(edges, *kinds))
