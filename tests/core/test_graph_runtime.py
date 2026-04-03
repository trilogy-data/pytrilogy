import networkx as nx
import pytest

from trilogy.core import graph as rust_nx


def build_weighted_directed_pair():
    native = rust_nx.DiGraph()
    reference = nx.DiGraph()
    for graph in [native, reference]:
        graph.add_node("a", type="start")
        graph.add_node("b", type="mid")
        graph.add_node("c", type="mid")
        graph.add_node("d", type="end")
        graph.add_edge("a", "b")
        graph.add_edge("a", "c")
        graph.add_edge("b", "d")
        graph.add_edge("c", "d")
    return native, reference


def test_directed_views_and_copy_parity():
    native, reference = build_weighted_directed_pair()

    native.edges["a", "b"]["weight"] = 3
    reference.edges["a", "b"]["weight"] = 3

    assert list(native.nodes) == list(reference.nodes)
    assert list(native.edges) == list(reference.edges)
    assert native.nodes["a"]["type"] == reference.nodes["a"]["type"]
    assert native.edges["a", "b"]["weight"] == reference.edges["a", "b"]["weight"]
    assert list(native.successors("a")) == list(reference.successors("a"))
    assert list(native.predecessors("d")) == list(reference.predecessors("d"))
    assert native.in_degree("d") == reference.in_degree("d")
    assert native.out_degree("a") == reference.out_degree("a")

    copied = native.copy()
    assert list(copied.nodes) == list(reference.copy().nodes)
    assert list(copied.edges) == list(reference.copy().edges)


def test_subgraph_and_to_undirected_parity():
    native, reference = build_weighted_directed_pair()

    native_sub = native.subgraph(["a", "b", "d"])
    reference_sub = reference.subgraph(["a", "b", "d"]).copy()
    assert list(native_sub.nodes) == list(reference_sub.nodes)
    assert list(native_sub.edges) == list(reference_sub.edges)

    native_undirected = native.to_undirected()
    reference_undirected = reference.to_undirected()
    assert list(native_undirected.edges) == list(reference_undirected.edges)
    assert [set(c) for c in rust_nx.connected_components(native_undirected)] == [
        set(c) for c in nx.connected_components(reference_undirected)
    ]


def test_subgraph_preserves_attrs():
    native, reference = build_weighted_directed_pair()

    native.nodes["a"]["kind"] = "source"
    reference.nodes["a"]["kind"] = "source"
    native.edges["a", "b"]["weight"] = 7
    reference.edges["a", "b"]["weight"] = 7

    native_sub = native.subgraph(["a", "b"])
    reference_sub = reference.subgraph(["a", "b"]).copy()

    assert native_sub.nodes["a"]["kind"] == reference_sub.nodes["a"]["kind"]
    assert native_sub.edges["a", "b"]["weight"] == reference_sub.edges["a", "b"][
        "weight"
    ]


def test_topological_sort_cycle_and_shortest_path_parity():
    native, reference = build_weighted_directed_pair()

    assert list(rust_nx.topological_sort(native)) == list(
        nx.topological_sort(reference)
    )
    assert rust_nx.shortest_path(native, "a", "d") == nx.shortest_path(
        reference, "a", "d"
    )
    assert rust_nx.shortest_path_length(native, "a", "d") == nx.shortest_path_length(
        reference, "a", "d"
    )

    native.add_edge("d", "a")
    reference.add_edge("d", "a")

    with pytest.raises(rust_nx.NetworkXUnfeasible):
        list(rust_nx.topological_sort(native))
    with pytest.raises(nx.NetworkXUnfeasible):
        list(nx.topological_sort(reference))


def test_native_digraph_supports_real_networkx_shortest_path():
    native, reference = build_weighted_directed_pair()

    assert nx.shortest_path(native, "a", "d") == nx.shortest_path(reference, "a", "d")
    assert nx.shortest_path_length(native, "a", "d") == nx.shortest_path_length(
        reference, "a", "d"
    )


def test_ego_graph_isolates_and_weak_connectivity_parity():
    native = rust_nx.DiGraph()
    reference = nx.DiGraph()
    for graph in [native, reference]:
        graph.add_edge("center", "x1")
        graph.add_edge("x1", "x2")
        graph.add_edge("isolated", "isolated")
        graph.remove_edges_from([("isolated", "isolated")])
        graph.add_node("isolated")

    native_ego = rust_nx.ego_graph(native, "center", radius=1)
    reference_ego = nx.ego_graph(reference, "center", radius=1)

    assert list(native_ego.nodes) == list(reference_ego.nodes)
    assert list(rust_nx.isolates(native)) == list(nx.isolates(reference))
    assert rust_nx.is_weakly_connected(native) == nx.is_weakly_connected(reference)


def test_multi_source_dijkstra_path_parity():
    native = rust_nx.Graph()
    reference = nx.Graph()
    for graph in [native, reference]:
        graph.add_edge("a", "b")
        graph.add_edge("b", "c")
        graph.add_edge("a", "d")
        graph.add_edge("d", "c")
        graph.edges["a", "b"]["weight"] = 1
        graph.edges["b", "c"]["weight"] = 1
        graph.edges["a", "d"]["weight"] = 4
        graph.edges["d", "c"]["weight"] = 1

    assert rust_nx.multi_source_dijkstra_path(
        native, ["a"], weight="weight"
    ) == nx.multi_source_dijkstra_path(reference, ["a"], weight="weight")


def test_steiner_tree_parity_on_unique_solution():
    native = rust_nx.Graph()
    reference = nx.Graph()
    for graph in [native, reference]:
        graph.add_edge("a", "b")
        graph.add_edge("b", "c")
        graph.add_edge("c", "e")
        graph.add_edge("b", "d")
        graph.add_edge("d", "e")
        graph.edges["a", "b"]["weight"] = 1
        graph.edges["b", "c"]["weight"] = 1
        graph.edges["c", "e"]["weight"] = 1
        graph.edges["b", "d"]["weight"] = 3
        graph.edges["d", "e"]["weight"] = 5

    native_tree = rust_nx.approximation.steinertree.steiner_tree(
        native, ["a", "c", "e"], weight="weight"
    )
    reference_tree = nx.algorithms.approximation.steinertree.steiner_tree(
        reference, ["a", "c", "e"], weight="weight"
    )

    assert set(native_tree.nodes) == set(reference_tree.nodes)
    assert set(native_tree.edges) == set(reference_tree.edges)


def test_steiner_tree_indexed_matches_legacy_exactly():
    native = rust_nx.Graph()
    for left, right in [
        ("a", "b"),
        ("b", "c"),
        ("c", "f"),
        ("a", "d"),
        ("d", "e"),
        ("e", "f"),
        ("b", "e"),
    ]:
        native.add_edge(left, right)
    native.edges["a", "b"]["weight"] = 1
    native.edges["b", "c"]["weight"] = 1
    native.edges["c", "f"]["weight"] = 1
    native.edges["a", "d"]["weight"] = 1
    native.edges["d", "e"]["weight"] = 1
    native.edges["e", "f"]["weight"] = 1
    native.edges["b", "e"]["weight"] = 1

    weights = [
        (left, right, native.edges[left, right].get("weight", 1))
        for left, right in native.edges
    ]

    assert native._core.steiner_tree_nodes(["a", "c", "f"], weights) == (
        native._core.steiner_tree_nodes_legacy(["a", "c", "f"], weights)
    )


def test_steiner_tree_parity_on_disconnected_and_missing_terminals():
    native = rust_nx.Graph()
    reference = nx.Graph()
    for graph in [native, reference]:
        graph.add_edge("a", "b")
        graph.add_edge("c", "d")

    native_tree = rust_nx.approximation.steinertree.steiner_tree(native, ["a", "c"])
    reference_tree = nx.algorithms.approximation.steinertree.steiner_tree(
        reference, ["a", "c"]
    )

    assert list(native_tree.nodes) == list(reference_tree.nodes)
    assert list(native_tree.edges) == list(reference_tree.edges)

    with pytest.raises(rust_nx.NodeNotFound):
        rust_nx.approximation.steinertree.steiner_tree(native, ["a", "missing"])
    with pytest.raises(nx.NodeNotFound):
        nx.algorithms.approximation.steinertree.steiner_tree(
            reference, ["a", "missing"]
        )


def test_insertion_order_and_tie_break_parity():
    native = rust_nx.DiGraph()
    reference = nx.DiGraph()
    for graph in [native, reference]:
        graph.add_node("start")
        graph.add_node("c")
        graph.add_node("b")
        graph.add_node("end")
        graph.add_edge("start", "c")
        graph.add_edge("start", "b")
        graph.add_edge("c", "end")
        graph.add_edge("b", "end")

    assert list(native.nodes) == list(reference.nodes)
    assert list(native.edges) == list(reference.edges)
    assert list(native.successors("start")) == list(reference.successors("start"))
    assert rust_nx.shortest_path(native, "start", "end") == nx.shortest_path(
        reference, "start", "end"
    )

    native_sub = native.subgraph(["end", "start", "b"])
    reference_sub = reference.subgraph(["end", "start", "b"]).copy()
    assert list(native_sub.nodes) == list(reference_sub.nodes)


def test_remove_nodes_from_does_not_dispatch_remove_node():
    class CountingDiGraph(rust_nx.DiGraph):
        def __init__(self):
            super().__init__()
            self.removed: list[str] = []

        def remove_node(self, node: str) -> None:
            self.removed.append(node)
            super().remove_node(node)

    native = CountingDiGraph()
    reference = nx.DiGraph()
    for graph in [native, reference]:
        graph.add_edge("a", "b")
        graph.add_edge("b", "c")

    native.remove_nodes_from(["a", "missing"])
    reference.remove_nodes_from(["a", "missing"])

    assert native.removed == []
    assert list(native.nodes) == list(reference.nodes)
    assert list(native.edges) == list(reference.edges)
