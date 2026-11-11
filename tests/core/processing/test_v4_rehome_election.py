"""A partial spine is a read: elected with the siblings and dimensions, ranked
below both, and applied as one lineage edge."""

from types import SimpleNamespace

from trilogy.core import graph as nx
from trilogy.core.enums import Derivation
from trilogy.core.processing.v4_helper import group_graph as gg
from trilogy.core.processing.v4_helper.edges import EdgeAttrs, EdgeKind


class _Candidates:
    def __init__(self, sibling=None, dimension=None, spine=None) -> None:
        self.answers = {False: sibling, True: spine}
        self.dimension = dimension
        self.asked: list[object] = []

    def regraft(self, *args, allow_partial, **kwargs):
        self.asked.append(
            ("spine", kwargs.get("environment")) if allow_partial else "sibling"
        )
        return self.answers[allow_partial]

    def dimension_read(self, *args):
        self.asked.append("dimension")
        return self.dimension


def _elect(monkeypatch, candidates: _Candidates):
    monkeypatch.setattr(gg, "_regraft_candidate", candidates.regraft)
    monkeypatch.setattr(
        gg, "_synthetic_dimension_regraft_parent", candidates.dimension_read
    )
    return gg._elect_read(None, {}, {}, {}, {}, "env", "g")


def test_a_spine_is_asked_for_only_after_a_sibling_and_a_dimension(monkeypatch):
    candidates = _Candidates(spine="p")
    assert _elect(monkeypatch, candidates) == gg.RowProvider("p")
    assert candidates.asked == ["sibling", "dimension", ("spine", "env")]


def test_a_sibling_takes_no_spine(monkeypatch):
    candidates = _Candidates(sibling="s", spine="p")
    assert _elect(monkeypatch, candidates) == gg.RowProvider("s")
    assert candidates.asked == ["sibling"]


def test_a_dimension_takes_no_spine(monkeypatch):
    read = gg.RowProvider("r", frozenset({"q"}))
    candidates = _Candidates(dimension=read, spine="p")
    assert _elect(monkeypatch, candidates) is read
    assert candidates.asked == ["sibling", "dimension"]


def test_a_group_with_no_candidate_takes_no_read(monkeypatch):
    assert _elect(monkeypatch, _Candidates()) is None


def _graph(*edges) -> nx.DiGraph:
    group_graph = nx.DiGraph()
    group_graph.add_nodes_from(["g", "p", gg.FINAL_NODE_ID])
    group_graph.add_edges_from(edges)
    return group_graph


_ATTRS = {
    "g": SimpleNamespace(derivation=Derivation.BASIC),
    "p": SimpleNamespace(derivation=Derivation.AGGREGATE),
}


def test_a_spine_is_one_lineage_edge(monkeypatch):
    monkeypatch.setattr(gg, "_elect_read", lambda *a: gg.RowProvider("p"))
    group_graph, edges = _graph(), {}
    gg._apply_reads(group_graph, edges, _ATTRS, {}, {}, None, frozenset())
    assert edges == {("p", "g"): EdgeAttrs(EdgeKind.LINEAGE)}


def test_a_spine_the_graph_already_holds_is_no_rewrite(monkeypatch):
    monkeypatch.setattr(gg, "_elect_read", lambda *a: gg.RowProvider("p"))
    group_graph, edges = _graph(("p", "g")), {}
    gg._apply_reads(group_graph, edges, _ATTRS, {}, {}, None, frozenset())
    assert edges == {}
    assert set(group_graph.edges) == {("p", "g")}
