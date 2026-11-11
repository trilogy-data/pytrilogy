"""A group a sibling covers in full is re-homed onto it, elected with the
dimension and spine reads over one graph: it takes no other read."""

from types import SimpleNamespace

import pytest

from tests.core.processing.test_v4_virt_filter_extra_cte import (
    _gen_having_basic_key_sql,
)
from trilogy.core import graph as nx
from trilogy.core.enums import Derivation
from trilogy.core.processing.v4_helper import group_graph as gg
from trilogy.core.processing.v4_helper.edges import (
    EdgeKind,
    add_edge,
    lineage_predecessors,
)

_APPLY = gg._apply_reads


class _ReadLog:
    def __init__(self) -> None:
        self.passes: list[dict] = []

    def apply(self, group_graph, group_edges, attrs, *rest):
        reads = gg._elect_reads(group_graph, group_edges, attrs, *rest)
        siblings = {
            gid: r.parent
            for gid, r in reads.items()
            if r.parent is not None
            and attrs[r.parent].derivation not in (Derivation.ROOT, Derivation.BASIC)
            and not group_graph.has_edge(r.parent, gid)
        }
        _APPLY(group_graph, group_edges, attrs, *rest)
        parents = {
            gid: set(lineage_predecessors(group_graph, group_edges, gid))
            for gid in siblings
        }
        self.passes.append({"siblings": siblings, "parents": parents})


@pytest.fixture
def log(monkeypatch) -> _ReadLog:
    log = _ReadLog()
    monkeypatch.setattr(gg, "_apply_reads", log.apply)
    _gen_having_basic_key_sql()
    return log


def test_each_sibling_read_becomes_its_readers_parent(log):
    assert any(p["siblings"] for p in log.passes)
    for p in log.passes:
        for gid, sibling in p["siblings"].items():
            assert sibling in p["parents"][gid]


def _graph() -> tuple[nx.DiGraph, dict, dict]:
    group_graph = nx.DiGraph()
    edges: dict = {}
    for u, v in [("r", "g"), ("r", "s"), ("g", gg.FINAL_NODE_ID)]:
        add_edge(group_graph, edges, u, v, EdgeKind.LINEAGE)
    attrs = {
        gid: SimpleNamespace(derivation=d)
        for gid, d in [
            ("g", Derivation.BASIC),
            ("s", Derivation.AGGREGATE),
            ("r", Derivation.ROOT),
        ]
    }
    return group_graph, edges, attrs


def test_a_sibling_read_adds_only_its_edge(monkeypatch):
    group_graph, edges, attrs = _graph()
    monkeypatch.setattr(gg, "_regraft_candidate", lambda *a, **k: "s")
    gg._apply_reads(group_graph, edges, attrs, {}, {}, None, frozenset())
    assert set(lineage_predecessors(group_graph, edges, "g")) == {"r", "s"}


def test_a_held_sibling_is_no_rewrite(monkeypatch):
    group_graph, edges, attrs = _graph()
    add_edge(group_graph, edges, "s", "g", EdgeKind.LINEAGE)
    monkeypatch.setattr(gg, "_regraft_candidate", lambda *a, **k: "s")
    before = dict(edges)
    reads = gg._elect_reads(group_graph, edges, attrs, {}, {}, None, frozenset())
    gg._apply_reads(group_graph, edges, attrs, {}, {}, None, frozenset())
    assert reads == {"g": gg.RowProvider("s")}
    assert edges == before
