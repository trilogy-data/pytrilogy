"""A group a sibling covers in full is re-homed onto it before the rehome,
elected with the dimension reads over one graph: the spine election sees it
read and takes none."""

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

_READ = gg._apply_reads
_SPINES = gg._elect_spines


class _ReadLog:
    def __init__(self) -> None:
        self.passes: list[dict] = []

    def read(self, group_graph, group_edges, attrs, buckets, concept_attrs, placed):
        reads = gg._elect_reads(
            group_graph, group_edges, attrs, buckets, concept_attrs, placed
        )
        siblings = {
            gid: r.parent
            for gid, r in reads.items()
            if r.parent is not None
            and attrs[r.parent].derivation != Derivation.ROOT
            and not group_graph.has_edge(r.parent, gid)
        }
        self.passes.append({"siblings": siblings})
        return _READ(group_graph, group_edges, attrs, buckets, concept_attrs, placed)

    def spines(self, group_graph, group_edges, attrs, environment, settled):
        self.passes[-1]["settled"] = settled
        self.passes[-1]["parents"] = {
            gid: set(lineage_predecessors(group_graph, group_edges, gid))
            for gid in self.passes[-1]["siblings"]
        }
        return _SPINES(group_graph, group_edges, attrs, environment, settled)


@pytest.fixture
def log(monkeypatch) -> _ReadLog:
    log = _ReadLog()
    monkeypatch.setattr(gg, "_apply_reads", log.read)
    monkeypatch.setattr(gg, "_elect_spines", log.spines)
    _gen_having_basic_key_sql()
    return log


def test_every_sibling_is_read_before_any_spine_is_elected(log):
    assert any(p["siblings"] for p in log.passes)
    for p in log.passes:
        for gid, sibling in p["siblings"].items():
            assert sibling in p["parents"][gid]


def test_a_sibling_read_takes_no_spine(log):
    for p in log.passes:
        assert set(p["siblings"]) <= p["settled"]


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
    settled = gg._apply_reads(group_graph, edges, attrs, {}, {}, frozenset())
    assert settled == {"g"}
    assert set(lineage_predecessors(group_graph, edges, "g")) == {"r", "s"}


def test_a_held_sibling_still_settles_its_group(monkeypatch):
    group_graph, edges, attrs = _graph()
    add_edge(group_graph, edges, "s", "g", EdgeKind.LINEAGE)
    monkeypatch.setattr(gg, "_regraft_candidate", lambda *a, **k: "s")
    before = dict(edges)
    settled = gg._apply_reads(group_graph, edges, attrs, {}, {}, frozenset())
    assert settled == {"g"}
    assert edges == before


def test_the_spine_election_asks_only_for_a_spine(monkeypatch):
    calls: list[bool] = []

    def candidate(*args, allow_partial, **kwargs):
        calls.append(allow_partial)

    group_graph, edges, attrs = _graph()
    monkeypatch.setattr(gg, "_regraft_candidate", candidate)
    gg._elect_spine(group_graph, edges, attrs, None, "g")
    assert calls == [True]
