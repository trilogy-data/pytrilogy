"""A BASIC that reads its inputs off a dimension ROOT is re-homed onto it
as a read: every read is elected over one graph, a full-coverage
sibling outranks it, and it outranks a spine."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from trilogy import Dialects, Environment
from trilogy.core import graph as nx
from trilogy.core.enums import Derivation
from trilogy.core.processing.v4_helper import group_graph as gg
from trilogy.core.processing.v4_helper.edges import (
    EdgeKind,
    add_edge,
    lineage_predecessors,
)

_ADHOC04 = Path(__file__).parents[2] / "modeling" / "thelook_duckdb" / "adhoc04.preql"
_APPLY = gg._apply_reads


class _ReadLog:
    def __init__(self) -> None:
        self.reads: dict[str, gg.RowProvider] = {}
        self.applied: list[tuple] = []

    def apply(self, group_graph, group_edges, attrs, *rest):
        self.reads = gg._elect_reads(group_graph, group_edges, attrs, *rest)
        _APPLY(group_graph, group_edges, attrs, *rest)
        self.applied.append((group_graph.copy(), dict(group_edges)))


@pytest.fixture
def log(monkeypatch) -> _ReadLog:
    log = _ReadLog()
    monkeypatch.setattr(gg, "_apply_reads", log.apply)
    env, statements = Environment(working_path=_ADHOC04.parent).parse(
        _ADHOC04.read_text()
    )
    Dialects.DUCK_DB.default_executor(environment=env).generate_sql(statements[-1])
    return log


def test_each_dimension_read_replaces_the_roots_it_reads(log):
    minted = {gid: r for gid, r in log.reads.items() if r.minted is not None}
    assert minted
    for group_graph, group_edges in log.applied:
        for gid, read in minted.items():
            parents = set(lineage_predecessors(group_graph, group_edges, gid))
            assert read.minted.group_id in parents
            assert not parents & read.drop


def _graph() -> tuple[nx.DiGraph, dict, dict]:
    group_graph = nx.DiGraph()
    edges: dict = {}
    for u, v in [("r", "g"), ("q", "g"), ("g", gg.FINAL_NODE_ID)]:
        add_edge(group_graph, edges, u, v, EdgeKind.LINEAGE)
    attrs = {
        gid: SimpleNamespace(derivation=d)
        for gid, d in [
            ("g", Derivation.BASIC),
            ("r", Derivation.ROOT),
            ("q", Derivation.ROOT),
        ]
    }
    return group_graph, edges, attrs


def test_a_full_coverage_sibling_outranks_a_dimension(monkeypatch):
    group_graph, edges, attrs = _graph()
    monkeypatch.setattr(gg, "_regraft_candidate", lambda *a, **k: "s")
    monkeypatch.setattr(
        gg, "_synthetic_dimension_regraft_parent", lambda *a: gg.RowProvider("r")
    )
    reads = gg._elect_reads(
        group_graph, edges, attrs, gg.ConceptSets(), {}, {}, None, frozenset()
    )
    assert reads == {"g": gg.RowProvider("s")}


def test_a_placed_projection_reads_no_dimension(monkeypatch):
    group_graph, edges, attrs = _graph()
    monkeypatch.setattr(gg, "_regraft_candidate", lambda *a, **k: None)
    monkeypatch.setattr(
        gg, "_synthetic_dimension_regraft_parent", lambda *a: gg.RowProvider("r")
    )
    reads = gg._elect_reads(
        group_graph, edges, attrs, gg.ConceptSets(), {}, {}, None, frozenset({"g"})
    )
    assert reads == {}


def test_a_dimension_read_drops_the_roots_it_replaces(monkeypatch):
    group_graph, edges, attrs = _graph()
    monkeypatch.setattr(gg, "_regraft_candidate", lambda *a, **k: None)
    read = gg.RowProvider("r", frozenset({"q"}))
    monkeypatch.setattr(gg, "_synthetic_dimension_regraft_parent", lambda *a: read)
    gg._apply_reads(
        group_graph, edges, attrs, gg.ConceptSets(), {}, {}, None, frozenset()
    )
    assert set(lineage_predecessors(group_graph, edges, "g")) == {"r"}


def test_a_read_the_graph_already_holds_is_no_rewrite(monkeypatch):
    group_graph, edges, attrs = _graph()
    monkeypatch.setattr(gg, "_regraft_candidate", lambda *a, **k: None)
    monkeypatch.setattr(
        gg, "_synthetic_dimension_regraft_parent", lambda *a: gg.RowProvider("r")
    )
    gg._apply_reads(
        group_graph, edges, attrs, gg.ConceptSets(), {}, {}, None, frozenset()
    )
    assert set(lineage_predecessors(group_graph, edges, "g")) == {"r", "q"}
