"""A BASIC that reads its inputs off a dimension ROOT is re-homed onto it
before the rehome: every read is elected over one graph, a full-coverage
sibling outranks it, and the row-provider election takes none."""

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
_READ = gg._read_dimensions
_PROVIDERS = gg._elect_row_providers


class _ReadLog:
    def __init__(self) -> None:
        self.reads: dict[str, gg.RowProvider] = {}
        self.settled: frozenset[str] = frozenset()
        self.at_election: list[tuple] = []

    def read(self, group_graph, group_edges, attrs, buckets, concept_attrs, placed):
        self.reads = gg._elect_dimension_reads(
            group_graph, group_edges, attrs, buckets, concept_attrs, placed
        )
        return _READ(group_graph, group_edges, attrs, buckets, concept_attrs, placed)

    def providers(self, group_graph, group_edges, attrs, environment, settled):
        self.settled = settled
        self.at_election.append((group_graph.copy(), dict(group_edges)))
        return _PROVIDERS(group_graph, group_edges, attrs, environment, settled)


@pytest.fixture
def log(monkeypatch) -> _ReadLog:
    log = _ReadLog()
    monkeypatch.setattr(gg, "_read_dimensions", log.read)
    monkeypatch.setattr(gg, "_elect_row_providers", log.providers)
    env, statements = Environment(working_path=_ADHOC04.parent).parse(
        _ADHOC04.read_text()
    )
    Dialects.DUCK_DB.default_executor(environment=env).generate_sql(statements[-1])
    return log


def test_every_dimension_is_read_before_any_row_provider_is_elected(log):
    minted = {gid: r for gid, r in log.reads.items() if r.minted is not None}
    assert minted
    for group_graph, group_edges in log.at_election:
        for gid, read in minted.items():
            parents = set(lineage_predecessors(group_graph, group_edges, gid))
            assert read.minted.group_id in parents
            assert not parents & read.drop


def test_a_dimension_read_takes_no_row_provider(log):
    assert set(log.reads) <= log.settled


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
    reads = gg._elect_dimension_reads(group_graph, edges, attrs, {}, {}, frozenset())
    assert reads == {}


def test_a_placed_projection_reads_no_dimension(monkeypatch):
    group_graph, edges, attrs = _graph()
    monkeypatch.setattr(gg, "_regraft_candidate", lambda *a, **k: None)
    monkeypatch.setattr(
        gg, "_synthetic_dimension_regraft_parent", lambda *a: gg.RowProvider("r")
    )
    reads = gg._elect_dimension_reads(
        group_graph, edges, attrs, {}, {}, frozenset({"g"})
    )
    assert reads == {}


def test_a_dimension_read_drops_the_roots_it_replaces(monkeypatch):
    group_graph, edges, attrs = _graph()
    monkeypatch.setattr(gg, "_regraft_candidate", lambda *a, **k: None)
    read = gg.RowProvider("r", frozenset({"q"}))
    monkeypatch.setattr(gg, "_synthetic_dimension_regraft_parent", lambda *a: read)
    settled = gg._read_dimensions(group_graph, edges, attrs, {}, {}, frozenset())
    assert settled == {"g"}
    assert set(lineage_predecessors(group_graph, edges, "g")) == {"r"}


def test_a_read_that_changes_nothing_still_settles_its_group(monkeypatch):
    group_graph, edges, attrs = _graph()
    monkeypatch.setattr(gg, "_regraft_candidate", lambda *a, **k: None)
    monkeypatch.setattr(
        gg, "_synthetic_dimension_regraft_parent", lambda *a: gg.RowProvider("r")
    )
    settled = gg._read_dimensions(group_graph, edges, attrs, {}, {}, frozenset())
    assert settled == {"g"}
    assert set(lineage_predecessors(group_graph, edges, "g")) == {"r", "q"}


def test_the_row_provider_election_takes_no_dimension(monkeypatch):
    group_graph, edges, attrs = _graph()
    monkeypatch.setattr(gg, "_regraft_candidate", lambda *a, **k: None)
    monkeypatch.setattr(
        gg, "_synthetic_dimension_regraft_parent", lambda *a: gg.RowProvider("r")
    )
    assert gg._elect_row_provider(group_graph, edges, attrs, None, "g") is None
