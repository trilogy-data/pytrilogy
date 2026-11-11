"""A terminal projection is placed at its grouping provider before any row
provider is elected: stage 2 states its grain and its lineage edge, and the
rehome neither adopts the grain nor elects the edge."""

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

_MODEL = """
key order_id int;
property order_id.region string;
property order_id.amount float;
datasource orders (id: order_id, region: region, amount: amount)
grain (order_id) address orders_tbl;
"""

_QUERY = "select region, coalesce(sum(amount), 0) as total;"


class _GrainLog:
    def __init__(self) -> None:
        self.built: dict[str, frozenset[str]] = {}
        self.grouping: dict[str, str] = {}
        self.at_election: dict[str, frozenset[str]] = {}
        self.placed: frozenset[str] = frozenset()
        self.parents: dict[str, set[str]] = {}
        self.elected: dict = {}

    def state(self, attrs, buckets, grouping):
        self.built = {gid: a.grain_components for gid, a in attrs.items()}
        self.grouping = grouping
        _STATE(attrs, buckets, grouping)

    def elect(self, group_graph, group_edges, attrs, *rest):
        self.placed = rest[-1]
        self.at_election = {gid: a.grain_components for gid, a in attrs.items()}
        self.parents = {
            gid: set(lineage_predecessors(group_graph, group_edges, gid))
            for gid in self.grouping
        }
        self.elected = _ELECT(group_graph, group_edges, attrs, *rest)
        return self.elected


_STATE = gg._state_projection_grains
_ELECT = gg._elect_row_providers


@pytest.fixture
def log(monkeypatch) -> _GrainLog:
    log = _GrainLog()
    monkeypatch.setattr(gg, "_state_projection_grains", log.state)
    monkeypatch.setattr(gg, "_elect_row_providers", log.elect)
    env = Environment()
    env.parse(_MODEL)
    Dialects.DUCK_DB.default_executor(environment=env).generate_sql(_QUERY)
    return log


def test_each_projection_stands_at_its_providers_grain_when_providers_are_elected(
    log,
):
    assert log.grouping
    for gid, provider in log.grouping.items():
        assert log.at_election[gid] == log.at_election[provider]


def test_a_stated_grain_is_not_the_grain_stage_two_grouped_at(log):
    assert any(log.built[gid] != log.at_election[gid] for gid in log.grouping)


def test_a_stated_grain_is_set_on_the_group_and_its_bucket():
    attrs = {
        "g": SimpleNamespace(grain_components=frozenset({"k"})),
        "p": SimpleNamespace(grain_components=frozenset({"j"})),
    }
    buckets = {"g": SimpleNamespace(grain_components=frozenset({"k"}))}
    gg._state_projection_grains(attrs, buckets, {"g": "p"})
    assert attrs["g"].grain_components == {"j"}
    assert buckets["g"].grain_components == {"j"}


def test_each_placed_projection_reads_its_provider_when_providers_are_elected(
    log,
):
    assert log.placed
    for gid in log.placed:
        assert log.grouping[gid] in log.parents[gid]
        assert gid not in log.elected


def _placement_graph() -> tuple[nx.DiGraph, dict, dict]:
    group_graph = nx.DiGraph()
    group_graph.add_nodes_from(["g", "p", "r", gg.FINAL_NODE_ID])
    edges: dict = {}
    for u, v in [("r", "p"), ("r", "g"), ("g", gg.FINAL_NODE_ID)]:
        add_edge(group_graph, edges, u, v, EdgeKind.LINEAGE)
    attrs = {
        "g": SimpleNamespace(grain_components=frozenset({"k"})),
        "p": SimpleNamespace(grain_components=frozenset({"j"})),
        "r": SimpleNamespace(grain_components=frozenset()),
    }
    return group_graph, edges, attrs


def test_a_projection_is_placed_at_its_grouping_provider():
    group_graph, edges, attrs = _placement_graph()
    placements = gg._projection_placements(group_graph, edges, attrs, {"g": "p"})
    assert placements == {"g": gg.RowProvider("p", frozenset({"r"}))}


def test_placing_a_projection_states_its_grain_and_its_lineage():
    group_graph, edges, attrs = _placement_graph()
    placed = gg._place_projections(group_graph, edges, attrs, {}, {"g": "p"})
    assert placed == {"g"}
    assert attrs["g"].grain_components == {"j"}
    assert set(lineage_predecessors(group_graph, edges, "g")) == {"p"}


def test_a_projection_an_independent_sibling_needs_is_stated_but_not_placed():
    group_graph, edges, attrs = _placement_graph()
    group_graph.add_node("s")
    add_edge(group_graph, edges, "s", gg.FINAL_NODE_ID, EdgeKind.LINEAGE)
    attrs["s"] = SimpleNamespace(grain_components=frozenset({"j"}))
    placed = gg._place_projections(group_graph, edges, attrs, {}, {"g": "p"})
    assert placed == frozenset()
    assert attrs["g"].grain_components == {"j"}
    assert set(lineage_predecessors(group_graph, edges, "g")) == {"r"}


def test_a_placed_projection_is_not_elected(monkeypatch):
    group_graph, edges, attrs = _placement_graph()
    for gid, derivation in [
        ("g", Derivation.BASIC),
        ("p", Derivation.AGGREGATE),
        ("r", Derivation.ROOT),
    ]:
        attrs[gid].derivation = derivation
    monkeypatch.setattr(gg, "_elect_row_provider", lambda *a: gg.RowProvider("x"))
    providers = gg._elect_row_providers(
        group_graph, edges, attrs, {}, {}, None, frozenset({"g"})
    )
    assert providers == {}


def test_the_row_provider_election_takes_no_inline_window(monkeypatch):
    group_graph, edges, attrs = _placement_graph()
    monkeypatch.setattr(gg, "_inline_window", lambda *a: "w")
    monkeypatch.setattr(gg, "_regraft_candidate", lambda *a, **k: None)
    monkeypatch.setattr(gg, "_synthetic_dimension_regraft_parent", lambda *a: None)
    provider = gg._elect_row_provider(group_graph, edges, attrs, {}, {}, None, "g")
    assert provider is None


def test_a_folded_projection_takes_no_grouping_provider(monkeypatch):
    group_graph = nx.DiGraph()
    group_graph.add_nodes_from(["g", gg.FINAL_NODE_ID])
    monkeypatch.setattr(gg, "_terminal_basic_grouping_provider", lambda *a: "p")
    attrs = {"g": SimpleNamespace(derivation=Derivation.BASIC)}
    assert gg._elect_grouping_providers(group_graph, attrs, None, {"g": "w"}) == {}


def _siblings() -> tuple[nx.DiGraph, dict]:
    group_graph = nx.DiGraph()
    group_graph.add_nodes_from(["x", "y", "w", gg.FINAL_NODE_ID])
    group_graph.add_edges_from([("x", gg.FINAL_NODE_ID), ("y", gg.FINAL_NODE_ID)])
    attrs = {
        gid: SimpleNamespace(grain_components=frozenset({"s"}))
        for gid in ("x", "y", "w")
    }
    return group_graph, attrs


def test_a_sibling_at_its_own_provider_grain_is_independent():
    group_graph, attrs = _siblings()
    grouping = {"x": "w"}
    assert gg._grain_needed_by_independent_sibling(
        group_graph, attrs, "x", "w", grouping
    )


def test_a_sibling_placed_at_the_same_provider_is_not_independent():
    group_graph, attrs = _siblings()
    grouping = {"x": "w", "y": "w"}
    assert not gg._grain_needed_by_independent_sibling(
        group_graph, attrs, "x", "w", grouping
    )
