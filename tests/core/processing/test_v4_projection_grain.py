"""A terminal projection stands at its grouping provider's grain before any row
provider is elected: the grain is stated by stage 2, not adopted by the
rehome."""

from types import SimpleNamespace

import pytest

from trilogy import Dialects, Environment
from trilogy.core import graph as nx
from trilogy.core.enums import Derivation
from trilogy.core.processing.v4_helper import group_graph as gg
from trilogy.core.processing.v4_helper.edges import EdgeKind, add_edge

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

    def state(self, attrs, buckets, grouping):
        self.built = {gid: a.grain_components for gid, a in attrs.items()}
        _STATE(attrs, buckets, grouping)

    def elect(self, group_graph, group_edges, attrs, *rest):
        self.grouping = rest[-1]
        self.at_election = {gid: a.grain_components for gid, a in attrs.items()}
        return _ELECT(group_graph, group_edges, attrs, *rest)


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


def test_a_projection_is_provided_by_its_grouping_provider():
    group_graph = nx.DiGraph()
    group_graph.add_nodes_from(["g", "p", "r", gg.FINAL_NODE_ID])
    edges: dict = {}
    for u, v in [("r", "p"), ("r", "g"), ("g", gg.FINAL_NODE_ID)]:
        add_edge(group_graph, edges, u, v, EdgeKind.LINEAGE)
    attrs = {
        "g": SimpleNamespace(grain_components=frozenset({"k"})),
        "p": SimpleNamespace(grain_components=frozenset({"k"})),
        "r": SimpleNamespace(grain_components=frozenset()),
    }
    provider = gg._elect_row_provider(
        group_graph, edges, attrs, {}, {}, None, {"g": "p"}, "g"
    )
    assert provider == gg.RowProvider("p", frozenset({"r"}))


def test_a_folded_projection_takes_no_grouping_provider(monkeypatch):
    group_graph = nx.DiGraph()
    group_graph.add_nodes_from(["g", gg.FINAL_NODE_ID])
    monkeypatch.setattr(gg, "_terminal_basic_grouping_provider", lambda *a: "p")
    monkeypatch.setattr(gg, "_inline_window", lambda *a: "w")
    attrs = {"g": SimpleNamespace(derivation=Derivation.BASIC)}
    assert gg._elect_grouping_providers(group_graph, {}, attrs, {}, None) == {}


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
