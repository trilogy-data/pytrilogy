"""One election re-homes every unsettled group over one graph, as the edge
it adds, applied only once all are elected."""

from types import SimpleNamespace

import pytest

from tests.core.processing.test_v4_row_provider_election import (
    _MODEL,
    _QUERY,
    _graph_state,
)
from trilogy import Dialects, Environment
from trilogy.core import graph as nx
from trilogy.core.enums import Derivation
from trilogy.core.processing.v4_helper import group_graph as gg
from trilogy.core.processing.v4_helper.edges import EdgeAttrs, EdgeKind

_ELECT_SPINE = gg._elect_spine


class _RehomeLog:
    def __init__(self) -> None:
        self.seen: list[tuple] = []
        self.provided: set[str] = set()

    def provider(self, group_graph, group_edges, attrs, *rest):
        self.seen.append(_graph_state(group_graph, group_edges, attrs))
        provider = _ELECT_SPINE(group_graph, group_edges, attrs, *rest)
        if provider is not None:
            self.provided.add(rest[-1])
        return provider


@pytest.fixture
def log(monkeypatch) -> _RehomeLog:
    log = _RehomeLog()
    monkeypatch.setattr(gg, "_elect_spine", log.provider)
    env = Environment()
    env.parse(_MODEL)
    Dialects.DUCK_DB.default_executor(environment=env).generate_sql(_QUERY)
    return log


def test_spines_are_elected_over_one_graph(log):
    assert len(log.provided) >= 2
    assert all(state == log.seen[0] for state in log.seen)


def _graph(*edges) -> nx.DiGraph:
    group_graph = nx.DiGraph()
    group_graph.add_nodes_from(["g", "p", gg.FINAL_NODE_ID])
    group_graph.add_edges_from(edges)
    return group_graph


def test_a_spine_the_graph_already_holds_is_no_rewrite(monkeypatch):
    monkeypatch.setattr(gg, "_elect_spine", lambda *a: "p")
    attrs = {
        "g": SimpleNamespace(derivation=Derivation.BASIC),
        "p": SimpleNamespace(derivation=Derivation.AGGREGATE),
    }
    assert gg._elect_spines(_graph(("p", "g")), {}, attrs, None, frozenset()) == {}


def test_a_spine_is_one_lineage_edge(monkeypatch):
    monkeypatch.setattr(gg, "_elect_spine", lambda *a: "p")
    attrs = {
        "g": SimpleNamespace(derivation=Derivation.BASIC),
        "p": SimpleNamespace(derivation=Derivation.AGGREGATE),
    }
    group_graph, edges = _graph(), {}
    gg._rehome_groups(group_graph, edges, attrs, None, frozenset())
    assert edges == {("p", "g"): EdgeAttrs(EdgeKind.LINEAGE)}
