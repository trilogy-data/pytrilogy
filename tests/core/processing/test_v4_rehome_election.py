"""Window absorbers and row providers are elected over one graph, the one
stage 2 built; folds and providers are applied only once all are elected."""

from types import SimpleNamespace

import pytest

from tests.core.processing.test_v4_row_provider_election import _graph_state
from tests.core.processing.test_v4_window_absorber_election import _QUERY
from tests.core.processing.test_v4_window_basic_merge import _MODEL
from trilogy import Dialects, Environment
from trilogy.core import graph as nx
from trilogy.core.enums import Derivation
from trilogy.core.processing.v4_helper import group_graph as gg

_ELECT_ABSORBER = gg._elect_window_absorber
_ELECT_PROVIDER = gg._elect_row_provider


class _RehomeLog:
    def __init__(self) -> None:
        self.seen: list[tuple] = []
        self.absorbed: set[str] = set()
        self.provided: set[str] = set()

    def _record(self, group_graph, group_edges, attrs) -> None:
        self.seen.append(_graph_state(group_graph, group_edges, attrs))

    def absorber(self, group_graph, group_edges, attrs, concept_attrs, gid):
        self._record(group_graph, group_edges, attrs)
        parent = _ELECT_ABSORBER(group_graph, group_edges, attrs, concept_attrs, gid)
        if parent is not None:
            self.absorbed.add(gid)
        return parent

    def provider(self, group_graph, group_edges, attrs, *rest):
        self._record(group_graph, group_edges, attrs)
        provider = _ELECT_PROVIDER(group_graph, group_edges, attrs, *rest)
        if provider is not None:
            self.provided.add(rest[-1])
        return provider


@pytest.fixture
def log(monkeypatch) -> _RehomeLog:
    log = _RehomeLog()
    monkeypatch.setattr(gg, "_elect_window_absorber", log.absorber)
    monkeypatch.setattr(gg, "_elect_row_provider", log.provider)
    env = Environment()
    env.parse(_MODEL)
    Dialects.DUCK_DB.default_executor(environment=env).generate_sql(_QUERY)
    return log


def test_absorbers_and_providers_are_elected_over_one_graph(log):
    assert len(log.absorbed) >= 2
    assert log.provided
    assert all(state == log.seen[0] for state in log.seen)


def test_a_folded_basic_takes_no_row_provider(log):
    assert not log.absorbed & log.provided


def test_a_dropped_parent_that_is_folded_is_dropped_as_its_window(monkeypatch):
    group_graph = nx.DiGraph()
    group_graph.add_nodes_from(["g", gg.FINAL_NODE_ID])
    attrs = {"g": SimpleNamespace(derivation=Derivation.BASIC)}
    monkeypatch.setattr(
        gg,
        "_elect_row_provider",
        lambda *args: gg.RowProvider("p", drop=frozenset({"b", "r"})),
    )
    providers = gg._elect_row_providers(
        group_graph, {}, attrs, {}, {}, None, {"b": "w"}
    )
    assert providers["g"].drop == {"w", "r"}
