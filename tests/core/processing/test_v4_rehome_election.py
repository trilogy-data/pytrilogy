"""One election re-homes every group over the graph stage 2 built: a fold is
a row provider that renders its group inline, applied only once all are
elected."""

from types import SimpleNamespace

import pytest

from tests.core.processing.test_v4_row_provider_election import _graph_state
from tests.core.processing.test_v4_window_absorber_election import _QUERY
from tests.core.processing.test_v4_window_basic_merge import _MODEL
from trilogy import Dialects, Environment
from trilogy.core import graph as nx
from trilogy.core.enums import Derivation
from trilogy.core.processing.v4_helper import group_graph as gg

_ELECT_PROVIDER = gg._elect_row_provider


class _RehomeLog:
    def __init__(self) -> None:
        self.seen: list[tuple] = []
        self.provided: set[str] = set()

    def provider(self, group_graph, group_edges, attrs, *rest):
        self.seen.append(_graph_state(group_graph, group_edges, attrs))
        provider = _ELECT_PROVIDER(group_graph, group_edges, attrs, *rest)
        if provider is not None:
            self.provided.add(rest[-1])
        return provider


@pytest.fixture
def log(monkeypatch) -> _RehomeLog:
    log = _RehomeLog()
    monkeypatch.setattr(gg, "_elect_row_provider", log.provider)
    env = Environment()
    env.parse(_MODEL)
    Dialects.DUCK_DB.default_executor(environment=env).generate_sql(_QUERY)
    return log


def test_folds_and_providers_are_elected_over_one_graph(log):
    assert len(log.provided) >= 2
    assert all(state == log.seen[0] for state in log.seen)


def _elect(*args) -> gg.RowProvider:
    if args[-1] == "b":
        return gg.RowProvider("w", inline=True)
    return gg.RowProvider("p", drop=frozenset({"b", "r"}))


def test_a_dropped_parent_that_is_folded_is_dropped_as_its_window(monkeypatch):
    group_graph = nx.DiGraph()
    group_graph.add_nodes_from(["g", "b", gg.FINAL_NODE_ID])
    attrs = {gid: SimpleNamespace(derivation=Derivation.BASIC) for gid in "gb"}
    monkeypatch.setattr(gg, "_elect_row_provider", _elect)
    providers = gg._elect_row_providers(group_graph, {}, attrs, {}, {}, None)
    assert providers["b"].inline
    assert providers["g"].drop == {"w", "r"}
