"""One election re-homes every unplaced group over one graph, as the rewrite
it makes, applied only once all are elected."""

from types import SimpleNamespace

import pytest

from tests.core.processing.test_v4_row_provider_election import _graph_state
from tests.core.processing.test_v4_window_absorber_election import _QUERY
from tests.core.processing.test_v4_window_basic_merge import _MODEL
from trilogy import Dialects, Environment
from trilogy.core import graph as nx
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


def test_providers_are_elected_over_one_graph(log):
    assert len(log.provided) >= 2
    assert all(state == log.seen[0] for state in log.seen)


def _graph(*edges) -> nx.DiGraph:
    group_graph = nx.DiGraph()
    group_graph.add_nodes_from(["g", "p", "r"])
    group_graph.add_edges_from(edges)
    return group_graph


def test_a_provider_the_graph_already_holds_is_no_rewrite():
    provider = gg.RowProvider("p", frozenset({"r"}))
    assert gg._as_rewrite(_graph(("p", "g")), "g", provider) is None


def test_a_rewrite_keeps_only_what_the_graph_lacks():
    provider = gg.RowProvider("p", frozenset({"r", "q"}))
    rewrite = gg._as_rewrite(_graph(("r", "g")), "g", provider)
    assert rewrite == gg.RowProvider("p", frozenset({"r"}))


def test_a_drop_on_a_held_parent_is_a_rewrite():
    provider = gg.RowProvider("p", frozenset({"r"}))
    rewrite = gg._as_rewrite(_graph(("p", "g"), ("r", "g")), "g", provider)
    assert rewrite == gg.RowProvider(None, frozenset({"r"}))


def test_a_minted_root_already_in_the_graph_is_not_minted_again():
    minted = SimpleNamespace(group_id="p")
    provider = gg.RowProvider("p", minted=minted)
    rewrite = gg._as_rewrite(_graph(("r", "g")), "g", provider)
    assert rewrite == gg.RowProvider("p")
