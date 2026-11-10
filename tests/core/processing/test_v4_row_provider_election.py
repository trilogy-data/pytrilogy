"""Each regraftable group's row provider is elected over the group graph as
stage 2 built it; every provider is applied only once all are elected."""

import pytest

from tests.core.processing.test_v4_window_avg_difference import _MODEL, _QUERY
from trilogy import Dialects, Environment
from trilogy.core.processing.v4_helper import group_graph as gg
from trilogy.core.processing.v4_helper.edges import lineage_predecessors


def _graph_state(group_graph, group_edges, attrs) -> tuple:
    return (
        frozenset(group_graph.edges),
        {edge: repr(attrs_) for edge, attrs_ in group_edges.items()},
        {gid: a.grain_components for gid, a in attrs.items()},
    )


class _ElectionLog:
    def __init__(self) -> None:
        self.seen: list[tuple] = []
        self.elected: dict[str, gg.RowProvider] = {}
        self.applied: list[tuple] = []
        self.rewrites: dict[str, gg.RowProvider] = {}

    def elect(self, group_graph, group_edges, attrs, *rest):
        before = _graph_state(group_graph, group_edges, attrs)
        provider = _ELECT(group_graph, group_edges, attrs, *rest)
        assert _graph_state(group_graph, group_edges, attrs) == before
        self.seen.append(before)
        if provider is not None:
            self.elected[rest[-1]] = provider
        return provider

    def rehome(self, group_graph, group_edges, attrs, *rest):
        self.built = _graph_state(group_graph, group_edges, attrs)
        self.rewrites = gg._elect_row_providers(group_graph, group_edges, attrs, *rest)
        _REHOME(group_graph, group_edges, attrs, *rest)
        self.applied.append((group_graph, group_edges))


_ELECT = gg._elect_row_provider
_REHOME = gg._rehome_groups


@pytest.fixture
def log(monkeypatch) -> _ElectionLog:
    log = _ElectionLog()
    monkeypatch.setattr(gg, "_elect_row_provider", log.elect)
    monkeypatch.setattr(gg, "_rehome_groups", log.rehome)
    env = Environment()
    env.parse(_MODEL)
    Dialects.DUCK_DB.default_executor(environment=env).generate_sql(_QUERY)
    return log


def test_every_row_provider_is_elected_over_the_graph_stage_two_built(log):
    assert len(log.elected) >= 2
    assert all(state == log.seen[0] for state in log.seen)


def test_each_elected_provider_becomes_its_groups_lineage_parent(log):
    group_graph, group_edges = log.applied[0]
    for gid, provider in log.elected.items():
        assert provider.parent in lineage_predecessors(group_graph, group_edges, gid)


def test_a_pick_the_graph_already_holds_is_not_elected(log):
    edges, _, grains = log.built
    held = {
        gid
        for gid, p in log.elected.items()
        if (p.parent, gid) in edges
        and p.grain in (None, grains[gid])
        and not {(d, gid) for d in p.drop} & edges
        and p.minted is None
    }
    assert held
    assert not held & set(log.rewrites)
