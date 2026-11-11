"""Each unsettled group's spine is elected over one graph; every spine is
added only once all are elected."""

import pytest

from tests.core.processing.test_v4_window_input_carry import _MODEL
from trilogy import Dialects, Environment
from trilogy.core.processing.v4_helper import group_graph as gg
from trilogy.core.processing.v4_helper.edges import lineage_predecessors

_QUERY = "select vehicle, gap_class;"


def _graph_state(group_graph, group_edges, attrs) -> tuple:
    return (
        frozenset(group_graph.edges),
        {edge: repr(attrs_) for edge, attrs_ in group_edges.items()},
        {gid: a.grain_components for gid, a in attrs.items()},
    )


class _ElectionLog:
    def __init__(self) -> None:
        self.seen: list[tuple] = []
        self.elected: dict[str, str] = {}
        self.applied: list[tuple] = []
        self.rewrites: dict[str, str] = {}

    def elect(self, group_graph, group_edges, attrs, *rest):
        before = _graph_state(group_graph, group_edges, attrs)
        spine = _ELECT(group_graph, group_edges, attrs, *rest)
        assert _graph_state(group_graph, group_edges, attrs) == before
        self.seen.append(before)
        if spine is not None:
            self.elected[rest[-1]] = spine
        return spine

    def rehome(self, group_graph, group_edges, attrs, *rest):
        self.built = _graph_state(group_graph, group_edges, attrs)
        self.rewrites = gg._elect_spines(group_graph, group_edges, attrs, *rest)
        _REHOME(group_graph, group_edges, attrs, *rest)
        self.applied.append((group_graph, group_edges))


_ELECT = gg._elect_spine
_REHOME = gg._rehome_groups


@pytest.fixture
def log(monkeypatch) -> _ElectionLog:
    log = _ElectionLog()
    monkeypatch.setattr(gg, "_elect_spine", log.elect)
    monkeypatch.setattr(gg, "_rehome_groups", log.rehome)
    env = Environment()
    env.parse(_MODEL)
    Dialects.DUCK_DB.default_executor(environment=env).generate_sql(_QUERY)
    return log


def test_every_spine_is_elected_over_one_graph(log):
    assert len(log.elected) >= 2
    assert all(state == log.seen[0] for state in log.seen)


def test_each_elected_spine_becomes_its_groups_lineage_parent(log):
    group_graph, group_edges = log.applied[0]
    for gid, spine in log.elected.items():
        assert spine in lineage_predecessors(group_graph, group_edges, gid)


def test_a_spine_the_graph_already_holds_is_not_elected(log):
    edges, _, _ = log.built
    held = {gid for gid, spine in log.elected.items() if (spine, gid) in edges}
    assert held
    assert not held & set(log.rewrites)
