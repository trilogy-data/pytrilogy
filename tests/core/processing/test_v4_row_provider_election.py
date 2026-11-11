"""Every unplaced group's read, a sibling, a dimension or a spine, is elected
over one graph, and each is applied only once all are elected."""

import pytest

from tests.core.processing.test_v4_window_input_carry import _MODEL
from trilogy import Dialects, Environment
from trilogy.core.enums import Derivation
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
        self.elected: dict[str, gg.RowProvider] = {}
        self.built: tuple = ()
        self.applied: list[tuple] = []

    def elect(self, group_graph, group_edges, attrs, *rest):
        before = _graph_state(group_graph, group_edges, attrs)
        read = _ELECT(group_graph, group_edges, attrs, *rest)
        assert _graph_state(group_graph, group_edges, attrs) == before
        self.seen.append(before)
        if read is not None:
            self.elected[rest[-1]] = read
        return read

    def apply(self, group_graph, group_edges, attrs, *rest):
        self.built = _graph_state(group_graph, group_edges, attrs)
        _APPLY(group_graph, group_edges, attrs, *rest)
        self.applied.append((group_graph, group_edges, attrs))


_ELECT = gg._elect_read
_APPLY = gg._apply_reads


@pytest.fixture
def log(monkeypatch) -> _ElectionLog:
    log = _ElectionLog()
    monkeypatch.setattr(gg, "_elect_read", log.elect)
    monkeypatch.setattr(gg, "_apply_reads", log.apply)
    env = Environment()
    env.parse(_MODEL)
    Dialects.DUCK_DB.default_executor(environment=env).generate_sql(_QUERY)
    return log


def _spines(log: _ElectionLog) -> dict[str, gg.RowProvider]:
    _, _, attrs = log.applied[0]
    return {
        gid: read
        for gid, read in log.elected.items()
        if attrs[gid].derivation == Derivation.BASIC and not read.drop
    }


def test_every_read_is_elected_over_one_graph(log):
    assert len(_spines(log)) >= 2
    assert all(state == log.seen[0] for state in log.seen)
    assert log.seen[0] == log.built


def test_each_elected_read_becomes_its_groups_lineage_parent(log):
    group_graph, group_edges, _ = log.applied[0]
    for gid, read in log.elected.items():
        assert read.parent in lineage_predecessors(group_graph, group_edges, gid)


def test_a_spine_the_graph_already_holds_is_still_elected(log):
    edges, _, _ = log.built
    assert any((read.parent, gid) in edges for gid, read in _spines(log).items())
