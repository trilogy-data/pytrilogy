"""Each scalar BASIC's window absorber is elected over the group graph as stage
2 built it; every BASIC is folded only once all absorbers are elected."""

import pytest

from tests.core.processing.test_v4_row_provider_election import _graph_state
from tests.core.processing.test_v4_window_basic_merge import _MODEL
from trilogy import Dialects, Environment
from trilogy.core.processing.v4_helper import group_graph as gg

_QUERY = """
auto wk_amt <- sum(amt) by cat, wk;
auto cat_amt <- sum(amt) by cat;
auto wk_ratio <- round(wk_amt / (lead(wk_amt, 1) over (partition by cat order by wk asc)), 2);
auto cat_ratio <- round(cat_amt / (lag(cat_amt, 1) over (order by cat asc)), 2);
select cat, wk, wk_ratio, cat_ratio;
"""


class _AbsorberLog:
    def __init__(self) -> None:
        self.seen: list[tuple] = []
        self.elected: dict[str, str] = {}
        self.members: dict[str, tuple[str, ...]] = {}
        self.folded: list[dict] = []

    def elect(self, group_graph, group_edges, attrs, concept_attrs, gid):
        before = _graph_state(group_graph, group_edges, attrs)
        parent = _ELECT(group_graph, group_edges, attrs, concept_attrs, gid)
        assert _graph_state(group_graph, group_edges, attrs) == before
        self.seen.append(before)
        if parent is not None:
            self.elected[gid] = parent
            self.members[gid] = attrs[gid].primary_members
        return parent

    def merge(self, group_graph, group_edges, attrs, *rest):
        _MERGE(group_graph, group_edges, attrs, *rest)
        self.folded.append(attrs)


_ELECT = gg._elect_window_absorber
_MERGE = gg._merge_basic_into_window_parent


@pytest.fixture
def log(monkeypatch) -> _AbsorberLog:
    log = _AbsorberLog()
    monkeypatch.setattr(gg, "_elect_window_absorber", log.elect)
    monkeypatch.setattr(gg, "_merge_basic_into_window_parent", log.merge)
    env = Environment()
    env.parse(_MODEL)
    Dialects.DUCK_DB.default_executor(environment=env).generate_sql(_QUERY)
    return log


def test_every_window_absorber_is_elected_over_the_graph_stage_two_built(log):
    assert len(log.elected) >= 2
    assert all(state == log.seen[0] for state in log.seen)


def test_each_elected_basic_folds_into_its_window(log):
    attrs = log.folded[0]
    for gid, parent in log.elected.items():
        assert gid not in attrs
        assert attrs[parent].derivation == gg.Derivation.WINDOW
        assert set(log.members[gid]) <= set(attrs[parent].primary_members)
