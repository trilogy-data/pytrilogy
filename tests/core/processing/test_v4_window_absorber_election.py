"""A scalar BASIC folds into the WINDOW that renders it inline. Every fold is
elected over the graph stage 2 built, outranks a grouping provider, and is
made before any row provider is elected."""

from types import SimpleNamespace

import pytest

from tests.core.processing.test_v4_window_basic_merge import _MODEL
from trilogy import Dialects, Environment
from trilogy.core import graph as nx
from trilogy.core.enums import Derivation
from trilogy.core.processing.v4_helper import group_graph as gg
from trilogy.core.processing.v4_helper.edges import (
    EdgeKind,
    add_edge,
    lineage_predecessors,
)

_QUERY = """
auto wk_amt <- sum(amt) by cat, wk;
auto cat_amt <- sum(amt) by cat;
auto wk_ratio <- round(wk_amt / (lead(wk_amt, 1) over (partition by cat order by wk asc)), 2);
auto cat_ratio <- round(cat_amt / (lag(cat_amt, 1) over (order by cat asc)), 2);
select cat, wk, wk_ratio, cat_ratio;
"""


class _FoldLog:
    def __init__(self) -> None:
        self.folds: dict[str, str] = {}
        self.members: dict[str, tuple[str, ...]] = {}
        self.at_election: set[str] = set()
        self.grouping: dict[str, str] = {}
        self.folded: dict = {}

    def elect(self, group_graph, group_edges, attrs, concept_attrs):
        self.folds = _ELECT(group_graph, group_edges, attrs, concept_attrs)
        self.members = {gid: attrs[gid].primary_members for gid in self.folds}
        return self.folds

    def elect_grouping(self, group_graph, attrs, environment, folds):
        self.environment = environment
        self.grouping = _GROUPING(group_graph, attrs, environment, folds)
        return self.grouping

    def elect_providers(self, group_graph, group_edges, attrs, *rest):
        self.at_election = set(group_graph.nodes)
        self.folded = attrs
        return _PROVIDERS(group_graph, group_edges, attrs, *rest)


_ELECT = gg._elect_folds
_GROUPING = gg._elect_grouping_providers
_PROVIDERS = gg._elect_row_providers


def _patch(monkeypatch, log: _FoldLog) -> _FoldLog:
    monkeypatch.setattr(gg, "_elect_folds", log.elect)
    monkeypatch.setattr(gg, "_elect_grouping_providers", log.elect_grouping)
    monkeypatch.setattr(gg, "_elect_row_providers", log.elect_providers)
    return log


@pytest.fixture
def log(monkeypatch) -> _FoldLog:
    log = _patch(monkeypatch, _FoldLog())
    env = Environment()
    env.parse(_MODEL)
    Dialects.DUCK_DB.default_executor(environment=env).generate_sql(_QUERY)
    return log


def test_each_fold_renders_its_basic_in_its_window(log):
    assert len(log.folds) >= 2
    attrs = log.folded
    for gid, window in log.folds.items():
        assert gid not in attrs
        assert attrs[window].derivation == Derivation.WINDOW
        assert set(log.members[gid]) <= set(attrs[window].primary_members)


def test_every_fold_is_made_before_any_row_provider_is_elected(log):
    assert log.folds
    assert not set(log.folds) & log.at_election
    assert set(log.folds.values()) <= log.at_election


def test_a_folded_basic_takes_no_grouping_provider(log):
    assert not set(log.folds) & set(log.grouping)


def test_a_parent_a_placement_drops_is_gone_before_it_folds():
    group_graph = nx.DiGraph()
    edges: dict = {}
    for u, v in [("b", "p"), ("b", "g"), ("r", "g"), ("g", gg.FINAL_NODE_ID)]:
        add_edge(group_graph, edges, u, v, EdgeKind.LINEAGE)
    group_graph.add_node("w")
    attrs = {
        gid: SimpleNamespace(
            derivation=d,
            grain_components=frozenset(),
            primary_members=(gid,),
            member_depths={},
        )
        for gid, d in [
            ("g", Derivation.BASIC),
            ("b", Derivation.BASIC),
            ("p", Derivation.AGGREGATE),
            ("r", Derivation.ROOT),
            ("w", Derivation.WINDOW),
        ]
    }
    gg._place_projections(group_graph, edges, attrs, {}, {"g": "p"})
    gg._fold_groups(group_graph, edges, attrs, {}, {"b": "w"})
    assert "b" not in group_graph
    assert set(lineage_predecessors(group_graph, edges, "g")) == {"p", "r"}


def _basic(**kwargs) -> SimpleNamespace:
    base = {
        "derivation": Derivation.BASIC,
        "condition_atoms": (),
        "primary_members": ("m",),
        "member_depths": {},
        "grain_components": frozenset({"k"}),
        "input_concepts": ("x",),
    }
    return SimpleNamespace(**{**base, **kwargs})


def _window(**kwargs) -> SimpleNamespace:
    base = {
        "derivation": Derivation.WINDOW,
        "primary_members": ("x",),
        "member_depths": {},
        "grain_components": frozenset({"k"}),
        "output_concepts": ("x",),
        "input_concepts": (),
    }
    return SimpleNamespace(**{**base, **kwargs})


def _inline(attrs) -> bool:
    return gg._renders_inline(attrs, {}, "g", "w")


def test_a_same_grain_window_carrying_every_input_renders_inline():
    assert _inline({"g": _basic(), "w": _window()})


def test_a_basic_holding_a_row_shape_barrier_member_has_no_inline_window():
    attrs = {"g": _basic()}
    concept_attrs = {"m": SimpleNamespace(derivation=Derivation.AGGREGATE)}
    assert gg._inline_window(None, {}, attrs, concept_attrs, "g") is None


def test_a_window_at_another_grain_does_not_render_inline():
    assert not _inline({"g": _basic(), "w": _window(grain_components=frozenset())})


def test_a_window_missing_an_input_does_not_render_inline():
    assert not _inline({"g": _basic(input_concepts=("x", "y")), "w": _window()})


def test_an_aggregate_provider_does_not_render_inline():
    assert not _inline({"g": _basic(), "w": _window(derivation=Derivation.AGGREGATE)})


_HAVING_MODEL = """
key sale_id int;
property sale_id.store int;
property sale_id.day int;
property sale_id.amt float;
datasource sales (sid: sale_id, st: store, dy: day, amt: amt)
grain (sale_id) address sales_tbl;
"""

_HAVING_QUERY = """
import sales as sales;
select
    sales.store as store,
    sales.day as day,
    sum(sales.amt) as amt,
    max(sum(sales.amt)) over (partition by sales.store order by sales.day) as rm
having max(sum(sales.amt)) over (partition by sales.store order by sales.day) > 0
order by store asc;
"""


class _OutrankLog(_FoldLog):
    def elect_grouping(self, group_graph, attrs, environment, folds):
        self.outranked = {
            gid: gg._terminal_basic_grouping_provider(
                group_graph, attrs, gid, environment
            )
            for gid in folds
        }
        return super().elect_grouping(group_graph, attrs, environment, folds)


def test_a_fold_outranks_a_grouping_provider(monkeypatch, tmp_path):
    log = _OutrankLog()
    _patch(monkeypatch, log)
    (tmp_path / "sales.preql").write_text(_HAVING_MODEL)
    env = Environment(working_path=tmp_path)
    Dialects.DUCK_DB.default_executor(environment=env).generate_sql(_HAVING_QUERY)
    assert log.outranked
    for gid, grouping in log.outranked.items():
        assert grouping is not None
        assert grouping != log.folds[gid]
        assert gid not in log.grouping
        assert gid not in log.folded
