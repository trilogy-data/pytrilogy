"""A scalar BASIC folds into a WINDOW as its row provider: the first choice of
its election is a WINDOW that renders it inline."""

from types import SimpleNamespace

import pytest

from tests.core.processing.test_v4_window_basic_merge import _MODEL
from trilogy import Dialects, Environment
from trilogy.core.enums import Derivation
from trilogy.core.processing.v4_helper import group_graph as gg

_QUERY = """
auto wk_amt <- sum(amt) by cat, wk;
auto cat_amt <- sum(amt) by cat;
auto wk_ratio <- round(wk_amt / (lead(wk_amt, 1) over (partition by cat order by wk asc)), 2);
auto cat_ratio <- round(cat_amt / (lag(cat_amt, 1) over (order by cat asc)), 2);
select cat, wk, wk_ratio, cat_ratio;
"""


class _FoldLog:
    def __init__(self) -> None:
        self.elected: dict[str, gg.RowProvider] = {}
        self.members: dict[str, tuple[str, ...]] = {}
        self.folded: dict = {}

    def elect(self, group_graph, group_edges, attrs, *rest):
        providers = _ELECT(group_graph, group_edges, attrs, *rest)
        self.elected = providers
        self.members = {gid: attrs[gid].primary_members for gid in providers}
        return providers

    def rehome(self, group_graph, group_edges, attrs, *rest):
        _REHOME(group_graph, group_edges, attrs, *rest)
        self.folded = attrs


_ELECT = gg._elect_row_providers
_REHOME = gg._rehome_groups


@pytest.fixture
def log(monkeypatch) -> _FoldLog:
    log = _FoldLog()
    monkeypatch.setattr(gg, "_elect_row_providers", log.elect)
    monkeypatch.setattr(gg, "_rehome_groups", log.rehome)
    env = Environment()
    env.parse(_MODEL)
    Dialects.DUCK_DB.default_executor(environment=env).generate_sql(_QUERY)
    return log


def test_each_inline_provider_folds_its_basic(log):
    inline = {gid: p.parent for gid, p in log.elected.items() if p.inline}
    assert len(inline) >= 2
    attrs = log.folded
    for gid, parent in inline.items():
        assert gid not in attrs
        assert attrs[parent].derivation == Derivation.WINDOW
        assert set(log.members[gid]) <= set(attrs[parent].primary_members)


def test_a_provider_that_is_not_inline_keeps_its_group(log):
    for gid, provider in log.elected.items():
        if not provider.inline:
            assert gid in log.folded


def _basic(**kwargs) -> SimpleNamespace:
    base = {
        "derivation": Derivation.BASIC,
        "condition_atoms": (),
        "primary_members": ("m",),
        "grain_components": frozenset({"k"}),
        "input_concepts": ("x",),
    }
    return SimpleNamespace(**{**base, **kwargs})


def _window(**kwargs) -> SimpleNamespace:
    base = {
        "derivation": Derivation.WINDOW,
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
    def __init__(self) -> None:
        super().__init__()
        self.grouping: dict[str, str | None] = {}

    def elect(self, group_graph, group_edges, attrs, *rest):
        providers = super().elect(group_graph, group_edges, attrs, *rest)
        environment = rest[-1]
        self.grouping = {
            gid: gg._terminal_basic_grouping_provider(
                group_graph, attrs, gid, environment
            )
            for gid, p in providers.items()
            if p.inline
        }
        return providers


def test_an_inline_window_outranks_a_grouping_provider(monkeypatch, tmp_path):
    log = _OutrankLog()
    monkeypatch.setattr(gg, "_elect_row_providers", log.elect)
    monkeypatch.setattr(gg, "_rehome_groups", log.rehome)
    (tmp_path / "sales.preql").write_text(_HAVING_MODEL)
    env = Environment(working_path=tmp_path)
    Dialects.DUCK_DB.default_executor(environment=env).generate_sql(_HAVING_QUERY)
    assert log.grouping
    for gid, grouping in log.grouping.items():
        assert grouping is not None
        assert grouping != log.elected[gid].parent
        assert gid not in log.folded
