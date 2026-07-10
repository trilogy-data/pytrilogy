"""Post-aggregation HAVING on fields that are not SELECT outputs.

HAVING filters *after* aggregation (WHERE filters before). These exercise the
ways a HAVING reference that is not a projected output is resolved:

* a bare or coarser aggregate (its value is one-per-grain-row) is promoted to a
  hidden output and computed at the select grain — broadcasting it never changes
  the grain;
* a dimension finer than / outside the select grain, OR a *finer* off-grain
  aggregate (`agg(x) by k` where `k` is finer than the grain), becomes a
  post-aggregation semijoin on the grain key (`key in (filter key where
  <predicate>)`), so the select-grain aggregate is computed over *all* rows and
  only the surviving groups are kept — promoting a finer aggregate would instead
  fan the output out (the q21 bug).

Whatever the resolution, HAVING is a filter and must never change the select
grain; ``test_having_never_changes_select_grain`` asserts that invariant across a
grain × reference-kind matrix.
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

MODEL = """
key item_id int;
property item_id.brand_id int;
property item_id.class_id int;
key sale_id int;
property sale_id.item_id int;
property sale_id.region string;
property sale_id.quantity int;

datasource items ( id: item_id, brand: brand_id, class: class_id)
grain (item_id)
query '''select * from (values (1,10,3),(2,10,5),(3,20,3),(4,30,5)) as t(id,brand,class)''';

datasource sales ( id: sale_id, item: item_id, reg: region, qty: quantity)
grain (sale_id)
query '''select * from (values
    (1,1,'E',100),(2,2,'E',50),(3,3,'W',200),(4,4,'E',40),(5,1,'W',7)
) as t(id,item,reg,qty)''';
"""


def _exec():
    e = Dialects.DUCK_DB.default_executor(environment=Environment())
    e.parse_text(MODEL)
    return e


def _rows(body: str):
    return [tuple(r) for r in _exec().execute_query(body).fetchall()]


def _sql(body: str) -> str:
    return "\n".join(_exec().generate_sql(body)).lower()


# --- grain × having invariant matrix -----------------------------------------
#
# The defining property of HAVING is that it is a *filter* on the select-grain
# rows: it may drop groups but must never change the grain. So for every select
# shape, adding any HAVING clause must leave the output a subset of the no-HAVING
# output — same grain keys, identical aggregate values, no duplicated keys. A
# clause that fans the result out (the q21 finer-aggregate bug) violates this by
# either repeating a row or emitting a row with an aggregate value the ungrouped
# query never produced. The matrix crosses single- and composite-key grains with
# every HAVING reference kind (bare/coarse/finer aggregate, filtered aggregate,
# finer dimension, mixed, and the projected aggregate itself).

_SELECTS = {
    "single_key": "select brand_id, sum(quantity) as total",
    "composite_key": "select brand_id, region, sum(quantity) as total",
}

_HAVINGS = {
    "bare_aggregate": "count(sale_id) > 1",
    "coarse_by_aggregate": "sum(quantity) by brand_id > 0",
    "finer_aggregate": "count(sale_id) by item_id >= 2",
    "finer_filtered_aggregate": "count(sale_id ? region = 'W') by item_id >= 1",
    "finer_dimension": "class_id = 3",
    "mixed_dim_and_finer_aggregate": "class_id = 3 and count(sale_id) by item_id >= 1",
    "projected_aggregate": "sum(quantity) > 50",
}


@pytest.mark.parametrize("select_id", list(_SELECTS))
@pytest.mark.parametrize("having_id", list(_HAVINGS))
def test_having_never_changes_select_grain(select_id, having_id):
    select = _SELECTS[select_id]
    baseline = set(_rows(select + ";"))
    filtered = _rows(f"{select} having {_HAVINGS[having_id]};")
    assert len(filtered) == len(set(filtered)), "HAVING fanned out (duplicate rows)"
    assert set(filtered).issubset(
        baseline
    ), "HAVING produced rows absent from the no-HAVING result — the grain changed"
    assert len(filtered) <= len(baseline)


# --- aggregate references (grain-safe hidden promotion) ----------------------


def test_bare_aggregate_in_having_not_in_select():
    # Filter on a *different* aggregate than the projected one. Sale counts per
    # brand: 10 -> 3, 20 -> 1, 30 -> 1; keep brands with more than one sale.
    rows = _rows(
        "select brand_id, sum(quantity) as total "
        "having count(sale_id) > 1 order by brand_id;"
    )
    assert rows == [(10, 157)]


def test_aggregate_in_having_does_not_appear_in_output():
    sql = _sql("select brand_id, sum(quantity) as total having count(sale_id) > 1;")
    assert "invalid_reference_bug" not in sql
    # the filter aggregate is hidden, not a projected column
    assert sql.count('as "total"') <= 1


# --- finer-grain aggregate semijoin (off-grain `by`) -------------------------


def test_finer_grain_aggregate_having_does_not_fan_out():
    # `count(sale_id) by item_id` is at item grain, finer than the {brand_id}
    # select grain. Only item 1 has >= 2 sales (it is brand 10), so brand 10 is
    # the single surviving group and its total spans all of brand 10's sales.
    rows = _rows(
        "select brand_id, sum(quantity) as total "
        "having count(sale_id) by item_id >= 2 order by brand_id;"
    )
    assert rows == [(10, 157)]


def test_finer_grain_aggregate_having_renders_semijoin():
    sql = _sql(
        "select brand_id, sum(quantity) as total "
        "having count(sale_id) by item_id >= 2;"
    )
    # filtered as a grain-key semijoin, never promoted to a fan-out output
    assert " in (select" in sql
    assert "invalid_reference_bug" not in sql


def test_multiple_finer_grain_aggregate_having_conditions():
    # Two order-grain (here item-grain) conditions, as in TPC-H Q21: each is its
    # own semijoin and the output stays at the {brand_id} grain (one row max).
    # Item 1 (brand 10) has >= 2 sales and a 'W' sale; brand 10 survives both.
    rows = _rows(
        "select brand_id, sum(quantity) as total "
        "having count(sale_id) by item_id >= 2 "
        "and count(sale_id ? region = 'W') by item_id >= 1 "
        "order by brand_id;"
    )
    assert rows == [(10, 157)]


def test_finer_grain_aggregate_having_with_coarser_aggregate():
    # The coarse `sum(quantity) > 180` (select grain) is a hidden promotion; the
    # finer `count(sale_id) by item_id >= 2` is a semijoin. Brand 10 has a >= 2
    # item (passes the semijoin) and total 157 < 180 (fails the coarse filter);
    # nothing survives both.
    rows = _rows(
        "select brand_id, sum(quantity) as total "
        "having count(sale_id) by item_id >= 2 and sum(quantity) > 180 "
        "order by brand_id;"
    )
    assert rows == []


# --- finer-dimension semijoin (single-key grain) ----------------------------


def test_finer_dim_having_is_post_aggregation_semijoin():
    # brand 10 has a class-3 item (item 1) and a class-5 item (item 2); the total
    # must still include the class-5 sales (post-aggregation semantics).
    rows = _rows(
        "select brand_id, sum(quantity) as total "
        "having class_id = 3 order by brand_id;"
    )
    assert rows == [(10, 157), (20, 200)]


def test_finer_dim_having_differs_from_where():
    # WHERE filters before aggregation, so brand 10 only sums its class-3 sales.
    rows = _rows(
        "select brand_id, sum(quantity) as total "
        "where class_id = 3 order by brand_id;"
    )
    assert rows == [(10, 107), (20, 200)]


def test_finer_dim_having_renders_membership():
    sql = _sql("select brand_id, sum(quantity) as total having class_id = 3;")
    assert " in (select" in sql
    assert "invalid_reference_bug" not in sql


# --- composite-grain semijoin (correlated keys) -----------------------------


def test_composite_grain_finer_dim_semijoin_is_correlated():
    rows = _rows(
        "select brand_id, region, sum(quantity) as total "
        "having class_id = 3 order by brand_id, region;"
    )
    # surviving (brand, region) groups have a class-3 row; totals over all items.
    assert rows == [(10, "E", 150), (10, "W", 7), (20, "W", 200)]


# --- WHERE-universe consistency ---------------------------------------------


def test_finer_dim_having_respects_where_universe():
    # WHERE region='W' is the pre-aggregation universe. No brand has a class-5 row
    # in region 'W' (the class-5 items only sell in 'E'), so the post-aggregation
    # semijoin must keep nothing — the existence set is evaluated within WHERE.
    rows = _rows(
        "select brand_id, sum(quantity) as total "
        "where region = 'W' having class_id = 5 order by brand_id;"
    )
    assert rows == []


# --- mixed aggregate + dimension predicate ----------------------------------


def test_mixed_dim_and_aggregate_having():
    rows = _rows(
        "select brand_id, sum(quantity) as total "
        "having class_id = 3 and sum(quantity) > 180 order by brand_id;"
    )
    # full totals (10 -> 157, 20 -> 200); class-3 set is {10, 20}; > 180 keeps 20.
    assert rows == [(20, 200)]


# --- BASIC-wrapped grainless scalar comparison (q44) --------------------------
#
# `having <select-grain agg> > <grainless scalar * literal>` combined with an
# off-grain WHERE dimension. The BASIC wrapper (`base * 0.5`) left the scalar
# tagged MULTI_ROW, so instead of hidden scalar promotion the predicate was
# misrouted into the finer-dim grain-key semijoin, whose filter CTE grouped by
# the WHERE dimension and emitted the ungrouped grain key (DuckDB binder error).


def test_having_vs_basic_wrapped_grainless_scalar_with_offgrain_where():
    # E-region item averages: 1 -> 100, 2 -> 50, 4 -> 40; every threshold form
    # (half of filtered avg 63.33 or unfiltered avg 79.4) keeps all three.
    rows = _rows(
        "auto base <- avg(quantity) by *;\n"
        "auto threshold <- base * 0.5;\n"
        "where region = 'E' "
        "select item_id, avg(quantity) as avg_qty "
        "having avg_qty > threshold;"
    )
    assert sorted(rows) == [(1, 100.0), (2, 50.0), (4, 40.0)]


def test_having_basic_identity_wrapper_matches_bare_scalar():
    bare = _rows(
        "auto threshold <- avg(quantity) by *;\n"
        "where region = 'E' "
        "select item_id, avg(quantity) as avg_qty "
        "having avg_qty > threshold;"
    )
    wrapped = _rows(
        "auto base <- avg(quantity) by *;\n"
        "auto threshold <- base * 1.0;\n"
        "where region = 'E' "
        "select item_id, avg(quantity) as avg_qty "
        "having avg_qty > threshold;"
    )
    assert sorted(wrapped) == sorted(bare)


# --- named off-grain threshold + off-grain WHERE (q44 silent-empty) ----------
#
# `having <select-grain agg> > <named agg grained by an off-grain dim>` with a
# query WHERE on that same off-grain dim. The HAVING is lowered to a value-
# membership semijoin whose subselect recomputes the select-grain aggregate; the
# query WHERE must be pushed pre-aggregation into that subselect (as it is on the
# output path) or the membership compares a whole-universe aggregate against the
# filtered output value and silently drops matching rows.


def test_having_named_offgrain_threshold_pushes_where_into_membership_aggregate():
    # region-'E' item averages: item1 100 (its 'W' sale excluded), item2 50,
    # item4 40; region_avg('E')=63.33 so threshold 31.67 keeps all three. item1
    # also sells in 'W' (7), so its whole-universe avg (53.5) differs from its
    # filtered 100 — the membership must compare the filtered value or item1 drops.
    rows = _rows(
        "auto region_avg <- avg(quantity) by region;\n"
        "auto threshold <- 0.5 * region_avg;\n"
        "auto item_avg <- avg(quantity) by item_id;\n"
        "where region = 'E' "
        "select brand_id, item_avg "
        "having item_avg > threshold "
        "order by item_avg;"
    )
    assert sorted(rows) == [(10, 50.0), (10, 100.0), (30, 40.0)]


def test_having_named_offgrain_threshold_matches_inline_form():
    named = _rows(
        "auto region_avg <- avg(quantity) by region;\n"
        "auto threshold <- 0.5 * region_avg;\n"
        "auto item_avg <- avg(quantity) by item_id;\n"
        "where region = 'E' "
        "select brand_id, item_avg having item_avg > threshold;"
    )
    inline = _rows(
        "auto region_avg <- avg(quantity) by region;\n"
        "auto item_avg <- avg(quantity) by item_id;\n"
        "where region = 'E' "
        "select brand_id, item_avg having item_avg > 0.5 * region_avg;"
    )
    assert sorted(named) == sorted(inline)


# --- clean errors ------------------------------------------------------------


def test_undefined_having_reference_errors_cleanly():
    from trilogy.core.exceptions import InvalidSyntaxException

    with pytest.raises((InvalidSyntaxException, SyntaxError)) as exc:
        _sql("select brand_id, sum(quantity) as total having nonexistent = 3;")
    assert "not defined" in str(exc.value)


def test_scalar_select_finer_dim_having_errors_cleanly():
    from trilogy.core.exceptions import InvalidSyntaxException

    with pytest.raises((InvalidSyntaxException, SyntaxError)) as exc:
        _sql("select sum(quantity) as total having class_id = 3;")
    assert "where" in str(exc.value).lower()
