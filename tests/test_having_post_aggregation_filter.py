"""Post-aggregation HAVING on fields that are not SELECT outputs.

HAVING filters *after* aggregation (WHERE filters before). These exercise the
two ways a HAVING reference that is not a projected output is resolved:

* an aggregate (bare, off-grain, or nested) is promoted to a hidden output and
  computed at the select grain — adding it never changes the select grain because
  metrics do not contribute to grain;
* a dimension finer than / outside the select grain becomes a post-aggregation
  semijoin on the grain key (`key in (filter key where <predicate>)`), so the
  select-grain aggregate is computed over *all* rows and only the surviving groups
  are kept.
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
