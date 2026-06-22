"""Repro: a window-over-aggregate predicate in HAVING lowers to a QUALIFY whose
window is RE-RENDERED at the outer scope, where the window's inner aggregate is
not resolvable -> the rendered SQL contains `INVALID_REFERENCE_BUG` and
`generate_sql` raises `ValueError: Invalid reference string found in query`.

This is the still-open window-in-HAVING bug (see
evals/tpcds_agent/bug_window_function_in_having.md). The qualify-routing in
`trilogy/dialect/base.py` (render of a CTE condition, ~L1979-2050) DOES detect the
window and move it to QUALIFY, but it re-renders the whole window expression
`max(sum(amt)) over (...)` from scratch. The inner `sum(amt)` is materialized only
in an upstream CTE as `_virt_agg_sum_*` and is not in scope at the final
SELECT/QUALIFY, so it renders as `INVALID_REFERENCE_BUG`.

The window column is ALREADY materialized as an output (`rm` below) in the
upstream CTE; the QUALIFY should reference that materialized column (or the bug
should lower to a wrapper-select + outer WHERE on the materialized column, which is
the dialect-agnostic form the `control` test confirms already works).

Surfaced on enriched TPC-DS q51 (web-vs-store running-max compare) and q75
(year-over-year lag ratio) — ~4.5M tokens combined.

When fixed: delete the xfail markers; both tests assert clean SQL.
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

SALES = """key sale_id int;
property sale_id.store int;
property sale_id.day int;
property sale_id.amt float;
datasource sales (sid: sale_id, st: store, dy: day, amt: amt)
grain (sale_id) address sales_tbl;
"""


@pytest.fixture
def models(tmp_path: Path) -> Path:
    (tmp_path / "sales.preql").write_text(SALES)
    return tmp_path


def _gen(models: Path, body: str) -> str:
    eng = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=models)
    )
    return eng.generate_sql(body)[-1]


@pytest.mark.xfail(
    strict=True, reason="window-over-aggregate in HAVING re-renders in QUALIFY"
)
def test_window_over_aggregate_in_having_renders_valid_sql(models: Path):
    # BUG: the QUALIFY re-renders max(sum(amt)) over (...) -> max(INVALID_REFERENCE_BUG) over (...)
    body = """
import sales as sales;
select
    sales.store as store,
    sales.day as day,
    sum(sales.amt) as amt,
    max(sum(sales.amt)) over (partition by sales.store order by sales.day) as rm
having max(sum(sales.amt)) over (partition by sales.store order by sales.day) > 0
order by store asc;
"""
    sql = _gen(models, body)
    assert "INVALID_REFERENCE_BUG" not in sql


def test_window_over_aggregate_in_having_currently_raises(models: Path):
    # Pin the CURRENT failure mode (delete once the xfail above flips to passing).
    body = """
import sales as sales;
select
    sales.store as store,
    sales.day as day,
    sum(sales.amt) as amt,
    max(sum(sales.amt)) over (partition by sales.store order by sales.day) as rm
having max(sum(sales.amt)) over (partition by sales.store order by sales.day) > 0
order by store asc;
"""
    with pytest.raises(ValueError, match="Invalid reference string"):
        _gen(models, body)


def test_window_filtered_in_wrapper_select_is_the_working_idiom(models: Path):
    # Control: the dialect-agnostic lowering the fix should converge on — project
    # the window in an inner select, filter it in a wrapping select. Works today.
    body = """
import sales as sales;
with daily as
select
    sales.store as store,
    sales.day as day,
    sum(sales.amt) as amt,
    max(sum(sales.amt)) over (partition by sales.store order by sales.day) as rm;
select daily.store, daily.day, daily.amt, daily.rm
where daily.rm > 0
order by daily.store asc;
"""
    sql = _gen(models, body)
    assert sql and "INVALID_REFERENCE_BUG" not in sql
