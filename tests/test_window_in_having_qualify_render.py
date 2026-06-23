"""A window-over-aggregate predicate in HAVING used to lower to a QUALIFY whose
window was RE-RENDERED at the outer scope, where the window's inner aggregate is
not resolvable -> the rendered SQL contained `INVALID_REFERENCE_BUG` and
`generate_sql` raised `ValueError: Invalid reference string found in query`.

Fixed in `select_finalize._substitute_having_windows`: a HAVING window whose
signature matches a SELECT window output is rewritten to reference that
materialized alias (`rm` below), so the filter points at the already-computed
column instead of re-deriving the window. See
evals/tpcds_agent/bug_window_function_in_having.md.

Surfaced on enriched TPC-DS q51 (web-vs-store running-max compare) and q75
(year-over-year lag ratio) — ~4.5M tokens combined.
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


def test_window_over_aggregate_in_having_renders_valid_sql(models: Path):
    # The QUALIFY/WHERE references the materialized window column `rm` rather
    # than re-rendering max(sum(amt)) over (...) -> max(INVALID_REFERENCE_BUG).
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


def test_numbering_window_over_aggregate_in_having_renders_valid_sql(models: Path):
    # rank() over (order by sum(...)) — the NumberingWindowItem variant.
    body = """
import sales as sales;
select
    sales.store as store,
    sum(sales.amt) as amt,
    rank() over (order by sum(sales.amt) desc) as rnk
having rank() over (order by sum(sales.amt) desc) <= 10
order by store asc;
"""
    sql = _gen(models, body)
    assert "INVALID_REFERENCE_BUG" not in sql


def test_window_filtered_in_wrapper_select_is_the_working_idiom(models: Path):
    # The dialect-agnostic lowering this converges on: project the window in an
    # inner select, filter it in a wrapping select. Works today and is the
    # equivalent form the HAVING substitution produces.
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
