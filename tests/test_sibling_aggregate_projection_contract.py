"""Repro: two sibling unfiltered aggregates at the same output grain, where one
reads a BASIC value computed by a row-grain parent (`revenue <- item_count *
sale_price`) and the other reads a GROUP_TO value (`item_quantity <-
group(item_count) by item_id`), rendered with only the GROUP_TO aggregate. The
BASIC one was computed in a CTE and then never projected.

Reported from trilogy-cloud 2026-08-16 against `thelook_ecommerce`: a persist
declaring eight columns emitted seven, dropping the *seventh of eight*, which a
positional INSERT turns into a column shift.

FIXED 2026-08-16: `_satisfy_parent_projection_contract` measured what a parent
could supply as its *grandparents'* outputs only, so every value the parent
computed itself was invisible to `parent_needed` and the projection stripped it.
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

ORDERS = """key order_id int;
property order_id.item_count int;
datasource orders (order_id: order_id, num_of_item: item_count)
grain (order_id)
query '''
select 1 as order_id, 2 as num_of_item union all
select 2, 3 ''';
"""

ORDER_ITEMS = """import orders as order;
key item_id int;
property item_id.created_at timestamp;
property item_id.sale_price float;

auto revenue <- order.item_count * sale_price;
auto total_revenue <- sum(revenue);
auto item_quantity <- group(order.item_count) by item_id;
auto total_quantity <- sum(item_quantity);

datasource order_items (
    id: item_id,
    order_id: order.order_id,
    created_at: created_at,
    sale_price: sale_price,
)
grain (item_id)
query '''
select 1 as id, 1 as order_id, timestamp '2024-01-01' as created_at, 10.0 as sale_price union all
select 2, 1, timestamp '2024-01-01', 5.0 union all
select 3, 2, timestamp '2024-01-02', 7.0 ''';
"""

_QUERY = """
import order_items as item;
select
    item.order.order_id,
    item.created_at.date,
    item.total_revenue,
    item.total_quantity
order by item.order.order_id asc;
"""


@pytest.fixture
def models(tmp_path: Path) -> Path:
    (tmp_path / "orders.preql").write_text(ORDERS)
    (tmp_path / "order_items.preql").write_text(ORDER_ITEMS)
    return tmp_path


def _executor(models: Path) -> Executor:
    return Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=models)
    )


def test_sibling_aggregates_both_project(models: Path):
    ex = _executor(models)
    sql = ex.generate_sql(_QUERY)[-1]
    final = sql[sql.rfind("SELECT") :]
    assert "total_revenue" in final, sql
    assert "total_quantity" in final, sql

    rows = [tuple(r) for r in ex.execute_text(_QUERY)[0].fetchall()]
    assert [(r[0], str(r[1]), r[2], r[3]) for r in rows] == [
        (1, "2024-01-01", 30.0, 4),
        (2, "2024-01-02", 21.0, 3),
    ]


def test_sibling_aggregates_persist_projects_every_column(models: Path):
    ex = _executor(models)
    persist = """
import order_items as item;
persist target into target from
select
    item.order.order_id,
    item.created_at.date,
    item.total_revenue,
    item.total_quantity;
"""
    sql = ex.generate_sql(persist)[-1]
    final = sql[sql.rfind("SELECT") :]
    for column in ("order_id", "created_at_date", "total_revenue", "total_quantity"):
        assert column in final, sql
