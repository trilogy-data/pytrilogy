"""Repro: a boolean filter concept built on a grouped metric compared to a
global aggregate (`flag <- metric > 0.5 * (max(metric) by *)`), used in WHERE
with the metric ALSO projected past a property join, emitted invalid SQL — the
filter CTE carried the metric in its GROUP BY but hid it from its SELECT, while
the downstream property-join CTE referenced it.

Surfaced on TPC-DS q23 (run 20260624-133456):
`Binder Error: Values list "<filter-cte>" does not have a column named "cust_store_rev"`.

FIXED 2026-06-24: `_expose_downstream_referenced_columns` un-hides any producer
column a consumer CTE sources by address when no visible pseudonym-equivalent
renders it.
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

CUSTOMER = """key id int;
property id.last_name string;
datasource customers (cid: id, ln: last_name)
grain (id)
query '''
select 1 as cid, 'Alice' as ln union all
select 2, 'Bob' union all
select 3, 'Cara' ''';
"""

SALES = """import customer as customer;
key sale_id int;
property sale_id.quantity int;
datasource sales (sid: sale_id, cust: customer.id, qty: quantity)
grain (sale_id)
query '''
select 1 as sid, 1 as cust, 70 as qty union all
select 2, 1, 30 union all
select 3, 2, 60 union all
select 4, 3, 8 ''';
"""


@pytest.fixture
def models(tmp_path: Path) -> Path:
    (tmp_path / "sales.preql").write_text(SALES)
    (tmp_path / "customer.preql").write_text(CUSTOMER)
    return tmp_path


def _executor(models: Path) -> Executor:
    return Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=models)
    )


# Per-customer totals: Alice=100, Bob=60, Cara=8; global max=100, threshold=50.
_QUERY = """
import sales as store_sales;
auto cust_store_rev <- sum(store_sales.quantity) by store_sales.customer.id;
auto best_cust_rev <- cust_store_rev > 0.5 * max(cust_store_rev) by *;
where best_cust_rev
select store_sales.customer.last_name, cust_store_rev
order by cust_store_rev desc;
"""


def test_grouped_metric_survives_filter_cte_past_property_join(models: Path):
    ex = _executor(models)
    sql = ex.generate_sql(_QUERY)[-1]
    assert "INVALID" not in sql
    # Executing is the guard: the filter CTE must project the metric it groups by,
    # or DuckDB raises a missing-column BinderError.
    rows = [tuple(r) for r in ex.execute_text(_QUERY)[0].fetchall()]
    assert rows == [("Alice", 100), ("Bob", 60)]
