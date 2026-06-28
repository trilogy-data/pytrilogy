"""A query-scoped LEFT join that preserves ONE anchor rowset against TWO optional
rowsets must stay anchored on that source — each optional source joins as
LEFT_OUTER, never collapsing to FULL.

Regression for TPC-DS q78. The construct is `left join store.k = web.k = catalog.k`
(store is the explicit LEFT anchor; web and catalog are optional). Both web and
catalog are marked partial against the shared store keys, so join resolution picked
a *partial* source as the join base and `reduce_join_types` saw both a left- and a
right-preserving need across the two co-anchored partials and collapsed the whole
plan to FULL. The query ran but silently returned web-only / catalog-only rows with
NULL store columns (store can never be null-extended under a real LEFT anchor).
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

# store: custs 1,2 ; web: custs 2,3 ; catalog: custs 2,4.
# Anchored on store, only custs 1 and 2 may appear, never 3 or 4.
MODEL = """
key sale_id int;
property sale_id.channel string;
property sale_id.cust int;
property sale_id.val float;
datasource sales (id: sale_id, ch: channel, c: cust, v: val) grain (sale_id)
query '''
select 1 id,'STORE'   ch,1 c,100.0 v union all
select 2 id,'STORE'   ch,2 c,200.0 v union all
select 3 id,'WEB'     ch,2 c,20.0 v union all
select 4 id,'WEB'     ch,3 c,30.0 v union all
select 5 id,'CATALOG' ch,2 c,5.0 v union all
select 6 id,'CATALOG' ch,4 c,40.0 v''';
"""

ROWSETS = """
rowset store_nr <- where channel='STORE' select cust as cust_id, sum(val) as store_qty;
rowset web_nr <- where channel='WEB' select cust as cust_id, sum(val) as web_qty;
rowset catalog_nr <- where channel='CATALOG' select cust as cust_id, sum(val) as catalog_qty;
"""

QUERY = MODEL + ROWSETS + """
select
    store_nr.cust_id,
    store_nr.store_qty,
    coalesce(web_nr.web_qty, 0) + coalesce(catalog_nr.catalog_qty, 0) as other_qty
left join store_nr.cust_id = web_nr.cust_id = catalog_nr.cust_id
order by store_nr.cust_id;
"""


@pytest.fixture
def executor(tmp_path: Path) -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


def test_left_anchor_not_collapsed_to_full(executor: Executor):
    sql = executor.generate_sql(QUERY)[-1]
    assert "FULL JOIN" not in sql, f"LEFT anchor collapsed to FULL:\n{sql}"


def test_left_anchor_preserves_only_store_rows(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(QUERY)[0].fetchall()]
    # only store custs 1,2 survive; store_qty is never NULL.
    assert rows == [(1, 100.0, 0.0), (2, 200.0, 25.0)]
