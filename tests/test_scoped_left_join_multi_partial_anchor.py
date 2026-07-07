"""A query-scoped chained subset join declares a TRANSITIVE subset domain
(docs/subset_union_join_design.md).

TPC-DS q78 family: `subset join catalog.k = web.k = store.k` declares
catalog ⊆ web ⊆ store, so `store` is the ultimate superset anchor. Each rowset
is a filtered channel aggregate, but a rowset output is complete AT ITS OWN
rename boundary (the body filter DEFINES the domain), so the genuine directional
subsets (web ⊆ store, catalog ⊆ store) narrow to the store-anchored LEFT:
web-only and catalog-only customers (non-members of the store superset) drop,
store customers with no other-channel activity survive NULL-padded. "Keep every
channel's customers" is the `union join` / FULL form instead.
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

# store: custs 1,2 ; web: custs 2,3 ; catalog: custs 2,4.
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

SELECT = """
select
    store_nr.cust_id,
    store_nr.store_qty,
    coalesce(web_nr.web_qty, 0) + coalesce(catalog_nr.catalog_qty, 0) as other_qty
subset join catalog_nr.cust_id = web_nr.cust_id = store_nr.cust_id
"""

PRESERVING_QUERY = MODEL + ROWSETS + SELECT + "order by store_nr.cust_id;"
RESTRICTED_QUERY = (
    MODEL
    + ROWSETS
    + "where store_nr.store_qty is not null"
    + SELECT
    + "order by store_nr.cust_id;"
)


@pytest.fixture
def executor(tmp_path: Path) -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


def test_chained_left_join_narrows_to_store_anchor(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(PRESERVING_QUERY)[0].fetchall()]
    # store is the transitive superset (catalog ⊆ web ⊆ store): web-only cust 3
    # and catalog-only cust 4 are non-members and drop; store customers survive.
    assert rows == [(1, 100.0, 0.0), (2, 200.0, 25.0)]


def test_explicit_filter_matches_directional(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(RESTRICTED_QUERY)[0].fetchall()]
    # `where store_qty is not null` now coincides with the directional narrowing.
    assert rows == [(1, 100.0, 0.0), (2, 200.0, 25.0)]
