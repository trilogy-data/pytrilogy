"""A query-scoped LEFT join declares SUBSET domains; rendering is always
row-preserving (docs/subset_union_join_design.md, phase 2).

TPC-DS q78 family: `left join store.k = web.k = catalog.k`. The anchor rowset
is FILTERED (channel='STORE'), and a filtered superset side never proves
subset-match, so the narrowing pass leaves the preserving join in place: web-
and catalog-only customers survive with NULL store columns. Row restriction is
an explicit author predicate — the `is not null` idiom — never a silent join
drop. (Pre-flip this file pinned the directional-anchor machinery; the old
defect of web-only rows appearing under a supposed hard LEFT anchor is moot
now that preservation is the declared contract.)
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
left join store_nr.cust_id = web_nr.cust_id = catalog_nr.cust_id
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


def test_left_join_preserves_all_sides(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(PRESERVING_QUERY)[0].fetchall()]
    assert rows == [
        (1, 100.0, 0.0),
        (2, 200.0, 25.0),
        (3, None, 30.0),
        (4, None, 40.0),
    ]


def test_explicit_filter_restricts_to_store_rows(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(RESTRICTED_QUERY)[0].fetchall()]
    assert rows == [(1, 100.0, 0.0), (2, 200.0, 25.0)]
