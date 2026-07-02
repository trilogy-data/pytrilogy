"""A membership restriction (`x in rowset.key`) against a FILTERING rowset must
materialize the rowset and apply it as a semi-join. The rowset lists the qualifying
key tuples; `where fact_key in rowset_key` keeps only fact rows whose key is in that
set. (This was formerly spelled `inner join fact_key = rowset.key`, a query-scoped
join type since removed from the language; the rowset contributes only the join
key, so the intersection is expressed as a membership filter rather than a
LEFT-join + is-not-null.)

Regression for TPC-DS q14: the agent built a rowset of (brand, class, category)
tuples present in all 3 channels, then restricted the fact's item keys to it.
Discovery must materialize the rowset body (its WHERE filter) rather than sourcing
the rowset key from the fact's raw column and skipping the filter, which produced
wrong rows or invalid SQL ("Values list does not have a column named q_bid").
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

# brand 100: STORE+WEB+CATALOG (qualifies). 200: STORE only. 300: WEB+CATALOG only.
MODEL = """
key item_id int;
property item_id.brand int;
property item_id.cls int;
property item_id.cat int;
key line_id int;
property line_id.channel string;
property line_id.amt float;
property line_id.item_id int;
datasource items (i: item_id, b: brand, c: cls, ct: cat) grain (item_id)
query '''select 1 i,100 b,11 c,21 ct union all select 2 i,200 b,12 c,22 ct union all select 3 i,300 b,13 c,23 ct''';
datasource lines (id: line_id, ch: channel, a: amt, it: item_id) grain (line_id)
query '''
select 1 id,'STORE' ch,5.0 a,1 it union all
select 2 id,'WEB' ch,6.0 a,1 it union all
select 3 id,'CATALOG' ch,7.0 a,1 it union all
select 4 id,'STORE' ch,8.0 a,2 it union all
select 5 id,'WEB' ch,9.0 a,3 it union all
select 6 id,'CATALOG' ch,10.0 a,3 it''';
"""

_FILTER = """
auto in_store   <- sum(case when channel='STORE'   then 1 else 0 end) by brand, cls, cat;
auto in_web     <- sum(case when channel='WEB'     then 1 else 0 end) by brand, cls, cat;
auto in_catalog <- sum(case when channel='CATALOG' then 1 else 0 end) by brand, cls, cat;
"""

SINGLE_KEY = MODEL + _FILTER + """
with q as where in_store>0 and in_web>0 and in_catalog>0 select brand as qb;
select brand, sum(amt) as total
where brand in q.qb
order by brand;
"""

MULTI_KEY = MODEL + _FILTER + """
with q as where in_store>0 and in_web>0 and in_catalog>0
select brand as qb, cls as qc, cat as qt;
select brand, sum(amt) as total
where brand in q.qb and cls in q.qc and cat in q.qt
order by brand;
"""


@pytest.fixture
def executor(tmp_path: Path) -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


def test_single_key_inner_rowset_join_applies_filter(executor: Executor):
    # Only brand 100 is in all three channels; its total is 5+6+7 = 18.
    rows = [tuple(r) for r in executor.execute_text(SINGLE_KEY)[0].fetchall()]
    assert rows == [(100, 18.0)]


def test_multi_key_inner_rowset_join_compiles_and_filters(executor: Executor):
    # The q14 shape: three keys equated onto the rowset. Must not emit invalid SQL
    # (the "q_bid" BinderException), and must restrict to the qualifying tuple.
    sql = executor.generate_sql(MULTI_KEY)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql
    rows = [tuple(r) for r in executor.execute_text(MULTI_KEY)[0].fetchall()]
    assert rows == [(100, 18.0)]
