"""A WHERE that compares outputs across several query-scoped-joined rowsets must
materialize every referenced rowset, merge them on the join key, and apply the
predicate post-join.

Regression for TPC-DS q11/q23 (the "compare growth/ratio across periods or
channels" family): four rowsets (store/web x 2001/2002) scoped-joined on the
customer key, then a WHERE comparing their revenues. The operands from the other
rowsets aren't in the output rowset's columns, so gen_rowset_node returned the
bare rowset node and dropped the pushed-up condition entirely — silently losing
the cross-rowset filter (single comparison) or raising a cryptic
`Have {RowsetNode<...>} and need ...` SyntaxError (two or more comparisons
spanning >=3 rowsets). gen_rowset_node now sources the rowset output together
with those operand rowsets as one scoped-join merge and applies the predicate
there.
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

# cust 1: web both years beat store both years -> passes single AND multi.
# cust 2: web_2002 (15) < store_2002 (20) -> fails both.
# cust 3: no web_2002 row -> dropped by the INNER scoped join.
# cust 4: web_2002 (50) > store_2002 (20) but web_2001 (5) < store_2001 (10)
#         -> passes single comparison, fails the second one.
MODEL = """
key sale_id int;
property sale_id.channel string;
property sale_id.yr int;
property sale_id.cust int;
property sale_id.val float;
datasource sales (id: sale_id, ch: channel, y: yr, c: cust, v: val) grain (sale_id)
query '''
select 1 id,'STORE' ch,2001 y,1 c,10.0 v union all
select 2 id,'STORE' ch,2002 y,1 c,20.0 v union all
select 3 id,'WEB'   ch,2001 y,1 c,30.0 v union all
select 4 id,'WEB'   ch,2002 y,1 c,50.0 v union all
select 5 id,'STORE' ch,2001 y,2 c,10.0 v union all
select 6 id,'STORE' ch,2002 y,2 c,20.0 v union all
select 7 id,'WEB'   ch,2001 y,2 c,5.0 v union all
select 8 id,'WEB'   ch,2002 y,2 c,15.0 v union all
select 9 id,'STORE' ch,2001 y,3 c,10.0 v union all
select 10 id,'STORE' ch,2002 y,3 c,20.0 v union all
select 11 id,'WEB'  ch,2001 y,3 c,30.0 v union all
select 12 id,'STORE' ch,2001 y,4 c,10.0 v union all
select 13 id,'STORE' ch,2002 y,4 c,20.0 v union all
select 14 id,'WEB'  ch,2001 y,4 c,5.0 v union all
select 15 id,'WEB'  ch,2002 y,4 c,50.0 v''';
"""

ROWSETS = """
with store_2001 as where channel='STORE' and yr=2001 select cust as cust_id, sum(val) as rev;
with store_2002 as where channel='STORE' and yr=2002 select cust as cust_id, sum(val) as rev;
with web_2001 as where channel='WEB' and yr=2001 select cust as cust_id, sum(val) as rev;
with web_2002 as where channel='WEB' and yr=2002 select cust as cust_id, sum(val) as rev;
"""

JOINS = """
inner join store_2001.cust_id = store_2002.cust_id
inner join store_2001.cust_id = web_2001.cust_id
inner join store_2001.cust_id = web_2002.cust_id
"""

SINGLE = (
    MODEL
    + ROWSETS
    + (
        "select store_2001.cust_id"
        + JOINS
        + "where web_2002.rev > store_2002.rev order by store_2001.cust_id;"
    )
)

MULTI = (
    MODEL
    + ROWSETS
    + (
        "select store_2001.cust_id"
        + JOINS
        + "where web_2002.rev > store_2002.rev and web_2001.rev > store_2001.rev "
        "order by store_2001.cust_id;"
    )
)


@pytest.fixture
def executor(tmp_path: Path) -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


def test_single_cross_rowset_comparison_applies_filter(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(SINGLE)[0].fetchall()]
    assert rows == [(1,), (4,)]


def test_multi_cross_rowset_comparison_resolves_and_filters(executor: Executor):
    sql = executor.generate_sql(MULTI)[-1]
    assert "store_2002" in sql and "web_2001" in sql
    rows = [tuple(r) for r in executor.execute_text(MULTI)[0].fetchall()]
    assert rows == [(1,)]
