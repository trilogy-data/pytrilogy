"""A HAVING that compares two per-item aggregate rowsets is lowered to a
post-aggregation grain-key semijoin (`key in (filter key where <predicate>)`).
The rowset's `inner join a.iid = b.iid` collapses the two item-id concepts onto
one survivor column; the membership left key and the final FROM resolve through
that collapse, but the *projected output* on the dropped side does not -- its
lineage bottoms out at a column absent from the scanned CTE, so the renderer
emitted a bare `INVALID_REFERENCE_BUG` sentinel (`generate_sql` "succeeded",
execution threw an uncaught ValueError).

Regression for TPC-DS q64 ("catalog items whose cumulative ext_list_price exceeds
2x cumulative refund"). A grain-key membership filters exactly the CTE's grain
key, so its left operand IS that grain key's materialized column -- the dropped
twin is the same logical key and can borrow it. Sibling of the q38 INNER-join
intersect guard (`test_cross_rowset_inner_join_intersect.py`) and the q75
grain-key membership materialization (`test_non_benchmark_queries.py`).
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

MODEL = """
key sale_id int;
property sale_id.sales_item_text string;
property sale_id.list_price float;
datasource sales (id: sale_id, it: sales_item_text, lp: list_price) grain (sale_id)
query '''
select 1 id, 'A' it, 100.0 lp
union all select 2 id, 'A' it, 50.0 lp
union all select 3 id, 'B' it, 30.0 lp
''';

key return_id int;
property return_id.returns_item_text string;
property return_id.refund float;
datasource returns (id: return_id, it: returns_item_text, rf: refund) grain (return_id)
query '''
select 1 id, 'A' it, 10.0 rf
union all select 2 id, 'B' it, 20.0 rf
''';

with cat_sales_item  as select sales_item_text as iid, sum(list_price) as s1;
with cat_refund_item as select returns_item_text as iid, sum(coalesce(refund, 0)) as s2;
"""

# project the SALES side -- the operand the INNER-join collapse drops (was the
# sentinel). Projecting the survivor side (refund) always rendered fine.
ELIGIBLE_PROJECT_SALES = """
with eligible_items as
select cat_sales_item.iid as eiid
inner join cat_sales_item.iid = cat_refund_item.iid
having cat_sales_item.s1 > 2 * coalesce(cat_refund_item.s2, 0);
"""

# Same dropped-side projection, but a SECOND ANDed HAVING predicate nests the
# grain-key membership under a BuildConditional in the CTE condition. The
# redirect only fires if its tree-walk descends left/right to reach the
# membership -- a root-only match leaves the sentinel and the query fails.
ELIGIBLE_PROJECT_SALES_NESTED = """
with eligible_items as
select cat_sales_item.iid as eiid
inner join cat_sales_item.iid = cat_refund_item.iid
having cat_sales_item.s1 > 2 * coalesce(cat_refund_item.s2, 0)
  and cat_sales_item.s1 < 1000;
"""


@pytest.fixture
def executor() -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


def test_having_membership_projected_dropped_side_renders_clean(executor: Executor):
    sql = executor.generate_sql(
        MODEL + ELIGIBLE_PROJECT_SALES + "select eligible_items.eiid;"
    )[-1]
    assert "INVALID_REFERENCE_BUG" not in sql


def test_having_membership_projected_dropped_side_executes_correctly(
    executor: Executor,
):
    # sales per item: A=150, B=30; returns per item: A=10, B=20.
    # eligible = inner-join items where s1 > 2*s2: A (150>20) yes, B (30>40) no.
    rows = executor.execute_text(
        MODEL
        + ELIGIBLE_PROJECT_SALES
        + "select eligible_items.eiid order by eligible_items.eiid asc;"
    )[-1].fetchall()
    assert [r[0] for r in rows] == ["A"]


def test_nested_having_membership_redirect_renders_clean(executor: Executor):
    sql = executor.generate_sql(
        MODEL + ELIGIBLE_PROJECT_SALES_NESTED + "select eligible_items.eiid;"
    )[-1]
    assert "INVALID_REFERENCE_BUG" not in sql


def test_nested_having_membership_redirect_executes_correctly(executor: Executor):
    # s1 > 2*s2: A (150>20) yes, B (30>40) no; s1 < 1000: both -> AND keeps A.
    rows = executor.execute_text(
        MODEL
        + ELIGIBLE_PROJECT_SALES_NESTED
        + "select eligible_items.eiid order by eligible_items.eiid asc;"
    )[-1].fetchall()
    assert [r[0] for r in rows] == ["A"]
