"""A rowset whose own WHERE compares its operand rowsets (`item_sales.s_amt >
2 * item_refunds.r_amt`, joined `s_item = r_item`) must source EVERY operand the
predicate references into the cross-rowset merge -- including operands that live
in the rowset being processed, not just the ones missing from it.

Regression for TPC-DS q64. `gen_rowset_node`'s cross-rowset branch sourced a
fresh merge from `[concept] + local_optional + condition_targets`, where
`condition_targets` excludes operands already present in `node`. But the merge is
sourced anew and does NOT inherit `node`'s outputs, so an operand living in `node`
(e.g. the `s_amt` measure being compared) was never produced by the merge -- yet
the predicate referencing it was applied, rendering a dangling
`INVALID_REFERENCE_BUG` CTE and raising an uncaught ValueError. The crash fires
whether such a cross-rowset-join rowset is selected directly or used as a
membership set (`x in qual.q_item`).
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

MODEL = """
key sale_id int;
property sale_id.item int;
property sale_id.kind string;
property sale_id.amt float;
datasource sales (id: sale_id, it: item, k: kind, a: amt) grain (sale_id)
query '''
select 1 id, 10 it, 'SALE' k, 100.0 a union all
select 2 id, 10 it, 'SALE' k, 50.0 a union all
select 3 id, 20 it, 'SALE' k, 80.0 a union all
select 4 id, 30 it, 'SALE' k, 200.0 a union all
select 5 id, 10 it, 'REFUND' k, 10.0 a union all
select 6 id, 20 it, 'REFUND' k, 70.0 a union all
select 7 id, 30 it, 'REFUND' k, 5.0 a''';

key oid int;
property oid.o_item int;
property oid.o_val float;
datasource orders (id: oid, oi: o_item, ov: o_val) grain (oid)
query '''
select 1 id, 10 oi, 1000.0 ov union all
select 2 id, 20 oi, 2000.0 ov union all
select 3 id, 30 oi, 3000.0 ov''';

with item_sales as where kind = 'SALE' select item as s_item, sum(amt) as s_amt;
with item_refunds as where kind = 'REFUND' select item as r_item, sum(amt) as r_amt;

# qual = a CROSS-ROWSET JOIN rowset whose WHERE compares the two operand rowsets.
# item 10: 150 > 2*10  -> kept; item 20: 80 > 2*70 -> dropped; item 30: 200 > 2*5 -> kept
with qual as
  select item_sales.s_item as q_item
  subset join item_refunds.r_item = item_sales.s_item
  where item_sales.s_amt > 2 * coalesce(item_refunds.r_amt, 0);
"""


@pytest.fixture
def executor() -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


def test_cross_rowset_join_rowset_selected_directly(executor: Executor):
    query = MODEL + "select qual.q_item order by qual.q_item;"
    sql = executor.generate_sql(query)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql
    rows = sorted(int(r[0]) for r in executor.execute_text(query)[0].fetchall())
    assert rows == [10, 30]


def test_cross_rowset_join_rowset_as_membership_set(executor: Executor):
    query = (
        MODEL
        + "where o_item in qual.q_item "
        + "select o_item, sum(o_val) as t order by o_item;"
    )
    sql = executor.generate_sql(query)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql
    rows = [
        (int(r[0]), float(r[1])) for r in executor.execute_text(query)[0].fetchall()
    ]
    assert rows == [(10, 1000.0), (30, 3000.0)]
