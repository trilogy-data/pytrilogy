"""A WHERE that filters a rowset output column compared against a *named concept*
(not a literal), where that column is referenced ONLY in the WHERE (not also in
the consuming SELECT's outputs), must materialize the rowset, co-source the other
operand, and apply the predicate post-join.

Regression for TPC-DS q23 ("best customers" — those whose lifetime total beats a
fraction of the max). The agent's natural formulation selects only the customer
key while filtering on a sibling rowset column against a derived threshold:

    with customer_totals as
    select cust as cust_id, sum(val) as lifetime_total;
    auto threshold <- 0.5 * (max(customer_totals.lifetime_total) by *);
    select customer_totals.cust_id where customer_totals.lifetime_total > threshold;

The filtered column (`lifetime_total`) is a rowset output referenced only in the
WHERE, so it never entered the discovery `mandatory_list`; the planner sourced the
RowsetNode alone and never co-located the `threshold` scalar, leaving the WHERE
structurally unsatisfiable -> hard `SyntaxError: Have {RowsetNode<...>} and need
... > local.threshold` crash. A literal RHS dodged it (one row arg, supplied by
the rowset); selecting the filtered column dodged it (it entered mandatory_list).
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

# totals: cust1=100, cust2=500, cust3=50. max=500, threshold=0.5*500=250.
# only cust2 (500) beats the threshold.
MODEL = """
key cust_id int;
property cust_id.val float;
property cust_id.yr int;
datasource sales (c: cust_id, v: val, y: yr) grain (cust_id)
query '''
select 1 c, 100.0 v, 2000 y union all
select 2 c, 500.0 v, 2000 y union all
select 3 c,  50.0 v, 2000 y''';
"""

ROWSET = """
with customer_totals as
where yr >= 2000 and cust_id is not null
select cust_id as cid, sum(val) as lifetime_total;

auto threshold <- 0.5 * (max(customer_totals.lifetime_total) by *);
"""

# filter-only rowset output vs named concept, consumed at top level
TOP_LEVEL = MODEL + ROWSET + """
select customer_totals.cid
where customer_totals.lifetime_total > threshold
order by customer_totals.cid;
"""

# same, consumed through a wrapping rowset
WRAPPED = MODEL + ROWSET + """
with best as
select customer_totals.cid
where customer_totals.lifetime_total > threshold;
select best.cid order by best.cid;
"""


@pytest.fixture
def executor(tmp_path: Path) -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


def test_filter_only_rowset_output_vs_concept_top_level(executor: Executor):
    sql = executor.generate_sql(TOP_LEVEL)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql
    rows = [tuple(r) for r in executor.execute_text(TOP_LEVEL)[0].fetchall()]
    assert rows == [(2,)]


def test_filter_only_rowset_output_vs_concept_wrapped(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(WRAPPED)[0].fetchall()]
    assert rows == [(2,)]
