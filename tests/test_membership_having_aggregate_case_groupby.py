"""A membership over a rowset whose HAVING compares an aggregate to a *derived
aggregate* threshold lowered the HAVING into a per-row `_virt_filter` existence
column rendered as `CASE WHEN sum(...) > threshold THEN key ELSE NULL END`. The
group/aggregate classifier only inspected the filter's *content* (the bare key),
not the where condition, so the CASE-over-aggregate column was treated as a
dimension grouping key and emitted in the CTE's GROUP BY -- DuckDB then raised
"GROUP BY clause cannot contain aggregates" (generate_sql succeeded, execution
threw).

Regression for TPC-DS q23. The derived threshold (`max(per_customer_total) by *`)
forces the existence set into a *grouping* CTE rather than a plain WHERE, which
is what fuses the aggregate and the CASE into one CTE.
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

MODEL = """
key customer_id int;

key ss_id int;
property ss_id.ss_qty int;
property ss_id.ss_price float;
datasource store_sales (id: ss_id, c: customer_id, q: ss_qty, p: ss_price) grain (ss_id)
query '''
select 1 id, 1 c, 5 q, 10.0 p union all
select 2 id, 1 c, 5 q, 10.0 p union all
select 3 id, 2 c, 1 q, 1.0 p union all
select 4 id, 3 c, 9 q, 9.0 p''';

key cs_id int;
property cs_id.cs_list float;
datasource catalog_sales (id: cs_id, c: customer_id, l: cs_list) grain (cs_id)
query '''
select 1 id, 1 c, 100.0 l union all
select 2 id, 2 c, 200.0 l union all
select 3 id, 3 c, 300.0 l''';

auto all_customer_totals <- sum(ss_qty * ss_price) by customer_id;
auto best_customer_threshold <- (max(all_customer_totals) by *) * 0.5;

rowset best_customers <-
where customer_id is not null
select
    customer_id as cust_id,
    sum(ss_qty * ss_price) as customer_total
having customer_total > best_customer_threshold;
"""

QUERY = MODEL + """
where customer_id in best_customers.cust_id
select
    customer_id as cid,
    sum(cs_list) as t
order by cid asc;
"""


@pytest.fixture
def executor() -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


def test_membership_having_aggregate_not_in_group_by(executor):
    # generate_sql always succeeded; the bug was invalid SQL that DuckDB rejects
    # at execution ("GROUP BY clause cannot contain aggregates").
    # totals: c1=100, c2=1, c3=81; max=100, threshold=50 -> {1, 3} qualify
    rows = [
        (int(r[0]), float(r[1])) for r in executor.execute_text(QUERY)[0].fetchall()
    ]
    assert rows == [(1, 100.0), (3, 300.0)]
