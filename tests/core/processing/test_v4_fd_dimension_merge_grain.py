"""Regression lock for the v4 FINAL-merge grain over a rowset contributor
(`_assemble_final_node` grouping-grain includes ROWSET boundaries).

`city` is a property of `addr_id`, and `addr_id` is itself FD by `cust_id`
(`customers` binds it). Joined to a `bought` rowset (grain carries `cust_id`),
the customer-address scan must ride `cust_id` and join on it. v4 previously left
the rowset out of the merge grain, so `_wrap_for_grain` deduped `city` to its own
`addr_id` grain -- dropping `cust_id` -- and the FINAL merge cross-joined `ON 1=1`,
pairing every customer with every city (distilled q46)."""

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG

_MODEL = """
key cust_id int;
property cust_id.cname string;
key addr_id int;
property cust_id.addr_id int;
property addr_id.city string;

datasource customers (
    cid: cust_id,
    cname: cname,
    aid: addr_id,
)
grain (cust_id)
query '''
select 1 cid, 'alice' cname, 100 aid union all
select 2 cid, 'bob' cname, 100 aid union all
select 3 cid, 'carol' cname, 200 aid
''';

datasource addresses (
    aid: addr_id,
    city: city,
)
grain (addr_id)
query '''
select 100 aid, 'springfield' city union all
select 200 aid, 'shelbyville' city
''';

key sale_id int;
property sale_id.sale_cust int;
property sale_id.amt float;

datasource sales (
    sid: sale_id,
    scust: sale_cust,
    amt: amt,
)
grain (sale_id)
query '''
select 10 sid, 1 scust, 5.0 amt union all
select 11 sid, 1 scust, 7.0 amt union all
select 12 sid, 3 scust, 9.0 amt
''';

rowset bought <- select sale_cust, sum(amt) as total;
"""

_QUERY = """
inner join bought.sale_cust = cust_id
select cname, city, bought.total
order by cname asc;
"""


def test_fd_dimension_rides_rowset_grain_no_cross_join():
    env = Environment()
    env, _ = env.parse(_MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    prior = CONFIG.use_v4_discovery
    CONFIG.use_v4_discovery = True
    try:
        sql = executor.generate_sql(_QUERY)[-1]
        rows = executor.execute_text(_QUERY)[-1].fetchall()
    finally:
        CONFIG.use_v4_discovery = prior
    assert " on 1=1" not in sql, sql
    assert [tuple(r) for r in rows] == [
        ("alice", "springfield", 12.0),
        ("carol", "shelbyville", 9.0),
    ]
