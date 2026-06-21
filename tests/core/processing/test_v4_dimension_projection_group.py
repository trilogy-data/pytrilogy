"""Regression lock for the v4 dimension-projection group grain
(`_project_dimension_parents_to_group_grain`, the C9 bridge path).

An aggregate-over-aggregate where a row key (`date_id`) sits only in the WHERE
and is functionally determined by the aggregate input grain used to be added to
the projected GroupNode's grain even though the dimension parent never outputs
it -- the node then failed input validation (`['local.date_id'] are missing
non-hidden parent nodes`). This is the distilled q76 (TPC-DS) crash. The group
grain may only include concepts the parent actually outputs."""

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG

_MODEL = """
key order_id int;
key item_id int;
property <order_id,item_id>.region_id int;
property <order_id,item_id>.amt float;
key date_id int;
property date_id.cat string;

datasource facts (
    oid: order_id,
    iid: item_id,
    rid: region_id,
    amt: amt,
    did: date_id,
)
grain (order_id, item_id)
query '''
select 1 oid, 1 iid, null rid, 10.0 amt, 100 did union all
select 1 oid, 2 iid, 5 rid, 20.0 amt, 100 did union all
select 2 oid, 1 iid, null rid, 30.0 amt, 200 did
''';

datasource dates (
    did: date_id,
    cat: cat,
)
grain (date_id)
query '''
select 100 did, 'a' cat union all
select 200 did, 'b' cat
''';

auto row_flag <- sum(case when region_id is null then 1 else 0 end)
    by order_id, item_id;
"""

_QUERY = """
where date_id is not null
select cat, sum(row_flag) -> cnt, sum(amt) -> total
order by cat asc;
"""


def test_aggregate_over_aggregate_filter_key_does_not_join_group_grain():
    env = Environment()
    env, _ = env.parse(_MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    prior = CONFIG.use_v4_discovery
    CONFIG.use_v4_discovery = True
    try:
        rows = executor.execute_text(_QUERY)[-1].fetchall()
    finally:
        CONFIG.use_v4_discovery = prior
    assert [tuple(r) for r in rows] == [("a", 1, 30.0), ("b", 1, 30.0)]
