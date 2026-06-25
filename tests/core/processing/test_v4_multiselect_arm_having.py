"""Regression lock for per-arm HAVING in a v4 multiselect (`_resolve_multiselect`).

A HAVING attached to one MERGE arm is a post-aggregate filter over that arm's
producer. The top-level `_get_query_node_v4` HAVING wrap only sees the outer
query, not the arms, so v4 previously dropped per-arm HAVINGs entirely. Here arm
A (`count(ticket_id) as tickets having tickets > 1`) must exclude category `b`
(one ticket); it still surfaces via the FULL-join from arm B with NULL ticket
columns. Without the fix `b` leaks back with `tickets = 1`."""

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG

_MODEL = """
key ticket_id int;
property ticket_id.tcat string;
datasource tickets (
    tid: ticket_id,
    cat: tcat,
)
grain (ticket_id)
query '''
select 1 tid, 'a' cat union all
select 2 tid, 'a' cat union all
select 3 tid, 'a' cat union all
select 4 tid, 'b' cat
''';

key order_id int;
property order_id.ocat string;
property order_id.amt float;
datasource orders (
    oid: order_id,
    cat: ocat,
    amt: amt,
)
grain (order_id)
query '''
select 10 oid, 'a' cat, 5.0 amt union all
select 11 oid, 'b' cat, 7.0 amt
''';
"""

_QUERY = """
select tcat, count(ticket_id) as tickets having tickets > 1
merge
select ocat, sum(amt) as total
align cat: tcat, ocat
order by cat asc;
"""


def test_multiselect_arm_having_filters_that_arm():
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
    assert 'HAVING\n    "tickets" > 1' in sql, sql
    # cat, tcat, tickets, ocat, total -- arm A drops `b`, so its columns are NULL
    # while arm B still contributes `b` through the FULL join.
    assert [tuple(r) for r in rows] == [
        ("a", "a", 3, "a", 5.0),
        ("b", None, None, "b", 7.0),
    ]
