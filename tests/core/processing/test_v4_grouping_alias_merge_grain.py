"""Regression lock for the v4 FINAL-merge grain when the only grouping
contributor is disguised as a BASIC (an aggregate rename/derivation).

Distilled q30: `sum(amt) by cust_id, st` compared against `1.2 * avg(...) by
st` lives entirely in the WHERE scope, so the aggregate buckets feed the
condition subtree and the FINAL contributors are a ROOT customer scan plus a
BASIC alias of the aggregate (`cust_state_amt as total`). The BASIC advertised
no projection grain, the assembly-side merge grain collapsed to empty, and
`_wrap_for_grain` shattered the customer scan into self-grain GroupNodes
(review_id, caddr_id) with no join key -- the FINAL merge cross-joined them
`ON 1=1` (5.7B-row fan-out on tpcds, plus wrong rows: every customer paired
with every review id).

Two planner rules under test:
- `_group_final_grain_contribution`: a non-grouping contributor whose outputs
  ride a grouping concept is grain-pinned at that grouping grain.
- `_wrap_for_grain`: the FK-hop collapse projects FD dims at the part of the
  merge grain the parent can SUPPLY (here {cust_id}; the sibling aggregate's
  `st` is not the customer scan's to render)."""

from trilogy import Dialects, Environment

_MODEL = """
key cust_id int;
property cust_id.cname string;
key review_id int;
property cust_id.review_id int;
key caddr_id int;
property cust_id.caddr_id int;
property caddr_id.ca_state string;

key sale_id int;
property sale_id.st string;
property sale_id.amt float;
property sale_id.sale_year int;

datasource customers (
    cid: cust_id,
    cname: cname,
    rid: review_id,
    aid: caddr_id,
)
grain (cust_id)
query '''
select 1 cid, 'alice' cname, 900 rid, 500 aid union all
select 2 cid, 'bob' cname, 901 rid, 501 aid union all
select 3 cid, 'carol' cname, 902 rid, 502 aid union all
select 4 cid, 'dave' cname, 903 rid, 503 aid
''';

datasource cust_addresses (
    aid: caddr_id,
    ca_state: ca_state,
)
grain (caddr_id)
query '''
select 500 aid, 'GA' ca_state union all
select 501 aid, 'GA' ca_state union all
select 502 aid, 'TX' ca_state union all
select 503 aid, 'TX' ca_state
''';

datasource sales (
    sid: sale_id,
    cid: cust_id,
    st: st,
    amt: amt,
    yr: sale_year,
)
grain (sale_id)
query '''
select 10 sid, 1 cid, 'GA' st, 100.0 amt, 2002 yr union all
select 11 sid, 1 cid, 'GA' st, 100.0 amt, 2002 yr union all
select 12 sid, 2 cid, 'GA' st, 10.0 amt, 2002 yr union all
select 13 sid, 3 cid, 'TX' st, 50.0 amt, 2002 yr union all
select 14 sid, 4 cid, 'TX' st, 10.0 amt, 2002 yr
''';

auto cust_state_amt <- sum(amt ? sale_year = 2002) by cust_id, st;
auto state_avg <- avg(cust_state_amt) by st;
"""


def _run(query: str) -> tuple[str, list[tuple]]:
    env = Environment()
    env, _ = env.parse(_MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = executor.generate_sql(query)[-1]
    rows = executor.execute_text(query)[-1].fetchall()
    return sql, [(r[0], r[1], float(r[2])) for r in rows]


def test_where_agg_alias_with_subdim_filter_no_cross_join():
    """Full q30 shape: the non-mandatory sub-dim filter (`ca_state = 'GA'`)
    rides the ROOT bucket, which previously hid the bridge keys and made the
    keyless merge unrescuable by join inference."""
    sql, rows = _run("""
where
    cust_state_amt > 1.2 * state_avg
    and ca_state = 'GA'
    and st is not null
select
    cname,
    review_id,
    cust_state_amt as total,
order by cname asc;
""")
    assert " on 1=1" not in sql, sql
    assert rows == [("alice", 900, 200.0)]


def test_where_agg_alias_fd_dims_no_cross_join():
    sql, rows = _run("""
where
    cust_state_amt > 1.2 * state_avg
    and st is not null
select
    cname,
    review_id,
    cust_state_amt as total,
order by cname asc;
""")
    assert " on 1=1" not in sql, sql
    assert rows == [("alice", 900, 200.0), ("carol", 902, 50.0)]


def test_where_agg_expression_derivation_no_cross_join():
    """The grouping concept sits one lineage level below the output BASIC
    (`cust_state_amt * 2`), exercising the transitive lineage walk."""
    sql, rows = _run("""
where
    cust_state_amt > 1.2 * state_avg
    and ca_state = 'GA'
    and st is not null
select
    cname,
    review_id,
    cust_state_amt * 2 as total_doubled,
order by cname asc;
""")
    assert " on 1=1" not in sql, sql
    assert rows == [("alice", 900, 400.0)]
