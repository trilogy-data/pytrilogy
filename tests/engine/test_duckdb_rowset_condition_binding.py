"""q23 agent shapes (bug_q23_regression_20260706): conditions over rowset
outputs must bind to the rowset column, and never render an unaliased
self-join.

Bug 1: an outer rowset HAVING referencing another rowset's aggregate-measure
raw re-expanded its base lineage into a node that couldn't source it
(INVALID_REFERENCE_BUG sentinel at render).

Bug 2: a WHERE comparing two columns of one parent rowset, with the output
re-aliased in the final select, let join_hoist push the enrichment join into
a parent that already reads FROM the dim -> `FROM x INNER JOIN x` (DuckDB
BinderException: ambiguous reference)."""

from trilogy import Dialects

MODEL = r"""
key order_id int;
key cust_id int;

property order_id.qty int;
property order_id.price float;
property order_id.year int;

datasource orders (
    o: order_id,
    c: cust_id,
    q: qty,
    p: price,
    y: year)
grain (order_id)
query '''
select 1 as o, 10 as c, 2 as q, 5.0 as p, 2000 as y
union all
select 2, 10, 1, 3.0, 2001
union all
select 3, 20, 1, 1.0, 1999
union all
select 4, 30, 4, 10.0, 2000
union all
select 5, 40, 1, -8.0, 2001
''';
"""


def test_rowset_having_rowset_measure_binds_to_output():
    query = MODEL + r"""
rowset cust_totals <- select cust_id, sum(qty*price) as cust_total;
rowset best_cust <- select cust_totals.cust_id as cid, max(cust_totals.cust_total) as overall_max
  having cust_totals.cust_total > 0.5 * overall_max;
select best_cust.cid order by best_cust.cid asc;
"""
    exec = Dialects.DUCK_DB.default_executor()
    sql = exec.generate_sql(query)
    assert "INVALID_REFERENCE" not in sql[-1]
    # the bare max inherits the body select's grain {cid}, so overall_max is
    # per-customer identity: `t > 0.5*t` holds for positive totals and fails
    # for negative ones — cust 40 (-8.0) must drop, proving the HAVING binds
    # to the rowset column and is genuinely applied
    rows = exec.execute_query(query).fetchall()
    assert [r.best_cust_cid for r in rows] == [10, 20, 30]


def test_rowset_two_column_filter_realias_no_self_join():
    query = MODEL + r"""
rowset cust_totals <- where year >= 2000
  select cust_id, sum(qty*price) as cust_total;
rowset max_val <- select max(cust_totals.cust_total) as overall_max;
rowset best_filter <- select cust_totals.cust_id as cid, cust_totals.cust_total as ctotal, max_val.overall_max as omax;
where best_filter.ctotal > 0.5 * best_filter.omax
select best_filter.cid as out_id;
"""
    # year>=2000 totals: cust 10 -> 13.0, cust 30 -> 40.0 (cust 20 filtered);
    # max = 40 so only cust 30 clears 0.5 * max. Execution also proves no
    # unaliased self-join (DuckDB rejects duplicate table references at bind).
    exec = Dialects.DUCK_DB.default_executor()
    rows = exec.execute_query(query).fetchall()
    assert [r.out_id for r in rows] == [30]


def test_rowset_having_inline_aggregate_over_renamed_source():
    query = MODEL + r"""
rowset freq <- select
    cust_id as ckey,
    count_distinct(cust_id::string || '-' || year::string) as yr_cnt
having
    count_distinct(cust_id::string || '-' || year::string) > 1;
select freq.ckey order by freq.ckey asc;
"""
    # The HAVING aggregate is identical to the `yr_cnt` output, so it must bind to
    # that materialized column and render as a group HAVING. The bug: the pure
    # rename `cust_id as ckey` let `_rewrite_aliased_source_refs` rewrite the HAVING
    # copy's inner `cust_id` to `ckey` while `yr_cnt`'s lineage kept `cust_id`, so
    # the signatures no longer matched, the aggregate re-inlined into a post-group
    # filter with its now-grouped-away inputs, and rendering hit INVALID_REFERENCE.
    exec = Dialects.DUCK_DB.default_executor()
    sql = exec.generate_sql(query)[-1]
    assert "INVALID_REFERENCE" not in sql
    rows = exec.execute_query(query).fetchall()
    # only cust 10 spans >1 (cust_id, year) pair (2000 and 2001)
    assert [r.freq_ckey for r in rows] == [10]


def test_rowset_two_column_filter_plain_output():
    query = MODEL + r"""
rowset cust_totals <- where year >= 2000
  select cust_id, sum(qty*price) as cust_total;
rowset max_val <- select max(cust_totals.cust_total) as overall_max;
rowset best_filter <- select cust_totals.cust_id as cid, cust_totals.cust_total as ctotal, max_val.overall_max as omax;
where best_filter.ctotal > 0.5 * best_filter.omax
select best_filter.cid;
"""
    exec = Dialects.DUCK_DB.default_executor()
    rows = exec.execute_query(query).fetchall()
    assert [r.best_filter_cid for r in rows] == [30]
