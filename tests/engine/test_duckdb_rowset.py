from logging import INFO

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import DisconnectedConceptsException
from trilogy.hooks.query_debugger import DebuggingHook

_CONFLICTING_FILTER_FIXTURE = """
key line_id int;
property line_id.item_id int;
property line_id.yr int;

datasource lines (
    line_id: line_id,
    item_id: item_id,
    yr: yr,
)
grain (line_id)
query '''
select 1 as line_id, 10 as item_id, 1999 as yr union all
select 2 as line_id, 10 as item_id, 1999 as yr union all
select 3 as line_id, 10 as item_id, 2000 as yr union all
select 4 as line_id, 10 as item_id, 2000 as yr union all
select 5 as line_id, 10 as item_id, 2000 as yr union all
select 6 as line_id, 20 as item_id, 1999 as yr union all
select 7 as line_id, 20 as item_id, 2000 as yr union all
select 8 as line_id, 30 as item_id, 2000 as yr union all
select 9 as line_id, 40 as item_id, 1999 as yr
''';
"""


def test_rowset_query_scoped_join_conflicting_filter():
    # A query-scoped inner join to a rowset, where the rowset and the outer
    # query filter the SAME dimension to DIFFERENT values (year 2000 vs 1999).
    # This used to crash the planner with `Have {...} need ...` because the
    # validator demanded every node carry the outer condition, but the rowset
    # node legitimately carries its own (year=2000) scope. A rowset is a
    # self-contained subquery: its WHERE stays inside it and the outer WHERE
    # only filters the outer scan, yielding a meaningful year-over-year join.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_CONFLICTING_FILTER_FIXTURE)
    results = executor.execute_text("""
rowset yr2000 <-
    where yr = 2000
    select
        item_id as r_item_id,
        count(line_id) as cnt_2000;

where yr = 1999
inner join yr2000.r_item_id = item_id
select
    item_id,
    count(line_id) as cnt_1999,
    yr2000.cnt_2000 as cnt_2000
order by item_id asc;
""")[0].fetchall()
    # Inner join keeps only items present in BOTH years (10, 20); item 30
    # (2000 only) and item 40 (1999 only) drop out. Each count reflects its
    # own year scope independently.
    assert [tuple(r) for r in results] == [(10, 2, 3), (20, 1, 1)]


_SELF_WELD_FIXTURE = """
key sale_id int;
property sale_id.wk int;
property sale_id.yr int;
property sale_id.chan string;
property sale_id.amt float;

datasource sales (
    sale_id: sale_id,
    wk: wk,
    yr: yr,
    chan: chan,
    amt: amt,
)
grain (sale_id)
query '''
select 1 as sale_id, 1 as wk, 2001 as yr, 'WEB' as chan, 10.0 as amt union all
select 2 as sale_id, 2 as wk, 2001 as yr, 'WEB' as chan, 20.0 as amt union all
select 3 as sale_id, 54 as wk, 2002 as yr, 'WEB' as chan, 30.0 as amt
''';
"""


def test_rowset_membership_feeder_scoped_joined_to_own_output_no_recursion():
    # The TPC-DS q02 shape an agent stumbled into. A feeder rowset (`weeks`)
    # defines week keys plus their +53 counterparts; two sum rowsets each filter
    # their rows to a membership in a `weeks` column (`wk in weeks.ws` /
    # `wk in weeks.nxt`) AND the outer query scoped-joins each sum's own output
    # key back onto that same `weeks` column. The outer scoped join collapses the
    # feeder key onto the rowset's own output and adds it as a pseudonym; that
    # pseudonym used to leak into the rowset's independent-scope WHERE sourcing, so
    # building a sum rowset's membership existence resolved back to the rowset
    # itself — it depended on itself and the planner blew the Python stack
    # (`RecursionError`) instead of erroring cleanly. The fix strips scoped joins
    # touching a rowset's own outputs from that rowset's inner build, so the
    # self-weld no longer recurses. This bridge-via-a-third-rowset shape still
    # isn't resolvable (a separate scoped-join-to-rowset limitation), but now
    # surfaces as a clean, catchable error rather than a stack overflow.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_SELF_WELD_FIXTURE)
    with pytest.raises(DisconnectedConceptsException):
        executor.generate_sql("""
rowset weeks <- select wk as ws, wk + 53 as nxt where yr = 2001;
rowset cur_sums <-
    where chan = 'WEB' and wk in weeks.ws
    select wk as src_ws, sum(amt) as cur;
rowset nxt_sums <-
    where chan = 'WEB' and wk in weeks.nxt
    select wk as nxt_ws, sum(amt) as nxt;
select cur_sums.src_ws, cur_sums.cur, nxt_sums.nxt
left join cur_sums.src_ws = weeks.ws
inner join weeks.nxt = nxt_sums.nxt_ws
order by cur_sums.src_ws asc;
""")


_PER_ARM_FILTER_FIXTURE = """
key line_id int;
property line_id.item_id int;
property line_id.yr int;
property line_id.val int;

datasource lines (
    line_id: line_id,
    item_id: item_id,
    yr: yr,
    val: val,
)
grain (line_id)
query '''
select 1 as line_id, 1 as item_id, 2001 as yr, 10 as val union all
select 2 as line_id, 1 as item_id, 2002 as yr, 30 as val union all
select 3 as line_id, 2 as item_id, 2001 as yr, 5 as val union all
select 4 as line_id, 2 as item_id, 2002 as yr, 5 as val
''';
"""


def test_rowset_multiselect_per_arm_filter_on_exposed_column():
    # The q75 shape: a rowset (`deduped`) exposes a `yr` column, and the two
    # multi-select arms each narrow it to a different value (2002 / 2001). That
    # per-arm filter is a consumer filter on the rowset's exposed output and
    # MUST be applied to each arm independently. A too-broad independent-scope
    # exemption drops it, summing both years into both arms (10+30=40, 5+5=10)
    # so curr==prev everywhere; the correct result keeps the years separate.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_PER_ARM_FILTER_FIXTURE)
    results = executor.execute_text("""
rowset deduped <- where
    yr in (2001, 2002)
select
    item_id,
    yr,
    val,
;

rowset year_pair <- where
    deduped.yr = 2002
select
    deduped.item_id as item_curr,
    sum(deduped.val) as curr_total,
merge
where
    deduped.yr = 2001
select
    deduped.item_id as item_prev,
    sum(deduped.val) as prev_total,
align
    item: item_curr, item_prev
;

select
    year_pair.item,
    year_pair.prev_total,
    year_pair.curr_total,
order by year_pair.item asc;
""")[0].fetchall()
    assert [tuple(r) for r in results] == [(1, 10, 30), (2, 5, 5)]


_ROWSET_DEDUP_FIXTURE = """
key row_id int;
property row_id.group_key int;
property row_id.val1 int;
property row_id.val2 int;
property row_id.return_val1 int?;
property row_id.return_val2 int?;
property row_id.is_returned bool;

datasource src (
    row_id,
    group_key,
    val1,
    val2,
)
grain (row_id)
query '''
select 1 as row_id, 100 as group_key, 10 as val1, 100 as val2 union all
select 2 as row_id, 100 as group_key, 10 as val1, 200 as val2 union all
select 3 as row_id, 100 as group_key, 20 as val1, 100 as val2 union all
select 4 as row_id, 100 as group_key, 20 as val1, 200 as val2
''';

datasource src_returns (
    row_id: ~row_id,
    return_val1,
    return_val2,
    raw(''' True '''): is_returned,
)
grain (row_id)
complete where is_returned is True
query '''
select 0 as row_id, 0 as return_val1, 0 as return_val2 where false
''';
"""

# Source has 4 rows differing on (val1, val2) but sharing val1 ∈ {10, 20}
# and val2 ∈ {100, 200}. A full-tuple SELECT DISTINCT over (group_key,
# net_val1, net_val2) keeps all 4 rows; a per-column dedup collapses 4→2.


def test_rowset_full_tuple_dedup_plain_select():
    # Plain SELECT of the rowset's columns — no aggregates downstream. The
    # rowset's declared grain is the full projection; there should be one
    # materialization with GROUP BY over all 3 cols. This is the baseline:
    # if this fails the planner can't even get the dedup right when nothing
    # downstream consumes it.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_ROWSET_DEDUP_FIXTURE)
    results = executor.execute_text("""
auto net_val1 <- val1 - coalesce(return_val1, 0);
auto net_val2 <- val2 - coalesce(return_val2, 0);

SELECT group_key, net_val1, net_val2;
""")[0].fetchall()
    assert (
        len(results) == 4
    ), f"expected 4 distinct (gk, val1, val2) rows, got {len(results)}: {results}"
    assert {(r[0], r[1], r[2]) for r in results} == {
        (100, 10, 100),
        (100, 10, 200),
        (100, 20, 100),
        (100, 20, 200),
    }


def test_rowset_full_tuple_dedup_with_aggregates():
    # Same rowset shape, but with two SUM aggregates over the rowset's
    # row-level columns. This is the q75-shape bug: each aggregate plans
    # its own rowset materialization pruned to its own column, silently
    # splitting the rowset's declared grain across consumers. Both
    # aggregates should resolve via one shared rowset materialization at
    # the rowset's full grain.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_ROWSET_DEDUP_FIXTURE)
    results = executor.execute_text("""
auto net_val1 <- val1 - coalesce(return_val1, 0);
auto net_val2 <- val2 - coalesce(return_val2, 0);

with deduped as
SELECT group_key, net_val1, net_val2,
;

SELECT
    deduped.group_key as gk,
    sum(deduped.net_val1) as total_val1,
;
""")[0].fetchall()
    assert len(results) == 1
    row = results[0]
    assert row.gk == 100
    # 4 distinct (group_key, net_val1, net_val2) rows: sums should be 10+10+20+20=60 and 100+200+100+200=600
    assert (
        row.total_val1 == 60
    ), f"expected 60, got {row.total_val1} (per-column dedup of net_val1 collapsed 4 rows to 2)"


def test_rowset_full_tuple_dedup_with_aggregates_two():
    # Same rowset shape, but with two SUM aggregates over the rowset's
    # row-level columns. This is the q75-shape bug: each aggregate plans
    # its own rowset materialization pruned to its own column, silently
    # splitting the rowset's declared grain across consumers. Both
    # aggregates should resolve via one shared rowset materialization at
    # the rowset's full grain.
    executor = Dialects.DUCK_DB.default_executor()

    executor.execute_text(_ROWSET_DEDUP_FIXTURE)
    results = executor.execute_text("""
auto net_val1 <- val1 - coalesce(return_val1, 0);
auto net_val2 <- val2 - coalesce(return_val2, 0);

with deduped as
SELECT group_key, net_val1, net_val2,
;

SELECT
    deduped.group_key as gk,
    sum(deduped.net_val1) as total_val1,
    sum(deduped.net_val2) as total_val2,
;
""")[0].fetchall()
    assert len(results) == 1
    row = results[0]
    assert row.gk == 100
    # 4 distinct (group_key, net_val1, net_val2) rows: sums should be 10+10+20+20=60 and 100+200+100+200=600
    assert (
        row.total_val1 == 60
    ), f"expected 60, got {row.total_val1} (per-column dedup of net_val1 collapsed 4 rows to 2)"
    assert (
        row.total_val2 == 600
    ), f"expected 600, got {row.total_val2} (per-column dedup of net_val2 collapsed 4 rows to 2)"


# Variant: source has a 'year' column distinguishing rows. The q75 shape
# adds inline `?` filters per year (`sum(x ? year=YYYY)`), which trilogy
# implements by generating `_virt_filter_*` BASIC autos that wrap the
# rowset value via `CASE WHEN year=YYYY THEN x ELSE NULL END`. Those
# BASIC wrappers are an extra layer between the SUM and the rowset item;
# the planner must still see them as sourcing from the rowset and
# materialize the rowset *once* with its full declared grain.
_ROWSET_DEDUP_YEAR_FIXTURE = """
key row_id int;
property row_id.year int;
property row_id.group_key int;
property row_id.val1 int;
property row_id.val2 int;
property row_id.return_val1 int?;
property row_id.return_val2 int?;
property row_id.is_returned bool;

datasource src (
    row_id,
    year,
    group_key,
    val1,
    val2,
)
grain (row_id)
query '''
-- Two years × four (val1, val2) tuples that won't survive per-col dedup:
-- val1 ∈ {10, 20}, val2 ∈ {100, 200} per year.
select 1 as row_id, 2001 as year, 100 as group_key, 10 as val1, 100 as val2 union all
select 2 as row_id, 2001 as year, 100 as group_key, 10 as val1, 200 as val2 union all
select 3 as row_id, 2001 as year, 100 as group_key, 20 as val1, 100 as val2 union all
select 4 as row_id, 2001 as year, 100 as group_key, 20 as val1, 200 as val2 union all
select 5 as row_id, 2002 as year, 100 as group_key, 11 as val1, 110 as val2 union all
select 6 as row_id, 2002 as year, 100 as group_key, 11 as val1, 220 as val2 union all
select 7 as row_id, 2002 as year, 100 as group_key, 22 as val1, 110 as val2 union all
select 8 as row_id, 2002 as year, 100 as group_key, 22 as val1, 220 as val2
''';

datasource src_returns (
    row_id: ~row_id,
    return_val1,
    return_val2,
    raw(''' True '''): is_returned,
)
grain (row_id)
complete where is_returned is True
query '''
select 0 as row_id, 0 as return_val1, 0 as return_val2 where false
''';
"""


def test_rowset_full_tuple_dedup_with_filtered_aggregates():
    # The q75 shape: aggregates use inline `?` filters (e.g.,
    # `sum(deduped.x ? year = 2001)`). Each filtered SUM produces a
    # `_virt_filter_*` BASIC auto wrapping the rowset value, so the
    # SUM's direct argument is a BASIC concept — not a rowset item
    # directly. The planner must still recognize the underlying rowset
    # source and share one materialization across the four filtered
    # aggregates (val1 ∈ {2001, 2002} × val2 ∈ {2001, 2002}).
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_ROWSET_DEDUP_YEAR_FIXTURE)
    DebuggingHook(INFO)
    results = executor.execute_text("""
auto net_val1 <- val1 - coalesce(return_val1, 0);
auto net_val2 <- val2 - coalesce(return_val2, 0);

with deduped as
SELECT year, group_key, net_val1, net_val2,
;

SELECT
    deduped.group_key as gk,
    sum(deduped.net_val1 ? deduped.year = 2001) as v1_2001,
    sum(deduped.net_val1 ? deduped.year = 2002) as v1_2002,
    sum(deduped.net_val2 ? deduped.year = 2001) as v2_2001,
    sum(deduped.net_val2 ? deduped.year = 2002) as v2_2002,
;
""")[0].fetchall()
    assert len(results) == 1
    row = results[0]
    assert row.gk == 100
    # 4 distinct (year=2001, gk, v1, v2) rows: v1 sum = 10+10+20+20 = 60, v2 sum = 100+200+100+200 = 600
    # 4 distinct (year=2002, gk, v1, v2) rows: v1 sum = 11+11+22+22 = 66, v2 sum = 110+220+110+220 = 660
    assert row.v1_2001 == 60, f"expected 60, got {row.v1_2001}"
    assert row.v2_2001 == 600, f"expected 600, got {row.v2_2001}"
    assert row.v1_2002 == 66, f"expected 66, got {row.v1_2002}"
    assert row.v2_2002 == 660, f"expected 660, got {row.v2_2002}"


_TVF_UNION_FIXTURE = """
key line_id int;
property line_id.item_id int;
property line_id.yr int;
property line_id.val int;

datasource lines (
    line_id: line_id,
    item_id: item_id,
    yr: yr,
    val: val,
)
grain (line_id)
query '''
select 1 as line_id, 1 as item_id, 2001 as yr, 10 as val union all
select 2 as line_id, 1 as item_id, 2002 as yr, 30 as val union all
select 3 as line_id, 2 as item_id, 2001 as yr, 5 as val union all
select 4 as line_id, 2 as item_id, 2002 as yr, 5 as val
''';
"""


def test_tvf_union_named():
    # A named relational `union(...)` TVF: a column-positional row stack of two
    # arms. Output is exactly the bound columns; the result is a SQL UNION ALL
    # (rows stacked), NOT a key-join.
    executor = Dialects.DUCK_DB.default_executor()
    from trilogy.hooks import DebuggingHook
    DebuggingHook()
    executor.execute_text(_TVF_UNION_FIXTURE)
    query = """
with combined as union(
    (where yr = 2001 select item_id -> k, val -> v),
    (where yr = 2002 select item_id -> k, val -> v)
) -> (k, v);

select combined.k, combined.v
order by combined.k asc, combined.v asc;
"""
    sql = executor.generate_sql(query)[-1]
    assert "UNION ALL" in sql
    assert "FULL JOIN" not in sql
    results = [tuple(r) for r in executor.execute_text(query)[0].fetchall()]
    # Row count == sum of arm rows (stack, not join); each arm contributes 2 rows.
    assert results == [(1, 10), (1, 30), (2, 5), (2, 5)]


def test_tvf_union_named_aggregating_consumer():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_TVF_UNION_FIXTURE)
    results = executor.execute_text("""
with combined as union(
    (where yr = 2001 select item_id -> k, val -> v),
    (where yr = 2002 select item_id -> k, val -> v)
) -> (k, v);

select combined.k, sum(combined.v) -> total
order by combined.k asc;
""")[0].fetchall()
    assert [tuple(r) for r in results] == [(1, 40), (2, 10)]


def test_tvf_union_inline_from():
    # Inline `from union(...) -> (...)`: bare output names resolve in the
    # trailing select; same UNION semantics as the named form.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_TVF_UNION_FIXTURE)
    query = """
from union(
    (where yr = 2001 select item_id -> k, val -> v),
    (where yr = 2002 select item_id -> k, val -> v)
) -> (k, v)
select k, sum(v) -> total
order by k asc;
"""
    sql = executor.generate_sql(query)[-1]
    assert "UNION ALL" in sql
    results = executor.execute_text(query)[0].fetchall()
    assert [tuple(r) for r in results] == [(1, 40), (2, 10)]


def test_tvf_union_explicit_output_types():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_TVF_UNION_FIXTURE)
    results = executor.execute_text("""
with combined as union(
    (where yr = 2001 select item_id -> k, val -> v),
    (where yr = 2002 select item_id -> k, val -> v)
) -> (k int, v int?);

select combined.k, sum(combined.v) -> total
order by combined.k asc;
""")[0].fetchall()
    assert [tuple(r) for r in results] == [(1, 40), (2, 10)]


def test_tvf_union_arity_mismatch_errors():
    import pytest

    from trilogy.core.exceptions import InvalidSyntaxException

    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_TVF_UNION_FIXTURE)
    with pytest.raises(InvalidSyntaxException, match="exactly one"):
        executor.execute_text("""
with combined as union(
    (select item_id -> k, val -> v),
    (select item_id -> k)
) -> (k, v);
select combined.k;
""")


def test_tvf_union_output_alias_self_reference_errors():
    # Aliasing a union output column back to its own aligned name
    # (`combined.k as k`) closes a reference cycle: the rowset column `combined.k`
    # wraps the union's `k` output (`local.k`), and `as k` redefines `local.k` as
    # `alias(combined.k)`. This used to blow the stack with a RecursionError; it
    # must now raise a clean, actionable error.
    import pytest

    from trilogy.core.exceptions import InvalidSyntaxException

    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_TVF_UNION_FIXTURE)
    with pytest.raises(InvalidSyntaxException, match="refers back to itself"):
        executor.generate_sql("""
with combined as union(
    (where yr = 2001 select item_id -> k, val -> v),
    (where yr = 2002 select item_id -> k, val -> v)
) -> (k, v);
select combined.k as k, combined.v as v order by k asc;
""")


# Multiple rows per (item_id, yr) so an arm-level `sum(val)` is meaningful: a
# GROUP BY that wrongly includes the aggregate would either error or miscount.
_TVF_UNION_AGG_FIXTURE = """
key line_id int;
property line_id.item_id int;
property line_id.yr int;
property line_id.val int;

datasource lines (
    line_id: line_id,
    item_id: item_id,
    yr: yr,
    val: val,
)
grain (line_id)
query '''
select 1 as line_id, 1 as item_id, 2001 as yr, 10 as val union all
select 2 as line_id, 1 as item_id, 2001 as yr, 5 as val union all
select 3 as line_id, 2 as item_id, 2001 as yr, 5 as val union all
select 4 as line_id, 1 as item_id, 2002 as yr, 30 as val union all
select 5 as line_id, 2 as item_id, 2002 as yr, 5 as val union all
select 6 as line_id, 2 as item_id, 2002 as yr, 5 as val
''';
"""


def test_tvf_union_aggregating_arm_groups_by_dims_only():
    # An arm whose output column is itself an aggregate (`sum(val)`) must group
    # by the dimension column only, never by the aggregate. Per (item, yr):
    #   2001: item1 = 10+5 = 15, item2 = 5
    #   2002: item1 = 30,        item2 = 5+5 = 10
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_TVF_UNION_AGG_FIXTURE)
    query = """
with combined as union(
    (where yr = 2001 select item_id -> k, sum(val) -> v),
    (where yr = 2002 select item_id -> k, sum(val) -> v)
) -> (k, v);
select combined.k, combined.v
order by combined.k asc, combined.v asc;
"""
    sql = executor.generate_sql(query)[-1]
    assert "UNION ALL" in sql
    assert "GROUP BY" in sql
    results = [tuple(r) for r in executor.execute_text(query)[0].fetchall()]
    assert results == [(1, 15), (1, 30), (2, 5), (2, 10)]


def test_tvf_union_aggregating_arm_reaggregating_consumer():
    # The consumer can re-aggregate the stacked arm aggregates: total per item
    # across both years = item1: 15+30 = 45, item2: 5+10 = 15.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_TVF_UNION_AGG_FIXTURE)
    results = executor.execute_text("""
with combined as union(
    (where yr = 2001 select item_id -> k, sum(val) -> v),
    (where yr = 2002 select item_id -> k, sum(val) -> v)
) -> (k, v);
select combined.k, sum(combined.v) -> total
order by combined.k asc;
""")[0].fetchall()
    assert [tuple(r) for r in results] == [(1, 45), (2, 15)]


_TVF_UNION_JOIN_FIXTURE = """
key store_id int;
key return_store_id int;
property store_id.sale_amt int;
property return_store_id.return_amt int;

datasource sales (store_id: store_id, sale_amt: sale_amt)
grain (store_id)
query '''
select 1 as store_id, 100 as sale_amt union all
select 2 as store_id, 200 as sale_amt
''';

datasource returns (return_store_id: return_store_id, return_amt: return_amt)
grain (return_store_id)
query '''
select 1 as return_store_id, 10 as return_amt union all
select 2 as return_store_id, 20 as return_amt
''';

rowset srs <- select store_id as s_store, sum(sale_amt) as s_amt;
rowset rrs <- select return_store_id as r_store, sum(return_amt) as r_amt;
"""


def test_tvf_union_arm_local_join():
    # A union arm carries its OWN query-scoped join (across two rowsets with
    # distinct keys, mirroring q77's sales↔returns). The join is declared inside
    # the arm — not on the outer select — and must reach the arm's build via the
    # arm lineage's scoped_joins. Arm 0 nets returns off sales per store.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_TVF_UNION_JOIN_FIXTURE)
    query = """
with combined as union(
    (left join srs.s_store = rrs.r_store
     select srs.s_store -> k, srs.s_amt - coalesce(rrs.r_amt, 0) -> net),
    (select store_id -> k, sale_amt -> net)
) -> (k, net);
select combined.k, combined.net
order by combined.k asc, combined.net asc;
"""
    sql = executor.generate_sql(query)[-1]
    # the arm-local join was applied in the arm (joined on the scoped keys)
    assert "JOIN" in sql
    assert '"srs_s_store" = ' in sql and '"rrs_r_store"' in sql
    results = [tuple(r) for r in executor.execute_text(query)[0].fetchall()]
    # arm 0 (net of returns): store1 = 100-10 = 90, store2 = 200-20 = 180
    # arm 1 (raw sales):      store1 = 100,         store2 = 200
    assert results == [(1, 90), (1, 100), (2, 180), (2, 200)]


_LEFT_JOIN_UNMATCHED_FIXTURE = """
key store_id int;
key return_store_id int;
property store_id.sale_amt int;
property return_store_id.return_amt int;

datasource sales (store_id: store_id, sale_amt: sale_amt)
grain (store_id)
query '''
select 1 as store_id, 100 as sale_amt union all
select 2 as store_id, 200 as sale_amt
''';

datasource returns (return_store_id: return_store_id, return_amt: return_amt)
grain (return_store_id)
query '''select 1 as return_store_id, 10 as return_amt''';

rowset srs <- select store_id as s_store, sum(sale_amt) as s_amt;
rowset rrs <- select return_store_id as r_store, sum(return_amt) as r_amt;
"""


def test_scoped_left_join_coalesce_keeps_unmatched():
    # A scoped `left join` where the partial (right) rowset is referenced ONLY
    # through a non-null wrapper (coalesce). Returns covers store 1 only, so
    # store 2 is unmatched on the left and must survive with coalesce(NULL,0)=0.
    # Previously rendered INNER and dropped store 2.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_LEFT_JOIN_UNMATCHED_FIXTURE)
    query = """
left join srs.s_store = rrs.r_store
select srs.s_store -> k, srs.s_amt - coalesce(rrs.r_amt, 0) -> net
order by k asc;
"""
    sql = executor.generate_sql(query)[-1]
    assert "INNER JOIN" not in sql
    assert "LEFT OUTER JOIN" in sql
    results = [tuple(r) for r in executor.execute_text(query)[0].fetchall()]
    assert results == [(1, 90), (2, 200)]


def test_tvf_union_order_by_grouped_away_column():
    # Regression: an outer aggregate consumes a `union(...)` with asymmetric
    # measure arms (constant 0 vs sum), relabels a union sort column via CASE,
    # and ORDERS BY that sort column — which is NOT in the projection (it only
    # feeds the CASE). The aggregate groups the sort column away, so the renderer
    # used to crash ("Could not find upstream map for multiselect ..."). The sort
    # column must be carried into the grain so the ORDER BY resolves. Mirrors the
    # full-99 TPC-DS q05 union shape.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_TVF_UNION_FIXTURE)
    query = """
with combined as union(
    (where yr = 2001 select item_id -> sort_k, val -> a, 0 -> b),
    (where yr = 2002 select item_id -> sort_k, 0 -> a, val -> b)
) -> (sort_k, a, b);
select
  case combined.sort_k when 1 then 'one' when 2 then 'two' end -> label,
  sum(combined.a) -> total_a,
  sum(combined.b) -> total_b
order by combined.sort_k asc;
"""
    sql = executor.generate_sql(query)[-1]
    assert "UNION ALL" in sql
    results = [tuple(r) for r in executor.execute_text(query)[0].fetchall()]
    # item 1: a=val@2001=10, b=val@2002=30; item 2: a=val@2001=5, b=val@2002=5
    assert results == [("one", 10, 30), ("two", 5, 5)]


def test_tvf_union_signature_renamed_output_forward_reference():
    # Regression: a `union(...) -> (k, v)` whose arms project bare concept
    # literals (`item_id`, `val`) renamed ONLY by the output signature. A later
    # derived concept (`auto ... by combined.k`) forward-references those outputs
    # during BIND. collect_symbols declared the arm literals (combined.item_id)
    # and `as`-aliases but missed the signature renames, so combined.k was
    # undefined ("Suggestions: ['combined.v']"). The signature names must be
    # declared as forward-reference symbols.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_TVF_UNION_FIXTURE)
    query = """
with combined as union(
    (where yr = 2001 select item_id, val),
    (where yr = 2002 select item_id, val)
) -> (k, v);

auto total <- sum(combined.v) by combined.k;

select combined.k, total
order by combined.k asc;
"""
    sql = executor.generate_sql(query)[-1]
    assert "UNION ALL" in sql
    results = [tuple(r) for r in executor.execute_text(query)[0].fetchall()]
    # k=1: 10+30=40; k=2: 5+5=10
    assert results == [(1, 40), (2, 10)]


def test_tvf_union_binds_only_signature_outputs():
    # Negative of the above: a union(...) TVF rowset's outputs are EXACTLY its
    # `-> (...)` signature. The arm-internal names (`item_id`, `val`, the WHERE
    # column `yr`) must NOT leak as `combined.<name>` — collect_symbols used to
    # declare them as resolvable forward-reference placeholders, masking what
    # should be an undefined-reference error.
    from trilogy.core.models.environment import Environment
    from trilogy.parsing.parse_engine_v2 import TopLevelStatementParser, parse_syntax

    environment = Environment()
    environment.parse(_TVF_UNION_FIXTURE)
    parser = TopLevelStatementParser(environment=environment, import_keys=["root"])
    parser.parse(parse_syntax("""
with combined as union(
    (where yr = 2001 select item_id, val),
    (where yr = 2002 select item_id, val)
) -> (k, v);
"""))
    bound = {
        addr
        for addr in parser.hydrator.symbol_table.global_scope.definitions
        if "combined" in addr
    }
    assert bound == {"combined.k", "combined.v"}
