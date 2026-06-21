from trilogy import Dialects, Environment

# An outer WHERE row predicate must push into a `by *` global aggregate's INPUT
# (pre-aggregation), whether or not the aggregate carries its own post-aggregate
# `? ...` filter. Rows: (x, z, other)
#   (10,1,2) (20,1,3) (30,0,2) (40,0,3)
# `where z=1` selects rows 1&2; avg(x) over them = 15.
MODEL = """
key id int;
property id.x float;
property id.z int;
property id.other int;
datasource rows (
    id: id, x: x, z: z, other: other,
)
grain (id)
query '''
select 1 as id, 10.0 as x, 1 as z, 2 as other
union all select 2, 20.0, 1, 3
union all select 3, 30.0, 0, 2
union all select 4, 40.0, 0, 3
''';
"""


def _run(query: str):
    exec = Dialects.DUCK_DB.default_executor(environment=Environment())
    return [tuple(r) for r in exec.execute_query(MODEL + query).fetchall()]


def test_outer_where_pushes_into_plain_global_agg():
    # avg over z=1 rows = avg(10,20) = 15
    assert _run("where z=1 select avg(x) by * as result;") == [(15.0,)]


def test_outer_where_pushes_into_global_agg_with_post_agg_filter():
    # `? other=2` is a POST-aggregate gate, not pushed into the input. So the
    # input is still scoped only by the outer `where z=1`: avg(10,20)=15, and the
    # post-agg gate passes because a matching other=2 row exists.
    assert _run("where z=1 select avg(x) by * ? other = 2 as result;") == [(15.0,)]


def test_inside_filter_is_pre_agg():
    # Filter INSIDE the aggregate restricts its input: over z=1 rows, only row 1
    # has other=2, so avg = 10. (Contrasts with the post-agg form above.)
    assert _run("where z=1 select avg(x ? other = 2) by * as result;") == [(10.0,)]


# Cross-grain regression: a `by *` global agg with its own post-agg `? yr=1999`
# gate, projected alongside a grouped body, under an outer WHERE on a *different*
# dimension table than the gate. The gate dim (`yr`, on `dates`) had to be merged
# while carrying the outer `item_id>5` (on `facts`/`items`); condition pruning
# stranded `dates` and the query disconnected. Fact rows (x, item_id, date_id):
#   (10,7,100) (20,3,200) (5,9,100); items 7,3,9; dates 100->1999, 200->2000.
# `item_id>5` keeps rows 1 & 3 (items 7,9); avg over them = (10+5)/2 = 7.5, gated
# by yr=1999 existence (both are 1999). Groups: brand A=10, brand C=5.
CROSS_GRAIN = """
key item_id int;
property item_id.brand string;
key date_id int;
property date_id.yr int;
key row_id int;
property row_id.x float;
datasource facts (row_id: row_id, x: x, item_id: item_id, date_id: date_id)
grain (row_id)
query '''
select 1 row_id, 10.0 x, 7 item_id, 100 date_id
union all select 2, 20.0, 3, 200
union all select 3, 5.0, 9, 100
''';
datasource items (item_id: item_id, brand: brand)
grain (item_id)
query '''select 7 item_id, 'A' brand union all select 3, 'B' union all select 9, 'C' ''';
datasource dates (date_id: date_id, yr: yr)
grain (date_id)
query '''select 100 date_id, 1999 yr union all select 200, 2000 yr''';
auto a <- avg(x) by * ? yr = 1999;
"""


CROSS_GRAIN_QUERY = (
    "where item_id > 5 select brand as b, sum(x) as s, a as _avg order by b;"
)


def test_cross_grain_global_agg_gate_under_outer_where():
    exec = Dialects.DUCK_DB.default_executor(environment=Environment())
    rows = [
        tuple(r) for r in exec.execute_query(CROSS_GRAIN + CROSS_GRAIN_QUERY).fetchall()
    ]
    # avg = 7.5 (not 11.66...) proves item_id>5 filtered the aggregate's INPUT.
    # A disconnect would raise; a flatten regression would give 11.66... over all
    # rows. This one assert covers both failure modes.
    assert rows == [("A", 10.0, 7.5), ("C", 5.0, 7.5)]


def test_cross_grain_predicate_stays_in_aggregate_scan():
    # Structural guard for the merge-prune fallback: condition pruning must not
    # disconnect the gate dim's join-partner, AND the predicate must remain pushed
    # into the aggregate's own scan (push-down mode), not lifted to a post-join
    # WHERE (the broken flatten leaves the avg CTE scanning the unfiltered fact).
    exec = Dialects.DUCK_DB.default_executor(environment=Environment())
    exec.parse_text(CROSS_GRAIN)
    sql = exec.generate_sql(CROSS_GRAIN_QUERY)[-1]
    # isolate the CTE that computes the global avg (up to the next CTE header)
    avg_at = sql.index("avg(")
    next_cte = sql.find(" as (", avg_at)
    avg_cte = sql[avg_at:next_cte] if next_cte != -1 else sql[avg_at:]
    assert "item_id" in avg_cte and "> 5" in avg_cte, avg_cte
