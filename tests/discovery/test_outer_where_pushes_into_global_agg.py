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
