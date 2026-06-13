"""Regression (TPC-DS q14 inline-avg shape) — was an open completion bug, now fixed.

A `by *` global aggregate that carries its own embedded `? ...` filter, projected
alongside a grouped body, under an outer WHERE with **two** row-conditions (plus a
membership existence). The outer WHERE correctly applies to the aggregate's input
(verified: `where yr=2001 select avg_all` filters the avg, it is not independent),
so the agg's scan filters on `yr=2001 AND mo=11`. But completion fails:

`initialize_loop_context` sets `completion_mandatory = mandatory_list +
conditions.row_arguments`, so the avg's sub-search is asked to *output* both `yr`
and `mo`. `yr` is producible (it doubles as the avg's embedded-filter gate, already
in `mandatory_list`); `mo` is an outer row-filter consumed inside the Abstract-grain
agg's scan, so no parent can surface it — `generate_loop_completion`'s MergeNode
`validate_inputs` raises `Invalid input concepts ... ['...mo'] are missing`.

A single outer row-condition (only `yr`) resolves because it coincides with the
embedded gate; the second condition (`mo`) has no such cover. Fix lives in the
completion path: an agg-consumed condition row-arg that no parent can output (but
is applied below) must not be required as a completion output.
"""

from trilogy import Dialects, Environment

MODEL = """
key item_id int;
property item_id.brand string;
key date_id int;
property date_id.yr int;
property date_id.mo int;
key row_id int;
property row_id.x float;
datasource facts (row_id: row_id, x: x, item_id: item_id, date_id: date_id)
grain (row_id)
query '''
select 1 row_id, 10.0 x, 7 item_id, 100 date_id
union all select 2, 20.0, 3, 200
union all select 3, 5.0, 9, 300
''';
datasource items (item_id: item_id, brand: brand)
grain (item_id)
query '''select 7 item_id, 'A' brand union all select 3, 'B' union all select 9, 'C' ''';
datasource dates (date_id: date_id, yr: yr, mo: mo)
grain (date_id)
query '''
select 100 date_id, 2001 yr, 11 mo
union all select 200, 2001 yr, 5 mo
union all select 300, 2001 yr, 11 mo
''';
auto avg_all <- avg(x) by * ? yr between 1999 and 2001;
rowset hot <- where yr between 1999 and 2001 select item_id as iid;
"""

# Two outer row-conditions (yr AND mo) + membership; mo=11 keeps rows 1 (A,x=10)
# and 3 (C,x=5). avg over those = (10+5)/2 = 7.5; body sums A=10, C=5.
MULTI_CONDITION_QUERY = (
    "where yr = 2001 and mo = 11 and item_id in hot.iid "
    "select brand as b, sum(x) as s, avg_all as a order by b;"
)


def _run(query: str):
    exec = Dialects.DUCK_DB.default_executor(environment=Environment())
    return [tuple(r) for r in exec.execute_query(MODEL + query).fetchall()]


def test_single_outer_condition_resolves():
    # Control: one outer row-condition (coincides with the embedded gate) resolves.
    # (yr=2001 keeps all rows here, so avg = (10+20+5)/3.)
    assert _run(
        "where yr = 2001 and item_id in hot.iid "
        "select brand as b, sum(x) as s, avg_all as a order by b;"
    ) == [("A", 10.0, 35 / 3), ("B", 20.0, 35 / 3), ("C", 5.0, 35 / 3)]


def test_global_agg_with_two_outer_conditions():
    # FIXED (completion now restricts the re-applied WHERE to atoms the stack can
    # produce; `mo` is consumed in the agg's scan, so it is dropped from
    # re-application rather than demanded as a merge output). avg over the mo=11
    # rows = (10+5)/2 = 7.5; body A=10, C=5.
    assert _run(MULTI_CONDITION_QUERY) == [("A", 10.0, 7.5), ("C", 5.0, 7.5)]
