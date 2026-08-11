"""Execution semantics of `then where` staged filters (duckdb, v4).

The contract: a staged query returns exactly the rows of the manual
inline-filter spelling — stage N's aggregates/windows compute over only the
rows passing stages 1..N-1, and the final row gate is the AND of all stages.
"""

from typing import Any

import pytest

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG, ParserBackend
from trilogy.core.exceptions import UnresolvableQueryException

AGG_SCHEMA = """key id int;
property id.x int;
property id.z int;
property id.f int;
datasource d ( id, x, z, f ) grain (id)
query '''select 1 as id, 1 as x, 2 as z, 1 as f
union all select 2, 1, 10, 0
union all select 3, 2, 100, 1''';
"""
# Rows: (id=1, x=1, z=2, f=1), (id=2, x=1, z=10, f=0), (id=3, x=2, z=100, f=1).
# Filtered by f=1, sum(z) by x = {1: 2, 2: 100}; unfiltered = {1: 12, 2: 100}.

WINDOW_SCHEMA = """
key launch_id int;
property launch_id.vehicle_name string;
property launch_id.orb_pay float;
datasource launches ( launch_id, vehicle_name, orb_pay ) grain (launch_id)
query '''
select * from (values
    (1, 'A', 100.0), (2, 'A', 50.0), (3, 'B', 70.0),
    (4, 'B', 200.0), (5, 'C', 400.0), (6, 'C', 500.0)
) as t(launch_id, vehicle_name, orb_pay)
''';
"""


# Two CROSSING dimensions. A chain whose stages all group by the same key can
# never discriminate staged from flat — a group gate drops whole groups, which
# a later gate on that same key cannot see — so every gate-population test
# needs a second dimension to cut across the first.
CROSS_SCHEMA = """key id int;
property id.x string;
property id.y string;
property id.z int;
datasource d ( id, x, y, z ) grain (id)
query '''select * from (values
 (1,'a','p',10),(2,'a','q',1),(3,'b','p',1),
 (4,'b','q',1),(5,'c','p',5),(6,'c','p',5)
) as t(id,x,y,z)''';
"""
# sum(z) by x = {a: 11, b: 2, c: 10}; `> 5` keeps rows 1,2,5,6.
# count(id) by y over those = {p: 3, q: 1}; `> 1` keeps rows 1,5,6.
# count(id) by x over those = {a: 1, c: 2}; `> 1` keeps rows 5,6.


@pytest.fixture(params=[ParserBackend.PEST, ParserBackend.LARK])
def backend(request):
    prev = CONFIG.parser_backend
    CONFIG.parser_backend = request.param
    yield request.param
    CONFIG.parser_backend = prev


def _rows(model: str, query: str) -> list[tuple[Any, ...]]:
    env = Environment()
    env.parse(model)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    return sorted(
        (tuple(r) for r in executor.execute_query(query).fetchall()),  # type: ignore[union-attr]
        key=str,
    )


def test_staged_matches_manual_inline_filter(backend) -> None:
    staged = _rows(
        AGG_SCHEMA, "where f = 1 then where sum(z) by x > 5 select x, sum(z) as v;"
    )
    manual = _rows(
        AGG_SCHEMA,
        "where f = 1 and sum(z ? f = 1) by x > 5 select x, sum(z) as v;",
    )
    assert staged == manual == [(2, 100)], (staged, manual)


def test_staged_differs_from_flat(backend) -> None:
    # flat conjuncts do not filter each other: unfiltered sums pass both groups
    flat = _rows(AGG_SCHEMA, "where f = 1 and sum(z) by x > 5 select x, sum(z) as v;")
    staged = _rows(
        AGG_SCHEMA, "where f = 1 then where sum(z) by x > 5 select x, sum(z) as v;"
    )
    assert flat == [(1, 2), (2, 100)], flat
    assert staged == [(2, 100)], staged


def test_named_metric_in_later_stage(backend) -> None:
    rows = _rows(
        AGG_SCHEMA + "auto sx <- sum(z) by x;\n",
        "where f = 1 then where sx > 5 select x, sum(z) as v;",
    )
    assert rows == [(2, 100)], rows


def test_three_scalar_stages_before_aggregate(backend) -> None:
    rows = _rows(
        AGG_SCHEMA,
        "where f = 1 then where z > 1 then where sum(z) by x > 5 "
        "select x, sum(z) as v;",
    )
    assert rows == [(2, 100)], rows


def test_count_presence_guard(backend) -> None:
    # the guard counts only stage-filtered rows: x=2 has no f=0 rows
    rows = _rows(
        AGG_SCHEMA, "where f = 0 then where count(id) by x > 0 select x, sum(z) as v;"
    )
    assert rows == [(1, 10)], rows


def test_mixed_scalar_and_aggregate_in_later_stage(backend) -> None:
    # stage-2 scalar atom is a plain AND conjunct; the aggregate is stage-filtered
    rows = _rows(
        AGG_SCHEMA,
        "where f = 1 then where z > 1 and sum(z) by x > 5 select x, sum(z) as v;",
    )
    assert rows == [(2, 100)], rows


def test_same_aggregate_in_select_and_later_stage(backend) -> None:
    # the stage-2 gate is a distinct (stage-filtered) computation from the
    # select output even when spelled identically
    rows = _rows(
        AGG_SCHEMA,
        "where f = 1 then where sum(z) by x > 50 select x, sum(z) by x as v;",
    )
    assert rows == [(2, 100)], rows


def test_aggregate_stage_one_scalar_stage_two(backend) -> None:
    # an earlier cross-row stage with only scalar later stages is plain AND
    rows = _rows(AGG_SCHEMA, "where sum(z) by x > 5 then where f = 1 select x;")
    assert rows == [(1,), (2,)], rows


def test_window_in_later_stage_computes_over_filtered_rows(backend) -> None:
    # stage 1 excludes launch 6 (C 500). Staged rank over remaining sums:
    # C=400 #1, B=270 #2, A=150 #3 -> rank <= 2 admits C and B rows.
    rows = _rows(
        WINDOW_SCHEMA,
        "where orb_pay < 450 "
        "then where rank vehicle_name order by sum(orb_pay) by vehicle_name desc <= 2 "
        "select vehicle_name, sum(orb_pay) as total;",
    )
    assert [(r[0], float(r[1])) for r in rows] == [("B", 270.0), ("C", 400.0)], rows


def test_staged_sql_pushes_stage_filter_into_gate_scan(backend) -> None:
    env = Environment()
    env.parse(AGG_SCHEMA)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = executor.generate_sql(
        "where f = 1 then where sum(z) by x > 5 select x, sum(z) as v;"
    )[-1]
    # the gate aggregate's input scan carries the stage-1 filter
    assert '"f" = 1' in sql, sql
    rows = sorted(
        tuple(r)
        for r in executor.execute_query(
            "where f = 1 then where sum(z) by x > 5 select x, sum(z) as v;"
        ).fetchall()
    )
    assert rows == [(2, 100)], rows


def test_staged_where_inside_rowset_body(backend) -> None:
    rows = _rows(
        AGG_SCHEMA,
        "rowset r <- where f = 1 then where sum(z) by x > 5 select x, sum(z) as v;\n"
        "select r.x, r.v order by r.x asc;",
    )
    assert rows == [(2, 100)], rows


EXISTENCE_STAGE_ONE = [
    ("in (select ...)", "id in (select id where f = 1)"),
    ("not in (select ...)", "id not in (select id where f = 0)"),
]


@pytest.mark.parametrize("label,predicate", EXISTENCE_STAGE_ONE)
def test_existence_stage_delivers_into_later_aggregate(
    backend, label: str, predicate: str
) -> None:
    # an existence predicate rides the gate's input scan as a semi-join feeder,
    # so the staged rows match the manual inline-filter spelling
    staged = _rows(
        AGG_SCHEMA,
        f"where {predicate} then where sum(z) by x > 5 select x, sum(z) as v;",
    )
    manual = _rows(
        AGG_SCHEMA,
        f"where {predicate} and sum(z ? {predicate}) by x > 5 "
        "select x, sum(z) as v;",
    )
    assert staged == manual == [(2, 100)], (label, staged, manual)


def test_existence_stage_delivers_into_later_window(backend) -> None:
    staged = _rows(
        AGG_SCHEMA,
        "where id in (select id where f = 1) "
        "then where rank x order by sum(z) by x desc <= 1 select x, sum(z) as v;",
    )
    assert staged == [(2, 100)], staged


def test_existence_stage_semi_join_lands_on_the_gate_scan(backend) -> None:
    env = Environment()
    env.parse(AGG_SCHEMA)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = executor.generate_sql(
        "where id in (select id where f = 1) then where sum(z) by x > 5 "
        "select x, sum(z) as v;"
    )[-1]
    before_having = sql[: sql.index("HAVING")]
    # the gate aggregate reads a scan the existence feeder already filtered
    assert "exists (select 1 from" in before_having, sql


def test_staged_with_having(backend) -> None:
    rows = _rows(
        AGG_SCHEMA,
        "where f = 1 then where sum(z) by x > 5 select x, sum(z) as v having v > 1;",
    )
    assert rows == [(2, 100)], rows


def test_staged_with_order_by_and_limit(backend) -> None:
    rows = _rows(
        AGG_SCHEMA,
        "where f = 0 then where count(id) by x > 0 "
        "select x, sum(z) as v order by x desc limit 1;",
    )
    assert rows == [(1, 10)], rows


def test_staged_in_post_select_where_slot(backend) -> None:
    rows = _rows(
        AGG_SCHEMA,
        "select x, sum(z) as v where f = 1 then where sum(z) by x > 5;",
    )
    assert rows == [(2, 100)], rows


def test_staged_multiselect_arm(backend) -> None:
    # the staged arm contributes only x=2; the flat arm contributes both
    rows = _rows(
        AGG_SCHEMA,
        "where f = 1 then where sum(z) by x > 5 select x as xa, sum(z) as va\n"
        "merge\n"
        "where f = 1 select x as xb, count(id) as cb\n"
        "align xx: xa, xb;",
    )
    assert [(r[0], r[1]) for r in rows] == [(1, None), (2, 2)], rows


def test_staged_with_scoped_join(backend) -> None:
    model = AGG_SCHEMA + """
key bid int;
property bid.bx int;
property bid.bflag int;
datasource b ( bid, bx, bflag ) grain (bid)
query '''select 1 as bid, 1 as bx, 1 as bflag union all select 2, 2, 0''';
"""
    query = "select x, sum(z) as v subset join bx = x;"
    staged = _rows(model, "where bflag = 1 then where sum(z) by x > 5 " + query)
    manual = _rows(model, "where bflag = 1 and sum(z ? bflag = 1) by x > 5 " + query)
    assert staged == manual, (staged, manual)


def test_or_and_not_at_stage_root(backend) -> None:
    disjunction = _rows(
        AGG_SCHEMA,
        "where f = 1 then where sum(z) by x > 5 or sum(z) by x < 1 "
        "select x, sum(z) as v;",
    )
    negation = _rows(
        AGG_SCHEMA,
        "where f = 1 then where not sum(z) by x > 5 select x, sum(z) as v;",
    )
    assert disjunction == [(2, 100)], disjunction
    assert negation == [(1, 2)], negation


def test_cross_datasource_aggregate_in_later_stage(backend) -> None:
    model = AGG_SCHEMA + """
key oid int;
property oid.amt int;
datasource o ( oid, amt, oref:id ) grain (oid)
query '''select 10 as oid, 7 as amt, 1 as oref
union all select 11, 9, 2
union all select 12, 11, 3''';
"""
    staged = _rows(
        model, "where f = 1 then where sum(amt) by x > 8 select x, sum(z) as v;"
    )
    manual = _rows(
        model,
        "where f = 1 and sum(amt ? f = 1) by x > 8 select x, sum(z) as v;",
    )
    assert staged == manual == [(2, 100)], (staged, manual)


def test_grand_total_aggregate_in_later_stage(backend) -> None:
    # no `by` clause: the gate is a single keyless aggregate over stage-1 rows
    staged = _rows(
        AGG_SCHEMA, "where f = 1 then where sum(z) > 50 select x, sum(z) as v;"
    )
    manual = _rows(
        AGG_SCHEMA, "where f = 1 and sum(z ? f = 1) > 50 select x, sum(z) as v;"
    )
    assert staged == manual == [(2, 100)], (staged, manual)


def test_trailing_scalar_stage_does_not_bound_the_gate(backend) -> None:
    # `z > 1` runs AFTER the gate stage, so the gate's sum still sees every
    # f = 1 row -- it must not leak backwards into the aggregate's input
    env = Environment()
    env.parse(AGG_SCHEMA)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    query = (
        "where f = 1 then where sum(z) by x > 5 then where z > 1 "
        "select x, sum(z) as v;"
    )
    sql = executor.generate_sql(query)[-1]
    before_having = sql[: sql.index("HAVING")]
    gate_cte = before_having[before_having.rindex(" as (") :]
    assert '"f" = 1' in gate_cte, sql
    assert '"z" > 1' not in gate_cte, sql
    assert _rows(AGG_SCHEMA, query) == [(2, 100)]


def test_cross_row_stage_before_cross_row_stage(backend) -> None:
    # stage-1's gate computes over the FULL population (both x groups pass),
    # and stage-2's count over stage-1 survivors removes nothing, so the
    # staged result equals the stage-1-only spelling
    staged = _rows(
        AGG_SCHEMA,
        "where f = 1 and sum(z) by x > 5 then where count(id) by x > 0 "
        "select x, sum(z) as v;",
    )
    stage_one_only = _rows(
        AGG_SCHEMA, "where f = 1 and sum(z) by x > 5 select x, sum(z) as v;"
    )
    assert staged == stage_one_only == [(1, 2), (2, 100)], (staged, stage_one_only)


def test_later_gate_counts_only_stage_filtered_rows(backend) -> None:
    # stage-1 rows are the f = 1 rows (one per x group), so the staged count
    # is 1 per group and `> 1` empties the result; the flat spelling counts
    # the unfiltered population (x=1 has 2 rows) and keeps x=1
    staged = _rows(
        AGG_SCHEMA,
        "where f = 1 and sum(z) by x > 5 then where count(id) by x > 1 "
        "select x, sum(z) as v;",
    )
    flat = _rows(
        AGG_SCHEMA,
        "where f = 1 and sum(z) by x > 5 and count(id) by x > 1 "
        "select x, sum(z) as v;",
    )
    assert staged == [], staged
    assert flat == [(1, 2)], flat


def test_window_stage_before_aggregate_stage(backend) -> None:
    # stage-1 keeps the top-4 launches by payload (ids 6,5,4,1); stage-2's
    # per-vehicle count over those is A=1, B=1, C=2, so only C survives
    staged = _rows(
        WINDOW_SCHEMA,
        "where rank launch_id order by orb_pay desc <= 4 "
        "then where count(launch_id) by vehicle_name > 1 "
        "select vehicle_name, sum(orb_pay) as total;",
    )
    assert [(r[0], float(r[1])) for r in staged] == [("C", 900.0)], staged


def test_aggregate_stage_then_window_stage_then_aggregate_stage(backend) -> None:
    # stage-1 drops launch 6; stage-2 ranks vehicles by their stage-1 sums
    # (C=400 #1, B=270 #2, A=150 #3) keeping B and C; stage-3 counts the
    # rows passing both stages (B=2, C=1) keeping only B
    staged = _rows(
        WINDOW_SCHEMA,
        "where orb_pay < 450 "
        "then where rank vehicle_name order by sum(orb_pay) by vehicle_name desc <= 2 "
        "then where count(launch_id) by vehicle_name > 1 "
        "select vehicle_name, sum(orb_pay) as total;",
    )
    assert [(r[0], float(r[1])) for r in staged] == [("B", 270.0)], staged


def test_earlier_aggregate_gate_bounds_later_gate_input(backend) -> None:
    # stage-1's gate keeps only the x=2 row (id 3), so stage-2's grand total
    # counts 1 and passes; counting the unfiltered population (3 rows) would
    # fail the gate and empty the result
    staged = _rows(
        AGG_SCHEMA,
        "where sum(z) by x > 50 then where count(id) <= 2 select x, sum(z) as v;",
    )
    assert staged == [(2, 100)], staged


def test_existence_stage_before_two_cross_row_stages(backend) -> None:
    staged = _rows(
        AGG_SCHEMA,
        "where id in (select id where f = 1) "
        "then where sum(z) by x > 5 "
        "then where count(id) by x > 0 "
        "select x, sum(z) as v;",
    )
    assert staged == [(2, 100)], staged


def test_cross_row_stages_on_crossing_keys(backend) -> None:
    staged = _rows(
        CROSS_SCHEMA,
        "where sum(z) by x > 5 then where count(id) by y > 1 "
        "select y, count(id) as c;",
    )
    # flat conjuncts count the unfiltered population, where y=q also passes
    flat = _rows(
        CROSS_SCHEMA,
        "where sum(z) by x > 5 and count(id) by y > 1 select y, count(id) as c;",
    )
    assert staged == [("p", 3)], staged
    assert flat == [("p", 3), ("q", 1)], flat


def test_three_cross_row_stages_grand_total_last(backend) -> None:
    # stage-3 counts the rows passing stages 1 and 2 (3 of them); over the
    # stage-1-only population it would count 4 and empty the result
    staged = _rows(
        CROSS_SCHEMA,
        "where sum(z) by x > 5 then where count(id) by y > 1 "
        "then where count(id) <= 3 select x, sum(z) as v;",
    )
    assert staged == [("a", 10), ("c", 10)], staged


def test_scalar_stage_before_two_cross_row_stages(backend) -> None:
    # z > 1 keeps rows 1,5,6; count by y over those keeps all three; count by
    # x over those is a=1, c=2, so only c survives
    staged = _rows(
        CROSS_SCHEMA,
        "where z > 1 then where count(id) by y > 1 then where count(id) by x > 1 "
        "select x, sum(z) as v;",
    )
    assert staged == [("c", 10)], staged


def test_rowset_body_with_two_cross_row_stages(backend) -> None:
    rows = _rows(
        CROSS_SCHEMA,
        "rowset r <- where sum(z) by x > 5 then where count(id) by y > 1 "
        "select x, y, sum(z) as v;\n"
        "select r.x, r.v;",
    )
    assert rows == [("a", 10), ("c", 10)], rows


def test_cross_row_stage_bound_without_feeder_raises(backend) -> None:
    # the documented limitation: a rowset-fed host has no re-plannable feeder
    # scan for the earlier gate to ride, so it is a typed error, never a
    # silently dropped bound
    env = Environment()
    env.parse(CROSS_SCHEMA + "rowset r <- select id, x, z where z > 1;\n")
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    with pytest.raises(UnresolvableQueryException, match="no re-plannable feeder"):
        executor.generate_sql(
            "where sum(r.z) by r.x > 5 then where count(r.id) by r.x > 1 "
            "select r.x, sum(r.z) as v;"
        )


def test_plain_select_with_two_cross_row_stages(backend) -> None:
    staged = _rows(
        CROSS_SCHEMA,
        "where sum(z) by x > 5 then where count(id) by y > 1 select y;",
    )
    oracle = _rows(
        CROSS_SCHEMA,
        "where sum(z) by x > 5 and count(id ? sum(z) by x > 5) by y > 1 select y;",
    )
    assert staged == oracle == [("p",)], (staged, oracle)


def test_three_cross_row_stages_keyed_last(backend) -> None:
    # stage-3's gate must see stage-2's bound: over the stage-1-only
    # population both x groups have 2 rows and would pass
    staged = _rows(
        CROSS_SCHEMA,
        "where sum(z) by x > 5 then where count(id) by y > 1 "
        "then where count(id) by x > 1 select x, sum(z) as v;",
    )
    assert staged == [("c", 10)], staged


def test_four_cross_row_stages(backend) -> None:
    chain = (
        "where sum(z) by x > 5 then where count(id) by y > 1 "
        "then where count(id) by x > 1 "
    )
    # rows 5,6 survive the first three stages, so stage-4's max over them is 5;
    # over any earlier population it would be 10 (row 1) and pass the > 6 gate
    assert _rows(CROSS_SCHEMA, chain + "then where max(z) by y > 6 select id;") == []
    assert _rows(CROSS_SCHEMA, chain + "then where max(z) by y > 4 select id;") == [
        (5,),
        (6,),
    ]


def test_staged_persist_round_trips(backend) -> None:
    env = Environment()
    env.parse(AGG_SCHEMA)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    executor.execute_query(
        "persist pz into ptbl from where f = 1 then where sum(z) by x > 5 "
        "select id, x, z;"
    )
    rows = sorted(tuple(r) for r in executor.execute_raw_sql("select * from ptbl"))
    assert rows == [(3, 2, 100)], rows


def test_partial_datasource_still_admitted_by_scalar_conjunct(backend) -> None:
    # a `complete where` datasource matching the stage-1 scalar atom must stay
    # eligible under a staged clause
    model = AGG_SCHEMA + """
partial datasource d_f1 ( id, x, z, f ) grain (id)
complete where f = 1
query '''select 1 as id, 1 as x, 2 as z, 1 as f
union all select 3, 2, 100, 1''';
"""
    rows = _rows(model, "where f = 1 then where sum(z) by x > 5 select x, sum(z) as v;")
    assert rows == [(2, 100)], rows
