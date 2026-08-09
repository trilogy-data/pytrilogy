"""Execution semantics of `then where` staged filters (duckdb, v4).

The contract: a staged query returns exactly the rows of the manual
inline-filter spelling — stage N's aggregates/windows compute over only the
rows passing stages 1..N-1, and the final row gate is the AND of all stages.
"""

from typing import Any

import pytest

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG, ParserBackend

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
