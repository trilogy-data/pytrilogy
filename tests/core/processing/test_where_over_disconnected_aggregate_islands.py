"""A top-level WHERE defines ONE row population, so every output must read from a
source the filter can reach.

Two scalar aggregates over unrelated models are a well-defined cross join with no
WHERE. Add one and the shape becomes an author error: the filter can restrict only
its own island, leaving the other aggregate silently unfiltered. That shape used to
slip past every connectivity check (single-row outputs are skipped as crossjoinable)
and die at render on `INVALID_REFERENCE_BUG` sentinels with both measures unsourced.
"""

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import DisconnectedConceptsException
from trilogy.core.models.environment import Environment

_ISLANDS = """
key sale_id int;
property sale_id.amt float;
property sale_id.sdate date;
datasource sales (id: sale_id, a: amt, d: sdate) grain (sale_id)
query '''select 1 id, 10.0 a, date '2001-01-01' d
    union all select 2 id, 5.0 a, date '1999-01-01' d''';

key return_id int;
property return_id.ramt float;
property return_id.rdate date;
datasource returns (id: return_id, r: ramt, d: rdate) grain (return_id)
query '''select 1 id, 100.0 r, date '2001-01-01' d
    union all select 2 id, 200.0 r, date '1999-01-01' d''';
"""


def _engine():
    env = Environment()
    env.parse(_ISLANDS)
    return Dialects.DUCK_DB.default_executor(environment=env)


def test_two_scalar_aggregate_islands_cross_join_without_where():
    rows = (
        _engine()
        .execute_text(
            "select sum(amt) as s, sum(ramt) as r;",
        )[0]
        .fetchall()
    )
    assert [tuple(row) for row in rows] == [(15.0, 300.0)]


def test_where_on_one_island_raises_population_split():
    with pytest.raises(DisconnectedConceptsException) as exc:
        _engine().generate_sql(
            "select sum(amt) as s, sum(ramt) as r where sdate > '2000-01-01'::date;"
        )
    message = str(exc.value)
    assert "cannot restrict output(s)" in message
    assert "['sdate']" in message
    assert "['r']" in message
    assert "sum(x ? <condition>)" in message
    groups = {frozenset(group) for group in exc.value.subgraphs}
    assert groups == {frozenset({"local.r"}), frozenset({"local.sdate"})}


def test_where_on_both_islands_raises():
    with pytest.raises(DisconnectedConceptsException):
        _engine().generate_sql(
            "select sum(amt) as s, sum(ramt) as r "
            "where sdate > '2000-01-01'::date and rdate > '2000-01-01'::date;"
        )


def test_where_on_the_only_island_still_plans():
    rows = (
        _engine()
        .execute_text(
            "select sum(amt) as s where sdate > '2000-01-01'::date;",
        )[0]
        .fetchall()
    )
    assert [tuple(row) for row in rows] == [(10.0,)]


def test_where_beside_a_constant_output_still_plans():
    rows = (
        _engine()
        .execute_text(
            "select sum(amt) as s, 5 as five where sdate > '2000-01-01'::date;",
        )[0]
        .fetchall()
    )
    assert [tuple(row) for row in rows] == [(10.0, 5)]


def test_parentless_group_keeps_literal_producible_outputs():
    # The refusal above works by no longer trusting a group whose parents all
    # failed to build. A group that legitimately has no parents must survive it:
    # `sum(1)`, a constant and a parameter render from literals alone, and
    # `concept_satisfiable` reads their empty row lineage as a dead end.
    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    assert engine.execute_text("select sum(1) as c;")[0].fetchall() == [(1,)]
    assert (
        engine.execute_text("const c <- 10; select c as x where c > 50;")[0].fetchall()
        == []
    )
    assert engine.execute_text(
        "parameter p int default 10; select p as x where p < 50;"
    )[0].fetchall() == [(10,)]


def test_per_island_inline_filter_is_the_working_spelling():
    rows = (
        _engine()
        .execute_text(
            "select sum(amt ? sdate > '2000-01-01'::date) as s, sum(ramt) as r;",
        )[0]
        .fetchall()
    )
    assert [tuple(row) for row in rows] == [(10.0, 300.0)]
