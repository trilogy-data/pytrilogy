import pytest

from trilogy import Dialects
from trilogy.core.exceptions import InvalidSyntaxException

_FIXTURE = """
key sale_id int;
property sale_id.brand int;
property sale_id.cls int;
property sale_id.qty int;
property sale_id.yr int;

datasource sales (
    sale_id: sale_id,
    brand: brand,
    cls: cls,
    qty: qty,
    yr: yr,
)
grain (sale_id)
query '''
select 1 as sale_id, 100 as brand, 1 as cls, 5 as qty, 1999 as yr union all
select 2 as sale_id, 200 as brand, 2 as cls, 7 as qty, 2000 as yr union all
select 3 as sale_id, 100 as brand, 2 as cls, 50 as qty, 1998 as yr union all
select 4 as sale_id, 300 as brand, 9 as cls, 2 as qty, 1998 as yr
''';
"""


def test_composite_tuple_membership_row_wise():
    # `combos` = {(100,1),(200,2)} (rows in the 1999-2001 window). Membership of
    # the (brand, cls) column tuple must match ROW-WISE: sale 3 is (100,2) — both
    # 100 and 2 appear individually in combos, but (100,2) is not a pair, so it
    # must be excluded (a per-column `brand in ... and cls in ...` would wrongly
    # include it and inflate brand 100 to 55).
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_FIXTURE)
    results = executor.execute_text("""
with combos as
where yr between 1999 and 2001
select brand, cls;

where (brand, cls) in (combos.brand, combos.cls)
select brand, sum(qty) as q
order by brand asc;
""")[0].fetchall()
    assert [tuple(r) for r in results] == [(100, 5), (200, 7)]


def test_composite_tuple_not_in():
    # NOT IN excludes the two member pairs (sale 1, sale 2), leaving sale 3
    # (100,2) and sale 4 (300,9).
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_FIXTURE)
    results = executor.execute_text("""
with combos as
where yr between 1999 and 2001
select brand, cls;

where (brand, cls) not in (combos.brand, combos.cls)
select brand, sum(qty) as q
order by brand asc;
""")[0].fetchall()
    assert [tuple(r) for r in results] == [(100, 50), (300, 2)]


def test_composite_tuple_arity_mismatch_clean_error():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_FIXTURE)
    with pytest.raises(InvalidSyntaxException):
        executor.generate_sql("""
with combos as
where yr between 1999 and 2001
select brand, cls;

where (brand, cls) in (combos.brand)
select brand;
""")


_HETERO_FIXTURE = """
key sale_id int;
property sale_id.nm string;
property sale_id.dt date;
property sale_id.qty int;
property sale_id.yr int;

datasource sales (
    sale_id: sale_id,
    nm: nm,
    dt: dt,
    qty: qty,
    yr: yr,
)
grain (sale_id)
query '''
select 1 as sale_id, 'alice' as nm, date '2000-01-01' as dt, 5 as qty, 1999 as yr union all
select 2 as sale_id, 'bob' as nm, date '2000-02-01' as dt, 7 as qty, 2000 as yr union all
select 3 as sale_id, 'alice' as nm, date '2000-02-01' as dt, 50 as qty, 1998 as yr union all
select 4 as sale_id, 'cara' as nm, date '2000-03-01' as dt, 2 as qty, 1998 as yr
''';
"""


def test_composite_tuple_heterogeneous_types():
    # A row tuple's positions are compared independently and may differ in type
    # (STRING, DATE). Row 3 (alice, 2000-02-01) shares each component with a
    # member pair but is not itself a pair, so it must be excluded.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_HETERO_FIXTURE)
    results = executor.execute_text("""
with combos as
where yr between 1999 and 2001
select nm, dt;

where (nm, dt) in (combos.nm, combos.dt)
select nm, sum(qty) as q
order by nm asc;
""")[0].fetchall()
    assert [tuple(r) for r in results] == [("alice", 5), ("bob", 7)]


def test_composite_tuple_heterogeneous_not_in():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_HETERO_FIXTURE)
    results = executor.execute_text("""
with combos as
where yr between 1999 and 2001
select nm, dt;

where (nm, dt) not in (combos.nm, combos.dt)
select nm, sum(qty) as q
order by nm asc;
""")[0].fetchall()
    assert [tuple(r) for r in results] == [("alice", 50), ("cara", 2)]


def test_composite_tuple_expression_operands():
    # Casting a tuple position (`dt::string`) makes the operand an expression
    # rather than a bare concept; both sides must still render as a composite
    # existence subquery.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_HETERO_FIXTURE)
    results = executor.execute_text("""
with combos as
where yr between 1999 and 2001
select nm, dt;

where (nm, dt::string) not in (combos.nm, combos.dt::string)
select nm, sum(qty) as q
order by nm asc;
""")[0].fetchall()
    assert [tuple(r) for r in results] == [("alice", 50), ("cara", 2)]


def test_composite_tuple_literal_position():
    # A literal RHS position pins that column to a constant; membership is
    # against {(100,2),(200,2)} so sales 3 (100,2) and 2 (200,2) match.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_FIXTURE)
    results = executor.execute_text("""
with combos as
where yr between 1999 and 2001
select brand, cls;

where (brand, cls) in (combos.brand, 2)
select brand, sum(qty) as q
order by brand asc;
""")[0].fetchall()
    assert [tuple(r) for r in results] == [(100, 50), (200, 7)]


def test_scalar_membership_heterogeneous_tuple_still_errors():
    # Scalar-vs-value-list membership requires every element to be comparable
    # to the left; the row-tuple leniency must not weaken this.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_HETERO_FIXTURE)
    with pytest.raises(Exception, match="[Cc]ompa"):
        executor.generate_sql("""
where nm in (dt, nm)
select nm;
""")


def test_scalar_value_list_membership_unchanged():
    # A scalar left with a tuple right is an ordinary value-list membership and
    # must keep working (not rewritten to composite).
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_FIXTURE)
    results = executor.execute_text("""
where brand in (100, 200)
select brand, sum(qty) as q
order by brand asc;
""")[0].fetchall()
    assert [tuple(r) for r in results] == [(100, 55), (200, 7)]
