import pytest

from trilogy import Dialects
from trilogy.core.exceptions import (
    DisconnectedConceptsException,
    InvalidSyntaxException,
)

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


_CROSS_FACT_FIXTURE = """
key ss_id int;
property ss_id.ss_customer int;
property ss_id.ss_item int;
property ss_id.ss_state string;
property ss_id.ss_qty int;
property ss_id.ss_yr int;

key cs_id int;
property cs_id.cs_customer int;
property cs_id.cs_item int;
property cs_id.cs_qty int;
property cs_id.cs_yr int;

datasource store_sales (
    ss_id: ss_id,
    customer: ss_customer,
    item: ss_item,
    state: ss_state,
    qty: ss_qty,
    yr: ss_yr,
)
grain (ss_id)
query '''
select 1 as ss_id, 10 as customer, 100 as item, 'CA' as state, 5 as qty, 2001 as yr union all
select 2 as ss_id, 20 as customer, 200 as item, 'NY' as state, 7 as qty, 2001 as yr union all
select 3 as ss_id, 10 as customer, 200 as item, 'CA' as state, 9 as qty, 1999 as yr
''';

datasource catalog_sales (
    cs_id: cs_id,
    customer: cs_customer,
    item: cs_item,
    qty: cs_qty,
    yr: cs_yr,
)
grain (cs_id)
query '''
select 1 as cs_id, 10 as customer, 100 as item, 3 as qty, 2001 as yr union all
select 2 as cs_id, 20 as customer, 200 as item, 4 as qty, 2001 as yr union all
select 3 as cs_id, 10 as customer, 200 as item, 8 as qty, 2001 as yr union all
select 4 as cs_id, 30 as customer, 100 as item, 6 as qty, 2001 as yr
''';
"""


def test_cross_fact_composite_membership_semi_join():
    # q17 shape: eligible (customer, item) pairs projected from one fact filter a
    # SECOND fact via composite membership. Pairs from 2001 store sales are
    # {(10,100),(20,200)}; catalog row (10,200) shares each component with a pair
    # but is not itself a pair, so it must be excluded row-wise.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_CROSS_FACT_FIXTURE)
    results = executor.execute_text("""
with pairs as
where ss_yr = 2001
select ss_customer as p_cust, ss_item as p_item;

where cs_yr = 2001
  and (cs_customer, cs_item) in (pairs.p_cust, pairs.p_item)
select cs_item, sum(cs_qty) as q
order by cs_item asc;
""")[0].fetchall()
    assert [tuple(r) for r in results] == [(100, 3), (200, 4)]


def test_cross_fact_composite_membership_eligibility_tracks_rowset_filter():
    # Widening the pair rowset's filter to include 1999 adds pair (10,200), so
    # catalog row (10,200) becomes eligible — the semi-join must respond to the
    # source rowset's WHERE, not silently drop it.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_CROSS_FACT_FIXTURE)
    results = executor.execute_text("""
with pairs as
where ss_yr between 1999 and 2001
select ss_customer as p_cust, ss_item as p_item;

where cs_yr = 2001
  and (cs_customer, cs_item) in (pairs.p_cust, pairs.p_item)
select cs_item, sum(cs_qty) as q
order by cs_item asc;
""")[0].fetchall()
    assert [tuple(r) for r in results] == [(100, 3), (200, 12)]


def test_cross_fact_membership_aggregate_rowset_joined():
    # Full q17 candidate shape: pair rowset from fact A; fact B aggregated under
    # composite membership into a rowset; that aggregate joined to an aggregate
    # rowset over fact A.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_CROSS_FACT_FIXTURE)
    results = executor.execute_text("""
with pairs as
where ss_yr = 2001
select ss_customer as p_cust, ss_item as p_item;

with cs_stats as
where cs_yr = 2001
  and (cs_customer, cs_item) in (pairs.p_cust, pairs.p_item)
select cs_item as c_item, sum(cs_qty) as cs_total;

with ss_stats as
where ss_yr = 2001
select ss_item as s_item, sum(ss_qty) as ss_total;

select
    ss_stats.s_item,
    ss_stats.ss_total,
    cs_stats.cs_total,
union join ss_stats.s_item = cs_stats.c_item
order by ss_stats.s_item asc;
""")[0].fetchall()
    assert [tuple(r) for r in results] == [(100, 5, 3), (200, 7, 4)]


def test_cross_fact_membership_blend_disconnect_names_predicate():
    # Blending aggregates of BOTH facts in one select with only a membership
    # predicate relating them is correctly disconnected (a semi-join filters the
    # left side; it does not attribute catalog rows to store dims). The error
    # must name the spanning membership predicate — its right side plans as a
    # separate existence island and is absent from the reported subgraphs, so
    # without the note the authored predicate looks silently dropped.
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_CROSS_FACT_FIXTURE)
    with pytest.raises(DisconnectedConceptsException) as exc_info:
        executor.generate_sql("""
with pairs as
where ss_yr = 2001
select ss_customer as p_cust, ss_item as p_item;

where ss_yr = 2001
  and cs_yr = 2001
  and (cs_customer, cs_item) in (pairs.p_cust, pairs.p_item)
select
    ss_state,
    sum(ss_qty) as total_ss_qty,
    sum(cs_qty) as total_cs_qty
;
""")
    message = str(exc_info.value)
    assert "membership predicate" in message
    assert "(cs_customer, cs_item) in (pairs.p_cust, pairs.p_item)" in message
    assert "does not join the two sides" in message


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
