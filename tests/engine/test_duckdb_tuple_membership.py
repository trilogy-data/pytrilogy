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
