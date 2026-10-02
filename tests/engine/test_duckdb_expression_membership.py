import pytest

from trilogy import Dialects

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


def _exec():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_FIXTURE)
    return executor


def test_membership_expression_rhs_single_column():
    # `combos` brands in the 1999-2001 window = {100, 200}. An EXPRESSION RHS
    # (`combos.brand::string`) must render as an existence subselect that puts
    # `combos` in a FROM — historically it leaked a bare `combos.col` reference
    # against a never-joined CTE → Binder "Referenced table not found".
    results = _exec().execute_text("""
with combos as
where yr between 1999 and 2001
select brand;

where brand::string in (combos.brand::string)
select brand, sum(qty) as q
order by brand asc;
""")[0].fetchall()
    assert [tuple(r) for r in results] == [(100, 55), (200, 7)]


def test_membership_expression_rhs_not_in():
    results = _exec().execute_text("""
with combos as
where yr between 1999 and 2001
select brand;

where brand::string not in (combos.brand::string)
select brand, sum(qty) as q
order by brand asc;
""")[0].fetchall()
    assert [tuple(r) for r in results] == [(300, 2)]


def test_membership_expression_rhs_concat_multi_column():
    # A concat over TWO rowset columns: both resolve to the same existence CTE,
    # so the subselect projects the full concat expression. (brand|cls) pairs in
    # the window = {100|1, 200|2}; sale 3 is (100|2) — present column-wise but
    # not as a pair — so it is excluded.
    results = _exec().execute_text("""
with combos as
where yr between 1999 and 2001
select brand, cls;

where (brand::string || '|' || cls::string)
    in (combos.brand::string || '|' || combos.cls::string)
select brand, sum(qty) as q
order by brand asc;
""")[0].fetchall()
    assert [tuple(r) for r in results] == [(100, 5), (200, 7)]


def test_membership_bare_concept_rhs_unchanged():
    results = _exec().execute_text("""
with combos as
where yr between 1999 and 2001
select brand;

where brand in combos.brand
select brand, sum(qty) as q
order by brand asc;
""")[0].fetchall()
    assert [tuple(r) for r in results] == [(100, 55), (200, 7)]


_SELF_SET = """
key id string;
property id.x int;
property id.g string;
auto dx <- x ? x != 20;
"""

_SELF_SET_SOURCES = [
    "datasource t (id: id, x: x, g: g) grain (id) address t;",
    "datasource t (id: id, x: x, g: g) grain (id) query '''select * from tt''';",
]


def _self_set_rows(source: str, query: str) -> list[tuple]:
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_raw_sql(
        "create table t as select * from (values ('a', 10, 'B'), ('b', 20, 'g1'),"
        " ('c', 30, 'C')) v(id, x, g)"
    )
    executor.execute_raw_sql("create table tt as select * from t")
    executor.execute_text(_SELF_SET + source)
    rows = executor.execute_text(query)[-1].fetchall()
    return sorted((tuple(r) for r in rows), key=str)


@pytest.mark.parametrize("source", _SELF_SET_SOURCES)
@pytest.mark.parametrize(
    "query,expected",
    [
        ("select id, x where dx in x;", [("a", 10), ("c", 30)]),
        ("select id where dx in x;", [("a",), ("c",)]),
        ("select x where dx in x;", [(10,), (30,)]),
        ("select id, dx where dx in x;", [("a", 10), ("c", 30)]),
        ("select g, x where dx in x;", [("B", 10), ("C", 30)]),
        ("select g, count(id) as n where dx in x;", [("B", 1), ("C", 1)]),
        ("select id, x where lower(g) in id;", [("a", 10), ("c", 30)]),
    ],
)
def test_membership_over_a_set_the_row_also_reads(
    source: str, query: str, expected: list[tuple]
):
    assert _self_set_rows(source, query) == expected

