from decimal import Decimal

from tests.engine.test_duckdb_union_arm_cast import MODEL
from trilogy import Dialects

OVERLAP_MODEL = r"""
key k1 int;
key k2 int;

property k1.amt numeric(15,2);
property k2.pad numeric(15,2);

auto all_k <- union(k1, k2);
auto all_amt <- union(amt, pad);

datasource sales (
    x: k1,
    y: amt)
grain (k1)
query '''
select 3 as x, cast(5.0 as numeric(15,2)) as y
union all
select 4, cast(5.0 as numeric(15,2))
''';

datasource returns (
    x: k2,
    y: pad)
grain (k2)
query '''
select 3 as x, cast(5.0 as numeric(15,2)) as y
''';
"""


def test_union_bare_aggregate():
    exec = Dialects.DUCK_DB.default_executor()
    rows = exec.execute_query(MODEL + "\nselect sum(all_amt) -> total;").fetchall()
    assert rows[0].total == Decimal("0.30")


def test_union_bare_aggregate_repeated_planning():
    exec = Dialects.DUCK_DB.default_executor()
    first = exec.generate_sql(MODEL + "\nselect sum(all_amt) -> total;")
    second = exec.generate_sql("select sum(all_amt) -> total;")
    assert first[-1] == second[-1]


def test_union_grouped_aggregate():
    exec = Dialects.DUCK_DB.default_executor()
    rows = exec.execute_query(
        MODEL + "\nselect all_k, sum(all_amt) -> amt_total order by all_k asc;"
    ).fetchall()
    assert [(r.all_k, r.amt_total) for r in rows] == [
        (1, Decimal("0.10")),
        (2, Decimal("0.20")),
        (3, Decimal("0.00")),
        (4, Decimal("0.00")),
    ]


def test_union_overlapping_keys_set_semantics():
    """union() is a keyspace (set) union: a key present in both arms counts
    once, so the bare aggregate must equal aggregating the keyed select."""
    exec = Dialects.DUCK_DB.default_executor()
    keyed = exec.execute_query(OVERLAP_MODEL + "\nselect all_k, all_amt;").fetchall()
    oracle = sum(r.all_amt for r in keyed)
    assert oracle == Decimal("10.00")

    exec2 = Dialects.DUCK_DB.default_executor()
    bare = exec2.execute_query(
        OVERLAP_MODEL + "\nselect sum(all_amt) -> total;"
    ).fetchall()
    assert bare[0].total == oracle
