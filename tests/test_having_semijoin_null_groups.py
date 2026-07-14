"""HAVING grain-key semijoins must not delete NULL-keyed groups (q11/q59).

The post-aggregation semijoin filters select-grain groups by row identity:
``(k1, k2) in (filter k1 where p, filter k2 where p)``. SQL row-``IN`` is
NULL-unsafe (``(1, NULL) IN (select 1, NULL)`` is NULL, never TRUE), so the
membership must render as a null-safe existence subquery — a group whose key
tuple carries a NULL (a NULL dimension value, or a nullable derived measure
that is a grain component) is a genuine group and must survive when its rows
pass the predicate.

Composite (row-tuple) membership is row *identity* matching everywhere: a NULL
component matches a NULL component, for authored tuples as well as generated
semijoins. A model that needs SQL parity (tpc_ds query14 vs its reference's
inner joins) excludes NULL components from the set side explicitly.
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

# customer_id / first_name are properties of the surrogate key, so a rowset
# over (customer_id, first_name) has BOTH as grain components — the composite
# semijoin shape. C2's first_name is NULL.
MODEL = """
key customer_sk int;
property customer_sk.customer_id string;
property customer_sk.first_name string;
key sale_id int;
property sale_id.customer_sk int;
property sale_id.amount int;

datasource customers (sk: customer_sk, id: customer_id, fname: first_name)
grain (customer_sk)
query '''select * from (values (1,'C1','Alice'),(2,'C2',null),(3,'C3','Cara')) as t(sk,id,fname)''';

datasource sales (id: sale_id, cust: customer_sk, amt: amount)
grain (sale_id)
query '''select * from (values (1,1,100),(2,1,60),(3,2,200),(4,3,50)) as t(id,cust,amt)''';
"""
# per-customer totals: C1 -> 160, C2 -> 200 (NULL name), C3 -> 50


def _exec():
    e = Dialects.DUCK_DB.default_executor(environment=Environment())
    e.parse_text(MODEL)
    return e


def _rows(body: str):
    return [tuple(r) for r in _exec().execute_query(body).fetchall()]


def _sql(body: str) -> str:
    return "\n".join(_exec().generate_sql(body)).lower()


REV = """with rev as
select customer_id as cid, first_name as fname, sum(amount) as tot;
"""


def test_composite_semijoin_keeps_null_dim_group():
    # q11: HAVING on the non-projected measure builds a (cid, fname) semijoin;
    # the NULL-first_name group C2 passes the predicate and must survive.
    rows = _rows(
        REV + "select rev.cid, rev.fname having rev.tot > 90 order by rev.cid asc;"
    )
    assert rows == [("C1", "Alice"), ("C2", None)]


def test_composite_semijoin_matches_projected_measure_form():
    # Projecting the measure sidesteps the semijoin entirely — the semijoin
    # route must keep exactly the same groups.
    with_semijoin = _rows(
        REV + "select rev.cid, rev.fname having rev.tot > 90 order by rev.cid asc;"
    )
    projected = _rows(
        REV
        + "select rev.cid, rev.fname, rev.tot having rev.tot > 90 order by rev.cid asc;"
    )
    assert with_semijoin == [(cid, fname) for cid, fname, _ in projected]


def test_single_key_semijoin_keeps_null_key_group():
    # Single-key grain where the key itself is nullable: the NULL-name group
    # (200) passes and must survive.
    rows = _rows(
        "with rev as select first_name as fname, sum(amount) as tot;\n"
        "select rev.fname having rev.tot > 90 order by rev.fname asc;"
    )
    assert rows == [("Alice",), (None,)]


def test_single_key_semijoin_non_nullable_key_unchanged():
    rows = _rows(REV + "select rev.cid having rev.tot > 90 order by rev.cid asc;")
    assert rows == [("C1",), ("C2",)]


def test_semijoin_renders_null_safe_existence():
    sql = _sql(REV + "select rev.cid, rev.fname having rev.tot > 90;")
    assert "exists (select" in sql
    # the NULL-unsafe row-IN form must be gone, including its not-null guards
    assert ") in (select" not in sql
    assert "invalid_reference_bug" not in sql


# --- q59 shape: nullable derived measure as a grain component -----------------

Q59_MODEL = """
key sale_id int;
property sale_id.store string;
property sale_id.week int;
property sale_id.yr int;
property sale_id.day string;
property sale_id.amount int;

datasource sales (id: sale_id, st: store, wk: week, y: yr, d: day, amt: amount)
grain (sale_id)
query '''select * from (values
    (1,'A',1,2001,'Fri',10),
    (2,'A',1,2001,'Mon',4),
    (3,'A',2,2001,'Mon',5),
    (4,'B',1,2002,'Fri',7)
) as t(id,st,wk,y,d,amt)''';
"""


def _q59_rows(body: str):
    e = Dialects.DUCK_DB.default_executor(environment=Environment())
    e.parse_text(Q59_MODEL)
    return [tuple(r) for r in e.execute_query(body).fetchall()]


def test_nullable_measure_in_grain_tuple_survives_semijoin():
    # (A, week 2) has no Friday row, so its filtered sum is NULL; it is a
    # row-level rowset output and therefore a grain component of the outer
    # select. HAVING on the non-projected year must not delete it.
    rows = _q59_rows(
        "with wk as\n"
        "select store as sid, week as ws, yr as y, sum(amount ? day = 'Fri') as fri;\n"
        "select wk.sid, wk.ws, wk.fri having wk.y = 2001 "
        "order by wk.sid asc, wk.ws asc;"
    )
    assert rows == [("A", 1, 10), ("A", 2, None)]


def test_nullable_measure_matches_having_free_baseline():
    # HAVING is a filter: its output must be a subset of the no-HAVING rows.
    baseline = set(
        _q59_rows(
            "with wk as\n"
            "select store as sid, week as ws, yr as y, sum(amount ? day = 'Fri') as fri;\n"
            "select wk.sid, wk.ws, wk.fri;"
        )
    )
    filtered = _q59_rows(
        "with wk as\n"
        "select store as sid, week as ws, yr as y, sum(amount ? day = 'Fri') as fri;\n"
        "select wk.sid, wk.ws, wk.fri having wk.y = 2001;"
    )
    assert set(filtered).issubset(baseline)
    assert len(filtered) == 2


# --- authored composite membership: identity semantics ------------------------
#
# The rich set is {(C1, Alice), (C2, NULL)}; identity matching means C2's NULL
# name matches the set's NULL component, and IN / NOT IN partition the universe.

RICH = """with rich as
where amount > 90
select customer_id as cid, first_name as fname;
"""


def test_authored_tuple_in_matches_null_components():
    rows = _rows(
        RICH + "select customer_id, first_name "
        "where (customer_id, first_name) in (rich.cid, rich.fname) "
        "order by customer_id asc;"
    )
    assert rows == [("C1", "Alice"), ("C2", None)]


def test_authored_tuple_not_in_is_exact_complement():
    rows = _rows(
        RICH + "select customer_id, first_name "
        "where (customer_id, first_name) not in (rich.cid, rich.fname) "
        "order by customer_id asc;"
    )
    assert rows == [("C3", "Cara")]


def test_authored_tuple_in_and_not_in_partition_the_universe():
    base = _rows("select customer_id, first_name;")
    member = _rows(
        RICH + "select customer_id, first_name "
        "where (customer_id, first_name) in (rich.cid, rich.fname);"
    )
    non_member = _rows(
        RICH + "select customer_id, first_name "
        "where (customer_id, first_name) not in (rich.cid, rich.fname);"
    )
    assert sorted(member + non_member, key=repr) == sorted(base, key=repr)


def test_arity_mismatch_errors_cleanly():
    from trilogy.core.exceptions import InvalidSyntaxException

    with pytest.raises((InvalidSyntaxException, SyntaxError)):
        _sql(
            RICH + "select customer_id "
            "where (customer_id, first_name) in (rich.cid,);"
        )
