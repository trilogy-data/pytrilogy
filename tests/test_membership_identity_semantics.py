"""Membership (`in` / `not in`) is identity matching and total, scalar and
tuple alike.

- NULL matches NULL: a NULL probe is in a set containing a NULL, and
  `x in (1, null)` is true for NULL x.
- Total: membership is TRUE or FALSE, never NULL — safe as a projected flag.
- `not in` is the exact complement of `in`: every row lands in exactly one
  side (no SQL NOT-IN footgun; NULL-keyed rows are never silently dropped).

Strict SQL parity is opt-in by filtering NULLs explicitly (see the tpc_ds
query14 cross_tuples set for the canonical example).
"""

from trilogy import Dialects
from trilogy.core.models.environment import Environment

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

# rich-names set (customers with a sale > 90): {Alice, NULL}
RICH = """with rich as
where amount > 90
select first_name as fname;
"""


def _rows(body: str):
    e = Dialects.DUCK_DB.default_executor(environment=Environment())
    e.parse_text(MODEL)
    return [tuple(r) for r in e.execute_query(body).fetchall()]


# --- scalar subselect membership ----------------------------------------------


def test_subselect_in_matches_null():
    rows = _rows(
        RICH + "select customer_id, first_name "
        "where first_name in (rich.fname) order by customer_id asc;"
    )
    assert rows == [("C1", "Alice"), ("C2", None)]


def test_subselect_not_in_is_exact_complement():
    rows = _rows(
        RICH + "select customer_id, first_name "
        "where first_name not in (rich.fname) order by customer_id asc;"
    )
    assert rows == [("C3", "Cara")]


def test_subselect_in_and_not_in_partition_the_universe():
    base = _rows("select customer_id;")
    member = _rows(RICH + "select customer_id where first_name in (rich.fname);")
    non_member = _rows(
        RICH + "select customer_id where first_name not in (rich.fname);"
    )
    assert sorted(member + non_member) == sorted(base)


def test_not_in_null_free_set_keeps_null_probe():
    # The SQL NOT-IN footgun in reverse: a NULL-keyed row IS "not in" a set
    # that doesn't contain NULL.
    rows = _rows(
        "with named as where first_name = 'Alice' select first_name as fname;\n"
        "select customer_id where first_name not in (named.fname) "
        "order by customer_id asc;"
    )
    assert rows == [("C2",), ("C3",)]


# --- literal value lists --------------------------------------------------------


def test_value_list_with_null_matches_null_probe():
    rows = _rows(
        "select customer_id where first_name in ('Alice', null) "
        "order by customer_id asc;"
    )
    assert rows == [("C1",), ("C2",)]


def test_value_list_not_in_with_null_is_complement():
    rows = _rows(
        "select customer_id where first_name not in ('Alice', null) "
        "order by customer_id asc;"
    )
    assert rows == [("C3",)]


def test_value_list_without_null_keeps_null_probe_in_not_in():
    rows = _rows(
        "select customer_id where first_name not in ('Alice', 'Cara') "
        "order by customer_id asc;"
    )
    assert rows == [("C2",)]


# --- totality: membership as a projected boolean -------------------------------


def test_projected_membership_flag_is_never_null():
    rows = _rows(
        "select customer_id, (first_name in ('Alice', null)) as flag "
        "order by customer_id asc;"
    )
    assert rows == [("C1", True), ("C2", True), ("C3", False)]


def test_projected_not_in_flag_is_never_null():
    rows = _rows(
        "select customer_id, (first_name not in ('Alice',)) as flag "
        "order by customer_id asc;"
    )
    assert rows == [("C1", False), ("C2", True), ("C3", True)]
