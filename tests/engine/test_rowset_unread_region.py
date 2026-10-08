"""A rowset body's region the reader reads nothing of is not a row of the
reader.

The body pads every region it demands. A reader whose population spells no
entity present on one of them (`select s.o, s.st` over a body naming the
customer; the product family under `select s.u, count(s.i)`) has no region
for it in its own keyspace, so nothing marks it undemanded, and the padded
row came up as a row source: `(NULL, 0)` for a product no user bought. The
keyspace names those spans (`Keyspace.unread_spans`) and the boundary plans
its body without extending them, as it does for a span the reader owns.
A value the body PINS is a real row value, though: `status` absorbs NULL, so
the body evaluates it on cat's row and `select s.o, s.st` reads her
`(NULL, 'in-transit')`, a row the direct `select order_id, status` lacks.

A region the reader COLLAPSES is different: `select s.c, s.k` over an
aggregate body reads only customer-level handles, so the body's two regions
are one region of the reader, and its rows are all of the body's rows (the
orderless customer keeps her 0). Every rowset spelling is checked against
its direct one, on the oracle twin (both models) and on a two-family model.
"""

import pytest

from tests.helpers.models import (
    CUSTOMER_ACTIVITY,
    CUSTOMERS_DERIVED,
    CUSTOMERS_MATERIALIZED,
)
from tests.helpers.rows import customer_twins, executor_for, sorted_rows
from trilogy.executor import Executor

KEYED = "rowset s <- select customer_id as c, name as n, order_id as o, status as st, amount as a;\n"
KEYLESS = "rowset s <- select name as n, order_id as o, status as st, amount as a;\n"
AGGREGATE = "rowset s <- select customer_id as c, name as n, count(order_id) as k, sum(amount) as t;\n"
FILTERED = "rowset s <- select customer_id as c, name as n, status as st where status is null or status = 'delivered';\n"
NESTED = (
    "rowset t <- select customer_id as c, name as n, order_id as o, status as st;\n"
    "rowset s <- select t.c as c2, t.n as n2, t.o as o2, t.st as st2;\n"
)

TWIN_PAIRS = [
    # the customer region is unread: not a row
    (
        KEYED + "select s.o, s.a where s.st is null;",
        "select order_id, amount where status is null;",
    ),
    # the customer region is read, or collapsed into the reader's one region
    (KEYED + "select s.c, s.st;", "select customer_id, status;"),
    (KEYED + "select s.n, count(s.o) as k;", "select name, count(order_id) as k;"),
    (KEYLESS + "select s.n, s.st;", "select name, status;"),
    (KEYLESS + "select s.n where s.st is null;", "select name where status is null;"),
    (AGGREGATE + "select s.c, s.k;", "select customer_id, count(order_id) as k;"),
    (
        AGGREGATE + "select s.c where s.k = 0;",
        "select customer_id where count(order_id) by customer_id = 0;",
    ),
    (
        FILTERED + "select s.c, s.n;",
        "select customer_id, name where status is null or status = 'delivered';",
    ),
    (NESTED + "select s.c2, s.st2;", "select customer_id, status;"),
    (NESTED + "select s.n2, count(s.o2) as k;", "select name, count(order_id) as k;"),
]

# the body pinned cat's status: her row carries a value
PINNED_ROWS = [
    (100, "delivered"),
    (101, "in-transit"),
    (102, "delivered"),
    (None, "in-transit"),
]
PINNED = [KEYED + "select s.o, s.st;", NESTED + "select s.o2, s.st2;"]

FAMILY_MODEL = """
key user_id int;
property user_id.state string;
key product_id int;
property product_id.cost float;
key item_id int;
property item_id.qty int;

datasource users (u: user_id, st: state)
grain (user_id)
query '''select 1 as u, 'CA' as st union all select 2, 'NY' union all select 3, 'TX' ''';

datasource products (p: product_id, c: cost)
grain (product_id)
query '''select 10 as p, 1.5 as c union all select 20, 2.5 union all select 30, 3.5''';

datasource items (i: item_id, u: ~user_id, p: ~product_id, q: qty)
grain (item_id)
query '''select 100 as i, 1 as u, 10 as p, 2 as q
union all select 101, 1, 20, 3
union all select 102, 2, 10, 4''';
"""

FAMILY = "rowset s <- select item_id as i, user_id as u, product_id as p, state as st, cost as c, qty as q;\n"

FAMILY_PAIRS = [
    (FAMILY + "select s.u, count(s.i) as k;", "select user_id, count(item_id) as k;"),
    (
        FAMILY + "select s.p, count(s.i) as k;",
        "select product_id, count(item_id) as k;",
    ),
    (FAMILY + "select s.st, count(s.i) as k;", "select state, count(item_id) as k;"),
    (
        FAMILY + "select s.u where count(s.i) by s.u = 0;",
        "select user_id where count(item_id) by user_id = 0;",
    ),
    (
        FAMILY + "select s.u, s.st where s.q is null;",
        "select user_id, state where qty is null;",
    ),
    # the region under an aggregate argument keyed on the product is read but
    # undemanded: `qty * cost` is a function of the item
    (
        FAMILY + "select s.u, s.st, sum(s.q * s.c) as rev;",
        "select user_id, state, sum(qty * cost) as rev;",
    ),
    # both families read: no `{user, product}` cell
    (
        FAMILY + "select s.u, s.p, count(s.i) as k;",
        "select user_id, product_id, count(item_id) as k;",
    ),
]

FAMILY_ROWS = {
    "select s.u, count(s.i) as k;": [(1, 2), (2, 1), (3, 0)],
    "select s.u, s.p, count(s.i) as k;": [
        (1, 10, 1),
        (1, 20, 1),
        (2, 10, 1),
        (3, None, 0),
        (None, 30, 0),
    ],
}


@pytest.fixture(scope="module")
def customers() -> tuple[Executor, Executor]:
    return customer_twins(CUSTOMER_ACTIVITY)


@pytest.fixture(scope="module")
def derived(customers: tuple[Executor, Executor]) -> Executor:
    return customers[0]


@pytest.fixture(scope="module")
def materialized(customers: tuple[Executor, Executor]) -> Executor:
    return customers[1]


@pytest.fixture(scope="module")
def family() -> Executor:
    return executor_for(FAMILY_MODEL)


@pytest.mark.parametrize("rowset_query,direct_query", TWIN_PAIRS)
def test_rowset_matches_direct_on_both_twins(
    derived: Executor, materialized: Executor, rowset_query: str, direct_query: str
):
    for executor in (derived, materialized):
        assert sorted_rows(executor, rowset_query) == sorted_rows(
            executor, direct_query
        )


@pytest.mark.parametrize("rowset_query", PINNED)
@pytest.mark.parametrize("model", [CUSTOMERS_DERIVED, CUSTOMERS_MATERIALIZED])
def test_rowset_reads_a_value_its_body_pinned(model: str, rowset_query: str):
    executor = executor_for(model + CUSTOMER_ACTIVITY)
    assert sorted_rows(executor, rowset_query) == PINNED_ROWS


def test_redefined_rowset_reads_the_same_rows():
    executor = executor_for(CUSTOMERS_DERIVED + CUSTOMER_ACTIVITY)
    sorted_rows(executor, KEYED + "select s.o, s.st;")
    assert sorted_rows(executor, PINNED[-1]) == PINNED_ROWS


# `local._a_st` sorts before `local._t_st` as the status family's canonical
# spelling; the witness of `t` keys its handles by what t's body declared
@pytest.mark.parametrize("model", [CUSTOMERS_DERIVED, CUSTOMERS_MATERIALIZED])
def test_an_earlier_rowsets_alias_does_not_respell_a_later_body(model: str):
    executor = executor_for(model + CUSTOMER_ACTIVITY)
    executor.execute_text("rowset a <- select customer_id as c, status as st;")
    assert (
        sorted_rows(
            executor,
            "rowset t <- select customer_id as c, order_id as o, status as st;"
            " select t.o, t.st;",
        )
        == PINNED_ROWS
    )


@pytest.mark.parametrize("rowset_query,direct_query", FAMILY_PAIRS)
def test_rowset_matches_direct_on_two_families(
    family: Executor, rowset_query: str, direct_query: str
):
    rows = sorted_rows(family, rowset_query)
    assert rows == sorted_rows(family, direct_query)
    expected = FAMILY_ROWS.get(rowset_query.splitlines()[-1])
    if expected is not None:
        assert rows == expected
