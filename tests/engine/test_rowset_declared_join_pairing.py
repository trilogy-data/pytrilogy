"""A rowset joins its reader only on the join the statement declares.

`r.status` beside `status` pairs on `r.customer_id = customer_id` alone, so
customer 1's two orders cross with the rowset's two rows, and cat (no order)
keeps her row with both statuses 'in-transit': `status` absorbs NULL, so it is
pinned to the select's row, and so it was in the rowset's body. No row has a
NULL status. Checked on the oracle twin, where `status` is derived on one model
and a column on the other.
"""

import pytest

from tests.helpers.models import CUSTOMERS_DERIVED, CUSTOMERS_MATERIALIZED
from tests.helpers.rows import executor_for, twin_rows
from trilogy.executor import Executor

ROWSET = "with r as select customer_id, status;\n"
PAIRED = [
    (1, "delivered", "delivered"),
    (1, "delivered", "in-transit"),
    (1, "in-transit", "delivered"),
    (1, "in-transit", "in-transit"),
    (2, "delivered", "delivered"),
    (3, "in-transit", "in-transit"),
]


@pytest.fixture(scope="module")
def twins() -> tuple[Executor, Executor]:
    return executor_for(CUSTOMERS_DERIVED), executor_for(CUSTOMERS_MATERIALIZED)


SUBSET = "subset join r.customer_id = customer_id"
UNION = "union join r.customer_id = customer_id"


@pytest.mark.parametrize(
    "query, expected",
    [
        (f"select customer_id, status, r.status {SUBSET};", PAIRED),
        (f"select customer_id, status, r.status {SUBSET} where status is null;", []),
        (f"select customer_id, status, r.status {SUBSET} where r.status is null;", []),
        (f"select customer_id, r.status {SUBSET} where status is null;", []),
        (f"select customer_id, status, r.status {UNION};", PAIRED),
        (
            f"select customer_id, status, r.status {UNION} where status = 'delivered';",
            [(1, "delivered", "delivered"), (1, "delivered", "in-transit")]
            + [(2, "delivered", "delivered")],
        ),
        (f"select customer_id, status, r.status {UNION} where status is null;", []),
    ],
)
def test_rowset_pairs_on_the_declared_join_only(
    twins: tuple[Executor, Executor], query: str, expected: list[tuple]
) -> None:
    assert twin_rows(*twins, ROWSET + query) == expected


ORDER_ROWSET = "with rs as select order_id as oid, amount as amt;\n"


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "select customer_id, name, sum(rs.amt) as spend subset join rs.oid = order_id;",
            [(1, "ann", 30), (2, "bob", 30), (3, "cat", None)],
        ),
        (
            "select customer_id, count(rs.oid) as n subset join rs.oid = order_id;",
            [(1, 2), (2, 1), (3, 0)],
        ),
    ],
)
def test_aggregate_over_a_rowset_joined_on_its_row_key(
    twins: tuple[Executor, Executor], query: str, expected: list[tuple]
) -> None:
    assert twin_rows(*twins, ORDER_ROWSET + query) == expected


def test_declared_join_beside_a_property_of_the_superset_key(
    twins: tuple[Executor, Executor],
) -> None:
    rowset = "with rs2 as select customer_id as cid, order_id as oid; "
    query = "select name, count(rs2.oid) as n subset join rs2.cid = customer_id;"
    assert twin_rows(*twins, rowset + query) == [
        ("ann", 2),
        ("bob", 1),
        ("cat", 0),
    ]


FILTERED = "with rs2 as where amount > 15 select customer_id as cid, order_id as oid;\n"
CID_JOIN = "subset join rs2.cid = customer_id"


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            f"select customer_id, name, rs2.oid {CID_JOIN};",
            [(1, "ann", 101), (2, "bob", 102), (3, "cat", None)],
        ),
        (
            f"select customer_id, count(rs2.oid) as n {CID_JOIN};",
            [(1, 1), (2, 1), (3, 0)],
        ),
        (
            f"select name, count(rs2.oid) as n {CID_JOIN};",
            [("ann", 1), ("bob", 1), ("cat", 0)],
        ),
        (
            f"select customer_id, rs2.oid {CID_JOIN} where rs2.oid is not null;",
            [(1, 101), (2, 102)],
        ),
    ],
)
def test_filtered_rowset_keeps_the_superset_rows_it_misses(
    twins: tuple[Executor, Executor], query: str, expected: list[tuple]
) -> None:
    assert twin_rows(*twins, FILTERED + query) == expected
