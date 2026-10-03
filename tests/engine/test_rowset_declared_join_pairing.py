"""A rowset joins its reader only on the join the statement declares.

`r.status` beside `status` pairs on `r.customer_id = customer_id` alone, so
customer 1's two orders cross with the rowset's two rows, and cat (no order)
keeps her row with both statuses NULL: `status` is a property of `order_id`,
absent for her, and her rowset row carries no status either. Checked on the
oracle twin, where `status` is derived on one model and a column on the other.
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
    (3, None, None),
]
CAT = [(3, None, None)]


@pytest.fixture(scope="module")
def twins() -> tuple[Executor, Executor]:
    return executor_for(CUSTOMERS_DERIVED), executor_for(CUSTOMERS_MATERIALIZED)


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "select customer_id, status, r.status subset join r.customer_id = customer_id;",
            PAIRED,
        ),
        (
            "select customer_id, status, r.status subset join r.customer_id = customer_id "
            "where status is null;",
            CAT,
        ),
        (
            "select customer_id, status, r.status subset join r.customer_id = customer_id "
            "where r.status is null;",
            CAT,
        ),
        (
            "select customer_id, r.status subset join r.customer_id = customer_id "
            "where status is null;",
            [(3, None)],
        ),
    ],
)
def test_rowset_pairs_on_the_declared_join_only(
    twins: tuple[Executor, Executor], query: str, expected: list[tuple]
) -> None:
    assert twin_rows(*twins, ROWSET + query) == expected
