"""An OR of aggregate gates keyed by columns the statement never projects is
applied at FINAL; each gate joins the row stream on its own key."""

import pytest

from tests.helpers.models import CUSTOMERS_DERIVED, CUSTOMERS_MATERIALIZED
from tests.helpers.rows import executor_for, twin_rows


@pytest.mark.parametrize(
    "query,expected",
    [
        (
            (
                "select order_id where sum(amount) by status > 35"
                " or count(order_id) by customer_id > 1"
            ),
            [(100,), (101,), (102,)],
        ),
        (
            (
                "select order_id where sum(amount) by status > 35"
                " or count(order_id) by customer_id > 5"
            ),
            [(100,), (102,)],
        ),
        (
            (
                "select order_id where sum(amount) by status > 45"
                " or count(order_id) by customer_id > 1"
            ),
            [(100,), (101,)],
        ),
        (
            (
                "select order_id, amount where sum(amount) by status > 45"
                " or count(order_id) by customer_id > 5"
            ),
            [],
        ),
    ],
)
def test_or_of_gates_on_unprojected_keys(query: str, expected: list[tuple]):
    derived = executor_for(CUSTOMERS_DERIVED)
    materialized = executor_for(CUSTOMERS_MATERIALIZED)
    assert twin_rows(derived, materialized, query) == expected
