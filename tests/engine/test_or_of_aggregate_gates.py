"""An OR of aggregate gates keyed by columns the statement never projects is
applied at FINAL; each gate joins the row stream on its own key."""

import pytest

from tests.helpers.rows import customer_twins, twin_rows
from trilogy.executor import Executor


@pytest.fixture(scope="module")
def customers() -> tuple[Executor, Executor]:
    return customer_twins()


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
def test_or_of_gates_on_unprojected_keys(
    customers: tuple[Executor, Executor], query: str, expected: list[tuple]
):
    assert twin_rows(*customers, query) == expected
