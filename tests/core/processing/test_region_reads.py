"""What a `~` region's rows decide (`v4_helper/region_reads.py`), asked of the
customers-with-no-order region of the derived-key-domain model."""

import pytest

from tests.helpers.models import CUSTOMERS_DERIVED
from tests.helpers.planning import plan
from trilogy.core.processing.v4_helper import region_reads

QUERY = """select customer_id,
    sum(case when undelivered then 1 else 0 end) as n_undelivered,
    sum(amount) as total,
    count(order_id) by * as n_orders,
    count(customer_id) by * as n_customers;"""


@pytest.fixture(scope="module")
def planned():
    info, environment = plan(CUSTOMERS_DERIVED, QUERY)
    (region,) = [r for r in info.keyspace.live_regions if r.spans]
    return region, info.keyspace, environment


@pytest.mark.parametrize(
    "address, expected",
    [
        # the CASE answers 0 on a padded row, where no row at all is NULL
        ("local.n_undelivered", True),
        # a plain concept argument is NULL on the padding
        ("local.total", False),
        ("local.customer_id", False),
    ],
)
def test_argument_takes_a_value_on_padding(planned, address: str, expected: bool):
    region, keyspace, environment = planned
    assert (
        region_reads.argument_takes_a_value_on_padding(
            address, region, keyspace, environment
        )
        is expected
    )


@pytest.mark.parametrize(
    "address, expected",
    [
        ("local.n_orders", True),
        ("local.n_customers", True),
        ("local.total", False),
        ("local.customer_id", False),
    ],
)
def test_keyless(planned, address: str, expected: bool):
    _, keyspace, _ = planned
    assert region_reads.keyless(address, keyspace) is expected


@pytest.mark.parametrize(
    "address, expected",
    [
        # counts the customers, the ones with no order included
        ("local.n_customers", True),
        # keyless, but the region holds no order to count
        ("local.n_orders", False),
        ("local.total", False),
    ],
)
def test_fed_gate(planned, address: str, expected: bool):
    region, keyspace, environment = planned
    assert region_reads.fed_gate(address, region, keyspace, environment) is expected
