"""`formal/Trilogy/SelectGrain.lean` (`constant_key_pairs_every_row`,
`keyless_pairing_fans_out`, `alias_is_not_the_pinned_read`): a `by *` total
beside a gate keyed on a column the trees reach through a lookup
(`tree_form`), and a FINAL whose plan pairs a value with one of two scans of
the trees. Both raised on main; rows checked by hand against
`test_oracle_sweep_laws.ROWS`. An alias of a row value is no spelling of the
select's pinned read of it: customer 3 has no order, and its pinned `status`
is `'in-transit'`, never the NULL a stored column pads."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize
from tests.helpers.models import CUSTOMER_ACTIVITY
from tests.helpers.rows import customer_twins, twin_rows

CASES = [
    (
        "select city where sum(height) by tree_form < sum(height) by *;",
        [("A",), ("B",)],
    ),
    (
        "select city where sum(height) by city, tree_form > max(height) by *;",
        [("A",)],
    ),
    (
        "select tree_id where sum(height) by tree_form - sum(height) by * < -10;",
        [("b2",)],
    ),
    (
        "select tree_id, tag as v0 where max(height) by tree_form < sum(height) by *;",
        [("a1", "x"), ("a2", "y"), ("a3", "x"), ("b1", "y"), ("b2", "x")],
    ),
    (
        (
            "select tree_id, avg(height) by species as v0, min(height ? not (tag ="
            " 'x')) as v1 where max(height) by city, tree_form != 6 having"
            " count_distinct(tag) by city >= 2;"
        ),
        [
            ("a1", 5, None),
            ("a2", 5, None),
            ("a3", 5, None),
            ("b1", 5, 7),
            ("b2", 2, None),
        ],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query,expected", CASES)
def test_total_beside_lookup_gate(query: str, expected: list, model: str):
    sql = executor(model).generate_sql(query)[-1]
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), f"{query}\n{sql}"


ALIAS_CASES = [
    (
        (
            "select customer_id, status where customer_id >= avg(customer_id) by *"
            " and status is null"
        ),
        [],
    ),
    (
        (
            "select customer_id, status where customer_id >= avg(customer_id) by *"
            " and status = 'in-transit'"
        ),
        [(3, "in-transit")],
    ),
]


@pytest.mark.parametrize("query,expected", ALIAS_CASES)
def test_alias_of_row_value_is_not_the_pinned_read(query: str, expected: list):
    derived, materialized = customer_twins(CUSTOMER_ACTIVITY + "auto s2 <- status;")
    assert twin_rows(derived, materialized, query) == expected
    derived.close()
    materialized.close()
