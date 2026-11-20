"""An aggregate a WHERE reads both inside a membership's filtered set and as an
atom of its own (`species in (species ? count(tree_id) by species > 1) and
count(tree_id) by species > 0`) raised DisconnectedConceptsException: the
aggregate's only consumer was the set, a side channel, so it was never joined
to the outputs and the atom had no host.

Rows checked by hand against `test_oracle_sweep_laws.ROWS` (Acer has three
trees, every other species one; city A has four, B two)."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

CASES = [
    (
        (
            "select tree_id where species in (species ? count(tree_id) by species > 1)"
            " and count(tree_id) by species > 0;"
        ),
        [("a1",), ("a2",), ("b1",)],
    ),
    (
        (
            "select tree_id where species in (species ? count(tree_id) by species > 1)"
            " and count(tree_id) by species < 3;"
        ),
        [],
    ),
    (
        (
            "select tree_id where species in (species ? count(tree_id) by city > 2)"
            " and count(tree_id) by city > 0;"
        ),
        [("a1",), ("a2",), ("a3",), ("a4",), ("b1",)],
    ),
    (
        (
            "select tree_id, height where species in (species ? count(tree_id) by species"
            " > 1) and city = 'A' and max(height) by city < 6 and count(tree_id) by"
            " species > 0;"
        ),
        [("a1", 3), ("a2", None)],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query, expected", CASES)
def test_membership_beside_where_aggregate(
    query: str, expected: list[tuple], model: str
):
    ex = executor(model)
    sql = ex.generate_sql(query)[-1]
    assert normalize(ex.execute_raw_sql(sql).fetchall()) == normalize(expected), sql
