"""`formal/Trilogy/SelectGrain.lean` (`constant_key_pairs_every_row`,
`keyless_pairing_fans_out`): a `by *` total beside a gate keyed on a column the
trees reach through a lookup (`tree_form`), and a FINAL whose plan pairs a
value with one of two scans of the trees. Both raised on main; rows checked by
hand against `test_oracle_sweep_laws.ROWS`."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

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
