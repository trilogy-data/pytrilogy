"""The reduced shapes of the expressions family's 500,000-650,000 sweep and the
leads it closed (0.3.520).

- A per-tree value beside `max(height) by city, tree_form + 1` under a WHERE
  (expressions 645783): the group never marked a `max` over nullable heights
  nullable, so FINAL's re-join of that group paired the value with `=` and
  dropped a4, whose group's heights are all NULL
  (`Nullability.max_of_null_group_is_null`).
- Two `by *` totals in the select beside two in the WHERE, on the summary
  model (expressions 564837): the FINAL projection took a total's grain key
  as an input that its merge parent hid, and raised building its source map.

Rows checked by hand against `test_oracle_sweep_laws.ROWS` (heights 3, NULL,
5, NULL, 7, 2; tags x, y, x, x, y, x; forms broadleaf x4 with b1, NULL for
Ulmus, linden for b2)."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

CASES = [
    # expressions 645783, reduced
    (
        "select tree_id, max(height) by city, tree_form + 1 as v0,"
        " count(tag) by tree_form * count(tree_id) as v2 where city = 'A';",
        [("a1", 6, 3), ("a2", 6, 3), ("a3", 6, 3), ("a4", None, 1)],
    ),
    # expressions 645783
    (
        "select tree_id, max(height) by city, tree_form + count(tree_id) by * as v0,"
        " sum(height * 2) as v1, count_distinct(tag) by tree_form * count(tree_id)"
        " as v2 where city = 'A';",
        [("a1", 9, 6, 2), ("a2", 9, None, 2), ("a3", 9, 10, 2), ("a4", None, None, 1)],
    ),
    # expressions 564837, reduced, with the WHERE kept and rejected
    (
        "select count(tree_id) by * as v0, max(height) by * / count(tree_id) as v1"
        " where count_distinct(tag) by * < sum(height) by *;",
        [(6, 7 / 6)],
    ),
    (
        "select count(tree_id) by * as v0, max(height) by * / count(tree_id) as v1"
        " where count_distinct(tag) by * > sum(height) by *;",
        [],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query, expected", CASES)
def test_reduced_shape(query: str, expected: list[tuple], model: str):
    ex = executor(model)
    sql = ex.generate_sql(query)[-1]
    rows = ex.execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), sql
