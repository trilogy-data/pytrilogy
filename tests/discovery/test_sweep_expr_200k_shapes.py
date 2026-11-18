"""The reduced shapes of the expressions family's 150,000-200,000 sweep (0.3.515).

- A value over groups by `species` (`count_distinct(tag) + 1`) beside a value
  `by tree_form`, off the summary: it was never widened to the sibling's key,
  and no row stream held `tree_form` to bridge. It looks the root property up
  off a scan beside `species`.
- Beside a constant, the trees were wrapped and deduped to `city`, dropping
  the `species` they pair with `tree_form` on: the two were cross-joined.
- `sum(height) by tree_form is null` was read off the gated value FINAL LEFT
  joins, so its padding passed the atom and returned rejected species.
- A per-tree value beside a value `by tree_form`, off the summary: the per-tree
  bridge holds `tree_form` below but never carried it, and joined on nothing.

Rows checked by hand against `test_oracle_sweep_laws.ROWS` (heights 3, NULL,
5, NULL, 7, 2; tags x, y, x, x, y, x; forms broadleaf x4 with b1, NULL for
Ulmus, linden for b2)."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

CASES = [
    # expressions 17715, reduced
    (
        "select species, count_distinct(tag) + 1 as v0, min(height) by tree_form as v2;",
        [("Acer", 3, 3), ("Quercus", 2, 3), ("Tilia", 2, 2), ("Ulmus", 2, None)],
    ),
    # expressions 193139, reduced
    (
        (
            "select city, tree_form, 12 as v0 where tree_form = 'broadleaf' or"
            " avg(height) by species <= 2;"
        ),
        [("A", "broadleaf", 12), ("B", "broadleaf", 12), ("B", "linden", 12)],
    ),
    # expressions 176152, reduced
    (
        (
            "select species, tree_form, count(tree_id) by tree_form + 1 as v0 where"
            " count(tree_id) by species = 1 and sum(height) by tree_form is null;"
        ),
        [("Ulmus", None, 2)],
    ),
    # expressions 187859 (and 139791, 160693, 160820, 177047, 187270), reduced
    (
        (
            "select tree_id, sum(height) + count(tree_id) by tree_form as v0,"
            " sum(height) + max(height) by city, species as v1;"
        ),
        [
            ("a1", 7, 6),
            ("a2", None, None),
            ("a3", 9, 10),
            ("a4", None, None),
            ("b1", 11, 14),
            ("b2", 3, 4),
        ],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query, expected", CASES)
def test_reduced_shape(query: str, expected: list[tuple], model: str):
    ex = executor(model)
    sql = ex.generate_sql(query)[-1]
    rows = ex.execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), sql
