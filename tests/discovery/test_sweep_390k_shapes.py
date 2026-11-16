"""The reduced shapes of the 330,000-390,000 sweep (0.3.510).

- `formal/Trilogy/SelectGrain.lean` (`blend_beside_finer_rows_fans_out`,
  `unique_side_one_partner`): two HAVING groups, by species and by city,
  tree_form, beside tree rows share only `tree_form`, which identifies
  neither. Joined to each other on it, every tree met each species of its
  form and came back once per species; the row stream now pairs each group
  on its own keys.

Rows checked by hand against `test_oracle_sweep_laws.ROWS` (heights 3, NULL,
5, NULL, 7, 2)."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

CASES = [
    # spellings 356401, reduced: the species group joined on tree_form alone
    (
        (
            "select tree_id, sum(height ? min(height) by city, tree_form -"
            " min(height) by species < 3) as v1 where not (sum(height) by city,"
            " tree_form = 0) having sum(height) by species != 0 or max(height) by"
            " city, tree_form > 2;"
        ),
        [("a1", 3), ("a2", None), ("a3", 5), ("b1", None), ("b2", 2)],
    ),
    # spellings 356401
    (
        (
            "select tree_id, sum(height ? coalesce(tree_form, 'none') = 'none') as"
            " v0, sum(height ? min(height) by city, tree_form - min(height) by"
            " species < 3) as v1, tall as v2 where not (sum(height) by city,"
            " tree_form - min(height) by city = 0) having count(tree_id) by species"
            " != 0 or (max(height) by city, tree_form > 2 and sum(height) by"
            " species <= 5);"
        ),
        [
            ("a1", None, 3, False),
            ("a2", None, None, None),
            ("a3", None, 5, True),
            ("a4", None, None, None),
            ("b1", None, None, True),
        ],
    ),
]


@pytest.mark.parametrize("query,expected", CASES)
@pytest.mark.parametrize("model", ["base", "summary"])
def test_sweep_390k_shape(model: str, query: str, expected: list):
    sql = executor(model).generate_sql(query)[-1]
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), sql
