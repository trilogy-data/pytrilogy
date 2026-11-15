"""The reduced shapes of the 150,000-210,000 sweep (0.3.505).

- `formal/Trilogy/SelectGrain.lean` (`pruned_coalesce_member_pads_gate`): a key
  several left sides pair renders their coalesce; pruning the side nothing pads
  left the gate keyed on a padded side's NULL.
- `formal/Trilogy/SelectGrain.lean` (`mirrored_cross_join_is_one`): `a ⋈ b` and
  `b ⋈ a` on nothing are one cross product in a merged CTE.
- `formal/Trilogy/Rollup.lean` (`merged_groups_read_each_other`): aggregate
  groups that read each other through a re-aggregated partial split by depth.

Rows checked by hand against `test_oracle_sweep_laws.ROWS` (heights 3, NULL,
5, NULL, 7, 2)."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

CASES = [
    # spellings 185026, reduced: wrong rows on both models
    (
        "select tree_id, count(tree_id) by city, tree_form as v0, tag as v1 where"
        " not (min(height) by species != count_distinct(tag) by tree_form);",
        [("a4", 1, "x")],
    ),
    # spellings 177430, reduced: a CTE joined twice, invalid on both models
    (
        "select tree_id, city as v0, max(height ? max(height) by * < 1) as v2 where"
        " max(height) by * < 1 or species = 'Acer' having count(tree_id) by city >= 0;",
        [("a1", "A", None), ("a2", "A", None), ("b1", "B", None)],
    ),
    # spellings 172552, reduced: a group-graph cycle with the summary in scope
    (
        "select species, sum(height ? min(height) by species - min(height) by"
        " tree_form > 1) as v0, max(height ? sum(height) by tree_form = 3) as v1,"
        " sum(height) by species as v2;",
        [
            ("Acer", None, None, 10),
            ("Quercus", 5, None, 5),
            ("Tilia", None, None, 2),
            ("Ulmus", None, None, None),
        ],
    ),
]


@pytest.mark.parametrize("query,expected", CASES)
@pytest.mark.parametrize("model", ["base", "summary"])
def test_sweep_210k_shape(model: str, query: str, expected: list):
    sql = executor(model).generate_sql(query)[-1]
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), sql
