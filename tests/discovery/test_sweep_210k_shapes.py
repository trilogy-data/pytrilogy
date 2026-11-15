"""The reduced shapes of the 150,000-210,000 sweep (0.3.505).

- `formal/Trilogy/SelectGrain.lean` (`pruned_coalesce_member_pads_gate`): a key
  several left sides pair renders their coalesce; pruning the side nothing pads
  left the gate keyed on a padded side's NULL.
- `formal/Trilogy/SelectGrain.lean` (`mirrored_cross_join_is_one`): `a ⋈ b` and
  `b ⋈ a` on nothing are one cross product in a merged CTE.
- `formal/Trilogy/Rollup.lean` (`merged_groups_read_each_other`): aggregate
  groups that read each other through a re-aggregated partial split by depth.
- `formal/Trilogy/SelectGrain.lean` (`final_reads_finer_column_off_gate`): FINAL
  re-applies no atom over a column finer than its rows.
- `formal/Trilogy/Rollup.lean` (`coarse_resum_joined_on_its_grain`): an
  aggregate coarser than the select's rows re-sums the summary at its own
  grain and joins back on it.

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
    # spellings 11684, reduced: one row of NULLs on main
    (
        "select species, tree_form, min(height) as v0 where (max(height) by species"
        " > 8 or city = 'B') and max(height) by city > 20;",
        [],
    ),
]

TREE_SCAN = 'as "trees"'

# (query, rows): an aggregate coarser than the summary's grain, beside its rows
COARSE = [
    (
        "select city, species, sum(height) by city as v;",
        [
            ("A", "Acer", 8),
            ("A", "Quercus", 8),
            ("A", "Ulmus", 8),
            ("B", "Acer", 9),
            ("B", "Tilia", 9),
        ],
    ),
    (
        "select city, species, max(height) by species as v, count(tree_id) by city"
        " as n;",
        [
            ("A", "Acer", 7, 4),
            ("A", "Quercus", 5, 4),
            ("A", "Ulmus", None, 4),
            ("B", "Acer", 7, 2),
            ("B", "Tilia", 2, 2),
        ],
    ),
]


@pytest.mark.parametrize("query,expected", CASES)
@pytest.mark.parametrize("model", ["base", "summary"])
def test_sweep_210k_shape(model: str, query: str, expected: list):
    sql = executor(model).generate_sql(query)[-1]
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), sql


@pytest.mark.parametrize("query,expected", COARSE)
def test_coarser_aggregate_reads_summary(query: str, expected: list):
    for model in ("base", "summary"):
        sql = executor(model).generate_sql(query)[-1]
        rows = executor(model).execute_raw_sql(sql).fetchall()
        assert normalize(rows) == normalize(expected), f"{model}\n{sql}"
    assert "tree_summary" in sql, sql
    assert TREE_SCAN not in sql, sql
