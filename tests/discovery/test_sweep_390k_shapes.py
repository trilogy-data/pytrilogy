"""The reduced shapes of the 330,000-390,000 sweep (0.3.510).

- `formal/Trilogy/SelectGrain.lean` (`blend_beside_finer_rows_fans_out`,
  `unique_side_one_partner`): two HAVING groups, by species and by city,
  tree_form, beside tree rows share only `tree_form`, which identifies
  neither. Joined to each other on it, every tree met each species of its
  form and came back once per species; the row stream now pairs each group
  on its own keys.
- `formal/Trilogy/Rollup.lean` (`property_resum_exact`,
  `mixed_grain_pairs_on_property`): an aggregate by a property of the row key
  (`by tree_form` beside `species`) is re-summed off the summary and joined
  back on the property; a grain mixing a row key in (`by city, tree_form`)
  still rides the rows.

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
    # the mixed grain the property re-sum leaves on the rows
    (
        (
            "select city, species where count_distinct(tag) by species -"
            " max(height) by city, tree_form < 1;"
        ),
        [("A", "Acer"), ("A", "Quercus"), ("B", "Acer"), ("B", "Tilia")],
    ),
]

PROPERTY_RESUMS = [
    (
        "select species, min(height) by tree_form as v0;",
        [("Acer", 3), ("Quercus", 3), ("Tilia", 2), ("Ulmus", None)],
    ),
    (
        "select species, tree_form, sum(height) by tree_form as v0;",
        [
            ("Acer", "broadleaf", 15),
            ("Quercus", "broadleaf", 15),
            ("Tilia", "linden", 2),
            ("Ulmus", None, None),
        ],
    ),
]


@pytest.mark.parametrize("query,expected", CASES)
@pytest.mark.parametrize("model", ["base", "summary"])
def test_sweep_390k_shape(model: str, query: str, expected: list):
    sql = executor(model).generate_sql(query)[-1]
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), sql


@pytest.mark.parametrize("query,expected", PROPERTY_RESUMS)
def test_property_grain_reads_summary(query: str, expected: list):
    sql = executor("summary").generate_sql(query)[-1]
    rows = executor("summary").execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), sql
    assert 'as "tree_summary"' in sql and 'as "trees"' not in sql, sql
