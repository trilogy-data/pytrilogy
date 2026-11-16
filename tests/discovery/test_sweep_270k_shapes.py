"""The reduced shapes of the 210,000-270,000 sweep (0.3.507).

- `formal/Trilogy/Rollup.lean` (`filtered_passenger_multiplies`,
  `key_filter_is_semijoin`): a re-sum of the summary's partials beside a row
  scan the WHERE filters, when the summary's rows carry the same atoms, joins
  each partial once per kept tree.
- `formal/Trilogy/SelectGrain.lean` (`unfiltered_peel_keeps_rejected`): a value
  peeled onto the summary at a key the WHERE's atom is a function of carries
  the atom; joined back outer, the unfiltered peel kept every key.
- `formal/Trilogy/SelectGrain.lean` (`rejoined_cte_is_one_conjunction`): two
  merged plans each joining one CTE from their own side are one INNER
  conjunction, not a CTE referenced twice.
- `formal/Trilogy/Rollup.lean` (`group_filter_commutes_with_resum`): a WHERE of
  columns an aggregate's keys determine keeps whole groups, so the aggregate
  coarser than the rows re-sums the summary under it.

Rows checked by hand against `test_oracle_sweep_laws.ROWS` (heights 3, NULL,
5, NULL, 7, 2)."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

CASES = [
    # spellings 237480, reduced: every species back on the summary
    (
        (
            "select species, sum(height) by species as v0, sum(height ? (min(height)"
            " by tree_form < 10 or max(height) by tree_form != 6)) as v1 where"
            " tree_form is null having min(height) by species >= 0;"
        ),
        [],
    ),
    # spellings 243699, reduced: a CTE joined twice, invalid on the base model
    (
        (
            "select tree_id, sum(height ? tree_form is null) as v0, max(height ?"
            " count_distinct(tag) by species = 1) as v1, city as v2 having"
            " min(height) by tree_form <= 5 or (avg(height) by species <= 6 and v1 >="
            " 6);"
        ),
        [
            ("a1", None, None, "A"),
            ("a2", None, None, "A"),
            ("a3", None, 5, "A"),
            ("b1", None, None, "B"),
            ("b2", None, 2, "B"),
        ],
    ),
    # spellings 262499, reduced: `count by city` 5 on the summary
    (
        (
            "select city, tree_form, count(tree_id) as v0, count(tree_id) by city as"
            " v1, sum(min(height) by city, species, tree_form) as v2 where tree_form ="
            " 'broadleaf';"
        ),
        [("A", "broadleaf", 3, 3, 8), ("B", "broadleaf", 1, 1, 7)],
    ),
    # spellings 267243: city A dropped on the summary
    (
        (
            "select city, tree_form, max(count(tree_id) by city, species, tree_form)"
            " as v0, min(count(tree_id) by city, tree_form) as v1, count(tree_id ?"
            " height is not null) as v2 where coalesce(tree_form, 'none') ="
            " 'broadleaf' having count(tree_id) by city, tree_form < 5;"
        ),
        [("A", "broadleaf", 2, 3, 2), ("B", "broadleaf", 1, 1, 1)],
    ),
]


TREE_SCAN = 'as "trees"'

# (query, rows): a coarse aggregate under a WHERE of its own keys
COARSE_UNDER_WHERE = [
    (
        "select city, species, sum(height) by city as v where city = 'A';",
        [("A", "Acer", 8), ("A", "Quercus", 8), ("A", "Ulmus", 8)],
    ),
    (
        "select city, species, sum(height) by city as v where upper(city) = 'B';",
        [("B", "Acer", 9), ("B", "Tilia", 9)],
    ),
]


@pytest.mark.parametrize("query,expected", CASES)
@pytest.mark.parametrize("model", ["base", "summary"])
def test_sweep_270k_shape(model: str, query: str, expected: list):
    sql = executor(model).generate_sql(query)[-1]
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), sql


@pytest.mark.parametrize("query,expected", COARSE_UNDER_WHERE)
def test_coarse_aggregate_under_own_key_where_reads_summary(query: str, expected: list):
    for model in ("base", "summary"):
        sql = executor(model).generate_sql(query)[-1]
        rows = executor(model).execute_raw_sql(sql).fetchall()
        assert normalize(rows) == normalize(expected), f"{model}\n{sql}"
    assert "tree_summary" in sql, sql
    assert TREE_SCAN not in sql, sql
