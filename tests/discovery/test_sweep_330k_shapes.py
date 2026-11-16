"""The reduced shapes of the 270,000-330,000 sweep (0.3.509).

- `formal/Trilogy/SelectGrain.lean` (`lookup_adds_no_tuple`,
  `lookup_beside_split_keys_pairs_unkept`): a lookup of a key (`tree_form` of
  `species`) beside keys split over two WHERE aggregates adds no tuple, so
  FINAL still re-applies the WHERE; it skipped it and returned a pair the
  WHERE dropped.
- `formal/Trilogy/SelectGrain.lean` (`exposed_column_joins_nothing`,
  `keyed_lookup_keeps_rows`): a merge joins a side only on the keys its plan
  pairs it on, so a column a side merely exposes bridges no two groups; read as
  one, the bridge that pairs them was left out and the merge raised a keyless
  join.

Rows checked by hand against `test_oracle_sweep_laws.ROWS` (heights 3, NULL,
5, NULL, 7, 2)."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

CASES = [
    # spellings 304584, reduced: (A, Acer) kept off the unfiltered groups
    (
        (
            "select city, species, tree_form where max(height) by species -"
            " min(height) by city, species < 1 having min(height) by species > 3 or"
            " max(height) by city > 80;"
        ),
        [("A", "Quercus", "broadleaf"), ("B", "Acer", "broadleaf")],
    ),
    # spellings 310634, reduced: the filter bridge read as joining the city groups
    (
        (
            "select tree_id, min(height ? min(height) by * >= 1) as v0, max(height)"
            " by city as v1, max(height) by city, tree_form as v2 where min(height)"
            " by species >= 1 having max(height) by species <= 10;"
        ),
        [
            ("a1", 3, 5, 5),
            ("a2", None, 5, 5),
            ("a3", 5, 5, 5),
            ("b1", 7, 7, 7),
            ("b2", 2, 7, 2),
        ],
    ),
    # spellings 273853, reduced: a peel's `tree_form` read as joining the forms
    (
        (
            "select species, max(height) by tree_form as v0, count(tree_id ? tag ="
            " 'x') as v1, max(height) as v2 where tree_form = 'broadleaf' having"
            " min(height) by tree_form = 10;"
        ),
        [],
    ),
    (
        (
            "select species, max(height) by tree_form as v0, count(tree_id ? tag ="
            " 'x') as v1, max(height) as v2 where tree_form = 'broadleaf' having"
            " min(height) by tree_form = 3;"
        ),
        [("Acer", 7, 1, 7), ("Quercus", 7, 1, 5)],
    ),
]


@pytest.mark.parametrize("query,expected", CASES)
@pytest.mark.parametrize("model", ["base", "summary"])
def test_sweep_330k_shape(model: str, query: str, expected: list):
    sql = executor(model).generate_sql(query)[-1]
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), sql
