"""The reduced shapes of the expressions family's 100,000-150,000 sweep (0.3.514).

- A value over a summary's rows by `species` beside a value `by tree_form`:
  the plan pairs the two on `tree_form`, which `species` determines but the
  summary does not store, and no built group produced it keyed by `species`.
  It is looked up off a scan of `tree_form` beside `species`.
- `SelectGrain.value_over_group_holds_no_finer_key`: a value at `(city,
  tree_form)` over two aggregates is bridged by the rows by `(city,
  species)`. Joined as built, the bridge repeated each pair once per species,
  and FINAL joining the value back on `(city, tree_form)` doubled those rows.
  A value over groups alone is deduped to its outputs before FINAL pairs it.

Rows checked by hand against `test_oracle_sweep_laws.ROWS` (heights 3, NULL,
5, NULL, 7, 2; forms broadleaf x4 with b1, NULL for Ulmus, linden for b2)."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

CASES = [
    # expressions 62974, 66824, reduced
    (
        "select species, count(tree_id) by tree_form as a, max(height) + 1 as b;",
        [("Acer", 4, 8), ("Quercus", 4, 6), ("Tilia", 1, 3), ("Ulmus", 1, None)],
    ),
    # expressions 105627
    (
        (
            "select species, sum(height) by tree_form as v0, max(height) +"
            " max(height) by species as v1;"
        ),
        [("Acer", 15, 14), ("Quercus", 15, 10), ("Tilia", 2, 4), ("Ulmus", None, None)],
    ),
    # expressions 112406: `v0` holds the rows by `(city, species)` apart
    (
        (
            "select city, species, tree_form, count(tree_id) / count(tree_id) as"
            " v0, count(tree_id) by tree_form / count(tree_id) by city as v1;"
        ),
        [
            ("A", "Acer", "broadleaf", 1, 1),
            ("A", "Quercus", "broadleaf", 1, 1),
            ("A", "Ulmus", None, 1, 0.25),
            ("B", "Acer", "broadleaf", 1, 2),
            ("B", "Tilia", "linden", 1, 0.5),
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
