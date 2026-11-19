"""The reduced shapes of the expressions family's 200,000-350,000 sweep and the
leads it closed (0.3.516).

- A value over `sum(height) by species` and `count(tree_id) by city`, under a
  WHERE atom reading both grains: each aggregate group hosting the atom was
  said to hold the other grain's key, the merge paired the two on it, and the
  join rendered ON 1=1, inventing the city-species pairs no tree holds.
- `min(height) by city, species` beside `max(height) by species` in an OR
  with a row atom, under a key derived from a property of `species`: the atom
  was hosted where the min was never joined, and the min was rendered at the
  host's `species` grain.
- An expression of constants (`1 - 10`) beside two keys and a mixed OR gate
  raised a keyless join: it was planned at no grain, where `1` takes the
  select's.
- An OR of gates at `city, tree_form` and `species` beside `city, species`
  raised a missing parent (the 78245 lead), on the same false claim as the
  first shape.
- A literal at the select's grain beside a WHERE aggregate its grain has no
  key for was constrained by that aggregate alone, and joined it on no key
  (on main too, for a bare `4`).
- A `by *` aggregate inlined into a selected value made the all-rows constant
  look read by the select's grain, so it lost that grain.
- Reading stated columns (#1038), the FINAL fold dropped the side rendering a
  literal folded onto a sibling, and the side rendering a key another side
  was owed but never given (`tag`, owed to a value by `city, tag` computed
  off aggregates alone).

Rows checked by hand against `test_oracle_sweep_laws.ROWS` (heights 3, NULL,
5, NULL, 7, 2; tags x, y, x, x, y, x; forms broadleaf x4 with b1, NULL for
Ulmus, linden for b2)."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

CASES = [
    # expressions 205085, reduced
    (
        (
            "select city, species, sum(height) by species / count(tree_id) by city"
            " as v0 where species != 'Tilia' or max(height) by species ="
            " count(tree_id) by city;"
        ),
        [
            ("A", "Acer", 2.5),
            ("A", "Quercus", 1.25),
            ("A", "Ulmus", None),
            ("B", "Acer", 5),
            ("B", "Tilia", 1),
        ],
    ),
    # expressions 246819 (and 333909), reduced
    (
        (
            "select case when tree_form = 'broadleaf' then 'b' else 'o' end as fc"
            " where height = 3 or min(height) by city, species = max(height) by"
            " species;"
        ),
        [("b",), ("o",)],
    ),
    # expressions 15826 and 89514, reduced
    (
        (
            "select city, species, 1 - 10 as v0 where height = 3 or"
            " count(tree_id) by city <= 2;"
        ),
        [("A", "Acer", -9), ("B", "Acer", -9), ("B", "Tilia", -9)],
    ),
    # expressions 78245, its WHERE loosened so that it keeps rows
    (
        (
            "select city, species, count_distinct(tag) by species /"
            " count(tree_id) by city, tree_form as v0 where (sum(height) by city,"
            " tree_form >= 5 and sum(height) by species >= min(height) by *) or"
            " (tree_form = 'linden' and city = 'B');"
        ),
        [
            ("A", "Acer", 2 / 3),
            ("A", "Quercus", 1 / 3),
            ("B", "Acer", 2),
            ("B", "Tilia", 1),
        ],
    ),
    # expressions 67331, with a literal
    (
        (
            "select city, tree_form, 4 as v0 where sum(height) by city, tree_form"
            " < 0 or height is not null;"
        ),
        [("A", "broadleaf", 4), ("B", "broadleaf", 4), ("B", "linden", 4)],
    ),
    # expressions 104893, with a literal
    (
        (
            "select tree_id, 11 as v0 where count_distinct(tag) by tree_form -"
            " count_distinct(tag) by * < 2;"
        ),
        [(t, 11) for t in ("a1", "a2", "a3", "a4", "b1", "b2")],
    ),
    # expressions 349206, with a literal
    (
        (
            "select city, height > 2 as big, 4 as v0 where count_distinct(tag) by"
            " city - min(height) by species = 3 or tree_form is not null;"
        ),
        [("A", None, 4), ("A", True, 4), ("B", False, 4), ("B", True, 4)],
    ),
    # expressions 99561, a literal folded onto a sibling
    (
        (
            "select species, tag, 2 * 2 as v0 where tag = 'x' and max(height) by"
            " city is not null;"
        ),
        [("Acer", "x", 4), ("Quercus", "x", 4), ("Tilia", "x", 4), ("Ulmus", "x", 4)],
    ),
    # expressions 21365, a key owed and never delivered
    (
        (
            "select city, tag, sum(height) by * / count(tree_id) by city as v0,"
            " sum(height) by * + min(height) by * as v1 where coalesce(tree_form,"
            " 'none') = 'broadleaf';"
        ),
        [("A", "x", 5, 18), ("A", "y", 5, 18), ("B", "y", 15, 18)],
    ),
    # expressions 225055
    (
        (
            "select tree_id, count(tree_id) by city, tree_form + count(tree_id) by"
            " * as v0, min(height) by city, species as v1, count(tree_id) as v2"
            " where avg(height) by tree_form is not null;"
        ),
        [
            ("a1", 8, 3, 1),
            ("a2", 8, 3, 1),
            ("a3", 8, 5, 1),
            ("b1", 6, 7, 1),
            ("b2", 6, 2, 1),
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


def test_constant_expression_takes_the_select_grain():
    ex = executor("base")
    sql = ex.generate_sql("select city, species, 1 - 10 as v0;")[-1]
    assert normalize(ex.execute_raw_sql(sql).fetchall()) == [
        ("A", "Acer", -9),
        ("A", "Quercus", -9),
        ("A", "Ulmus", -9),
        ("B", "Acer", -9),
        ("B", "Tilia", -9),
    ]
