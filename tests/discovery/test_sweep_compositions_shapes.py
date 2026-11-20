"""Reduced shapes of the `compositions` oracle family (seeds 0-30,000), each
failing on 0.3.521, with rows checked by hand against the six trees."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

FC = "case when tree_form = 'broadleaf' then 'b' else 'o' end as fc"

SHAPES = [
    # a membership OR a gate is applied to the species stream beside a total
    # (337): a1, a2, b1 by the gate, b1, b2 by the set
    (
        (
            "select species, count(tree_id) by * as v2 where tree_id in (tree_id ?"
            " city = 'B') or count(tree_id) by species > 2;"
        ),
        [("Acer", 4), ("Tilia", 4)],
    ),
    # ...and to the total's input beside a select aggregate (337): a4 is out
    (
        (
            "select species, max(height) as m, count(tree_id) by * as v2 where tree_id"
            " in (tree_id ? tree_form is not null) or sum(height) by species >= 3;"
        ),
        [("Acer", 7, 5), ("Quercus", 5, 5), ("Tilia", 2, 5)],
    ),
    # a set listed first is not the row stream's grain (5544): every form is in
    (
        (
            "select species, count(tree_id) as v1 where tree_form in (tree_form ?"
            " avg(height) by city > 3);"
        ),
        [("Acer", 3), ("Quercus", 1), ("Tilia", 1), ("Ulmus", 1)],
    ),
    # a set is no row source of a DISTINCT (2087)
    (
        (
            "select city, species, avg(height) by * as v0 where species in (species ?"
            " species = 'Acer');"
        ),
        [("A", "Acer", 5), ("B", "Acer", 5)],
    ),
    # a derived key's own grain (`species` of `fc`) is no group key (2081)
    (
        f"select city, {FC}, count(tree_id) by city + min(height) by fc as v0;",
        [("A", "b", 7), ("A", "o", 6), ("B", "b", 5), ("B", "o", 4)],
    ),
    (
        (
            f"select city, {FC}, count(tree_id) by city + min(height) by fc as v0"
            " where height > 2;"
        ),
        [("A", "b", 5), ("B", "b", 4)],
    ),
    # a total read off per-species partials is re-aggregated (876)
    (
        (
            "select sum(count(tree_id) by species) as v0, min(height) + 0 as v1"
            " where city = 'A';"
        ),
        [(4, 3)],
    ),
    (
        (
            "select sum(count(tree_id) by species) as v0, min(height) * sum(height)"
            " by * as v1 where tree_form = 'broadleaf';"
        ),
        [(4, 45)],
    ),
    # two sets and a gate over aggregates at shared grains plan (9202)
    (
        (
            "select species where (species in (species ? not (min(height) by * is not"
            " null)) or species in (species ? count_distinct(tag) by species -"
            " min(height) by city = 2)) or count(tree_id) by * - max(height) by city"
            " > 2;"
        ),
        [],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query, expected", SHAPES)
def test_composition_shape(query: str, expected: list[tuple], model: str):
    sql = executor(model).generate_sql(query)[-1]
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), sql
