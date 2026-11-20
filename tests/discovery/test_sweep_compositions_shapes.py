"""Reduced shapes of the `compositions` oracle family, each failing on the
release before the one that fixed it, with rows checked by hand against the
six trees."""

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
    # a gate keyed off the outputs beside a membership is re-applied on the
    # rows FINAL pairs the keys by (17410): a4 (Ulmus, no form) is out
    (
        (
            "select city, tree_form, count(tree_id) by city as v0 where (max(height)"
            " by species < 1 or species in (species ? height between 3 and 7));"
        ),
        [("A", "broadleaf", 3), ("B", "broadleaf", 1)],
    ),
    # a value re-summed off a nullable stored partial is nullable (12435)
    (
        (
            "select city, tree_form, max(max(height) by city, tree_form) as v0,"
            " max(height) as v1, min(height) - min(height) by city, tree_form as v2;"
        ),
        [
            ("A", "broadleaf", 5, 5, 0),
            ("A", None, None, None, None),
            ("B", "broadleaf", 7, 7, 0),
            ("B", "linden", 2, 2, 0),
        ],
    ),
    # a summary's rolled bucket is not the dimension peeled onto its key (28945)
    (
        (
            "select city, species, tree_form, max(height) by species as v0, rank"
            " city over species, tree_form order by sum(height) by city, species,"
            " tree_form desc as v1;"
        ),
        [
            ("A", "Acer", "broadleaf", 7, 2),
            ("A", "Quercus", "broadleaf", 5, 1),
            ("A", "Ulmus", None, None, 1),
            ("B", "Acer", "broadleaf", 7, 1),
            ("B", "Tilia", "linden", 2, 1),
        ],
    ),
    # a value re-summing partials at a derived key is not split by the key's
    # own grain (33686)
    (
        (
            f"select {FC}, sum(height) + count(tree_id) as v0 where city in (city ?"
            " sum(height) by species <= 5);"
        ),
        [("b", 19), ("o", 4)],
    ),
    # an aggregate of a value re-aggregated in its own group is that value
    # (35546): b2 alone, max(height) by city is 2
    (
        (
            "select city, rank city order by sum(height) by city asc as v0,"
            " sum(max(height) by city) + min(max(height) by city, tree_form) as v1"
            " where tree_form = 'linden';"
        ),
        [("B", 1, 4)],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query, expected", SHAPES)
def test_composition_shape(query: str, expected: list[tuple], model: str):
    sql = executor(model).generate_sql(query)[-1]
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), sql
