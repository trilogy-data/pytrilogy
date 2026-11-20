"""Reduced shapes of the `compositions` oracle family, each failing on the
release before the one that fixed it, with rows checked by hand against the
six trees."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize
from tests.discovery.test_rollup_resolution_laws import BASE, SUMMARY
from trilogy import Dialects

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
    # values by city and by fc are bridged by the rows holding both keys, which
    # takes two groups at once: the city-species rows and fc by species
    # (15258); Ulmus (A, o) fails the gate
    (
        (
            f"select city, {FC}, max(height) by fc as a, sum(height) by city as b"
            " where max(height) by species >= 1;"
        ),
        [("A", "b", 7, 8), ("B", "b", 7, 9), ("B", "o", 2, 9)],
    ),
    # a column a set reads stays on the select's rows when a gate is keyed by
    # it (57225): city A alone has more than two trees
    (
        (
            "select tag, max(height) by * as v0 where species in (species ? city ="
            " 'A') and count(tree_id) by city > 2;"
        ),
        [("x", 5), ("y", 5)],
    ),
    (
        (
            "select tag, max(height) by * as v0 where species in (species ?"
            " count(tree_id) by city > 1) and count(tree_id) by city > 2;"
        ),
        [("x", 5), ("y", 5)],
    ),
    # a derived key a sibling is grouped by is not split by its own grain
    # (50815): every city is in the set
    (
        (
            f"select city, {FC}, sum(height) by city * count_distinct(tag) by fc as"
            " v0 where city in (city ? count(tree_id) by species > 0);"
        ),
        [("A", "b", 16), ("A", "o", 8), ("B", "b", 18), ("B", "o", 9)],
    ),
    # a group gated by a set carries no value through it: the broadleaf
    # distinct tags are 2, not Quercus' 1 (55868); a4, b2 pass the gate and
    # a2, b1 the set
    (
        (
            "select count(tree_id) as v0 where count_distinct(tag) by tree_form ="
            " count(tree_id) by species or not (tag in (tag ? avg(height) by"
            " tree_form is null));"
        ),
        [(4,)],
    ),
    # a set's own rows bridge nothing (55114): the set keeps every tag, so
    # Ulmus stays though its own condition is NULL
    (
        (
            "select city, species, tree_form, count(tree_id) by city, tree_form as"
            " v0, max(height) as v1 where tag in (tag ? min(height) by city, species"
            " - count_distinct(tag) by tree_form > 0);"
        ),
        [
            ("A", "Acer", "broadleaf", 3, 3),
            ("A", "Quercus", "broadleaf", 3, 5),
            ("A", "Ulmus", None, 1, None),
            ("B", "Acer", "broadleaf", 1, 7),
            ("B", "Tilia", "linden", 1, 2),
        ],
    ),
    # ...nor supply a value FINAL re-reads (46333): the set is empty, and the
    # linden sum, 2, passes beside it
    (
        (
            "select city, sum(height) by * as v0 where sum(height) by tree_form <= 3"
            " or tree_form in (tree_form ? max(height) by tree_form < 0);"
        ),
        [("B", 2)],
    ),
    # one CTE planned twice under one WHERE nested two ways merges (49126): b2
    # alone is an x in a Tilia set and city B
    (
        (
            f"select city, {FC}, max(height) by fc as v0 where tag = 'x' and species"
            " in (species ? height = 2) and city in (city ? city = 'B');"
        ),
        [("B", "o", 2)],
    ),
    # a gate FINAL re-applies beside a total pairs on its whole grain, the
    # species it carries onto the value's side included (63640): Ulmus' NULL
    # minimum keeps (A, none) alone of city A
    (
        (
            "select city, tree_form, count(tree_id) by * as v0, count(tree_id) by"
            " tree_form / count(tree_id) by city as v1 where min(height) by city,"
            " species is null or city in ('B');"
        ),
        [("A", None, 3, 1.0), ("B", "broadleaf", 3, 0.5), ("B", "linden", 3, 0.5)],
    ),
    # a set in an OR is read by its own membership, not by the WHERE aggregate
    # the other set is computed from (41277, 66218 raised a cycle): only the
    # NULL form's (A, none) maximum is NULL
    (
        (
            "select tree_form where max(height) by city, tree_form - sum(height) by"
            " tree_form = 2 or (tree_form in (tree_form ? count(tree_id) by tree_form"
            " != 10) and tree_form in (tree_form ? max(height) by city, tree_form is"
            " null));"
        ),
        [(None,)],
    ),
    # a per-species gate beside a membership is no scan column of the summary's
    # (city, species) rows (37601): Acer's three trees are out, a3, a4, b2 in
    (
        (
            "select city, count(tree_id) as c where count(tree_id) by species != 3"
            " or city in (city ? tag = 'q');"
        ),
        [("A", 2), ("B", 1)],
    ),
    # a stored total a set reads is not re-maxed per species (47456): the
    # tallest tree is 7, so every species is in
    (
        (
            "select species where species in (species ? max(height) by * = 7) and"
            " max(height) by * is not null;"
        ),
        [("Acer",), ("Quercus",), ("Tilia",), ("Ulmus",)],
    ),
    (
        (
            "select species where species in (species ? max(height) by * = 2) and"
            " max(height) by * is not null;"
        ),
        [],
    ),
    # a scan whose one parent is a set's feeder holds its own rows, not the
    # feeder's (23531): the per-city maximum is re-maxed to the city before
    # it is counted
    (
        (
            "select city, count(max(height) by city) as v1, max(count(tree_id) by"
            " city, species, tree_form) as v2 where city in (city ? sum(height) by"
            " city > 0);"
        ),
        [("A", 1, 2), ("B", 1, 1)],
    ),
    # ...and a set's own `species = 'Acer'` pins none of them (15155): every
    # city has a tree, so the city minima are 3 and 2
    (
        (
            "select max(min(height) by city) as v0 where city in (city ? height > 0)"
            " or species in (species ? species = 'Acer');"
        ),
        [(3,)],
    ),
    # a value over per-species partials is no join key of the form's rows
    # (38002): the kept broadleaf trees sum to 15 over 4 trees, max 7, min 3
    (
        (
            "select tree_form, min(max(height) by tree_form) / (max(height) -"
            " min(height)) as v0, sum(height) by tree_form - count(tree_id) by"
            " tree_form as v1 where species in (species ? species != 'Tilia' and"
            " species != 'Ulmus');"
        ),
        [("broadleaf", 1.75, 11)],
    ),
    # ...nor a group key under a dedup beside a nested re-sum (94262): Acer's
    # 10 over 10 - 5 is one value, not one per city
    (
        (
            "select species, tree_form, min(max(height) by species) / (max(height) -"
            " min(height)) as v0, sum(height) by species / (sum(height) - 5) as v1,"
            " sum(min(height) by city, species) as v2;"
        ),
        [
            ("Acer", "broadleaf", 1.75, 2, 10),
            ("Quercus", "broadleaf", None, None, 5),
            ("Tilia", "linden", None, -0.666667, 2),
            ("Ulmus", None, None, None, None),
        ],
    ),
    # a set's count by (city, form) is no count by city off the summary's
    # rows (83051): no (city, form) group holds more than 3 trees, so the set
    # is empty, though city A holds 4
    (
        (
            "select city, tree_form, count_distinct(tag) + count(min(height) by"
            " city, species) as v1, min(height) as v2 where city in (city ?"
            " count(tree_id) by city, tree_form > 3);"
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


@pytest.mark.parametrize("model", ["base", "summary"])
def test_set_off_a_finer_stored_count(model: str):
    """A set's condition at keys its content lacks reads a row per (city,
    species) group (83051): only (A, Acer) holds two trees."""
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(BASE + (SUMMARY if model == "summary" else ""))
    rows = exec.execute_text(
        "auto s <- city ? count(tree_id) by city, species > 1; select s;"
    )[-1].fetchall()
    assert normalize(rows) == [("A",)]
