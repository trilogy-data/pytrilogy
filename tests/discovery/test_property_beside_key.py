"""`SelectGrain.property_without_its_key_misjoins`: a property selected beside
its key (`tree_form` beside `species`) is read off the key's rows, never off a
group holding it without the key (`max(height) by city, tree_form`), which
pairs each species with every form of its city. The summary cases are
`Rollup.gate_resummed_at_target_misfilters` and the shapes the joined plan
declines."""

import pytest

from tests.discovery.test_rollup_resolution_laws import BASE, SUMMARY, _run

PROPERTY_BESIDE_KEY = {
    "projected": (
        (
            "select city, species, tree_form, max(height) by city, tree_form as g,"
            " count(tree_id) as v order by city, species;"
        ),
        [
            ("A", "Acer", "broadleaf", 5, 2),
            ("A", "Quercus", "broadleaf", 5, 1),
            ("A", "Ulmus", None, None, 1),
            ("B", "Acer", "broadleaf", 7, 1),
            ("B", "Tilia", "linden", 2, 1),
        ],
    ),
    "gated": (
        (
            "select city, species, tree_form, count(tree_id) as v"
            " where sum(height) by city, tree_form > 6 order by city, species;"
        ),
        [
            ("A", "Acer", "broadleaf", 2),
            ("A", "Quercus", "broadleaf", 1),
            ("B", "Acer", "broadleaf", 1),
        ],
    ),
    "keys_only": (
        (
            "select city, tree_form, species where max(height) by city, tree_form > 6"
            " order by city, species;"
        ),
        [("B", "broadleaf", "Acer")],
    ),
}


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("name", sorted(PROPERTY_BESIDE_KEY))
def test_property_comes_with_its_key(name: str, model: str):
    query, expected = PROPERTY_BESIDE_KEY[name]
    sql, rows = _run(BASE + (SUMMARY if model == "summary" else ""), query)
    assert rows == expected, sql


# (query, rows, served): a coarse gate beside a key and its dimension column
# reads the summary; a total gate under an OR, or an unstored gate at the
# summary's grain beside a dimension gate, reads the trees: off the summary, a
# stored total (`max(height) by *`) was read per target group.
SUMMARY_GATES = {
    "key_and_form_city_gate": (
        (
            "select species, tree_form, count(tree_id) as v where sum(height) by city > 8"
            " order by species;"
        ),
        [("Acer", "broadleaf", 1), ("Tilia", "linden", 1)],
        True,
    ),
    "key_and_form_form_gate": (
        (
            "select species, tree_form, count(tree_id) as v"
            " where max(height) by tree_form > 6 order by species;"
        ),
        [("Acer", "broadleaf", 3), ("Quercus", "broadleaf", 1)],
        True,
    ),
    "key_and_form_not_null_gate": (
        (
            "select species, tree_form, sum(height) as v"
            " where sum(height) by city is not null order by species;"
        ),
        [
            ("Acer", "broadleaf", 10),
            ("Quercus", "broadleaf", 5),
            ("Tilia", "linden", 2),
            ("Ulmus", None, None),
        ],
        True,
    ),
    "total_gate_in_or": (
        (
            "select city, sum(height) as v"
            " where count(tree_id) by city > 6 or max(height) by * > 6 order by city;"
        ),
        [("A", 8), ("B", 9)],
        False,
    ),
    "total_gate_in_or_at_summary_grain": (
        (
            "select city, sum(height) as v"
            " where sum(height) by city, species > 6 or max(height) by * > 6"
            " order by city;"
        ),
        [("A", 8), ("B", 9)],
        False,
    ),
    "total_gate_in_or_beside_dimension_column": (
        (
            "select species, sum(height) as v"
            " where tree_form = 'linden' or max(height) by * > 6 order by species;"
        ),
        [("Acer", 10), ("Quercus", 5), ("Tilia", 2), ("Ulmus", None)],
        False,
    ),
    "total_gate_in_or_beside_target_column": (
        (
            "select city, sum(height) as v"
            " where city = 'A' or max(height) by * > 6 order by city;"
        ),
        [("A", 8), ("B", 9)],
        False,
    ),
    "unstored_gate_beside_dimension_gate": (
        (
            "select city, count(tree_id) as v where count_distinct(tag) by city, species = 1"
            " and sum(height) by city, tree_form > 1 order by city;"
        ),
        [("A", 1), ("B", 2)],
        False,
    ),
}


@pytest.mark.parametrize("name", sorted(SUMMARY_GATES))
def test_summary_gate_shapes(name: str):
    query, expected, served = SUMMARY_GATES[name]
    sql, rows = _run(BASE + SUMMARY, query)
    assert rows == expected, sql
    assert ("tree_summary" in sql) == served, sql
