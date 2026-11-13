"""The regressions behind `tests/discovery/test_oracle_sweep_laws.py`, run in
the build job (which skips `*_laws.py`). `SelectGrain.keptKeys`: a select's key
rows are the keys of the rows its WHERE keeps, whatever group supplies them;
`SelectGrain.by_property_without_its_key_misjoins`: a `by` grain holding a
property is joined on the property its key determines;
`Rollup.where_filters_every_scan`: a summary beside the base rows leaves no
scan unfiltered."""

import pytest

from tests.discovery.test_rollup_resolution_laws import BASE, SUMMARY, _run

# (query, rows) with no summary in scope.
BASE_CASES = {
    # `literal_membership_gate_dropped`: the species came off the gate's group
    "literal_membership_gate": (
        (
            "select species, min(height) by * as v0"
            " where max(height) by city, species in (7, 1) order by species;"
        ),
        [("Acer", 7)],
    ),
    "literal_membership_in_and": (
        (
            "select species, min(height) by * as v0"
            " where (count_distinct(tag) by species = 1 and city in ('B')) order by species;"
        ),
        [("Tilia", 2)],
    ),
    "literal_membership_in_or": (
        (
            "select city, species, max(height) by tree_form as v0"
            " where (city in ('B') or avg(height) by species = 3) order by city, species;"
        ),
        [("B", "Acer", 7), ("B", "Tilia", 2)],
    ),
    "literal_membership_absent_count": (
        (
            "select city, tree_form, min(sum(height) by city, species) as v0"
            " where count(tree_id) by species in (5, 3) order by city, tree_form;"
        ),
        [("A", "broadleaf", 3), ("B", "broadleaf", 7)],
    ),
    # `gate_group_skips_row_atoms`
    "gate_beside_row_atom": (
        (
            "select species, min(height) by * as v0"
            " where count_distinct(tag) by species = 1 and city = 'B' order by species;"
        ),
        [("Tilia", 2)],
    ),
    # `by_property_without_its_key_misjoins`
    "by_property_beside_count": (
        (
            "select city, species, max(height) by city, tree_form as v0,"
            " count(tree_id) as n order by city, species;"
        ),
        [
            ("A", "Acer", 5, 2),
            ("A", "Quercus", 5, 1),
            ("A", "Ulmus", None, 1),
            ("B", "Acer", 7, 1),
            ("B", "Tilia", 2, 1),
        ],
    ),
    "by_property_beside_gate": (
        (
            "select city, species, max(height) as v, min(height) by city, tree_form as v1"
            " where tag = 'x' and count(tree_id) by species < 2 order by city, species;"
        ),
        [("A", "Quercus", 5, 5), ("A", "Ulmus", None, None), ("B", "Tilia", 2, 2)],
    ),
    "gate_by_property": (
        (
            "select city, species, sum(height) by species as v0"
            " where count_distinct(tag) by tree_form > 1 order by city, species;"
        ),
        [("A", "Acer", 10), ("A", "Quercus", 5), ("B", "Acer", 10)],
    ),
    # `determined_by_on_grain`: a HAVING grain the select grain determines
    "having_by_property": (
        (
            "select species, count(min(height) by city, species, tree_form) as v0"
            " where species in ('Acer', 'Ulmus') having sum(height) by tree_form > 5;"
        ),
        [("Acer", 2)],
    ),
    # `SelectGrain.regrouped_gate_is_applied`: the re-aggregate holds only the
    # groups the gate kept, so FINAL does not re-apply it at `tree_form`
    "regrouped_gate_or_row": (
        (
            "select tree_form, min(sum(height) by city, species, tree_form) as v0"
            " where (min(height) by species > 1 or city = 'B');"
        ),
        [("broadleaf", 3), ("linden", 2)],
    ),
    "regrouped_gate_or_membership": (
        (
            "select tree_form, count(min(height) by species) as v0"
            " where (sum(height) by species in (5, 1) or sum(height) by * < 2)"
            " having v0 < 8;"
        ),
        [("broadleaf", 1)],
    ),
    "regrouped_gate_beside_key": (
        (
            "select city, tree_form, sum(sum(height) by city, species) as v0"
            " where (city = 'A' or max(height) by species = 10);"
        ),
        [("A", "broadleaf", 8), ("A", None, None)],
    ),
}

# (query, rows, served) with the summary in scope.
SUMMARY_CASES = {
    # `Rollup.where_filters_every_scan`
    "total_beside_summary_read": (
        (
            "select species, sum(height) by * as v0, max(min(height) by species) as v1"
            " where species != 'Tilia' order by species;"
        ),
        [("Acer", 15, 3), ("Quercus", 15, 5), ("Ulmus", 15, None)],
        False,
    ),
    "count_total_beside_summary_read": (
        (
            "select species, min(max(height) by city, species, tree_form) as v0,"
            " count(tree_id) by * as v1 where species = 'Acer';"
        ),
        [("Acer", 3, 3)],
        True,
    ),
    # `Cover.filtered_sum_exact` at the summary's own grain
    "filtered_at_summary_grain": (
        (
            "select city, species, sum(height ? city = 'A') as v0"
            " where min(height) by city > 1 order by city, species;"
        ),
        [
            ("A", "Acer", 3),
            ("A", "Quercus", 5),
            ("A", "Ulmus", None),
            ("B", "Acer", None),
            ("B", "Tilia", None),
        ],
        True,
    ),
    "filtered_count_at_summary_grain": (
        (
            "select city, species, count(tree_id ? city = 'A') as v0, bool_and(tall) as v1"
            " where count(tree_id) by tree_form > 2 order by city, species;"
        ),
        [("A", "Acer", 2, False), ("A", "Quercus", 1, True), ("B", "Acer", 0, True)],
        True,
    ),
}


@pytest.mark.parametrize("name", sorted(BASE_CASES))
def test_where_reaches_key_rows(name: str):
    query, expected = BASE_CASES[name]
    sql, rows = _run(BASE, query)
    assert sorted(rows, key=repr) == sorted(expected, key=repr), sql


@pytest.mark.parametrize("name", sorted(SUMMARY_CASES))
def test_where_reaches_every_scan(name: str):
    query, expected, served = SUMMARY_CASES[name]
    sql, rows = _run(BASE + SUMMARY, query)
    assert sorted(rows, key=repr) == sorted(expected, key=repr), sql
    assert ("tree_summary" in sql) == served, sql
