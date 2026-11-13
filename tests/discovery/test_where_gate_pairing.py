"""The regressions behind the 0.3.492 oracle sweep
(`tests/discovery/test_oracle_sweep_laws.py`), run in the build job.
`SelectGrain.atom_on_group_key_drops_groups`: an atom drops whole groups only
when it is decided per group key; otherwise it filters the rows the aggregates
read (`mixed_atom_after_count_misreads`);
`SelectGrain.pair_implied_off_its_side_misjoins`: a join pair is implied only
by pairs on the side that holds it; `Rollup.dimension_column_filters_every_scan`:
a dimension's column in the WHERE filters every scan of its key."""

import pytest

from tests.discovery.test_rollup_resolution_laws import BASE, SUMMARY, _run

# (query, rows) with no summary in scope.
BASE_CASES = {
    # `mixed_atom_after_count_misreads`: `tree_form` was peeled to a dimension join and
    # the atom applied after the count
    "mixed_atom_beside_property": (
        (
            "select species, tree_form, count(tree_id) as v0"
            " where (tree_form = 'linden' or max(height) by city, species = 3);"
        ),
        [("Acer", "broadleaf", 2), ("Tilia", "linden", 1)],
    ),
    "mixed_atom_total_beside_gate": (
        (
            "select city, species, tree_form, avg(height) by * as v0"
            " where ((sum(height) by city, species < 6 or tree_form is null)"
            " and species = 'Acer');"
        ),
        [("A", "Acer", "broadleaf", 3)],
    ),
    # `pair_implied_off_its_side_misjoins`: `tree_form` was paired off the species
    # gate, never off the `(city, tree_form)` gate joined on `city` alone
    "gate_pairs_through_property": (
        (
            "select city, species, tree_form, count(tree_id) as v0"
            " where sum(height) by city, tree_form < 8 or max(height) by species = 6;"
        ),
        [("B", "Acer", "broadleaf", 1), ("B", "Tilia", "linden", 1)],
    ),
    # a key read off the gate's group shares no key with a `by tree_form`
    "mixed_atom_key_off_row_stream": (
        (
            "select species, min(height) by tree_form as v0"
            " where ((tree_form = 'broadleaf' or count(tree_id) by city, species = 1)"
            " and species = 'Acer');"
        ),
        [("Acer", 3)],
    ),
    # the regrouped branch holds every output: no FINAL re-apply
    "negated_gates_regrouped": (
        (
            "select tree_form, min(sum(height) by city, species) as v0"
            " where not (avg(height) by * = 3 or avg(height) by city, species > 8);"
        ),
        [("broadleaf", 3), ("linden", 2), (None, None)],
    ),
    # an atom over a grain key is decided before the aggregate
    "or_atom_on_grain_key": (
        (
            "select city, species, max(height) as v0, count(tree_id) by tree_form as v1"
            " where (city = 'B' and tree_form is null) or count(tree_id) by species = 3;"
        ),
        [("A", "Acer", 3, 3), ("B", "Acer", 7, 3)],
    ),
}

# (query, rows, served) with the summary in scope.
SUMMARY_CASES = {
    "mixed_atom_beside_rolled_count": (
        (
            "select species, tree_form, count(tree_id) as v0"
            " where (tree_form = 'linden' or max(height) by city, species = 3);"
        ),
        [("Acer", "broadleaf", 2), ("Tilia", "linden", 1)],
        True,
    ),
    # `Rollup.gate_joined_on_part_of_its_grain_misfilters`
    "dimension_gate_beside_coarse_gate": (
        (
            "select species, count(tree_id) as v0"
            " where (sum(height) by city, tree_form < 8 or species = 'Acer')"
            " or count(tree_id) by city > 5;"
        ),
        [("Acer", 3), ("Tilia", 1)],
        False,
    ),
    "dimension_gate_or_coarse_or_row": (
        (
            "select species, count(tree_id) as v0 where sum(height) by tree_form < 8"
            " or count(tree_id) by city > 5 or species = 'Acer';"
        ),
        [("Acer", 3), ("Tilia", 1)],
        False,
    ),
    "dimension_gate_and_row_or_coarse": (
        (
            "select species, count(tree_id) as v0"
            " where (sum(height) by city, tree_form < 8 and species = 'Acer')"
            " or count(tree_id) by city > 5;"
        ),
        [("Acer", 1)],
        False,
    ),
    # `Rollup.dimension_column_filters_every_scan`
    "total_under_dimension_filter": (
        (
            "select species, tree_form, max(sum(height) by species) as v0,"
            " max(height) by * as v1 where tree_form is null;"
        ),
        [("Ulmus", None, None, None)],
        False,
    ),
    "having_total_under_dimension_filter": (
        (
            "select species, tree_form, min(count(tree_id) by species) as v0"
            " where tree_form is null having min(height) by * > 0;"
        ),
        [],
        False,
    ),
    # the scan a filtered partial shares carries its condition's column
    "filtered_partial_on_shared_scan": (
        (
            "select sum(height ? city = 'B') as v0,"
            " count(min(height) by species) as v1;"
        ),
        [(9, 3)],
        True,
    ),
    "unsourced_gate_beside_rolled_gate": (
        (
            "select species, tree_form, min(height) as v0"
            " where min(height) by species < 5 and count_distinct(tag) by city = 2;"
        ),
        [("Acer", "broadleaf", 3), ("Tilia", "linden", 2)],
        False,
    ),
    # the gate's twin is not a column of the rows the stored partial is read on
    "gate_twin_of_stored_partial": (
        (
            "select species, bool_and(tall) as v0,"
            " sum(sum(height) by city, species) as v1"
            " where sum(height) by city, species = 7;"
        ),
        [("Acer", True, 7)],
        True,
    ),
}


@pytest.mark.parametrize("name", sorted(BASE_CASES))
def test_gate_pairs_on_kept_rows(name: str):
    query, expected = BASE_CASES[name]
    sql, rows = _run(BASE, query)
    assert sorted(rows, key=repr) == sorted(expected, key=repr), sql


@pytest.mark.parametrize("name", sorted(SUMMARY_CASES))
def test_gate_pairs_beside_summary(name: str):
    query, expected, served = SUMMARY_CASES[name]
    sql, rows = _run(BASE + SUMMARY, query)
    assert sorted(rows, key=repr) == sorted(expected, key=repr), sql
    assert ("tree_summary" in sql) == served, sql
