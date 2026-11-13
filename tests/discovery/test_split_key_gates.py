"""`formal/Trilogy/SelectGrain.lean` (`split_keys_pair_unkept`,
`by_value_is_no_grain_key`, `conjunction_idempotent`) and
`formal/Trilogy/Rollup.lean` (`gate_partial_resummed_off_its_grain`): shapes
the second oracle family found, each checked against rows worked by hand, with
and without the summary in scope."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

CASES = {
    "split_keys_tree_form_gate": (
        (
            "select city, tree_form, count(tree_id) by tree_form as v0,"
            " count(tree_id) by city as v1 where min(height) by city, tree_form != 7;"
        ),
        [("A", "broadleaf", 3, 3), ("B", "linden", 1, 1)],
    ),
    "split_keys_species_gate": (
        (
            "select city, tree_form, count(tree_id) by tree_form as v0,"
            " count(tree_id) by city as v1 where min(height) by city, species != 7;"
        ),
        [("A", "broadleaf", 3, 3), ("B", "linden", 1, 1)],
    ),
    "split_keys_at_gate_grain": (
        (
            "select city, species, count(tree_id) by species as v0,"
            " count(tree_id) by city as v1 where max(height) by city, species < 7;"
        ),
        [("A", "Acer", 2, 3), ("A", "Quercus", 1, 3), ("B", "Tilia", 1, 1)],
    ),
    "split_keys_with_having": (
        (
            "select city, tree_form, count_distinct(tag) by tree_form as v0,"
            " count_distinct(tag) by city as v1"
            " where not (min(height) by city, tree_form in (1, 7)) having v0 < 3;"
        ),
        [("A", "broadleaf", 2, 2), ("A", None, 1, 2), ("B", "linden", 1, 1)],
    ),
    "keys_only_gate_off_grain": (
        "select species where sum(height) by city <= 2;",
        [],
    ),
    "keys_only_gate_keeps_null_key": (
        "select tree_form where min(height) by city > 2;",
        [("broadleaf",), (None,)],
    ),
    "by_value_beside_filtered_count": (
        (
            "select tree_id, sum(height) by tree_form as v0,"
            " count(tree_id ? sum(height) by tree_form <= 5) as v1;"
        ),
        [
            ("a1", 15, 0),
            ("a2", 15, 0),
            ("a3", 15, 0),
            ("a4", None, 0),
            ("b1", 15, 0),
            ("b2", 2, 1),
        ],
    ),
    "by_city_beside_form_filtered_count": (
        (
            "select tree_id, sum(height) by city as v0,"
            " count(tree_id ? max(height) by tree_form <= 5) as v1;"
        ),
        [
            ("a1", 8, 0),
            ("a2", 8, 0),
            ("a3", 8, 0),
            ("a4", 8, 0),
            ("b1", 9, 0),
            ("b2", 9, 1),
        ],
    ),
    "by_city_beside_form_filtered_sum": (
        (
            "select tree_id, sum(height) by city as v0,"
            " sum(height ? max(height) by tree_form <= 5) as v1;"
        ),
        [
            ("a1", 8, None),
            ("a2", 8, None),
            ("a3", 8, None),
            ("a4", 8, None),
            ("b1", 9, None),
            ("b2", 9, 2),
        ],
    ),
    "repeated_conjunct_beside_negated_gate": (
        (
            "select min(height) as v0, sum(height) by * as v1 where not"
            " (sum(height) by city, tree_form = 1) and (city = 'B' and city = 'B');"
        ),
        [(2, 9)],
    ),
}


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("case", list(CASES))
def test_split_key_gate_shapes(case: str, model: str):
    query, expected = CASES[case]
    sql = executor(model).generate_sql(query)[-1]
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), sql


@pytest.mark.parametrize(
    "query",
    [CASES["keys_only_gate_off_grain"][0], CASES["keys_only_gate_keeps_null_key"][0]],
)
def test_keys_only_gate_still_reads_summary(query: str):
    assert "tree_summary" in executor("summary").generate_sql(query)[-1]
