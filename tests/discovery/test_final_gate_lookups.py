"""`formal/Trilogy/SelectGrain.lean` (`gate_paired_short_of_its_grain`,
`count_arg_past_host_passes_absent`, `lookup_preserved_passes_padding`): a
gate the FINAL re-applies pairs on its whole grain; a COUNT read past a host of
the WHERE reads 0 there, not NULL; a WHERE argument's producer is a lookup onto
the FINAL's rows, never a side whose rows it keeps; and a HAVING pushed into a
group names only grouped columns. Rows checked by hand against `ROWS` of
`test_oracle_sweep_laws.py`."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

PAIRS = [
    ("A", "Acer", 3, 1),
    ("A", "Quercus", 5, 1),
    ("A", "Ulmus", None, 0),
    ("B", "Acer", 7, 1),
    ("B", "Tilia", 2, 1),
]
TOTALS = {"Acer": 10, "Quercus": 5, "Ulmus": None, "Tilia": 2}
COUNTS = {"Acer": 3, "Quercus": 1, "Ulmus": 1, "Tilia": 1}

CASES = [
    # the side holding `tree_form` was folded away: the gate met the rows on
    # `city` alone, and city A repeated once per kept form
    (
        (
            "select city, species, sum(height) by species as v0, sum(min(height) by "
            "city, species, tree_form) as v1 where count_distinct(tag) by city, "
            "tree_form < 3;"
        ),
        [(c, s, TOTALS[s], m) for c, s, m, _ in PAIRS],
    ),
    (
        (
            "select city, species, count(tree_id) by species as v0, sum(sum(height) "
            "by city, species, tree_form) as v1, count(max(height) by city, species, "
            "tree_form) as v2 where count_distinct(tag) by city, tree_form < 8 having "
            "sum(height) by * != 1;"
        ),
        [(c, s, COUNTS[s], m, n) for c, s, m, n in PAIRS],
    ),
    # the total count read past the WHERE's host is padding, 0: `5 - 0 > 3`
    (
        (
            "select city, species, tree_form, max(height ? (min(height) by * <= 3 and "
            "city = 'B')) as v0 where min(height) by species - count(tree_id) by * > 3;"
        ),
        [],
    ),
    # `tree_form = 'broadleaf' or sum(...) < 0` pushed into a group by species
    (
        (
            "select city, species, tree_form, max(height ? coalesce(tree_form, 'none') "
            "= 'broadleaf') as v0 where sum(height) by species is null having "
            "tree_form = 'broadleaf' or sum(height) by city, species < 0;"
        ),
        [],
    ),
    # a producer of the WHERE's arguments joined FULL: its species came back
    # padded, NULL for the `avg`, and `is null` held
    (
        (
            "select species, tree_form, sum(height) as v0 where (count(tree_id) by "
            "tree_form < count(tree_id) by species or avg(height) by city, tree_form "
            "is null);"
        ),
        [("Ulmus", None, None)],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query,expected", CASES)
def test_final_gate_lookups(query: str, expected: list[tuple], model: str):
    sql = executor(model).generate_sql(query)[-1]
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), f"{query}\n{sql}"
