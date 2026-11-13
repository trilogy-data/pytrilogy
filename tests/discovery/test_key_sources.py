"""`formal/Trilogy/SelectGrain.lean` (`keys_off_two_groups_pair_unheld`,
`dedup_carries_determined`, `conjunct_read_past_its_host_passes_absent`,
`dimension_side_pads_gate`, `key_off_unfiltered_stream_returns_rejected`): the
select's keys come off one row stream that holds their tuples and passed the
WHERE; a bridge holds the key it was elected for; each argument of a WHERE
atom is read off its own group when NULL lets the atom hold; a side the FINAL
filters carries the gate's keys itself. Rows checked by hand against `ROWS` of
`test_oracle_sweep_laws.py`."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

KEPT_PAIRS = [("A", "Acer"), ("A", "Quercus"), ("B", "Acer"), ("B", "Tilia")]

CASES = [
    # `city` off the `(city, tree_form)` gate, `species` off the `species` gate,
    # paired on `tree_form`: `(B, Quercus)` holds no tree
    (
        (
            "select city, species, sum(height) by * as v0 where (max(height) by city, "
            "tree_form <= 3 or min(height) by species != 6);"
        ),
        [(c, s, 17) for c, s in KEPT_PAIRS],
    ),
    (
        (
            "select city, species, tree_form, max(height) by tree_form as v0 where "
            "(avg(height) by city, tree_form <= 8 or max(height) by species >= 2);"
        ),
        [
            ("A", "Acer", "broadleaf", 7),
            ("A", "Quercus", "broadleaf", 7),
            ("B", "Acer", "broadleaf", 7),
            ("B", "Tilia", "linden", 2),
        ],
    ),
    # the bridge computing `v2` holds `tree_form` below a dedup
    (
        (
            "select tree_id, species, sum(height ? count_distinct(tag) by species < 0) "
            "as v0, min(height) by city, tree_form as v1, city as v2 where ((tag = 'x' "
            "or avg(height) by city >= 6) and species in ('Acer', 'Ulmus'));"
        ),
        [("a1", "Acer", None, 3, "A"), ("a4", "Ulmus", None, None, "A")],
    ),
    # the `?` stream read the total off the select's host, padded: `is null` held
    (
        (
            "select count(tree_id ? max(height) by * != 10) as v0, sum(height ? not "
            "(species = 'Acer')) as v2 where (sum(height) by * is null and max(height) "
            "by city > 3);"
        ),
        [],
    ),
    (
        (
            "select count(tree_id ? max(height) by * != 10) as v0, sum(height ? not "
            "(species = 'Acer')) as v2 where (sum(height) by * is null and max(height) "
            "by city > 3) having v2 <= 8 or min(height) by * != 3;"
        ),
        [],
    ),
    # the side supplying `tree_form` was the enrichment alone, beside a second
    # scan carrying `city` to the gate
    (
        (
            "select species, tree_form, max(height) as v0 where min(height) by city, "
            "tree_form is null;"
        ),
        [("Ulmus", None, None)],
    ),
    (
        (
            "select species, tree_form, avg(height) as v0 where min(height) by city, "
            "tree_form is null;"
        ),
        [("Ulmus", None, None)],
    ),
    # `(city, tree_form)` came off the `?` stream, which joined only beside the
    # per-city host: `(A, null)` returned
    (
        (
            "select city, tree_form, count(tree_id ? (city = 'A' or min(height) by city "
            "= 2)) as v0, count_distinct(tag) by city as v1 where (not "
            "(count_distinct(tag) by tree_form - count(tree_id) by city, tree_form = 3) "
            "and (coalesce(tree_form, 'none') = 'broadleaf' or sum(height) by city, "
            "tree_form <= 3));"
        ),
        [("A", "broadleaf", 3, 2), ("B", "broadleaf", 1, 2), ("B", "linden", 1, 2)],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query,expected", CASES)
def test_key_sources(query: str, expected: list[tuple], model: str):
    sql = executor(model).generate_sql(query)[-1]
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), f"{query}\n{sql}"
