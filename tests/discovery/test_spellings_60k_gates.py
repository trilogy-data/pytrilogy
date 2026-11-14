"""`formal/Trilogy/SelectGrain.lean` (`filtered_value_is_per_row`,
`gate_feeder_is_no_population`, `peel_skips_row_filter`,
`rename_keeps_filtered_rows`, `peel_paired_short_of_its_keys`,
`parenthesized_conjunction_is_its_atoms`) and `formal/Trilogy/Rollup.lean`
(`resum_beside_finer_rows_multiplies`): the spellings family's leads on seeds
60,000-90,000 and the ones #988 left open, reduced. Rows checked by hand
against `test_oracle_sweep_laws.ROWS`."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize
from trilogy import Environment, parse
from trilogy.core.models.author import Conditional

CASES = [
    # seed 12917: the filtered id read as a value of (city, species)
    (
        "select city, species, count(tree_id ? sum(height) by city, species is not"
        " null) as v0 where avg(height) by * is not null;",
        [
            ("A", "Acer", 2),
            ("A", "Quercus", 1),
            ("A", "Ulmus", 0),
            ("B", "Acer", 1),
            ("B", "Tilia", 1),
        ],
    ),
    # seed 70075: the species feeder preserved past the row atom
    (
        "select tree_id, species where count(tree_id) by tree_form <= count(tree_id)"
        " by species and tree_form = 'linden';",
        [("b2", "Tilia")],
    ),
    # seed 65409: tree_form peeled onto the gate's species
    (
        "select tree_form, avg(height) by * as v0 where max(height) by species -"
        " count_distinct(tag) by species > 0 and height > 2;",
        [("broadleaf", 5)],
    ),
    # seed 61261: the rename's fresh scan feeding the HAVING's count
    (
        "select tree_id, city as v0, min(height ? sum(height) by tree_form = 0) as"
        " v1 where height between 3 and 7 having count(tree_id) by city, species ="
        " 2;",
        [],
    ),
    (
        "select tree_id, city as v0, min(height ? sum(height) by tree_form = 0) as"
        " v1 where height between 3 and 7 having count(tree_id) by city = 2;",
        [("a1", "A", None), ("a3", "A", None)],
    ),
    # seed 70457: a re-sum of stored counts joined to the trees
    (
        "select city, tree_form, max(sum(height) by city, species) as v0,"
        " count(tree_id) as v1, count(tree_id) by tree_form as v2;",
        [
            ("A", "broadleaf", 5, 3, 4),
            ("A", None, None, 1, 1),
            ("B", "broadleaf", 7, 1, 4),
            ("B", "linden", 2, 1, 1),
        ],
    ),
    # seed 85553: a stored gate paired short of its keys
    (
        "select tree_id, species, sum(height ? count(tag) by city, species >= 1) as"
        " v0 having min(height) by city, tree_form != 6 or sum(height) by city,"
        " species = 5;",
        [
            ("a1", "Acer", 3),
            ("a2", "Acer", None),
            ("a3", "Quercus", 5),
            ("b1", "Acer", 7),
            ("b2", "Tilia", 2),
        ],
    ),
    # seeds 21266 and 33618: a stored HAVING gate without its keys
    (
        "select tree_id, sum(height ? count(tag) by city, species = 8) as v0 having"
        " min(height) by city, species <= 6;",
        [("a1", None), ("a2", None), ("a3", None), ("b2", None)],
    ),
    # seed 63248: a total read off the rows carrying it
    (
        "select tree_form, min(height ? count_distinct(tag) by * <= 6) as v0,"
        " sum(height) as v1 where count(tree_id) by * < 6;",
        [],
    ),
    (
        "select tree_form, min(height ? count_distinct(tag) by * <= 6) as v0,"
        " sum(height) as v1 where count(tree_id) by * < 7;",
        [("broadleaf", 3, 15), ("linden", 2, 2), (None, None, None)],
    ),
    # seed 18676: a parenthesized WHERE
    (
        "select city, species, min(min(height) by city, species, tree_form) as v0"
        " where (max(height) by city, species > 1 and tree_form = 'broadleaf')"
        " having min(height) by tree_form = 3;",
        [("A", "Acer", 3), ("A", "Quercus", 5), ("B", "Acer", 7)],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query,expected", CASES)
def test_spelling_gate_rows(query: str, expected: list, model: str):
    sql = executor(model).generate_sql(query)[-1]
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), f"{query}\n{sql}"


def test_parenthesized_where_is_its_conjunction():
    env, _ = parse("key x int; datasource d (x: x) grain (x) address d;", Environment())
    _, statements = parse("select x where (x > 1 and x < 3);", env)
    assert isinstance(statements[-1].where_clause.conditional, Conditional)
