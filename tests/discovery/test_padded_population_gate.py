"""`formal/Trilogy/SelectGrain.lean` (`padded_side_is_not_the_population`): a
select-phase `?` filter's row stream is the trees FULL-joined to the per-form
values that applied the WHERE. The FINAL read the stream as filtered and
LEFT-joined the aggregate that did apply the WHERE onto it, so every species
came back. All fail on main; rows checked by hand against
`test_oracle_sweep_laws.ROWS`."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

CASES = [
    (
        (
            "select city, species, tree_form, min(height) by city, tree_form as v0,"
            " sum(height ? min(height) by tree_form - count(tree_id) by tree_form < 0)"
            " as v1 where avg(height) by city, tree_form is null;"
        ),
        [("A", "Ulmus", None, None, None)],
    ),
    (
        (
            "select city, species, tree_form, min(height) by city, tree_form as v0,"
            " sum(height ? min(height) by tree_form - count(tree_id) by tree_form < 0)"
            " as v1 where (count_distinct(tag) by species != 10 and avg(height) by"
            " city, tree_form is null);"
        ),
        [("A", "Ulmus", None, None, None)],
    ),
    (
        (
            "select city, species, tree_form, max(height) as v0, max(height ? not"
            " (sum(height) by city, tree_form >= max(height) by city, tree_form)) as"
            " v1, sum(height ? count(tree_id) by tree_form != 1) as v2 where"
            " sum(height) by species is null;"
        ),
        [("A", "Ulmus", None, None, None, None)],
    ),
]

KEPT_CASES = [
    (
        (
            "select city, species, tree_form, min(height) by city, tree_form as v0,"
            " sum(height ? min(height) by tree_form - count(tree_id) by tree_form < 0)"
            " as v1 where avg(height) by city, tree_form is not null;"
        ),
        [
            ("A", "Acer", "broadleaf", 3, 3),
            ("A", "Quercus", "broadleaf", 3, 5),
            ("B", "Acer", "broadleaf", 7, 7),
            ("B", "Tilia", "linden", 2, None),
        ],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query,expected", CASES + KEPT_CASES)
def test_padded_stream_is_not_the_population(query: str, expected: list, model: str):
    sql = executor(model).generate_sql(query)[-1]
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), f"{query}\n{sql}"
