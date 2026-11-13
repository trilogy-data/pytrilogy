"""`formal/Trilogy/SelectGrain.lean` (`having_by_bound_key_is_coarser`,
`gate_read_past_its_host_passes_absent`, `value_bucket_pairs_short_of_its_grain`): a HAVING `by` keys the select's rows
bind (`city` beside `tree_id`, spelled `city` or its rename) is one value per
row, never a grain the HAVING would grow; and a group hosting a WHERE atom
reads the atom's aggregate from its own group, never off another host of the
atom, whose rejected rows are absent and pass `is null`; a value at a grain
the rows determine rides the rows' identity, never a bucket of its own. Rows checked by hand
against `ROWS` of `test_oracle_sweep_laws.py`."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

CASES = [
    # `count(tree_id) by city` beside `tree_id` was rejected as growing the grain
    (
        "select tree_id, max(height ? city = 'A') as v having count(tree_id) by "
        "city > 2;",
        [("a1", 3), ("a2", None), ("a3", 5), ("a4", None)],
    ),
    (
        "select tree_id, sum(height ? species = 'Acer') as v having max(height) "
        "by species >= 5;",
        [("a1", 3), ("a2", None), ("a3", None), ("b1", 7)],
    ),
    # finalize renames the HAVING's `by city` to `by v2`
    (
        "select tree_id, species, min(height ? city = 'A') as v1, city as v2 "
        "having min(height) by city = 2;",
        [("b1", "Acer", None, "B"), ("b2", "Tilia", None, "B")],
    ),
    (
        "select tree_id, species, min(height ? city = 'A') as v1, city as v2 "
        "having min(height) by city, species = 7;",
        [("b1", "Acer", None, "B")],
    ),
    # the per-key group read `avg(height) by *` off the `?` total's host,
    # empty once the WHERE rejected it, so `is null` held on its padding
    (
        "select city, species, min(height ? max(height) by * = 6) as v0 where "
        "(avg(height) by * is null and city = 'B');",
        [],
    ),
    (
        "select city, species, min(height ? (max(height) by * = 6 or city = 'B')) "
        "as v0 where (avg(height) by * is null and city = 'B');",
        [],
    ),
    (
        "select city, species, min(height ? (max(height) by * = 6 or city = 'B')) "
        "as v0 where (avg(height) by * is not null and city = 'B');",
        [("B", "Acer", 7), ("B", "Tilia", 2)],
    ),
    # the HAVING value read with the rows off the summary was bucketed at its
    # own grain `(city, species)` and paired with the rows on `species` alone
    (
        "select tree_id, species, max(height) by tree_form as v0 where species "
        "= 'Acer' having sum(height) by city, species >= 2;",
        [("a1", "Acer", 7), ("a2", "Acer", 7), ("b1", "Acer", 7)],
    ),
    (
        "select tree_id, species, --sum(height) by city, species as s, "
        "max(height) by tree_form as v0 where species = 'Acer' having s >= 2;",
        [("a1", "Acer", 7), ("a2", "Acer", 7), ("b1", "Acer", 7)],
    ),
    # 0.3.494's seed 8920: the WHERE's `max(height) by tree_form` was read off
    # the HAVING gate's twin
    (
        "select tree_id where (sum(height) by city - max(height) by tree_form > 2 "
        "and height is not null) having max(height) by tree_form >= 1;",
        [("b2",)],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query,expected", CASES)
def test_gate_value_sources(query: str, expected: list, model: str):
    sql = executor(model).generate_sql(query)[-1]
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), f"{query}\n{sql}"
