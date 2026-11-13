"""`formal/Trilogy/SelectGrain.lean` (`where_filters_group_rows`,
`gate_host_skips_select_rows`, `value_pair_drops_null_group`,
`guard_precedence_emits_empty_total`): a WHERE filters the rows every
aggregate reads, not only the group a filtered aggregate's condition reads; a
value is paired by its grain, never by itself; the empty-total guard conjoins
the whole HAVING. Rows checked by hand against `ROWS` of
`test_oracle_sweep_laws.py`."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

CASES = [
    # the WHERE sat only on `sum(height) by *`, the `?` condition's group
    (
        "select city, max(height ? (city = 'A' or sum(height) by * is null)) as v0 "
        "where (min(height) by * = 6 or city in ('B'));",
        [("B", None)],
    ),
    (
        "select tree_id, count(tree_id ? (sum(height) by tree_form is not null "
        "or height between 2 and 5)) as v0 where count_distinct(tag) by species "
        "< count_distinct(tag) by city, tree_form;",
        [("a3", 1)],
    ),
    # the hosted `sum(height) by city, tree_form` group fed a second filter
    (
        "select species, tree_form, sum(height) as v0, count(tree_id ? "
        "sum(height) by city, tree_form < 0) as v1, min(height ? sum(height) by "
        "city > count_distinct(tag) by tree_form) as v2 where avg(height) by "
        "city, species >= 5;",
        [("Acer", "broadleaf", 7, 0, 7), ("Quercus", "broadleaf", 5, 0, 5)],
    ),
    # a nested aggregate reads its inner group, filtered there
    (
        "select city, species, max(max(height) by city, species, tree_form) as v0, "
        "count(tree_id) by tree_form as v1 where ((city in ('B') and tree_form is "
        "null) or count(tree_id) by species in (5, 3));",
        [("A", "Acer", 3, 3), ("B", "Acer", 7, 3)],
    ),
    # both groups held `max(height) by tree_form`; `=` on it dropped Ulmus
    (
        "select city, species, tree_form, sum(height ? min(height) by tree_form - "
        "max(height) by city, species < 1) as v0 where not (min(height) by "
        "tree_form - max(height) by tree_form = 0);",
        [
            ("A", "Acer", "broadleaf", 3),
            ("A", "Quercus", "broadleaf", 5),
            ("A", "Ulmus", None, None),
            ("B", "Acer", "broadleaf", 7),
        ],
    ),
    # the row stream produces `height`, which no aggregate gate excludes it for
    (
        "select tree_id, tall as v0 where height > 3 or sum(height) by species - "
        "count(tree_id) by city = 2;",
        [("a3", True), ("b1", True)],
    ),
    (
        "select tree_id, species, tall as v0 where (height between 2 and 5 and "
        "sum(height) by species - count(tree_id) by city = 2);",
        [],
    ),
    # `a or b and count(*) > 0` emitted the empty total
    (
        "select min(height) as v0, count_distinct(tag) by * as v1 where "
        "(max(height) by * is null or (min(height) by city, tree_form != "
        "max(height) by city, tree_form and city = 'B')) having count(tree_id) "
        "by * <= 0 or sum(height) by * <= 0;",
        [],
    ),
    (
        "select bool_or(tall) as v0 where max(height) by tree_form < 0 having "
        "count(tree_id) by * > 6 or count(tree_id) by * != 3;",
        [],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query,expected", CASES)
def test_where_reaches_every_aggregate(query: str, expected: list, model: str):
    sql = executor(model).generate_sql(query)[-1]
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), f"{query}\n{sql}"
