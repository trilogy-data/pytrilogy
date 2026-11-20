"""Two scans of one table under one WHERE read the same rows once, and a scan
under a WHERE reads an unfiltered twin rather than the table again.

The planner names a scan before predicate pushdown moves the WHERE onto it,
so a scan filtered at plan time survived beside its pushdown-filtered twin
(expressions 362641, 364578: the trees read twice under `tag = 'x'`)."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize
from trilogy.constants import CONFIG

QUERY = (
    "select tree_id, count_distinct(tag) / count(tree_id) by city as v0,"
    " sum(height * 2) / count(tree_id) by tree_form as v1,"
    " sum(height) / count(tree_id) as v2 where tag = 'x';"
)
ROWS = [
    ("a1", 1 / 3, 3, 3),
    ("a3", 1 / 3, 5, 5),
    ("a4", 1 / 3, None, None),
    ("b2", 1, 4, 2),
]


def _scans(sql: str) -> int:
    return sql.count('as "trees"')


def test_twin_scans_read_the_table_once():
    ex = executor("base")
    sql = ex.generate_sql(QUERY)[-1]
    assert _scans(sql) == 1, sql
    assert normalize(ex.execute_raw_sql(sql).fetchall()) == normalize(ROWS)


def test_twin_scans_without_the_rule_read_it_twice():
    ex = executor("base")
    CONFIG.optimizations.merge_twin_scans = False
    try:
        sql = ex.generate_sql(QUERY)[-1]
    finally:
        CONFIG.optimizations.merge_twin_scans = True
    assert _scans(sql) == 2
    assert normalize(ex.execute_raw_sql(sql).fetchall()) == normalize(ROWS)


GROUPED = (
    "select tree_form, max(height) by * / count(tree_id) by tree_form as v0,"
    " min(height) as v1 where city = 'B';"
)


def test_grouped_scan_groups_its_row_twin():
    ex = executor("base")
    sql = ex.generate_sql(GROUPED)[-1]
    assert _scans(sql) == 1, sql
    assert normalize(ex.execute_raw_sql(sql).fetchall()) == normalize(
        [("broadleaf", 7, 7), ("linden", 7, 2)]
    )


def test_grouped_total_counts_repeated_rows():
    ex = executor("base")
    sql = ex.generate_sql(
        "select tree_form, count(tree_id) by * as n, max(height) as m"
        " where species in ('Acer', 'Ulmus');"
    )[-1]
    assert _scans(sql) == 1, sql
    assert normalize(ex.execute_raw_sql(sql).fetchall()) == normalize(
        [("broadleaf", 4, 7), (None, 4, None)]
    )


def test_summary_resum_keeps_its_partial_source():
    ex = executor("summary")
    sql = ex.generate_sql(
        "select city, species, tree_form, max(height) by city as v0 where city = 'A';"
    )[-1]
    assert normalize(ex.execute_raw_sql(sql).fetchall()) == normalize(
        [
            ("A", "Acer", "broadleaf", 5),
            ("A", "Quercus", "broadleaf", 5),
            ("A", "Ulmus", None, 5),
        ]
    )


FILTERED = [
    (
        "select tree_id, height where city = 'A' and count(tree_id) by species > 1"
        " and max(height) by city < 6;",
        [("a1", 3), ("a2", None)],
    ),
    # expressions 650115, reduced
    (
        "select city, min(height) as v0 where city = 'B'"
        " and avg(height) by species != 0 and count(tree_id) by city = 2;",
        [("B", 2)],
    ),
]


@pytest.mark.parametrize("query, expected", FILTERED)
def test_filtered_scan_reads_its_unfiltered_twin(query: str, expected: list[tuple]):
    ex = executor("base")
    sql = ex.generate_sql(query)[-1]
    assert _scans(sql) == 1, sql
    assert normalize(ex.execute_raw_sql(sql).fetchall()) == normalize(expected)
