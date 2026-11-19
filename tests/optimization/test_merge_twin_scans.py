"""Two scans of one table under one WHERE read the same rows once.

The planner names a scan before predicate pushdown moves the WHERE onto it,
so a scan filtered at plan time survived beside its pushdown-filtered twin
(expressions 362641, 364578: the trees read twice under `tag = 'x'`)."""

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
