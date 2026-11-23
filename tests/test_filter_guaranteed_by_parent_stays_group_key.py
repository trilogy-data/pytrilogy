"""A filter virtual whose predicate its parent already guarantees (the
filtered city of `sum(height) by city, species < 6`, whose group parent holds
that HAVING) renders as bare content, so the CTE grouping to it keeps it a
group key instead of collapsing it with MAX. `filter_case_elided` is the one
answer the renderer and the CTE's collapse share; when only the renderer
knew about the parent's guarantee, the CTE wrapped the bare column in MAX
and grouped by it as well."""

from tests.discovery.test_rollup_resolution_laws import BASE
from trilogy import Dialects

QUERY = (
    "select city, max(height ? city in (city ? sum(height) by city, species < 6))"
    " as v0;"
)


def test_parent_guaranteed_filter_is_not_collapsed():
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(BASE)
    sql = executor.generate_sql(QUERY)[-1]
    assert sql.count("max(") == 1, sql
    assert sorted(executor.execute_raw_sql(sql).fetchall()) == [("A", 5), ("B", 7)]
