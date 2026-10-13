"""A scan computing a filter whose predicate reads a derived concept folds into
its consumer only with that concept rendered from lineage: the raw datasource
has no column for it."""

from trilogy import Dialects, Environment

MODEL = """
key run_date date;
key run_org string;
property <run_date, run_org>.runs_total int;

datasource daily_run_stats (
    run_date:run_date,
    org_id:run_org,
    runs_total:runs_total,
    )
grain (run_date, run_org)
address daily_run_stats;

auto rwindow_1d <- run_date = date_add(current_date(), day, -1);
"""

QUERY = """
select
    sum(runs_total ? rwindow_1d) -> runs_1d,
    count_distinct(run_org ? rwindow_1d and runs_total > 0) -> active_orgs_1d;
"""


def test_filtered_count_distinct_beside_filtered_sum():
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.execute_raw_sql("""create table daily_run_stats as
        select current_date - 1 as run_date, 'a' as org_id, 3 as runs_total
        union all select current_date - 1, 'b', 0
        union all select current_date - 5, 'c', 1""")
    sql = executor.generate_sql(MODEL + QUERY)[-1]
    assert "INVALID" not in sql, sql
    assert executor.execute_text(MODEL + QUERY)[-1].fetchall() == [(3, 1)]
