from logging import INFO

from trilogy import Executor
from trilogy.core.enums import DatasourceState
from trilogy.hooks import DebuggingHook


def test_partition_persistence(executor: Executor):
    years = executor.execute_query(
        "import root; select ride_year order by ride_year asc;"
    ).fetchall()
    base_row = 0
    DebuggingHook(INFO)
    for row in years:
        executor.environment.set_parameters(load_year=row.ride_year)
        results = executor.execute_file("build_daily.preql")

        # count_result = executor.execute_raw_sql(
        #     """select count(*) as cnt from tbl_daily_fact where ride_year = :ride_year;""",
        #     variables={"ride_year": row.ride_year},
        # ).fetchone()
        count_result = executor.execute_raw_sql(
            """select count(*) as cnt from tbl_daily_fact;"""
        ).fetchone()
        assert count_result
        assert count_result.cnt > base_row
        base_row = count_result.cnt
        print(f"Processed year {row.ride_year}, total rows: {count_result.cnt}")

    q1 = executor.generate_sql(
        "select ride_year, ride_month, total_rides order by ride_year asc, ride_month asc;"
    )[0]
    results = executor.execute_raw_sql(q1).fetchall()
    assert "daily_fact" not in q1
    executor.execute_text(" publish datasources daily_fact;")
    assert (
        executor.environment.datasources["daily_fact"].status
        == DatasourceState.PUBLISHED
    )
    q2 = executor.generate_sql(
        "select ride_year, ride_month, total_rides order by ride_year asc, ride_month asc;"
    )[0]
    comp_results = executor.execute_raw_sql(q2).fetchall()
    assert results == comp_results
    assert "daily_fact" in q2


def test_simple_partition_persistence(executor: Executor):
    DebuggingHook()
    executor.environment.set_parameters(load_year=2000)
    executor.execute_file("implicit_build_full.preql")

    results = executor.execute_raw_sql(
        ""
        "select ride_year, count(*) as count from tbl_daily_fact group by ride_year order by ride_year;"
    )
    years = 0
    for row in results.fetchall():

        assert row.count > 0
        years += 1
    assert years == 5


def test_simple_incremental_partition_persistence(executor: Executor):
    DebuggingHook()
    executor.environment.set_parameters(load_year=2021)
    executor.execute_file("implicit_build_partial.preql")

    results = executor.execute_raw_sql(
        ""
        "select ride_year, count(*) as count from tbl_daily_fact group by ride_year order by ride_year;"
    )
    years = 0
    for row in results.fetchall():

        assert row.count > 0
        years += 1
    assert years == 1
