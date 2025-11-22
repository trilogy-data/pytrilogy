from trilogy import Executor
from trilogy.hooks import DebuggingHook


def test_partition_persistence(executor: Executor):
    years = executor.execute_query(
        "import root; select ride_year order by ride_year asc;"
    ).fetchall()
    base_row = 0
    DebuggingHook()
    for row in years:
        executor.environment.set_parameters(load_year=row.ride_year)
        results = executor.execute_file("daily.preql")

        # count_result = executor.execute_raw_sql(
        #     """select count(*) as cnt from tbl_daily_fact where ride_year = :ride_year;""",
        #     variables={"ride_year": row.ride_year},
        # ).fetchone()
        count_result = executor.execute_raw_sql(
            """select count(*) as cnt from tbl_daily_fact;"""
        ).fetchone()
        assert count_result.cnt > base_row
        base_row = count_result.cnt
    
    q1 = executor.generate_sql("select ride_year, ride_month, ride_count;")[0]
    assert 'daily_fact' not in q1
    executor.execute_text(' publish datasource daily_fact;')
    q2 = executor.generate_sql("select ride_year, ride_month, ride_count;")[0]
    assert 'daily_fact' in q2
