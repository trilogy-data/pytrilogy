from trilogy import Dialects, Executor


def test_partition_persistence(executor: Executor):
    years = executor.execute_query(
        "import root; select ride_year order by ride_year asc;"
    ).fetchall()
    for row in years:
        executor.environment.set_parameters(load_year=row.ride_year)
        results = executor.execute_file("daily.preql")

        count_result = executor.execute_raw_sql(
            """select count(*) as cnt from tbl_daily_fact where ride_year = :ride_year;""",
            variables={"ride_year": row.ride_year},
        ).fetchone()
        print(count_result)
