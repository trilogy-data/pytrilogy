from trilogy import Dialects, Environment
import pytest
import sys


# bigquery is not supported on 13 yet
@pytest.mark.skipif(
    sys.version_info >= (3, 13), reason="BigQuery not supported on 3.13"
)
def test_date_diff_rendering():
    environment = Environment()

    _, queries = environment.parse(
        """

    const today <- current_date();

    select today
    where date_add(current_date() , day, -30) < today;
    """
    )
    executor = Dialects.BIGQUERY.default_executor(environment=environment)
    sql = executor.generate_sql(queries[-1])

    assert "DATE_ADD(current_date(), INTERVAL -30 day)" in sql[0]


@pytest.mark.skipif(
    sys.version_info >= (3, 13), reason="BigQuery not supported on 3.13"
)
def test_readme():
    environment = Environment()
    from trilogy.hooks.query_debugger import DebuggingHook

    environment.parse(
        """

    key name string;
    key gender string;
    key state string;
    key year int;
    key yearly_name_count int;

    datasource usa_names(
        name:name,
        number:yearly_name_count,
        year:year,
        gender:gender,
        state:state
    )
    address `bigquery-public-data.usa_names.usa_1910_2013`;

    """
    )
    executor = Dialects.BIGQUERY.default_executor(
        environment=environment, hooks=[DebuggingHook()]
    )

    results = executor.execute_text(
        """SELECT
        name,
       sum(yearly_name_count) -> name_count
    WHERE
        name like '%lvis%'
    ORDER BY
        name_count desc
    LIMIT 10;
    """
    )
    # multiple queries can result from one text batch
    for row in results:
        # get results for first query
        answers = row.fetchall()
        for x in answers:
            print(x)
        assert len(answers) == 3
