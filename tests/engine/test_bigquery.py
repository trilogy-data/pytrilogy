from preql import Dialects, Environment
import pytest


@pytest.mark.skip(reason="set up BQ in CI")
def test_readme():

    environment = Environment()

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
    address bigquery-public-data.usa_names.usa_1910_2013;

    """
    )
    executor = Dialects.BIGQUERY.default_executor(environment=environment)

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
