import sys

import pytest

from trilogy import Dialects
from trilogy.constants import Rendering
from trilogy.core.models.environment import Environment
from trilogy.hooks.query_debugger import DebuggingHook

UNSUPPORTED_TUPLE = (3, 10)

# FORCE INSTALL sqlalchemy-bigquery (#--ignore-requires-python) to run this test if needed

# bigquery is not supported on 13 yet
@pytest.mark.skipif(
    sys.version_info >= UNSUPPORTED_TUPLE, reason="BigQuery not supported on 3.13"
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
    sys.version_info >= UNSUPPORTED_TUPLE, reason="BigQuery not supported on 3.13"
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
        assert len(answers) == 3


@pytest.mark.skipif(
    sys.version_info >= UNSUPPORTED_TUPLE, reason="BigQuery not supported on 3.13"
)
def test_unnest_rendering():
    environment = Environment()
    DebuggingHook()
    _, queries = environment.parse(
        """
key sentences string;

datasource sentences(

    sentences:sentences
    )
query '''
select 'the quick brown fox jumps over the lazy dog' as sentences
union all
select 'the lazy dog jumps over the quick brown fox' as sentences
''';

select sentences, unnest(split(sentences, ' ')) as words;
    """
    )
    executor = Dialects.BIGQUERY.default_executor(environment=environment)
    sql = executor.generate_sql(queries[-1])

    assert "CROSS JOIN unnest(split(" in sql[0], sql[0]


@pytest.mark.skipif(
    sys.version_info >= UNSUPPORTED_TUPLE, reason="BigQuery not supported on 3.13"
)
def test_unnest_constant():
    environment = Environment()
    _, queries = environment.parse(
        """


const list <- [1,2,3,4];

auto rows <- unnest(list);


select 
    rows;
    
    """
    )
    executor = Dialects.BIGQUERY.default_executor(
        environment=environment, rendering=Rendering(parameters=False)
    )
    sql = executor.generate_sql(queries[-1])

    assert (
        sql[0].strip()
        == """SELECT
    _unnest_alias as `rows`
FROM
    unnest([1, 2, 3, 4]) as `_unnest_alias`"""
    ), sql[0]
