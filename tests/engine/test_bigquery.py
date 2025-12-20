import sys

import pytest

from trilogy import Dialects
from trilogy.constants import Rendering
from trilogy.core.models.core import ArrayType, DataType, ListWrapper
from trilogy.core.models.environment import Environment
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.hooks.query_debugger import DebuggingHook

UNSUPPORTED_TUPLE = (3, 14)

# FORCE INSTALL sqlalchemy-bigquery (#--ignore-requires-python) to run this test if needed


# bigquery is not supported on 14 yet
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
    property <name, gender, state, year>.yearly_name_count int;

    datasource usa_names(
        name:name,
        number:yearly_name_count,
        year,
        gender,
        state
    )
    grain (name, year, gender, state)
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
    sys.version_info >= UNSUPPORTED_TUPLE, reason="BigQuery not supported on 3.14"
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


@pytest.mark.skipif(
    sys.version_info >= UNSUPPORTED_TUPLE, reason="BigQuery not supported on 3.13"
)
def test_in_with_array():
    environment = Environment()
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    _, queries = environment.parse(
        """


const list <- [1,2,3,4];

const two <- 2;

where two in list
select 
    two;
    
    """
    )
    executor = Dialects.BIGQUERY.default_executor(
        environment=environment, rendering=Rendering(parameters=False)
    )
    sql = executor.generate_sql(queries[-1])[0]
    listc = environment.concepts["list"]
    assert listc.datatype == ArrayType(type=DataType.INTEGER)
    assert isinstance(listc.lineage.arguments[0], ListWrapper), type(
        listc.lineage.arguments[0]
    )
    assert "unnest" in sql, sql


def test_datetime_functions():
    environment = Environment()
    _, queries = environment.parse(
        """
    const order_id <- 1;
    const order_timestamp <- current_datetime();
    select
        order_id,
        order_timestamp,
        date(order_timestamp) -> order_date,
        datetime(order_timestamp) -> order_timestamp_datetime,
        timestamp(order_timestamp) -> order_timestamp_dos,
        second(order_timestamp) -> order_second,
        minute(order_timestamp) -> order_minute,
        hour(order_timestamp) -> order_hour,
        day(order_timestamp) -> order_day,
        week(order_timestamp) -> order_week,
        month(order_timestamp) -> order_month,
        quarter(order_timestamp) -> order_quarter,
        year(order_timestamp) -> order_year,
        date_trunc(order_timestamp, month) -> order_month_trunc,
        date_add(order_timestamp, month, 1) -> one_month_post_order,
        date_sub(order_timestamp, month, 1) -> one_month_pre_order,
        date_trunc(order_timestamp, day) -> order_day_trunc,
        date_trunc(order_timestamp, year) -> order_year_trunc,
        date_trunc(order_timestamp, hour) -> order_hour_trunc,
        date_trunc(order_timestamp, minute) -> order_minute_trunc,
        date_trunc(order_timestamp, second) -> order_second_trunc,
        date_trunc(order_timestamp, quarter) -> order_quarter_trunc,
        date_trunc(order_timestamp, week) -> order_week_trunc,
        date_part(order_timestamp, month) -> order_month_part,
        date_part(order_timestamp, day) -> order_day_part,
        date_part(order_timestamp, year) -> order_year_part,
        date_part(order_timestamp, hour) -> order_hour_part,
        date_part(order_timestamp, minute) -> order_minute_part,
        date_part(order_timestamp, second) -> order_second_part,
        date_part(order_timestamp, quarter) -> order_quarter_part,
        date_part(order_timestamp, week) -> order_week_part,
        date_part(order_timestamp, day_of_week) -> order_day_of_week_part,
        month_name(order_timestamp) -> order_month_name,
        day_name(order_timestamp) -> order_day_name
    ;
    
    
        """
    )

    executor = Dialects.BIGQUERY.default_executor(
        environment=environment, rendering=Rendering(parameters=False)
    )

    executor.execute_query(queries[-1])


def test_date_functions():
    environment = Environment()
    _, queries = environment.parse(
        """
    const order_id <- 1;
    const order_timestamp <- current_date();
    select
        order_id,
        order_timestamp,
        date(order_timestamp) -> order_date,
        datetime(order_timestamp) -> order_timestamp_datetime,
        timestamp(order_timestamp) -> order_timestamp_dos,
        day(order_timestamp) -> order_day,
        week(order_timestamp) -> order_week,
        month(order_timestamp) -> order_month,
        quarter(order_timestamp) -> order_quarter,
        year(order_timestamp) -> order_year,
        date_trunc(order_timestamp, month) -> order_month_trunc,
        date_add(order_timestamp, month, 1) -> one_month_post_order,
        date_sub(order_timestamp, month, 1) -> one_month_pre_order,
        date_trunc(order_timestamp, day) -> order_day_trunc,
        date_trunc(order_timestamp, year) -> order_year_trunc,
        date_trunc(order_timestamp, quarter) -> order_quarter_trunc,
        date_trunc(order_timestamp, week) -> order_week_trunc,
        date_part(order_timestamp, month) -> order_month_part,
        date_part(order_timestamp, day) -> order_day_part,
        date_part(order_timestamp, year) -> order_year_part,
        date_part(order_timestamp, quarter) -> order_quarter_part,
        date_part(order_timestamp, week) -> order_week_part,
        date_part(order_timestamp, day_of_week) -> order_day_of_week_part,
        month_name(order_timestamp) -> order_month_name,
        day_name(order_timestamp) -> order_day_name
    ;
    
    
        """
    )

    executor = Dialects.BIGQUERY.default_executor(
        environment=environment, rendering=Rendering(parameters=False)
    )

    executor.execute_query(queries[-1])


def test_string_functions(test_environment):
    environment = Environment()
    _, queries = environment.parse(
        """
    const category_id <- 1;  
    auto category_name <- 'apple';
    auto test_name <- concat(category_name, '_test');
    auto upper_name <- upper(category_name);
    auto lower_name <- lower(category_name);
    auto substring_name <- substring(category_name, 1, 3);
    auto strpos_name <- strpos(category_name, 'a');
    auto like_name <- like(category_name, 'a%');
    auto like_alt <- category_name like 'a%';
    auto regex_contains <- regexp_contains(category_name, 'a');
    auto regex_substring <- regexp_extract(category_name, 'a');
    auto regex_replace <- regexp_replace(category_name, 'a', 'b');

    select
        category_id,
        test_name,
        upper_name,
        lower_name,
        substring_name,
        strpos_name,
        like_name,
        like_alt,
        regex_contains,
        regex_substring,
        regex_replace,
        hash(category_name, md5) -> hash_md5,
        hash(category_name, sha1) -> hash_sha1,
        hash(category_name, sha256) -> hash_sha256,
        # hash(category_name, sha512) -> hash_sha512
    ;"""
    )
    executor = Dialects.BIGQUERY.default_executor(
        environment=environment, rendering=Rendering(parameters=False)
    )

    executor.execute_query(queries[-1])


def test_math_functions():
    environment = Environment()
    _, queries = environment.parse(
        """
    const revenue <- 100.50;
    const order_id <- 1;
    
    
    auto inflated_order_value<- multiply(revenue, 2);
    auto fixed_order_value<- inflated_order_value / 2;
    auto order_add <- revenue + 2;
    auto order_sub <- revenue - 2;
    auto order_nested <- revenue * 2/2;
    auto rounded <- round(revenue + 2.01,2);
    auto rounded_default <- round(revenue + 2.01);
    auto floor <- floor(revenue + 2.01);
    auto ceil <- ceil(revenue + 2.01);
    auto squared <- revenue ** 2;
    auto random <- random(1);
    select
        order_id,
        inflated_order_value,
        order_nested,
        fixed_order_value,
        order_sub,
        order_add,
        rounded,
        rounded_default,
        floor,
        ceil,
        squared,
        random,
    ;
        """
    )

    executor = Dialects.BIGQUERY.default_executor(
        environment=environment, rendering=Rendering(parameters=False)
    )

    results = executor.execute_query(queries[-1])
    list_results = list(results.fetchall())
    assert len(list_results) == 1
    assert list_results[0].squared == 100.50**2
    assert list_results[0].ceil == 103
    assert list_results[0].floor == 102


def test_aggregate_functions():
    exec = Dialects.BIGQUERY.default_executor()
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    results = exec.execute_text(
        """
key order_id int;
property order_id.revenue float;
property order_id.quantity int;
property order_id.is_premium bool;
key category string;

datasource orders (
    order_id,
    revenue,
    quantity,
    is_premium,
    category
)
grain (order_id)
query '''
select 1 as order_id, 100.50 as revenue, 2 as quantity, true as is_premium, 'A' as category
union all
select 2, 250.75, 1, false, 'B'
union all
select 3, 75.25, 3, true, 'A'
union all
select 4, 300.00, 1, false, 'C'
union all
select 5, 150.80, 4, true, 'B'
''';

auto total_revenue <- sum(revenue);
auto max_revenue <- max(revenue);
auto min_revenue <- min(revenue);
auto avg_revenue <- avg(revenue);
auto order_count <- count(order_id);
auto distinct_categories <- count(category);
auto total_quantity <- sum(quantity);
auto any_premium <- bool_or(is_premium);
auto all_premium <- bool_and(is_premium);
auto revenue_array <- array_agg(revenue);

select
    total_revenue,
    max_revenue,
    min_revenue,
    avg_revenue,
    order_count,
    distinct_categories,
    total_quantity,
    any_premium,
    all_premium,
    revenue_array,
;
"""
    )

    result = list(results[-1].fetchall())[0]

    # Test aggregate calculations
    assert result.total_revenue == 877.30
    assert result.max_revenue == 300.00
    assert result.min_revenue == 75.25
    assert abs(result.avg_revenue - 175.46) < 0.01
    assert result.order_count == 5
    assert result.distinct_categories == 3
    assert result.total_quantity == 11
    assert result.any_premium is True
    assert result.all_premium is False
    assert len(result.revenue_array) == 5


def test_array():
    test_executor = Dialects.BIGQUERY.default_executor()
    test_select = """
    const num_list <- [1,2,3,3,4,5];

    SELECT
        len(num_list) AS length,
        array_sum(num_list) AS total,
        array_distinct(num_list) AS distinct_values,
        array_sort(num_list, asc) AS sorted_values,
    ;"""
    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert len(results) == 1
    assert results[0][0] == 6  # length
    assert results[0][1] == 18  # total
    assert set(results[0][2]) == {1, 2, 3, 4, 5}, "distinct matches"  # distinct_values
    assert results[0][3] == [1, 2, 3, 3, 4, 5]  # sorted_values


def test_array_agg():
    test_executor = Dialects.BIGQUERY.default_executor()
    test_select = """
    const num_list <- unnest([1,2,3,3,4,5]);

    SELECT
        array_agg(num_list) AS aggregated_values,
    ;"""

    results = list(test_executor.execute_text(test_select)[0].fetchall())
    assert len(results) == 1
    assert results[0] == ([1, 2, 3, 3, 4, 5],)  # aggregated_values


def test_hash_column_value():
    dialect = BigqueryDialect()
    result = dialect.hash_column_value("my_column")
    assert result == "FARM_FINGERPRINT(CAST(`my_column` AS STRING))"

    result_special = dialect.hash_column_value("column with spaces")
    assert result_special == "FARM_FINGERPRINT(CAST(`column with spaces` AS STRING))"


def test_aggregate_checksum():
    dialect = BigqueryDialect()
    result = dialect.aggregate_checksum("FARM_FINGERPRINT(CAST(`id` AS STRING))")
    assert result == "BIT_XOR(FARM_FINGERPRINT(CAST(`id` AS STRING)))"
