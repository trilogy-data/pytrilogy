import re
from decimal import Decimal

from trilogy import Dialects
from trilogy.constants import Rendering
from trilogy.core.models.environment import Environment


def test_render_query(snowflake_engine_parameterized):
    results = snowflake_engine_parameterized.generate_sql("""select pi;""")[0]

    assert ":pi" in results

    results2 = snowflake_engine_parameterized.generate_sql(
        """
        const today <- date_trunc(current_datetime() , day);
        const ten_days_from_now <- date_add(current_datetime() , day, 10);
        auto ten_day_diff <- date_diff(today, ten_days_from_now, day);
        select 
            today,
            ten_days_from_now,
            ten_day_diff;"""
    )[0]
    assert "date_add(current_datetime(),day" in results2, results2

    results3 = snowflake_engine_parameterized.execute_text("""select pi;""")[0]
    assert results3.fetchall() == [
        (
            Decimal(
                "3.14",
            ),
        )
    ]


def test_unnest_render(snowflake_engine):
    test = """
auto zeta <- unnest([1,2,3,4]);


select zeta;"""
    results2 = snowflake_engine.generate_sql(test)[0]
    assert (
        """FROM
    table(flatten(ARRAY_CONSTRUCT(1, 2, 3, 4))) as unnest_wrapper ( unnest1, unnest2, unnest3, unnest4, "zeta")"""
        in results2
    ), results2


def test_unnest_render_source(snowflake_engine):

    text = """
key array_int list<int>;

datasource test (
array1:array_int,
)
grain (array_int)  
query '''
select ARRAY_CONSTRUCT(1,2,3,4) "array1",
union all
select ARRAY_CONSTRUCT(1,2,3,5)
''';

auto zeta <- unnest(array_int);


select zeta order by zeta asc;"""

    results2 = snowflake_engine.generate_sql(text)[0]
    assert (
        re.search(
            r'LEFT JOIN LATERAL flatten\([A-z"]+."array_int"\) as unnest_wrapper \( unnest1, unnest2, unnest3, unnest4, "zeta"\)',
            results2,
        )
        is not None
    ), results2
    # TODO: fakesnow support
    # check = snowflake_engine.execute_text(text)
    # assert check[0].fetchall() == [(1,), (1,), (2,), (2,), (3,), (3,), (4,), (5,)], check


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
        random,
    ;
        """
    )

    executor = Dialects.BIGQUERY.default_executor(
        environment=environment, rendering=Rendering(parameters=False)
    )

    executor.execute_query(queries[-1])


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
