import re


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


def test_unnest_render(snowflake_engine):

    results2 = snowflake_engine.generate_sql(
        """
auto zeta <- unnest([1,2,3,4]);


select zeta;"""
    )[0]
    assert (
        """FROM
    table(flatten(ARRAY_CONSTRUCT(1, 2, 3, 4))) as unnest_wrapper ( unnest1, unnest2, unnest3, unnest4, "zeta")"""
        in results2
    ), results2


def test_unnest_render_source(snowflake_engine):

    results2 = snowflake_engine.generate_sql(
        """
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


select zeta;"""
    )[0]
    assert (
        re.search(
            r'LEFT JOIN LATERAL flatten\(\w+\."array_int"\) as unnest_wrapper \( unnest1, unnest2, unnest3, unnest4, "zeta"\)',
            results2,
        )
        is not None
    ), results2
