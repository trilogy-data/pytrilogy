def test_render_query(snowflake_engine):
    results = snowflake_engine.generate_sql("""select pi;""")[0]

    assert ":pi" in results

    results2 = snowflake_engine.generate_sql(
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

    snowflake_engine.use_variables = False
    results2 = snowflake_engine.generate_sql(
        """
auto zeta <- unnest([1,2,3,4]);


select zeta;"""
    )[0]
    assert "date_add(current_datetime(),day" in results2, results2
