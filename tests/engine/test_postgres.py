def test_render_query(postgres_engine):
    results = postgres_engine.generate_sql("""select pi;""")[0]

    assert ":pi" in results

    results2 = postgres_engine.generate_sql(
        """
        const today <- date_trunc(current_datetime() , day);
        const ten_days_from_now <- date_add(current_datetime() , day, 10);
        const ten_day_diff <- date_diff(today, ten_days_from_now, day);
        select 
            today,
            ten_days_from_now,
            ten_day_diff;"""
    )[0]
    assert "(current_datetime() + INTERVAL '10 day')" in results2, results2
    assert "date_part('day', " in results2
