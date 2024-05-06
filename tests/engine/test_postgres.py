def test_render_query(postgres_engine):
    results = postgres_engine.generate_sql("""select pi;""")[0]

    assert "3.14" in results

    results2 = postgres_engine.generate_sql(
        """select date_part(current_datetime() , day)->current_day;"""
    )[0]

    assert "day" in results2
