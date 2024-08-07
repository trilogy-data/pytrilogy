def test_render_query(presto_engine):
    results = presto_engine.generate_sql("""select pi;""")[0]

    assert "3.14" in results


def test_numeric_query(presto_engine):
    results = presto_engine.generate_sql(
        """select cast(1.235 as NUMERIC(12,2))->decimal_name;"""
    )[0]

    assert "DECIMAL(12,2)" in results
