def test_render_query(trino_engine):
    results = trino_engine.generate_sql("""select pi;""")[0]

    assert ":pi" in results
