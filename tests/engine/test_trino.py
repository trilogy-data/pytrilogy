def test_render_query(trino_engine):
    results = trino_engine.generate_sql("""select pi, greeting;""")[0]

    assert "3.14" in results
    assert ":pi" not in results
    assert ":greeting" in results
