def test_render_query(trino_engine):
    results = trino_engine.generate_sql("""select pi, greeting, answer;""")[0]

    # float / string parameterised; int inlined
    assert ":pi" in results
    assert ":greeting" in results
    assert "42 as" in results
    assert ":answer" not in results
