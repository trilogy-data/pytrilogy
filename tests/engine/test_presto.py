

def test_render_query(presto_engine):
    results = presto_engine.generate_sql("""select pi;""")[0]

    assert "3.14" in results
