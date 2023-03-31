def test_basic_query(duckdb_engine, expected_results):
    results = duckdb_engine.execute_text("""select total_count;""")[0].fetchall()
    assert results[0].total_count == expected_results["total_count"]


def test_render_query(duckdb_engine, expected_results):
    results = duckdb_engine.generate_sql("""select total_count;""")[0]

    assert "total" in results
