def test_basic_query(duckdb_engine, expected_results):
    results = duckdb_engine.execute_text("""select total_count;""")[0].fetchall()
    assert results[0].local_total_count == expected_results["total_count"]
