def test_basic_query(duckdb_engine, expected_results):
    results = duckdb_engine.execute_text("""select total_count;""")[0].fetchall()
    assert results[0].total_count == expected_results["total_count"]


def test_render_query(duckdb_engine, expected_results):
    results = duckdb_engine.generate_sql("""select total_count;""")[0]

    assert "total" in results


def test_aggregate_at_grain(duckdb_engine, expected_results):
    results = duckdb_engine.execute_text("""select avg_count_per_product;""")[
        0
    ].fetchall()
    assert results[0].avg_count_per_product == expected_results["avg_count_per_product"]


def test_constants(duckdb_engine, expected_results):
    results = duckdb_engine.execute_text(
        """const usd_conversion <- 2;
    
    select total_count * usd_conversion -> converted_total_count;
    """
    )[0].fetchall()
    assert results[0].converted_total_count == expected_results["converted_total_count"]
