from datetime import datetime
def test_basic_query(duckdb_engine, expected_results):
    results = duckdb_engine.execute_text("""select total_count;""")[0].fetchall()
    assert results[0].total_count == expected_results["total_count"]


def test_concept_derivation(duckdb_engine):

    test_datetime = datetime(hour = 12, day = 1, month =2, year = 2022, second =34)

    duckdb_engine.execute_text(f''' key test <- cast('{test_datetime.isoformat()}' as datetime);
    ''')
    for  property, check in [['hour', test_datetime.hour], 
                             ['second', test_datetime.second], 
                             ['minute', test_datetime.minute],
                             ['year', test_datetime.year],
                             ['month', test_datetime.month],
                             ]:
        #{test_datetime.isoformat()}
        results = duckdb_engine.execute_text(f""" 


        select test.{property};
        
        """)[0].fetchall()
        assert results[0][0] == check

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
