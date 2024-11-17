def test_render_query(presto_engine):
    results = presto_engine.generate_sql("""select pi;""")[0]

    assert ":pi" in results


def test_numeric_query(presto_engine):
    results = presto_engine.generate_sql(
        """select cast(1.235 as NUMERIC(12,2))->decimal_name;"""
    )[0]

    assert "DECIMAL(12,2)" in results


def test_unnest_query(presto_engine):
    from trilogy.hooks.query_debugger import DebuggingHook

    presto_engine.hooks = [DebuggingHook()]
    results = presto_engine.generate_sql(
        """
auto numbers <- unnest([1,2,3,4]);  
select numbers;"""
    )[0]

    assert 'unnest(ARRAY[1, 2, 3, 4]) as unnest_wrapper ("numbers")' in results, results
