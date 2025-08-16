import re


def test_render_query(presto_engine):
    results = presto_engine.generate_sql("""select pi;""")[0]

    assert ":pi" in results


def test_numeric_query(presto_engine):
    results = presto_engine.generate_sql(
        """select cast(1.235 as NUMERIC(12,2))->decimal_name;"""
    )[0]

    assert "DECIMAL(12,2)" in results


def test_unnest_query(presto_engine):
    from trilogy.constants import CONFIG
    from trilogy.hooks.query_debugger import DebuggingHook

    presto_engine.hooks = [DebuggingHook()]
    current = CONFIG.rendering.parameters
    CONFIG.rendering.parameters = False
    results = presto_engine.generate_sql(
        """
auto numbers <- unnest([1,2,3,4]);  
select numbers;"""
    )[0]
    CONFIG.rendering.parameters = current
    assert 'unnest(ARRAY[1, 2, 3, 4]) as t("_unnest_alias")' in results, results


def test_unnest_query_from_table(presto_engine):
    from trilogy.constants import CONFIG
    from trilogy.hooks.query_debugger import DebuggingHook

    presto_engine.hooks = [DebuggingHook()]
    current = CONFIG.rendering.parameters
    CONFIG.rendering.parameters = False
    results = presto_engine.generate_sql(
        """
key x int;
property x.values array<int>;

datasource numbers
(
    x: x,
    values: values
)
grain (x)
query '''

select 1 as x, [1,2,3,4] as values
''';

SELECT 
    x,
    unnest(values) as numbers
;
"""
    )[0]
    CONFIG.rendering.parameters = current
    assert re.search(
        'CROSS JOIN unnest\("[A-z0-9\_]+"."values"\) as t\("_unnest_alias"\)', results
    ), results


def test_group_by_index(presto_engine):
    from trilogy.constants import CONFIG
    from trilogy.hooks.query_debugger import DebuggingHook

    presto_engine.hooks = [DebuggingHook()]
    current = CONFIG.rendering.parameters
    CONFIG.rendering.parameters = False
    results = presto_engine.generate_sql(
        """
key x int;
key y int;
property x.value int;

datasource numbers
(
    x,
    y,
    value
)
grain (x)
address tbl_fun;

select y, sum(value) as tot_value
;
"""
    )[0]
    CONFIG.rendering.parameters = current
    assert re.search("GROUP BY\s+1", results), results
