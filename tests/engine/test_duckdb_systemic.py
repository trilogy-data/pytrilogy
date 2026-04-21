from trilogy.executor import Executor


def test_filters(duckdb_engine: Executor, expected_results):
    # generate inputs
    duckdb_engine.execute_text(""" key x int;
        key y int;
property x.a int;
property x.b int;


datasource x_y (
    x: x,
    y: y,
    a: a,
    b: b)
grain (x, y)
query '''
select 1 as x, 1 as y, 1 as a, 1 as b
union all
select 2 as x, 1 as y, 2 as a, 2 as b
union all
select 1 as x, 2 as y, 1 as a, 2 as b

''';
""")
    # test filters
    test_two = """

auto x_one <-  x ? x = 1;
auto x_two_alt <- (x +1) ? x+ 1 =2;
auto x_two <- (x+1) ? x = 1;

where x_one is not null
select
    x_one, 
    b,
    x_two_alt,
    x_two;
        """

    results = duckdb_engine.execute_text(test_two)[-1].fetchall()
    assert len(results) == 2
    assert results[0].x_one == 1
    assert results[0].x_two_alt == 2
    assert results[0].x_two == 2
