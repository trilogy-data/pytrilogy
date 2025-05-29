from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.core.enums import Derivation, Purpose


def test_user_function_def():
    x = Dialects.DUCK_DB.default_executor()

    results = x.execute_query(
        """
def percent_ratio(a, b, digits=3) -> round(a::float / b * 100, digits);

                 
select @percent_ratio(10, 100) as ratio;
                 
                 """
    )

    assert results.fetchall()[0].ratio == 10.0


def test_user_function_aggregate():
    x = Dialects.DUCK_DB.default_executor()

    results = x.execute_query(
        """
key x int;
property x.price float;

datasource raw_data (
x: x,
price: price
)
grain (x)
query '''
select 1 as x, 2.0 as price
union all
select 2 as x, 3.0 as price
union all
select 10 as x, 5.0 as price
''';

def sum_times(a)-> a * sum(x);

                 
select @sum_times(10) as total;
                 
                 """
    )

    assert results.fetchall()[0].total == 130


def test_user_function_nested():
    x = Dialects.DUCK_DB.default_executor()

    results = x.execute_query(
        """
key x int;
property x.price float;

datasource raw_data (
x: x,
price: price
)
grain (x)
query '''
select 1 as x, 2.0 as price
union all
select 2 as x, 3.0 as price
union all
select 10 as x, 5.0 as price
''';

def sum_times(a)-> a * sum(x + price);

                 
select @sum_times(10) as total;
                 
                 """
    )

    assert results.fetchall()[0].total == 230


def test_user_function_nested_rowset():
    x = Dialects.DUCK_DB.default_executor()

    results = x.execute_query(
        """
key x int;
property x.price float;

datasource raw_data (
x: x,
price: price
)
grain (x)
query '''
select 1 as x, 2.0 as price
union all
select 2 as x, 3.0 as price
union all
select 10 as x, 5.0 as price
''';

def sum_times(a)-> a * sum(x + price);

with rowset as  
select @sum_times(10) as total;

select rowset.total;
                 
                 """
    )

    assert results.fetchall()[0].rowset_total == 230


def test_user_function_case():
    x = Dialects.DUCK_DB.default_executor()

    results = x.execute_query(
        """
key x int;
key y int;
property x.price float;

datasource raw_data (
x: x,
price: price
)
grain (x)
query '''
select 1 as x, 2.0 as price
union all
select 2 as x, 3.0 as price
union all
select 10 as x, 5.0 as price
''';

datasource join_x_y (
x: x,
y: y)
grain (x, y)
query '''
select 1 as x, 1 as y
union all
select 2 as x, 1 as y
union all
select 10 as x, 2 as y
''';

def weekday_sales(weekday) ->  
    SUM(CASE WHEN 10 = weekday THEN x ELSE 0 END) + 
    SUM(CASE WHEN 10 = weekday THEN price ELSE 0.0 END)
;


select 
    y,
    @weekday_sales(10) -> test
order by y asc;
                
"""
    )
    results = results.fetchall()
    assert results[0].test == 8
    assert results[1].test == 15


def test_parsing():
    x = Dialects.DUCK_DB.default_executor()
    x.execute_query(
        """
key x int;
property x.price float;

datasource raw_data (
x: x,
price: price
)
grain (x)
query '''
select 1 as x, 2.0 as price
union all
select 2 as x, 3.0 as price
union all
select 10 as x, 5.0 as price
''';


auto test <-SUM(CASE WHEN 10 = weekday THEN x ELSE 0 END) + 
    SUM(CASE WHEN 10 = weekday THEN price ELSE 0.0 END);



                 
"""
    )
    test = x.environment.concepts["test"]
    assert test.keys == set()
    assert test.purpose == Purpose.METRIC
    assert test.derivation == Derivation.BASIC


def test_user_function_import():
    env = Environment(working_path=Path(__file__).parent)
    x = Dialects.DUCK_DB.default_executor(environment=env)
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    results = x.execute_query(
        """
import test_env_functions as test_env_functions;

key x int;

merge test_env_functions.quad_test into x;

select 
    x as quad_test,
    @test_env_functions.quadratic(2, 3, 4) as quad_two;

"""
    )
    results = results.fetchall()
    assert results[0].quad_test == 16.414213562373096


def test_user_function_nesting():
    x = Dialects.DUCK_DB.default_executor()

    results = x.execute_query(
        """
key x int;
key y int;
property x.price float;

datasource raw_data (
x: x,
price: price
)
grain (x)
query '''
select 1 as x, 2.0 as price
union all
select 2 as x, 3.0 as price
union all
select 10 as x, 5.0 as price
''';

datasource join_x_y (
x: x,
y: y)
grain (x, y)
query '''
select 1 as x, 1 as y
union all
select 2 as x, 1 as y
union all
select 10 as x, 2 as y
''';

def weekday_sales(weekday) ->  
    SUM(CASE WHEN 10 = weekday THEN x ELSE 0 END) + 
    SUM(CASE WHEN 10 = weekday THEN price ELSE 0.0 END)
;

def plus_two(a) -> a + 2;

# auto random_no_f <- SUM(CASE WHEN 10 = weekday THEN x ELSE 0 END) + SUM(CASE WHEN 10 = weekday THEN price ELSE 0.0 END) +2;
auto random_one_f <- @weekday_sales(10) +2;
auto random <- @plus_two(@weekday_sales(10));
                
"""
    )
    # assert x.environment.concepts['random_no_f'].purpose == Purpose.METRIC, x.environment.concepts['random']
    assert (
        x.environment.concepts["random_one_f"].purpose == Purpose.METRIC
    ), x.environment.concepts["random"]
    assert (
        x.environment.concepts["random"].purpose == Purpose.METRIC
    ), x.environment.concepts["random"]

    results = x.execute_query(
        """select 
        y,
        @plus_two(@weekday_sales(10)) -> test2
    order by y asc;"""
    )
    results = results.fetchall()
    assert results[0].test2 == 10
    assert results[1].test2 == 17
