from trilogy.parser import parse_text
from trilogy import Dialects, Environment
from trilogy.hooks.query_debugger import DebuggingHook

def test_basic():
    env = Environment()

    x = Dialects.DUCK_DB.default_executor(environment=env)

    results = x.execute_query('''select 1 -> test;''').fetchall()

    assert results[0].test == 1

def test_with_calc():
    env = Environment()

    x = Dialects.DUCK_DB.default_executor(environment=env)

    results = x.execute_query('''select 1+3 -> test;''').fetchall()

    assert results[0].test == 4

def test_from_constant_concept():
    env = Environment()

    x = Dialects.DUCK_DB.default_executor(environment=env)

    results = x.execute_query('''const x <-4;
                              
                              
    select x+1 -> test;''').fetchall()

    assert results[0].test == 5

def test_from_datasource_concept():
    env = Environment()
    dh = DebuggingHook()

    x = Dialects.DUCK_DB.default_executor(environment=env, hooks=[dh])

    results = x.execute_query("""key x int;
                              
    datasource x_source (
        x:x)
    grain(x)
    query '''
    select 1 as x union all select 2 as x
    ''';                        
                              
    select x
    order by x asc;""").fetchall()

    assert results[0].x == 1


    
def test_from_datasource_join():
    env = Environment()
    dh = DebuggingHook()

    x = Dialects.DUCK_DB.default_executor(environment=env, hooks=[dh])

    results = x.execute_query("""key x int;
                              
                    property x.name string;
                              

    datasource x_source (
        x:x)
    grain(x)
    query '''
    select 1 as x union all select 2 as x
    ''';                        
    datasource x_name (
                              x:x,
                              name:name)
    grain (x)
    query '''
    select 1 as 'fun', 2 as 'dice'
                              ''';

    select name
    order by name asc;""").fetchall()

    assert results[0].name == 'dice'



    
def test_from_datasource_join():
    env = Environment()
    dh = DebuggingHook()

    x = Dialects.DUCK_DB.default_executor(environment=env, hooks=[dh])

    results = x.execute_query("""key x int;
                              
                    property x.name string;
                              

    datasource x_source (
        x:x)
    grain(x)
    query '''
    select 1 as x union all select 2 as x
    ''';                        
    datasource x_name (
                              x:x,
                              name:name)
    grain (x)
    query '''
    select 1 as  x, 'fun' as name union all select 2, 'dice'
                              ''';

    select name
    order by name asc;""").fetchall()

    assert results[0].name == 'dice'