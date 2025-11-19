from pathlib import Path

from pytest import raises

from trilogy import Dialects, Executor, parse
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.environment import Environment
from trilogy.hooks import DebuggingHook

working_path = Path(__file__).parent


def test_adhoc02_error():
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc02.preql") as f:
        text = f.read()
        with raises(InvalidSyntaxException):
            env, queries = parse(text, env)


def test_adhoc03():
    env = Environment(working_path=working_path)
    DebuggingHook()
    with open(working_path / "adhoc03.preql") as f:
        text = f.read()

    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])

    results = engine.execute_query(text).fetchall()
    assert results[0].order_count == 289
    assert results[0].order_count_two == 289


def test_adhoc04():
    env = Environment(working_path=working_path)
    DebuggingHook()
    with open(working_path / "adhoc04.preql") as f:
        text = f.read()

    query = Dialects.DUCK_DB.default_executor(environment=env).generate_sql(text)[0]
    # engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])

    # results = engine.execute_query(text).fetchall()
    # really, this is checking that there is no extra inner join with the filtered quantity
    assert len(query) < 1400, query


def test_adhoc05():
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc05.preql") as f:
        text = f.read()

    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])

    results = engine.execute_query(text).fetchall()
    assert results[0].total_orders == 15000, results[0].total_orders


def test_adhoc06():
    env = Environment(working_path=working_path)
    DebuggingHook()
    with open(working_path / "adhoc06.preql") as f:
        text = f.read()
    with open(working_path / "cache_warm.preql") as f:
        cache_sql = f.read()
    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    # warm cache
    engine.execute_query(cache_sql)
    assert 'local._total_customers' in env.concepts, env.concepts.keys()
    assert 'local.total_customers' not in env.concepts, env.concepts.keys()
    engine.execute_query('import cache;')
    cache_table = env.datasources['dashboard_agg_1']
    for x in cache_table.columns:
        assert x.alias.startswith('_')

    query = engine.generate_sql(text)[0]
    results = engine.execute_query(text).fetchall()
    assert 'dashboard_agg_1' in query, query
    assert results[0].total_orders == 15000, results[0].total_orders
