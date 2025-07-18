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
