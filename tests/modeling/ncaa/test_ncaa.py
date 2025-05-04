from pathlib import Path

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from pytest import raises
from trilogy.core.exceptions import UnresolvableQueryException

working_path = Path(__file__).parent


def test_adhoc01():
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc01.preql") as f:
        text = f.read()

    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])

    engine.generate_sql(text)[0]

def test_adhoc02():
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc02.preql") as f:
        text = f.read()

    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    with raises(UnresolvableQueryException):
        engine.generate_sql(text)[0]