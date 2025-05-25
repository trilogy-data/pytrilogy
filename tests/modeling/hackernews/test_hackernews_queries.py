from pathlib import Path

from pytest import raises

from trilogy import Dialects, Executor, parse
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.environment import Environment
from trilogy.hooks import DebuggingHook
from trilogy.dialect.bigquery import BigqueryDialect

working_path = Path(__file__).parent


def test_adhoc01():
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc01.preql") as f:
        text = f.read()
    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])

    statement = engine.parse_text(text)[-1]
    generated = BigqueryDialect().compile_statement(statement)
    assert "WITH RECURSIVE" in generated


def test_adhoc02():
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc02.preql") as f:
        text = f.read()
    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])

    statement = engine.parse_text(text)[-1]
    generated = BigqueryDialect().compile_statement(statement)
    assert "WITH RECURSIVE" in generated


def test_adhoc03():
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc03.preql") as f:
        text = f.read()
    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])

    statement = engine.parse_text(text)[-1]
    generated = BigqueryDialect().compile_statement(statement)
    #TODO: better test
    assert "WITH RECURSIVE" in generated
