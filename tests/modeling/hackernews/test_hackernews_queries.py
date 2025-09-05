from pathlib import Path

from pytest import raises

from trilogy import Dialects, Executor
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.environment import Environment
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.hooks import DebuggingHook

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
    assert "WITH RECURSIVE" in generated, generated


def test_adhoc03():
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc03.preql") as f:
        text = f.read()
    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])

    statement = engine.parse_text(text)[-1]
    generated = BigqueryDialect().compile_statement(statement)
    # TODO: better test
    assert "WITH RECURSIVE" in generated, generated


def test_adhoc04():
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc04.preql") as f:
        text = f.read()
    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    with raises(InvalidSyntaxException):
        engine.parse_text(text)[-1]


def test_adhoc05():
    env = Environment(working_path=working_path)
    DebuggingHook()
    with open(working_path / "adhoc05.preql") as f:
        text = f.read()
    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    engine.parse_text(text)[-1]


def test_adhoc06():
    env = Environment(working_path=working_path)
    DebuggingHook()
    with open(working_path / "adhoc06.preql") as f:
        text = f.read()
    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    engine.parse_text(text)[-1]


def test_adhoc07():
    env = Environment(working_path=working_path)
    DebuggingHook()
    with open(working_path / "adhoc07.preql") as f:
        text = f.read()
    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    statement = engine.parse_text(text)[-1]
    generated = BigqueryDialect().compile_statement(statement)
    # TODO: better test
    assert (
        """    `questionable`.`github_language` as `github_language`,
    rank() over (order by `questionable`.`_virt_agg_count_7657693770587142` desc ) as `popularity_rank`"""
        in generated
    ), generated


def test_adhoc08():
    env = Environment(working_path=working_path)
    DebuggingHook()
    with open(working_path / "adhoc08.preql") as f:
        text = f.read()
    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    statement = engine.parse_text(text)[-1]
    BigqueryDialect().compile_statement(statement)
