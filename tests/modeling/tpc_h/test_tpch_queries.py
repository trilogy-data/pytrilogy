from pathlib import Path

from pytest import raises

from trilogy import parse
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.environment import Environment

working_path = Path(__file__).parent


def test_adhoc02_error():
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc02.preql") as f:
        text = f.read()
        with raises(InvalidSyntaxException):
            env, queries = parse(text, env)
