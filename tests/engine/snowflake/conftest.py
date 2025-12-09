"""Snowflake test fixtures."""

from pytest import fixture

from trilogy import Dialects, parse
from trilogy.constants import Rendering
from trilogy.dialect.config import SnowflakeConfig


@fixture(scope="session")
def fakesnow_happening():
    from unittest import mock

    import fakesnow
    import snowflake.connector

    # Check if fakesnow is already patched to avoid double-patching
    already_patched = isinstance(snowflake.connector.connect, mock.MagicMock)

    if already_patched:
        # Already patched, just yield without re-patching
        yield
    else:
        # Not patched yet, apply the patch
        with fakesnow.patch():
            yield


@fixture(scope="session")
def snowflake_model(environment):
    text = """
const pi <-3.14;
"""
    environment, statements = parse(text, environment=environment)
    yield environment


@fixture(scope="session")
def snowflake_engine(snowflake_model, fakesnow_happening):
    executor = Dialects.SNOWFLAKE.default_executor(
        environment=snowflake_model,
        conf=SnowflakeConfig(
            account="account",
            username="user",
            password="password",
            database="test",
            schema="public",
        ),
        rendering=Rendering(parameters=False),
    )
    yield executor
