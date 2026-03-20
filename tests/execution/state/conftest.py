from typing import Generator

from pytest import fixture
from sqlalchemy import create_engine

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment


@fixture(scope="session")
def duckdb_engine() -> Generator[Executor, None, None]:
    engine = create_engine("duckdb:///:memory:", future=True)

    executor = Executor(
        dialect=Dialects.DUCK_DB,
        engine=engine,
        environment=Environment(),
    )
    yield executor


@fixture(scope="session")
def snowflake_engine():
    from unittest import mock

    import fakesnow
    import snowflake.connector

    from trilogy.constants import Rendering
    from trilogy.dialect.config import SnowflakeConfig

    already_patched = isinstance(snowflake.connector.connect, mock.MagicMock)
    if already_patched:
        executor = Dialects.SNOWFLAKE.default_executor(
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
    else:
        with fakesnow.patch():
            executor = Dialects.SNOWFLAKE.default_executor(
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


@fixture(scope="function")
def expected_results():
    yield {}
