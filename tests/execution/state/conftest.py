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


@fixture(scope="function")
def expected_results():
    yield {}
