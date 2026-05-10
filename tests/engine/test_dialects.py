from typing import Any, Generator, List, Optional

import pytest
from pytest import raises

from trilogy import Dialects, Executor
from trilogy.constants import Rendering
from trilogy.core.models.environment import Environment
from trilogy.engine import EngineConnection, ExecutionEngine, ResultProtocol


def test_error_not_provided():
    for val in [Dialects.PRESTO, Dialects.TRINO, Dialects.SNOWFLAKE]:
        with raises(ValueError, match="Config must be provided"):
            val.default_engine()


class _MockResult(ResultProtocol):
    def fetchall(self) -> List[Any]:
        return []

    def keys(self) -> List[str]:
        return []

    def fetchone(self) -> Optional[Any]:
        return None

    def fetchmany(self, size: int) -> List[Any]:
        return []

    def __iter__(self) -> Generator[Any, None, None]:
        return iter([])


class _MockConnection(EngineConnection):
    def execute(self, statement, parameters=None) -> ResultProtocol:
        return _MockResult()


class _MockEngine(ExecutionEngine):
    def connect(self) -> EngineConnection:
        return _MockConnection()


GROUPING_DIALECTS = [
    Dialects.POSTGRES,
    Dialects.BIGQUERY,
    Dialects.SNOWFLAKE,
    Dialects.PRESTO,
    Dialects.TRINO,
    Dialects.CLICKHOUSE,
    Dialects.SQL_SERVER,
]

_SCHEMA = """
key a int;
key b int;
property <a, b>.x int;
datasource test_data (
    a: a,
    b: b,
    x: x
)
grain (a, b)
address test_data;
"""


def _make_executor(dialect: Dialects) -> Executor:
    return Executor(
        dialect=dialect,
        engine=_MockEngine(),
        environment=Environment(),
        rendering=Rendering(parameters=False),
    )


@pytest.mark.parametrize("dialect", GROUPING_DIALECTS)
def test_aggregate_grouping_modes_render(dialect):
    executor = _make_executor(dialect)
    executor.parse_text(_SCHEMA)

    rollup_sql = executor.generate_sql("select a, b, sum(x) by rollup a, b as sx;")[-1]
    cube_sql = executor.generate_sql("select a, b, sum(x) by cube a, b as sx;")[-1]
    grouping_sets_sql = executor.generate_sql(
        "select a, b, sum(x) by grouping sets (a, b), (a), () as sx;"
    )[-1]

    assert "ROLLUP" in rollup_sql.upper(), rollup_sql
    assert "CUBE" in cube_sql.upper(), cube_sql
    assert "GROUPING SETS" in grouping_sets_sql.upper(), grouping_sets_sql


def test_aggregate_grouping_modes_rejected_on_sqlite():
    executor = _make_executor(Dialects.SQLITE)
    executor.parse_text(_SCHEMA)

    with raises(NotImplementedError, match="aggregate grouping mode"):
        executor.generate_sql("select a, b, sum(x) by rollup a, b as sx;")
