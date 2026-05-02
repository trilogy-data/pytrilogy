"""Tests for the in-process chdb backend.

Skipped on platforms where chdb has no wheel (Windows). Linux/macOS CI runs
these and exercises both the chdb adapter and the ClickhouseDialect FUNCTION_MAP.
"""

from typing import Any, Generator

import pytest
from sqlalchemy import text as sa_text

from trilogy import Dialects, Executor, parse
from trilogy.constants import Rendering
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import ClickhouseConfig

from ._clickhouse_cases import (
    AGG_CASES,
    SMOKE_CASES,
    run_agg_case,
    run_smoke_case,
)

# Skip the whole module on platforms with no chdb wheel.
pytest.importorskip("chdb")


@pytest.fixture(scope="module")
def chdb_executor() -> Generator[Executor, None, None]:
    executor = Dialects.CLICKHOUSE.default_executor(
        environment=Environment(),
        conf=ClickhouseConfig(mode="chdb"),
        rendering=Rendering(parameters=False),
    )
    try:
        yield executor
    finally:
        executor.close()


def test_select_one(chdb_executor: Executor):
    rows = chdb_executor.execute_raw_sql("select 1 as answer").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 1
    assert rows[0].answer == 1


def test_today(chdb_executor: Executor):
    rows = chdb_executor.execute_raw_sql("select today() as d").fetchall()
    assert rows[0][0] is not None


def test_trilogy_const(chdb_executor: Executor):
    parse("const chdb_answer <- 42;", environment=chdb_executor.environment)
    rows = chdb_executor.execute_text("select chdb_answer;")[0].fetchall()
    assert rows[0].chdb_answer == 42


def test_result_iteration(chdb_executor: Executor):
    """Exercise fetchone, fetchmany, __iter__, keys on ChdbResult."""
    sql = "select arrayJoin([10, 20, 30, 40, 50]) as v"
    r = chdb_executor.execute_raw_sql(sql)
    assert r.keys() == ["v"]
    first = r.fetchone()
    assert first.v == 10
    chunk = r.fetchmany(2)
    assert [row.v for row in chunk] == [20, 30]
    rest = list(r)  # __iter__
    assert [row.v for row in rest] == [40, 50]
    assert r.fetchone() is None


def test_result_empty(chdb_executor: Executor):
    """DDL has no result rows. ChdbResult should still be a valid empty result."""
    r = chdb_executor.execute_raw_sql(
        "CREATE TABLE IF NOT EXISTS t_empty (x Int) ENGINE = Memory"
    )
    assert r.fetchall() == []
    assert r.fetchone() is None
    assert r.keys() == []


def test_connection_transaction_noops(chdb_executor: Executor):
    """commit/begin/rollback are no-ops on chdb but must not raise."""
    conn = chdb_executor.connection
    assert conn.commit() is None
    assert conn.begin() is None
    assert conn.rollback() is None


def test_executor_close_is_idempotent(chdb_executor: Executor):
    """Calling dispose twice should not raise."""
    chdb_executor.engine.dispose()
    chdb_executor.engine.dispose()


def test_parameter_binding_inlines_literals():
    """_statement_to_sql with parameters compiles literal_binds via SQLAlchemy."""
    from trilogy.dialect.clickhouse_chdb import _statement_to_sql

    out = _statement_to_sql(sa_text("select :x as x"), {"x": 42})
    assert "42" in out
    assert ":x" not in out


def test_setup_is_noop(chdb_executor: Executor):
    """ChdbEngine.setup is a no-op for the trilogy CH path."""
    assert chdb_executor.engine.setup(chdb_executor.environment, None) is None


@pytest.mark.parametrize(
    "case_id, expr, expected",
    SMOKE_CASES,
    ids=[c[0] for c in SMOKE_CASES],
)
def test_function_smoke(
    chdb_executor: Executor,
    case_id: str,
    expr: str,
    expected: Any,
):
    run_smoke_case(chdb_executor, case_id, expr, expected)


@pytest.mark.parametrize(
    "case_id, body, select_name, expected",
    AGG_CASES,
    ids=[c[0] for c in AGG_CASES],
)
def test_aggregate_smoke(
    chdb_executor: Executor,
    case_id: str,
    body: str,
    select_name: str,
    expected: Any,
):
    run_agg_case(chdb_executor, case_id, body, select_name, expected)
