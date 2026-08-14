import time

import pytest

from trilogy import Dialects, Executor
from trilogy.core.exceptions import ConfigurationException, QueryTimeoutException
from trilogy.dialect.cancel import resolve_query_canceller

# A join whose build side is far larger than any timeout under test.
SLOW_SQL = "select count(*) from range(100000000000) a join range(100000) b on a.range = b.range"


def duckdb_executor(query_timeout: float | None = None) -> Executor:
    return Executor(
        dialect=Dialects.DUCK_DB,
        engine=Dialects.DUCK_DB.default_engine(),
        query_timeout=query_timeout,
    )


def test_no_timeout_by_default():
    exec = duckdb_executor()
    assert exec.query_timeout is None
    assert exec._cancel_query is None


def test_timeout_resolves_a_canceller():
    exec = duckdb_executor(query_timeout=5)
    assert exec._cancel_query is not None


def test_timeout_cancels_a_long_statement():
    exec = duckdb_executor(query_timeout=1)
    start = time.monotonic()
    with pytest.raises(QueryTimeoutException):
        exec.execute_raw_sql(SLOW_SQL)
    assert time.monotonic() - start < 20


def test_connection_survives_a_timeout():
    exec = duckdb_executor(query_timeout=1)
    with pytest.raises(QueryTimeoutException):
        exec.execute_raw_sql(SLOW_SQL)
    assert exec.execute_raw_sql("select 42").fetchall() == [(42,)]


def test_fast_statements_unaffected():
    exec = duckdb_executor(query_timeout=30)
    exec.execute_raw_sql("create table t as select 1 as x")
    assert exec.execute_raw_sql("select x from t").fetchall() == [(1,)]


def test_timeout_is_not_retried(monkeypatch):
    """A timeout is the caller's own verdict, so the retry policy never sees it —
    retrying would spend the whole budget again per attempt."""
    from trilogy.dialect.config import DuckDBConfig, RetryConfig, RetryPolicy

    conf = DuckDBConfig()
    conf.retry_config = RetryConfig(patterns={".*": RetryPolicy(max_attempts=3)})
    exec = Executor(
        dialect=Dialects.DUCK_DB,
        engine=Dialects.DUCK_DB.default_engine(conf=conf),
        config=conf,
        query_timeout=1,
    )
    start = time.monotonic()
    with pytest.raises(QueryTimeoutException):
        exec.execute_raw_sql(SLOW_SQL)
    assert time.monotonic() - start < 20


def test_uncancellable_driver_rejects_the_timeout(monkeypatch):
    """An opt-in bound that cannot be enforced must fail, not silently not apply."""
    monkeypatch.setattr(
        "trilogy.executor.resolve_query_canceller", lambda connection: None
    )
    with pytest.raises(ConfigurationException, match="cancel a running statement"):
        duckdb_executor(query_timeout=1)


def test_canceller_declines_a_non_sqlalchemy_connection():
    assert resolve_query_canceller(object()) is None  # type: ignore[arg-type]
