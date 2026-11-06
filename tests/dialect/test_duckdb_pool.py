import gc

import duckdb
import pytest

from trilogy import Dialects
from trilogy.constants import CONFIG
from trilogy.dialect import duckdb_pool
from trilogy.dialect.config import DuckDBConfig


@pytest.fixture(autouse=True)
def _empty_pool():
    duckdb_pool._idle.clear()
    yield
    duckdb_pool._idle.clear()


def _instance(connection: duckdb_pool.PooledConnection) -> object:
    return connection._release.peek()[2][0]  # type: ignore[index]


def _kept(*statements: str) -> bool:
    connection = duckdb_pool.connect()
    instance = _instance(connection)
    for statement in statements:
        connection.execute(statement)
    connection.close()
    return instance in duckdb_pool._idle


def test_each_connection_gets_an_empty_memory_catalog():
    assert _kept("CREATE TABLE t AS SELECT 1 AS x", "CREATE MACRO m(a) AS a")
    connection = duckdb_pool.connect()
    assert connection.execute("SELECT current_database()").fetchone() == ("memory",)
    assert connection.execute("SELECT count(*) FROM duckdb_tables()").fetchone() == (0,)
    connection.execute('CREATE TABLE "memory"."main"."t" AS SELECT 2 AS x')
    assert connection.execute('SELECT x FROM "memory"."t"').fetchall() == [(2,)]
    connection.close()


def test_connection_scoped_state_does_not_retire_the_instance():
    assert _kept(
        "CREATE TEMP TABLE tt AS SELECT 1",
        "SET search_path = 'main'",
        "SET VARIABLE scoped = 1",
    )


@pytest.mark.parametrize(
    "statement",
    [
        "SET threads = 1",
        "ATTACH ':memory:' AS other",
        "CREATE SECRET leaked (TYPE s3, KEY_ID 'k', SECRET 's')",
    ],
)
def test_instance_state_retires_the_instance(statement: str):
    assert not _kept(statement)


def test_instance_level_python_api_retires_the_instance():
    connection = duckdb_pool.connect()
    instance = _instance(connection)
    connection.create_function("plus_one", lambda a: a + 1, ["INTEGER"], "INTEGER")
    connection.close()
    assert instance not in duckdb_pool._idle


def test_a_dropped_connection_is_released():
    connection = duckdb_pool.connect()
    instance = _instance(connection)
    connection.execute("CREATE TABLE t AS SELECT 1")
    del connection
    gc.collect()
    assert instance in duckdb_pool._idle


def test_executors_in_turn_do_not_see_each_others_tables():
    first = Dialects.DUCK_DB.default_executor()
    first.execute_raw_sql("CREATE TABLE leftover AS SELECT 1 AS x")
    first.close()
    second = Dialects.DUCK_DB.default_executor()
    tables = second.execute_raw_sql("SELECT table_name FROM duckdb_tables()").fetchall()
    assert tables == []
    second.close()


def test_configs_with_instance_state_open_their_own(monkeypatch):
    assert DuckDBConfig(enable_gcs=True).create_engine_args() == {}
    assert DuckDBConfig(path="x.db").create_engine_args() == {}
    monkeypatch.setattr(CONFIG, "reuse_duckdb_instances", False)
    assert DuckDBConfig().create_engine_args() == {}


def test_an_instance_that_cannot_reset_is_closed():
    instance = duckdb_pool._Instance()
    assert not instance.reset()
    cursor = instance.open()
    instance.db.close()
    duckdb_pool._release(instance, cursor, [False])
    assert instance not in duckdb_pool._idle


def test_opening_an_instance_twice_fails_cleanly():
    instance = duckdb_pool._Instance()
    instance.open()
    with pytest.raises(duckdb.Error):
        instance.open()


def test_a_forked_child_abandons_idle_instances():
    assert _kept()
    idle = list(duckdb_pool._idle)
    duckdb_pool._abandon_idle()
    assert duckdb_pool._idle == []
    assert all(instance in duckdb_pool._abandoned for instance in idle)
