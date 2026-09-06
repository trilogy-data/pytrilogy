from pathlib import Path

import duckdb
import pytest

from trilogy import Dialects, Environment
from trilogy.dialect.config import DuckDBConfig

SETUP = """
key i int;
property i.n string;
datasource raw (i:i, n:n) grain(i) address raw_t;
"""


def _executor(path: Path, working_path: Path):
    return Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=working_path),
        conf=DuckDBConfig(path=str(path)),
    )


def _tables(path: Path) -> set[str]:
    con = duckdb.connect(str(path), read_only=True)
    try:
        return {
            r[0]
            for r in con.execute("select table_name from duckdb_tables()").fetchall()
        }
    finally:
        con.close()


def _rows(path: Path, sql: str) -> list[tuple]:
    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "persist.duckdb"


def test_persist_survives_executor_close(db_path, tmp_path):
    exec = _executor(db_path, tmp_path)
    exec.execute_raw_sql(
        "CREATE TABLE raw_t AS SELECT 1 AS i, 'a' AS n UNION ALL SELECT 2, 'b'"
    )
    exec.execute_text(SETUP + "persist p into out_t from select i, n;")
    exec.close()

    assert {"raw_t", "out_t"} <= _tables(db_path)

    reader = _executor(db_path, tmp_path)
    assert reader.execute_raw_sql("select count(*) from out_t").fetchall() == [(2,)]
    reader.close()


def test_persist_result_readable_after_commit(db_path, tmp_path):
    exec = _executor(db_path, tmp_path)
    exec.execute_raw_sql("CREATE TABLE raw_t AS SELECT 1 AS i, 'a' AS n")
    results = exec.execute_text(SETUP + "persist p into out_t from select i, n;")
    assert [tuple(r) for r in results[-1].fetchall()] == [(1,)]
    exec.close()


def test_raw_sql_statement_survives_close(db_path, tmp_path):
    exec = _executor(db_path, tmp_path)
    exec.execute_text("raw_sql('''CREATE TABLE from_raw (i int)''');")
    exec.close()
    assert "from_raw" in _tables(db_path)


def test_create_statement_survives_close(db_path, tmp_path):
    exec = _executor(db_path, tmp_path)
    exec.execute_text(SETUP + "create datasources raw;")
    exec.close()
    assert "raw_t" in _tables(db_path)


def test_sql_file_survives_close(db_path, tmp_path):
    script = tmp_path / "setup.sql"
    script.write_text("CREATE TABLE from_file (i int)")
    exec = _executor(db_path, tmp_path)
    exec.execute_file(script)
    exec.close()
    assert "from_file" in _tables(db_path)


def test_explicit_transaction_still_rolls_back(db_path, tmp_path):
    exec = _executor(db_path, tmp_path)
    exec.connection.begin()
    exec.execute_raw_sql("CREATE TABLE scratch (i int)")
    exec.connection.rollback()
    exec.close()
    assert "scratch" not in _tables(db_path)


def test_reconnect_to_existing_db_still_persists(db_path, tmp_path):
    """A second executor against the same file must still commit its writes.

    Connect-time setup skips the guard-macro write when the macro is already
    defined, which is exactly the case on every connection after the first. That
    check is a read, and a read that leaves a transaction open makes every later
    write look caller-managed — so close() discards it while the caller is told
    the write succeeded.
    """
    first = _executor(db_path, tmp_path)
    first.execute_raw_sql("CREATE TABLE raw_t AS SELECT 1 AS i, 'a' AS n")
    first.close()

    second = _executor(db_path, tmp_path)
    assert not second.connection.in_transaction()
    second.execute_text(SETUP + "persist p into out_t from select i, n;")
    second.close()

    assert "out_t" in _tables(db_path)
    assert _rows(db_path, "select count(*) from out_t") == [(1,)]


def test_concurrent_executors_share_one_database(db_path, tmp_path):
    """Directory-wide probes build executors on a thread pool.

    Against one on-disk warehouse they all run the same connect-time setup DDL,
    and DuckDB aborts the losers of a catalog write race. In-memory databases
    never showed this: each executor owns a private catalog.
    """
    import concurrent.futures

    seed = _executor(db_path, tmp_path)
    seed.execute_raw_sql("CREATE TABLE raw_t AS SELECT 1 AS i, 'a' AS n")
    seed.close()

    def connect_and_read() -> int:
        exec = _executor(db_path, tmp_path)
        try:
            return exec.execute_raw_sql("select count(*) from raw_t").fetchall()[0][0]
        finally:
            exec.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        assert [
            f.result() for f in [pool.submit(connect_and_read) for _ in range(8)]
        ] == [1] * 8


def test_persist_commits_immediately(db_path, tmp_path):
    # duckdb locks the file while a writer is open, so durability mid-session is
    # asserted on transaction state rather than a second connection.
    exec = _executor(db_path, tmp_path)
    exec.execute_raw_sql("CREATE TABLE raw_t AS SELECT 1 AS i, 'a' AS n")
    exec.execute_text(SETUP + "persist p into out_t from select i, n;")
    assert not exec.connection.in_transaction()
    exec.close()


def test_failed_overwrite_keeps_old_rows_and_a_usable_connection(db_path, tmp_path):
    exec = _executor(db_path, tmp_path)
    exec.execute_write_sql("CREATE TABLE out_t AS SELECT 1 AS i, 'keep' AS n")
    exec.execute_write_sql(
        "CREATE VIEW raw_t AS SELECT 2 AS i, "
        "CASE WHEN i > 0 THEN error('boom') ELSE 'x' END AS n"
    )
    with pytest.raises(Exception, match="boom"):
        exec.execute_text(SETUP + "persist p into out_t from select i, n;")
    assert not exec.connection.in_transaction()
    assert exec.execute_raw_sql("select * from out_t").fetchall() == [(1, "keep")]
    exec.close()
    assert _rows(db_path, "select * from out_t") == [(1, "keep")]
