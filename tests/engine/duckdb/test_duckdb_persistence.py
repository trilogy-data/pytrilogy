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


def test_persist_commits_immediately(db_path, tmp_path):
    # duckdb locks the file while a writer is open, so durability mid-session is
    # asserted on transaction state rather than a second connection.
    exec = _executor(db_path, tmp_path)
    exec.execute_raw_sql("CREATE TABLE raw_t AS SELECT 1 AS i, 'a' AS n")
    exec.execute_text(SETUP + "persist p into out_t from select i, n;")
    assert not exec.connection.in_transaction()
    exec.close()
