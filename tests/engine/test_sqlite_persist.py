import sqlite3
from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.core.enums import CreateMode
from trilogy.core.statements.execute import (
    ProcessedQueryPersist,
)
from trilogy.dialect.config import SQLiteConfig
from trilogy.parser import parse_text

SETUP = """
key i int;
property i.n string;
datasource raw (i:i, n:n) grain(i) address raw_t;
"""


def _executor(path: Path, working_path: Path):
    return Dialects.SQLITE.default_executor(
        environment=Environment(working_path=working_path),
        conf=SQLiteConfig(path=str(path)),
    )


def _persist(dialect: Dialects) -> ProcessedQueryPersist:
    env = Environment()
    generator = dialect.default_renderer()
    _, parsed = parse_text(SETUP + "persist p into out_t from select i, n;", env)
    statements = generator.generate_queries(env, parsed[-1:])
    query = statements[-1]
    assert isinstance(query, ProcessedQueryPersist)
    return query


def test_sqlite_persist_round_trip(tmp_path):
    db = tmp_path / "s.db"
    exec = _executor(db, tmp_path)
    exec.execute_raw_sql("CREATE TABLE raw_t (i int, n text)")
    exec.execute_raw_sql("INSERT INTO raw_t VALUES (1,'a'),(2,'b')")
    exec.execute_text(SETUP + "persist p into out_t from select i, n;")
    exec.close()

    con = sqlite3.connect(str(db))
    try:
        assert con.execute("select * from out_t order by i").fetchall() == [
            (1, "a"),
            (2, "b"),
        ]
    finally:
        con.close()


def test_sqlite_persist_overwrites_existing(tmp_path):
    db = tmp_path / "s.db"
    exec = _executor(db, tmp_path)
    exec.execute_raw_sql("CREATE TABLE raw_t (i int, n text)")
    exec.execute_raw_sql("INSERT INTO raw_t VALUES (1,'a')")
    exec.execute_text(SETUP + "persist p into out_t from select i, n;")
    exec.execute_raw_sql("INSERT INTO raw_t VALUES (2,'b')")
    exec.execute_text(SETUP + "persist p into out_t from select i, n;")
    exec.close()

    con = sqlite3.connect(str(db))
    try:
        assert con.execute("select count(*) from out_t").fetchall() == [(2,)]
    finally:
        con.close()


def test_sqlite_create_statement_round_trip(tmp_path):
    db = tmp_path / "s.db"
    exec = _executor(db, tmp_path)
    exec.execute_text(SETUP + "create datasources raw;")
    exec.close()

    con = sqlite3.connect(str(db))
    try:
        assert con.execute(
            "select name from sqlite_master where type='table'"
        ).fetchall() == [("raw_t",)]
    finally:
        con.close()


def test_compile_statements_splits_sqlite_persist():
    generator = Dialects.SQLITE.default_renderer()
    statements = generator.compile_statements(_persist(Dialects.SQLITE))
    assert len(statements) == 3
    assert statements[0].startswith("DROP TABLE IF EXISTS")
    assert "CREATE TABLE" in statements[1]
    assert "INSERT INTO" in statements[2]


def test_compile_statements_keeps_duckdb_persist_together():
    generator = Dialects.DUCK_DB.default_renderer()
    statements = generator.compile_statements(_persist(Dialects.DUCK_DB))
    assert len(statements) == 2
    assert "CREATE OR REPLACE TABLE" in statements[0]
    assert "INSERT INTO" in statements[1]


def test_compile_statement_matches_joined_statements():
    for dialect in (Dialects.DUCK_DB, Dialects.SQLITE, Dialects.MYSQL):
        generator = dialect.default_renderer()
        query = _persist(dialect)
        joined = generator.compile_statement(query)
        parts = generator.compile_statements(query)
        assert joined.replace("\n", " ").split() == " ".join(parts).split()


def test_create_or_replace_ddl_splits_drop_from_create():
    from trilogy.core.table_processor import datasource_to_create_table_info

    env = Environment()
    parse_text(SETUP, env)
    info = datasource_to_create_table_info(env.datasources["raw"])
    for dialect, expected in (
        (Dialects.SQLITE, 2),
        (Dialects.MYSQL, 2),
        (Dialects.DUCK_DB, 1),
    ):
        statements = dialect.default_renderer().compile_create_table_statements(
            info, CreateMode.CREATE_OR_REPLACE
        )
        assert len(statements) == expected, (dialect, statements)
        assert not any("CREATE OR REPLACE" in s for s in statements[:-1])
