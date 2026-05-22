from pathlib import Path

import duckdb
from click.testing import CliRunner

from trilogy.scripts.trilogy import cli

TOML = '[engine]\ndialect = "duck_db"\n\n[engine.config]\ndb_location = "test.duckdb"\n'


def _setup() -> None:
    con = duckdb.connect("test.duckdb")
    con.execute("CREATE TABLE users (id INTEGER, name VARCHAR)")
    con.execute(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER, total DECIMAL(10,2))"
    )
    con.execute("INSERT INTO users VALUES (1, 'a'), (2, 'b')")
    con.execute("INSERT INTO orders VALUES (1, 1, 9.99), (2, 2, 5.00)")
    con.execute("CHECKPOINT")
    con.close()
    Path("trilogy.toml").write_text(TOML, encoding="utf-8")


def test_database_list(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _setup()
    result = CliRunner().invoke(cli, ["database", "list"])
    assert result.exit_code == 0, result.output
    assert "users" in result.output and "orders" in result.output


def test_database_describe(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _setup()
    result = CliRunner().invoke(cli, ["database", "describe", "users"])
    assert result.exit_code == 0, result.output
    assert "id" in result.output and "name" in result.output


def test_database_describe_missing_table_exits_nonzero(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _setup()
    result = CliRunner().invoke(cli, ["database", "describe", "nope"])
    assert result.exit_code == 1


def test_ingest_all_bootstraps_every_table(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _setup()
    result = CliRunner().invoke(cli, ["ingest", "--all"])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "raw" / "users.preql").exists()
    assert (tmp_path / "raw" / "orders.preql").exists()


def test_ingest_all_rejects_explicit_sources(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _setup()
    result = CliRunner().invoke(cli, ["ingest", "--all", "users"])
    assert result.exit_code != 0
    assert "not both" in result.output
