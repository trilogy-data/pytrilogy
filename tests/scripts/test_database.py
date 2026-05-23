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


def test_database_list_with_schema(tmp_path, monkeypatch):
    """`-s main` restricts list_tables to the named schema, exercising the
    schema-filtered query branch (BaseDialect.list_tables)."""
    monkeypatch.chdir(tmp_path)
    _setup()
    result = CliRunner().invoke(cli, ["database", "list", "-s", "main"])
    assert result.exit_code == 0, result.output
    assert "users" in result.output and "orders" in result.output


def test_database_list_empty_schema_prints_no_tables_found(tmp_path, monkeypatch):
    """A schema with no tables hits the `if not tables` branch in list_cmd."""
    monkeypatch.chdir(tmp_path)
    _setup()
    con = duckdb.connect("test.duckdb")
    con.execute("CREATE SCHEMA empty_schema")
    con.execute("CHECKPOINT")
    con.close()
    result = CliRunner().invoke(cli, ["database", "list", "-s", "empty_schema"])
    assert result.exit_code == 0, result.output
    assert "No tables found" in result.output


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


def test_database_list_without_engine_config(tmp_path, monkeypatch):
    """Missing engine.dialect in trilogy.toml exits non-zero with a clear error."""
    monkeypatch.chdir(tmp_path)
    Path("trilogy.toml").write_text("", encoding="utf-8")
    result = CliRunner().invoke(cli, ["database", "list"])
    assert result.exit_code != 0
    assert "No engine configured" in result.output


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


def test_ingest_all_rejects_name_combination(tmp_path, monkeypatch):
    """`--name` is single-source semantics — it can't combine with `--all`."""
    monkeypatch.chdir(tmp_path)
    _setup()
    result = CliRunner().invoke(cli, ["ingest", "--all", "--name", "renamed"])
    assert result.exit_code != 0
    assert "cannot be combined with --all" in result.output


def test_ingest_all_against_empty_schema_exits_one(tmp_path, monkeypatch):
    """`--all` against a schema with no tables errors clearly."""
    monkeypatch.chdir(tmp_path)
    _setup()
    con = duckdb.connect("test.duckdb")
    con.execute("CREATE SCHEMA empty_schema")
    con.execute("CHECKPOINT")
    con.close()
    result = CliRunner().invoke(cli, ["ingest", "--all", "-s", "empty_schema"])
    assert result.exit_code != 0
    assert "No tables found" in result.output
