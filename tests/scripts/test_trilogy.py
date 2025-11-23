import os
from pathlib import Path

from click.testing import CliRunner

from trilogy.scripts.trilogy import cli

path = Path(__file__).parent / "test.db"


def test_cli_string():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["run", "select 1-> test;", "duckdb"],
    )
    if result.exception:
        raise result.exception
    assert result.exit_code == 0
    assert "(1,)" in result.output.strip()


def test_cli_fmt_string():
    runner = CliRunner()
    with open("test.sql", "w") as f:
        f.write("select 1 -> test;")
    result = runner.invoke(
        cli,
        ["fmt", "test.sql"],
    )
    if result.exception:
        raise result.exception
    assert result.exit_code == 0
    with open("test.sql", "r") as f:
        assert (
            f.read().strip()
            == """SELECT
    1 -> test,
;"""
        )
    os.remove("test.sql")


def test_db_args_string():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "run",
            "key in int; datasource test_source ( i:in) grain(in) address test; select in;",
            "duckdb",
            "--path",
            str(path),
        ],
    )
    if result.exception:
        raise result.exception
    assert result.exit_code == 0
    assert "(42,)" in result.output.strip()


def test_parameters():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "run",
            str(Path(__file__).parent / "param_test.preql"),
            "duckdb",
            "--param",
            "scale=42",
            "--param",
            "float=3.14",
            "--param",
            "string=hello",
        ],
    )
    if result.exception:
        raise result.exception
    assert result.exit_code == 0
    assert "(42, 3.14, 'hello')" in result.output.strip()
