from click.testing import CliRunner
from preql.scripts.trilogy import cli
import os


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
        f.write("select 1-> test;")
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
    1->test,;"""
        )
    os.remove("test.sql")
