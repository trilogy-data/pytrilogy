import os
import re
from pathlib import Path

from click.testing import CliRunner

from trilogy.scripts.trilogy import cli, set_rich_mode


def strip_ansi(text):
    """Remove ANSI escape sequences from text."""
    ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
    return ansi_escape.sub("", text)


path = Path(__file__).parent / "test.db"

bad_syntax_fmt = Path(__file__).parent / "bad_syntax_fmt.preql"


def test_cli_string():
    with set_rich_mode(False):
        runner = CliRunner()

        result = runner.invoke(
            cli,
            ["run", "select 1-> test;", "duckdb"],
        )
        if result.exception:
            raise result.exception
        assert result.exit_code == 0
        assert "(1,)" in result.output.strip()


def test_exception():
    for mode in [True, False]:
        with set_rich_mode(mode):
            runner = CliRunner()

            result = runner.invoke(
                cli,
                ["run", "select 1  test;", "duckdb"],
            )

            assert result.exit_code == 1
            assert "Syntax [201]" in result.output


def test_exception_fmt():
    with set_rich_mode(False):
        runner = CliRunner()

        result = runner.invoke(
            cli,
            ["fmt", str(bad_syntax_fmt)],
        )

        assert result.exit_code == 1
        assert "Syntax [201]" in result.output
    with set_rich_mode(True):
        runner = CliRunner()

        result = runner.invoke(
            cli,
            ["fmt", str(bad_syntax_fmt)],
        )

        assert result.exit_code == 1
        assert "Syntax [201]" in result.output


def test_cli_string_progress():
    with set_rich_mode(True):
        runner = CliRunner()

        result = runner.invoke(
            cli,
            [
                "run",
                "select 1-> test; select 3 ->test2; select 4->test5;",
                "duckdb",
            ],
        )
        if result.exception:
            raise result.exception
        assert result.exit_code == 0
        assert "Statements: 3" in result.output.strip()


def test_cli_fmt_string():
    with set_rich_mode(False):
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
    with set_rich_mode(False):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "run",
                "key in int; datasource test_source ( i:in) grain(in) address test; publish datasource test_source; select in;",
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
    with set_rich_mode(False):
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
                "--param",
                "date=2023-01-01",
            ],
        )
        if result.exception:
            raise result.exception
        assert result.exit_code == 0
        assert "(42, 3.14, 'hello', datetime.date(2023, 1, 1))" in result.output.strip()
