from click.testing import CliRunner
from preql.scripts.trilogy import main


def test_cli_string():
    runner = CliRunner()
    result = runner.invoke(
        main,
        ["select 1-> test;", "duckdb"],
    )
    if result.exception:
        raise result.exception
    assert result.exit_code == 0
    assert "(1,)" in result.output.strip()
