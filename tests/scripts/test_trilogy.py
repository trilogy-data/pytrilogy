import importlib.util
import os
import re
from pathlib import Path

import pytest
from click.exceptions import Exit
from click.testing import CliRunner

from trilogy.scripts.common import handle_execution_exception
from trilogy.scripts.display import set_rich_mode
from trilogy.scripts.trilogy import cli

RICH_MODES = [False]

if importlib.util.find_spec("rich") is not None:
    RICH_MODES.append(True)
else:
    RICH_MODES.append(False)


def strip_ansi(text):
    """Remove ANSI escape sequences from text."""
    ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
    return ansi_escape.sub("", text)


path = Path(__file__).parent / "test.db"

bad_syntax_fmt = Path(__file__).parent / "bad_syntax_fmt.preql"


def test_cli_string():
    for val in RICH_MODES:
        with set_rich_mode(val):
            runner = CliRunner()

            result = runner.invoke(
                cli,
                ["run", "select 1-> test;", "duckdb"],
            )
            if result.exception:
                raise result.exception
            assert result.exit_code == 0
            assert "test" in result.output.strip() and "1" in result.output.strip()
            assert "Failed" not in result.output.strip(), result.output.strip()


def test_exception():
    for mode in RICH_MODES:
        with set_rich_mode(mode):
            runner = CliRunner()

            result = runner.invoke(
                cli,
                ["run", "select 1  test;", "duckdb"],
            )

            assert result.exit_code == 1
            assert "Syntax [201]" in result.output


def test_multi_exception_thrown_execution():
    for mode in RICH_MODES:
        with set_rich_mode(mode):
            runner = CliRunner()

            result = runner.invoke(
                cli,
                [
                    "run",
                    "select 1 as test; key x int; datasource funky_monkey (x) query '''select 'abc' as x'''; select x+1 as test2;",
                    "duckdb",
                ],
            )

            assert result.exit_code == 1
            assert (
                "Binder Error: No function" in strip_ansi(result.output)
                or "Execution Failed" in result.output
            )


def test_multi_no_exception():
    for mode in RICH_MODES:
        with set_rich_mode(mode):
            runner = CliRunner()

            result = runner.invoke(
                cli,
                [
                    "run",
                    "select 1 as test; key x int; datasource funky_monkey (x) query '''select 1 as x'''; select x+1 as test2;",
                    "duckdb",
                ],
            )

            assert result.exit_code == 0

            result = runner.invoke(
                cli,
                [
                    "run",
                    str(Path(__file__).parent / "multi_script.preql"),
                    "duckdb",
                ],
            )

            assert result.exit_code == 0


def test_exception_fmt():
    for mode in RICH_MODES:
        with set_rich_mode(mode):
            runner = CliRunner()

            result = runner.invoke(
                cli,
                ["fmt", str(bad_syntax_fmt)],
            )

            assert result.exit_code == 1
            assert "Syntax [201]" in result.output


def test_cli_string_progress():
    for mode in RICH_MODES:
        with set_rich_mode(mode):
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


def test_cli_string_progress_debug():
    for mode in RICH_MODES:
        with set_rich_mode(mode):
            runner = CliRunner()

            result = runner.invoke(
                cli,
                [
                    "run",
                    "select 1-> test; select 3 ->test2; select 4->test5;",
                    "duckdb",
                    "--debug",
                ],
            )
            if result.exception:
                raise result.exception
            assert result.exit_code == 0
            assert "Statements: 3" in result.output.strip()


def test_cli_fmt_string():
    for mode in RICH_MODES:
        with set_rich_mode(mode):
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
    for mode in RICH_MODES:
        with set_rich_mode(mode):
            runner = CliRunner()
            result = runner.invoke(
                cli,
                [
                    "run",
                    "key in int; datasource test_source ( i:in) grain(in) address test; publish datasource test_source; select in as int_aliased;",
                    "duckdb",
                    "--path",
                    str(path),
                ],
            )
            if result.exception:
                raise result.exception
            assert result.exit_code == 0
            assert "int_aliased" in result.output.strip()
            assert "42" in result.output.strip()


def test_run_folder():
    target_path = Path(__file__).parent / "directory"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "run",
            str(target_path),
            "duckdb",
        ],
    )
    if result.exception:
        raise ValueError(result.output)
    assert result.exit_code == 0
    assert "Total Scripts" in result.output.strip()


def test_parameters():
    for mode in RICH_MODES:
        with set_rich_mode(mode):
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
                    "--param",
                    "dt=2023-01-01T12:30:00",
                ],
            )
            if result.exception:
                raise result.exception
            assert result.exit_code == 0
            if mode is False:
                assert (
                    "(42, 3.14, 'hello', datetime.date(2023, 1, 1), datetime.datetime(2023, 1, 1, 12, 30))"
                    in result.output.strip()
                )
            else:
                assert "3.14" in result.output.strip()


def test_snowflake():
    runner = CliRunner()
    # Test with all required parameters
    results = runner.invoke(
        cli,
        [
            "run",
            "select 1 as test;",
            "snowflake",
            "--password",
            "mypassword",
            "--username",
            "myusername",
            "--account",
            "myaccount",
        ],
    )
    assert (
        "Missing required Snowflake connection parameters:" not in results.stdout
    ), results.stdout

    # Test missing required parameters
    results = runner.invoke(
        cli,
        [
            "run",
            "select 1 as test;",
            "snowflake",
        ],
    )
    assert (
        "Missing required Snowflake connection parameters:" in results.stdout
    ), results.stdout


def test_sql_server():
    runner = CliRunner()
    # Test with all required parameters
    results = runner.invoke(
        cli,
        [
            "run",
            "select 1 as test;",
            "sql_server",
            "--host",
            "localhost",
            "--port",
            "1433",
            "--username",
            "myusername",
            "--password",
            "mypassword",
            "--database",
            "mydatabase",
        ],
    )
    assert "Missing required SQL Server" not in results.stdout, results.stdout

    # Test missing required parameters
    results = runner.invoke(
        cli,
        [
            "run",
            "select 1 as test;",
            "sql_server",
        ],
    )
    assert "Missing required SQL Server" in results.stdout, results.stdout


def test_postgres():
    runner = CliRunner()
    # Test with all required parameters
    results = runner.invoke(
        cli,
        [
            "run",
            "select 1 as test;",
            "postgres",
            "--host",
            "localhost",
            "--port",
            "5432",
            "--username",
            "myusername",
            "--password",
            "mypassword",
            "--database",
            "mydatabase",
        ],
    )
    assert (
        "Missing required Postgres connection parameters:" not in results.stdout
    ), results.stdout

    # Test missing required parameters
    results = runner.invoke(
        cli,
        [
            "run",
            "select 1 as test;",
            "postgres",
        ],
    )
    assert (
        "Missing required Postgres connection parameters:" in results.stdout
    ), results.stdout


def test_presto():
    runner = CliRunner()
    # Test with all required parameters
    results = runner.invoke(
        cli,
        [
            "run",
            "select 1 as test;",
            "presto",
            "--host",
            "localhost",
            "--port",
            "8080",
            "--username",
            "myusername",
            "--password",
            "mypassword",
            "--catalog",
            "mycatalog",
        ],
    )
    assert (
        "Missing required Presto connection parameters:" not in results.stdout
    ), results.stdout

    # Test missing required parameters
    results = runner.invoke(
        cli,
        [
            "run",
            "select 1 as test;",
            "presto",
        ],
    )
    assert (
        "Missing required Presto connection parameters:" in results.stdout
    ), results.stdout


def test_duck_db():
    runner = CliRunner()
    # Test with minimal parameters (DuckDB typically doesn't require connection params)
    results = runner.invoke(
        cli,
        [
            "run",
            "select 1 as test;",
            "duck_db",
        ],
    )
    assert "Failed to configure dialect" not in results.stdout, results.stdout

    # Test with optional parameters
    results = runner.invoke(
        cli,
        [
            "run",
            "select 1 as test;",
            "duck_db",
            "--database",
            ":memory:",
        ],
    )
    assert "Failed to configure dialect" not in results.stdout, results.stdout


def test_bigquery():
    runner = CliRunner()
    # Test with minimal parameters (BigQuery typically uses service account or environment auth)
    results = runner.invoke(
        cli,
        [
            "run",
            "select 1 as test;",
            "bigquery",
        ],
    )
    assert "Failed to configure dialect" not in results.stdout, results.stdout

    # Test with optional project parameter
    results = runner.invoke(
        cli,
        [
            "run",
            "select 1 as test;",
            "bigquery",
            "--project",
            "my-project-id",
        ],
    )
    assert "Failed to configure dialect" not in results.stdout, results.stdout


# Parametrized test for engines that require connection parameters
@pytest.mark.parametrize(
    "dialect,required_params,test_params",
    [
        (
            "snowflake",
            ["username", "password", "account"],
            {
                "--username": "testuser",
                "--password": "testpass",
                "--account": "testaccount",
            },
        ),
        (
            "sql_server",
            ["host", "port", "username", "password", "database"],
            {
                "--host": "localhost",
                "--port": "1433",
                "--username": "testuser",
                "--password": "testpass",
                "--database": "testdb",
            },
        ),
        (
            "postgres",
            ["host", "port", "username", "password", "database"],
            {
                "--host": "localhost",
                "--port": "5432",
                "--username": "testuser",
                "--password": "testpass",
                "--database": "testdb",
            },
        ),
        (
            "presto",
            ["host", "port", "username", "password", "catalog"],
            {
                "--host": "localhost",
                "--port": "8080",
                "--username": "testuser",
                "--password": "testpass",
                "--catalog": "testcatalog",
            },
        ),
    ],
)
def test_engine_missing_single_parameter(dialect, required_params, test_params):
    """Test that each required parameter is properly validated."""
    runner = CliRunner()

    # Test missing each required parameter one at a time
    for missing_param in required_params:
        # Create args with all params except the missing one
        args = ["run", "select 1 as test;", dialect]
        for param_key, param_value in test_params.items():
            param_name = param_key.lstrip("--")
            if param_name != missing_param:
                args.extend([param_key, param_value])

        results = runner.invoke(cli, args)

        # Should fail with missing parameter error
        assert (
            "Missing required" in results.stdout
        ), f"Expected missing {missing_param} error for {dialect}, got: {results.stdout}"
        assert (
            missing_param in results.stdout
        ), f"Missing parameter {missing_param} should be mentioned in error for {dialect}"


def test_invalid_dialect():
    """Test behavior with invalid/unsupported dialect."""
    runner = CliRunner()

    results = runner.invoke(
        cli,
        [
            "run",
            "select 1 as test;",
            "invalid_dialect",
        ],
    )

    # Should fail gracefully
    assert results.exit_code != 0, "Invalid dialect should cause non-zero exit code"


def test_validation_failure():
    path = Path(__file__).parent / "validation_failure.preql"
    runner = CliRunner()

    results = runner.invoke(cli, ["integration", str(path), "duckdb"])
    assert results.exit_code == 1
    assert "INTEGER(NULLABLE)" in results.stdout


def test_unit():
    path = Path(__file__).parent / "directory"
    runner = CliRunner()

    results = runner.invoke(
        cli,
        [
            "unit",
            str(path),
        ],
    )
    if results.exception:
        raise results.exception
    assert results.exit_code == 0


def test_unit_gbq():
    path = Path(__file__).parent / "gbq_syntax.preql"
    runner = CliRunner()

    results = runner.invoke(
        cli,
        [
            "unit",
            str(path),
        ],
    )
    if results.exception:
        raise results.exception
    assert results.exit_code == 0


def test_parallel_failure():
    path = Path(__file__).parent / "failing_directory"
    runner = CliRunner()

    results = runner.invoke(
        cli,
        ["run", str(path), "duckdb"],
    )
    assert results.exit_code == 1
    assert "Skipped due to failed dependency" in results.output


def test_exception_unexpected():
    with pytest.raises(Exit):
        handle_execution_exception(ValueError("Test exception handling"))


def test_empty_unit():
    path = Path(__file__).parent / "validate_directory" / "empty.preql"
    runner = CliRunner()

    results = runner.invoke(
        cli,
        [
            "unit",
            str(path),
        ],
    )
    if results.exception:
        raise results.exception
    assert results.exit_code == 0


def test_empty_integration():
    path = Path(__file__).parent / "validate_directory" / "empty.preql"
    runner = CliRunner()

    results = runner.invoke(
        cli,
        ["integration", str(path), "duckdb"],
    )
    if results.exception:
        raise results.exception
    assert results.exit_code == 0


def test_parallel_integration_unit():
    path = Path(__file__).parent / "validate_directory"
    runner = CliRunner()
    for cmd in [
        "run",
        "integration",
    ]:
        results = runner.invoke(
            cli,
            [cmd, str(path), "duckdb"],
        )
        if results.exception:
            raise ValueError(results.output)
        assert results.exit_code == 0
    for cmd in [
        "unit",
    ]:
        results = runner.invoke(
            cli,
            [
                cmd,
                str(path),
            ],
        )
        if results.exception:
            raise ValueError(results.output)
        assert results.exit_code == 0


def test_refresh_string():
    for val in RICH_MODES:
        with set_rich_mode(val):
            runner = CliRunner()

            result = runner.invoke(
                cli,
                ["refresh", "select 1-> test;", "duckdb"],
            )
            if result.exception:
                raise result.exception
            assert result.exit_code == 0


def test_refresh_folder():
    target_path = Path(__file__).parent / "validate_directory"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "refresh",
            str(target_path),
            "duckdb",
        ],
    )
    if result.exception:
        raise ValueError(result.output)
    assert result.exit_code == 0
    assert "Total Scripts" in result.output.strip()


def test_refresh_exception():
    for mode in RICH_MODES:
        with set_rich_mode(mode):
            runner = CliRunner()

            result = runner.invoke(
                cli,
                ["refresh", "select 1  test;", "duckdb"],
            )

            assert result.exit_code == 1
            assert "Syntax [201]" in result.output


def test_refresh_with_parameters():
    for mode in RICH_MODES:
        with set_rich_mode(mode):
            runner = CliRunner()
            result = runner.invoke(
                cli,
                [
                    "refresh",
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
                    "--param",
                    "dt=2023-01-01T12:30:00",
                ],
            )
            if result.exception:
                raise result.exception
            assert result.exit_code == 0


def test_refresh_parallel_failure():
    target_path = Path(__file__).parent / "failing_directory"
    runner = CliRunner()

    results = runner.invoke(
        cli,
        ["refresh", str(target_path), "duckdb"],
    )
    assert results.exit_code == 1
    assert "Skipped due to failed dependency" in results.output


def test_refresh_with_stale_assets(tmp_path: Path):
    """Test refresh command with actual stale assets that need refreshing.

    Uses a temp file and calls execute_script_for_refresh directly to exercise
    the full refresh path in refresh.py. This is more reliable than CLI testing
    since it avoids database connection isolation issues between CLI invocations.
    """
    from trilogy import Dialects
    from trilogy.execution.state.state_store import BaseStateStore
    from trilogy.scripts.dependency import ScriptNode
    from trilogy.scripts.refresh import execute_script_for_refresh

    # Create the setup script that creates the table with initial stale data
    script_content = """
key event_id int;
property event_id.event_ts datetime;

root datasource source_events (
    event_id: event_id,
    event_ts: event_ts
)
grain (event_id)
query '''
SELECT 1 as event_id, TIMESTAMP '2024-01-10 12:00:00' as event_ts
UNION ALL
SELECT 2 as event_id, TIMESTAMP '2024-01-15 12:00:00' as event_ts
UNION ALL
SELECT 3 as event_id, TIMESTAMP '2024-01-20 12:00:00' as event_ts
'''
incremental by event_ts;

datasource target_events (
    event_id: event_id,
    event_ts: event_ts
)
grain (event_id)
address target_events_table
incremental by event_ts;

CREATE IF NOT EXISTS DATASOURCE target_events;

RAW_SQL('''
INSERT INTO target_events_table
SELECT 1 as event_id, TIMESTAMP '2024-01-10 12:00:00' as event_ts
''');
"""
    test_file = tmp_path / "stale_test.preql"
    test_file.write_text(script_content)

    # First execute to create table and insert initial data
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(script_content)

    # Verify table was created with 1 row
    result = executor.execute_raw_sql(
        "SELECT count(*) FROM target_events_table"
    ).fetchone()
    assert result[0] == 1

    # Verify we have stale assets before refresh
    state_store = BaseStateStore()
    stale_before = state_store.get_stale_assets(executor.environment, executor)
    assert len(stale_before) == 1
    assert stale_before[0].datasource_id == "target_events"

    # Now call the refresh function directly (this is what the CLI calls)
    node = ScriptNode(path=test_file)
    stats = execute_script_for_refresh(executor, node, quiet=False)

    # Verify refresh happened
    assert stats.persist_count == 1

    # Verify table now has all 3 rows
    result = executor.execute_raw_sql(
        "SELECT count(*) FROM target_events_table"
    ).fetchone()
    assert result[0] == 3

    # Verify no more stale assets
    stale_after = state_store.get_stale_assets(executor.environment, executor)
    assert len(stale_after) == 0
