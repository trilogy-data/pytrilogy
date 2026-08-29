import importlib.util
import io
import os
import re
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from click.exceptions import Exit
from click.testing import CliRunner

from trilogy.scripts.common import handle_execution_exception
from trilogy.scripts.display import set_rich_mode
from trilogy.scripts.trilogy import _force_utf8_stdio, cli

RICH_MODES = [False]

if importlib.util.find_spec("rich") is not None:
    RICH_MODES.append(True)
else:
    RICH_MODES.append(False)


def strip_ansi(text):
    """Remove ANSI escape sequences from text."""
    ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
    return ansi_escape.sub("", text)


bad_syntax_fmt = Path(__file__).parent / "bad_syntax_fmt.preql"


def test_force_utf8_stdio_leaves_capture_streams_alone():
    """A capture stream (pytest's, click's CliRunner) is already UTF-8, but
    opened with errors='replace' over a buffer that also holds raw bytes
    subprocesses wrote to the captured fd. reconfigure() resets errors to
    'strict' when it isn't passed, and one stray byte then fails every capture
    read for the rest of the session -- so UTF-8 streams must be left alone."""
    captured = io.TextIOWrapper(io.BytesIO(), encoding="utf-8", errors="replace")
    narrow = io.TextIOWrapper(io.BytesIO(), encoding="cp1252", errors="replace")

    with patch.object(sys, "stdout", captured), patch.object(sys, "stderr", narrow):
        _force_utf8_stdio()

    assert (captured.encoding, captured.errors) == ("utf-8", "replace")
    assert (narrow.encoding, narrow.errors) == ("utf-8", "replace")


def test_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert result.output.startswith("v")


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

            assert result.exit_code == 0
            assert "[201]" in result.output


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
                assert f.read().strip() == """select
    1 as test,
;"""
            os.remove("test.sql")


def test_db_args_string(tmp_path):
    import duckdb

    db_path = tmp_path / "test.db"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE test (i INTEGER)")
    con.execute("INSERT INTO test VALUES (42)")
    con.close()

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
                    str(db_path),
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
    combined = results.stdout + results.stderr
    assert "Missing required Snowflake connection parameters:" not in combined, combined

    # Test missing required parameters
    results = runner.invoke(
        cli,
        [
            "run",
            "select 1 as test;",
            "snowflake",
        ],
    )
    combined = results.stdout + results.stderr
    assert "Missing required Snowflake connection parameters:" in combined, combined


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
    combined = results.stdout + results.stderr
    assert "Missing required SQL Server" not in combined, combined

    # Test missing required parameters
    results = runner.invoke(
        cli,
        [
            "run",
            "select 1 as test;",
            "sql_server",
        ],
    )
    combined = results.stdout + results.stderr
    assert "Missing required SQL Server" in combined, combined


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
    combined = results.stdout + results.stderr
    assert "Missing required Postgres connection parameters:" not in combined, combined

    # Test missing required parameters
    results = runner.invoke(
        cli,
        [
            "run",
            "select 1 as test;",
            "postgres",
        ],
    )
    combined = results.stdout + results.stderr
    assert "Missing required Postgres connection parameters:" in combined, combined


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
    combined = results.stdout + results.stderr
    assert "Missing required Presto connection parameters:" not in combined, combined

    # Test missing required parameters
    results = runner.invoke(
        cli,
        [
            "run",
            "select 1 as test;",
            "presto",
        ],
    )
    combined = results.stdout + results.stderr
    assert "Missing required Presto connection parameters:" in combined, combined


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
            param_name = param_key.removeprefix("--")
            if param_name != missing_param:
                args.extend([param_key, param_value])

        results = runner.invoke(cli, args)

        # Should fail with missing parameter error
        combined = results.stdout + results.stderr
        assert (
            "Missing required" in combined
        ), f"Expected missing {missing_param} error for {dialect}, got: {combined}"
        assert (
            missing_param in combined
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


def test_integration_rejects_duplicate_unique_property(tmp_path: Path):
    path = tmp_path / "duplicate_unique_property.preql"
    path.write_text("""
key id int;
unique property id.code string;

datasource items (id: id, code: code)
grain (id)
query '''
SELECT 1 AS id, 'shared' AS code UNION ALL
SELECT 2, 'shared'
''';
""")
    runner = CliRunner()

    result = runner.invoke(
        cli,
        ["--format", "json", "integration", str(path), "duckdb"],
    )

    assert result.exit_code == 1
    assert "Unique property" in result.output
    assert "local.code maps to multiple local.id values" in result.output


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


def test_function_argument_type_reported_as_type_error():
    # `year()` on an integer is a fixable author mistake; the harness must label
    # it a clean "Type error", not the "Unexpected error" catch-all (and not a
    # syntax error).
    for mode in RICH_MODES:
        with set_rich_mode(mode):
            runner = CliRunner()
            result = runner.invoke(
                cli,
                [
                    "run",
                    "key id int; datasource nums (id) grain (id) query '''select 1 as id'''; where year(id) = 2000 select id;",
                    "duckdb",
                ],
            )
            assert result.exit_code == 1
            output = strip_ansi(result.output)
            assert "Type error" in output, output
            assert "Unexpected error" not in output, output
            assert "Syntax error" not in output, output


def test_multi_column_subquery_reported_as_syntax_error():
    # A two-column `(select ...)` subquery is a fixable author mistake; the
    # harness must label it a "Syntax error" with a source location, not the
    # "Unexpected error" catch-all.
    for mode in RICH_MODES:
        with set_rich_mode(mode):
            runner = CliRunner()
            result = runner.invoke(
                cli,
                [
                    "run",
                    "key id int; property id.val int; datasource t (id: id, val: val) grain (id) query '''select 1 as id, 10 as val'''; select id where id in (select id -> a, val -> b);",
                    "duckdb",
                ],
            )
            assert result.exit_code == 1
            output = " ".join(strip_ansi(result.output).split())
            assert "Syntax error" in output, output
            # scalar left vs a 2-column subquery → arity-specific rejection
            assert "projects 2 columns" in output, output
            assert "(line 1, column" in output, output
            assert "Unexpected error" not in output, output


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
            # Exit code 2 means nothing needed to be refreshed (all up to date)
            assert result.exit_code == 2


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
    assert result.exit_code == 2


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
            # Exit code 2 means nothing needed to be refreshed (all up to date)
            assert result.exit_code == 2


def test_refresh_parallel_failure():
    target_path = Path(__file__).parent / "failing_directory"
    runner = CliRunner()

    results = runner.invoke(
        cli,
        ["refresh", str(target_path), "duckdb"],
    )
    assert results.exit_code == 1


def test_refresh_with_stale_assets(tmp_path: Path):
    """Test refresh command with actual stale assets that need refreshing.

    Uses the trilogy CLI to exercise the full refresh path.
    """
    # Create the script with stale assets
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
''';

datasource target_events (
    event_id: event_id,
    event_ts: event_ts
)
grain (event_id)
address target_events_table
incremental by event_ts;


"""
    test_file = tmp_path / "stale_test.preql"
    test_file.write_text(script_content)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "refresh",
            str(test_file),
            "duckdb",
        ],
    )
    if result.exception:
        raise result.exception
    assert result.exit_code == 0
    assert "stale asset" in result.output.lower()
    assert "target_events" in result.output
    assert "Refreshing" in result.output
    assert "Refreshed" in result.output


def test_refresh_directory_with_stale_assets(tmp_path: Path):
    """Test refresh command with actual stale assets that need refreshing.

    Uses the trilogy CLI to exercise the full refresh path.
    """
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
''';
datasource target_events (
    event_id: event_id,
    event_ts: event_ts
)
grain (event_id)
address target_events_table
incremental by event_ts;
    """
    test_file = tmp_path / "stale_test.preql"
    test_file.write_text(script_content)

    runner = CliRunner()
    with patch("click.confirm", return_value=True):
        result = runner.invoke(
            cli,
            [
                "refresh",
                str(tmp_path),
                "duckdb",
            ],
        )
    if result.exception:
        raise result.exception
    assert result.exit_code == 0
    assert "stale asset" in result.output.lower()
    assert "target_events" in result.output
    assert "Datasources Updated" in result.output
    assert "All scripts executed successfully!" in result.output


def test_refresh_directory_dry_run_wording(tmp_path: Path):
    """Directory dry-run must say "would", matching single-file mode."""
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
''';
datasource target_events (
    event_id: event_id,
    event_ts: event_ts
)
grain (event_id)
address target_events_table
incremental by event_ts;
    """
    test_file = tmp_path / "stale_test.preql"
    test_file.write_text(script_content)

    runner = CliRunner()
    result = runner.invoke(cli, ["refresh", str(tmp_path), "duckdb", "--dry-run"])
    if result.exception:
        raise result.exception
    assert result.exit_code == 0
    assert "1 datasource would be updated" in result.output
    assert "Datasources That Would Be Updated" in result.output
    assert "Dry run: 1 asset(s) would be refreshed" in result.output
    assert "All scripts executed successfully!" not in result.output
    assert "Datasources Updated" not in result.output


@pytest.mark.parametrize(
    "cmd,args",
    [
        ("run", ["select 1-> test;", "duckdb"]),
        ("refresh", ["select 1-> test;", "duckdb"]),
        ("fmt", ["select 1-> test;"]),
        ("unit", [str(Path(__file__).parent / "validate_directory" / "empty.preql")]),
        (
            "integration",
            [
                str(Path(__file__).parent / "validate_directory" / "empty.preql"),
                "duckdb",
            ],
        ),
    ],
)
def test_debug_flag_before_subcommand(cmd, args):
    """--debug must be placed before the subcommand name as a top-level flag."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--debug", cmd] + args)
    assert "No such command" not in result.output, result.output
    assert "is not a valid Dialects" not in result.output, result.output
    assert "Debug mode enabled" in result.output


@pytest.mark.parametrize("cmd", ["refresh", "run"])
def test_flag_after_subcommand_is_hoisted(cmd):
    """``trilogy run --debug ...`` reads as naturally as ``trilogy --debug run ...``
    — the group-level ``--debug`` is hoisted by ``LazyGroup.parse_args``."""
    runner = CliRunner()
    result = runner.invoke(cli, [cmd, "select 1-> test;", "--debug"])
    assert "Debug mode enabled" in result.output, result.output
    assert "is not a valid Dialects" not in result.output, result.output
    assert "looks like a flag" not in result.output, result.output


@pytest.mark.parametrize("cmd", ["refresh", "run"])
def test_debug_before_file_path_resolves_cleanly(cmd):
    """``trilogy <cmd> --debug raw/foo.preql`` no longer collides with the
    dialect slot — ``--debug`` is consumed by the group and the missing path
    surfaces a clean "does not exist" message instead of the legacy "looks
    like a file path" hint."""
    runner = CliRunner()
    result = runner.invoke(cli, [cmd, "--debug", "raw/inventory.preql"])
    assert "Debug mode enabled" in result.output, result.output
    assert "does not exist" in result.output, result.output
    assert "is not a valid Dialects" not in result.output, result.output


RUN_DRY_RUN_SCRIPT = """
key id int;
property id.name string;

datasource people (
    id: id,
    name: name
)
grain (id)
query '''select 1 as id, 'a' as name''';

persist people_copy into people_out from select id, name;
"""


def test_run_dry_run_compiles_without_executing(tmp_path: Path):
    import duckdb

    script = tmp_path / "etl.preql"
    script.write_text(RUN_DRY_RUN_SCRIPT)
    db = tmp_path / "w.db"

    runner = CliRunner()
    result = runner.invoke(
        cli, ["run", str(script), "duckdb", "--dry-run", f"path={db}"]
    )
    if result.exception:
        raise result.exception
    assert result.exit_code == 0
    assert "CREATE OR REPLACE TABLE" in result.output
    assert "people_out" in result.output
    assert "Dry run: 1 statement compiled, none executed" in result.output

    tables = duckdb.connect(str(db)).sql("show tables").fetchall()
    assert tables == [], f"dry run must not have created a table, got {tables}"


def test_run_dry_run_directory_prints_sql_per_script(tmp_path: Path):
    (tmp_path / "etl.preql").write_text(RUN_DRY_RUN_SCRIPT)
    (tmp_path / "other.preql").write_text(
        RUN_DRY_RUN_SCRIPT.replace("people", "folk").replace("id", "fid")
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(tmp_path), "duckdb", "--dry-run"])
    if result.exception:
        raise result.exception
    assert result.exit_code == 0
    assert "-- etl.preql:" in result.output
    assert "-- other.preql:" in result.output
    assert "CREATE OR REPLACE TABLE" in result.output
    assert "Dry run: 2 statements compiled, none executed" in result.output
    assert "All scripts executed successfully!" not in result.output


def test_run_dry_run_short_alias_matches_refresh(tmp_path: Path):
    script = tmp_path / "etl.preql"
    script.write_text(RUN_DRY_RUN_SCRIPT)

    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(script), "duckdb", "-n"])
    assert result.exit_code == 0
    assert "Dry run:" in result.output


def test_run_dry_run_agent_mode_still_fails_a_no_op(tmp_path: Path):
    script = tmp_path / "defs.preql"
    script.write_text("key id int;")

    runner = CliRunner()
    result = runner.invoke(cli, ["--agent", "run", str(script), "duckdb", "-n"])
    assert result.exit_code == 1
    assert "Nothing was executed" in result.output


MIXED_STATEMENT_SCRIPT = "key id int;\nproperty id.name string;\n\ndatasource people (\n    id: id,\n    name: name\n)\ngrain (id)\nquery '''select 1 as id, 'a' as name''';\n\nraw_sql('''create table if not exists side (x int)''');\n\nchart layer barh (\n    y_axis <- name,\n    x_axis <- count(id) as c\n);"


def test_run_dry_run_renders_raw_sql_and_charts(tmp_path: Path):
    """generate_sql has to cover every statement a run can execute, or a dry
    run aborts on the first raw block or chart in an otherwise fine script."""
    script = tmp_path / "mixed.preql"
    script.write_text(MIXED_STATEMENT_SCRIPT)

    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(script), "duckdb", "--dry-run"])
    if result.exception:
        raise result.exception
    assert result.exit_code == 0
    assert "create table if not exists side" in result.output
    assert "count(" in result.output


def test_compile_queries_reports_statements_that_have_no_sql():
    """A validate statement runs outside query generation; a dry run says so
    rather than aborting over it."""
    from trilogy.dialect.enums import Dialects
    from trilogy.scripts.common import compile_queries

    class NotSql:
        pass

    executor = Dialects.DUCK_DB.default_executor()
    compiled = compile_queries(executor, [NotSql()])  # type: ignore[list-item]
    assert len(compiled) == 1
    assert "no SQL to render" in compiled[0].sql


def _walk_commands(command, ctx, prefix=()):
    import click

    yield prefix, command
    if isinstance(command, click.Group):
        for name in command.list_commands(ctx):
            sub = command.get_command(ctx, name)
            if sub is not None:
                yield from _walk_commands(sub, ctx, (*prefix, name))


def test_every_dry_run_flag_carries_the_same_short_alias():
    """`--dry-run` comes from one decorator so a new one cannot drift; `cloud
    sync` shipped without `-n` when each command spelled its own."""
    import click

    ctx = click.Context(cli)
    missing = []
    for path, command in _walk_commands(cli, ctx):
        for param in command.params:
            if "--dry-run" in param.opts and "-n" not in param.opts:
                missing.append(" ".join(path))
    assert not missing, f"--dry-run without -n: {missing}"


def test_dry_run_is_offered_by_every_writing_command():
    import click

    ctx = click.Context(cli)
    expected = {
        "run",
        "refresh",
        "ingest",
        "import",
        "env publish",
        "cloud sync",
        "cloud jobs push",
        "cloud workspaces push",
    }
    found = {
        " ".join(path)
        for path, command in _walk_commands(cli, ctx)
        if any("--dry-run" in p.opts for p in command.params)
    }
    assert expected <= found, f"missing --dry-run on: {sorted(expected - found)}"
