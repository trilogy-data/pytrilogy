import tempfile
from pathlib import Path

from click.testing import CliRunner

from trilogy.dialect.config import (
    BigQueryConfig,
    DuckDBConfig,
    PostgresConfig,
    PrestoConfig,
    SnowflakeConfig,
    SQLServerConfig,
)
from trilogy.dialect.enums import Dialects
from trilogy.execution.config import load_config_file
from trilogy.scripts.trilogy import cli


def test_config_bootstrap():
    path = Path(__file__).parent / "config_directory"
    runner = CliRunner()

    for cmd in ["run", "integration"]:
        result = runner.invoke(cli, [cmd, str(path), "duckdb"])
        if result.exception:
            raise AssertionError(
                f"Command '{cmd}' failed:\n"
                f"stdout:\n{result.stdout}\n"
                f"exc:\n{result.exception}"
            )
        assert result.exit_code == 0

    for cmd in ["unit"]:
        result = runner.invoke(cli, [cmd, str(path)])
        if result.exception:
            raise AssertionError(
                f"Command '{cmd}' failed:\n"
                f"stdout:\n{result.stdout}\n"
                f"exc:\n{result.exception}"
            )
        assert result.exit_code == 0

    # test config
    for cmd in ["run", "integration"]:
        result = runner.invoke(
            cli,
            [
                cmd,
                str(path),
                "duckdb",
                "--config",
                str(path / "trilogy_dev.toml"),
            ],
        )
        if result.exception:
            raise AssertionError(
                f"Command '{cmd}' failed:\n"
                f"stdout:\n{result.stdout}\n"
                f"exc:\n{result.exception}"
            )
        assert result.exit_code == 0

    for cmd in ["unit"]:
        result = runner.invoke(
            cli, [cmd, str(path), "--config", str(path / "trilogy_dev.toml")]
        )
        if result.exception:
            raise AssertionError(
                f"Command '{cmd}' failed:\n"
                f"stdout:\n{result.stdout}\n"
                f"exc:\n{result.exception}"
            )
        assert result.exit_code == 0


def test_config_bootstrap_dialect():
    path = Path(__file__).parent / "config_directory"
    runner = CliRunner()

    for cmd in ["run", "integration"]:
        result = runner.invoke(
            cli,
            [
                cmd,
                str(path),
            ],
        )
        if result.exception:
            raise AssertionError(
                f"Command '{cmd}' failed:\n"
                f"stdout:\n{result.stdout}\n"
                f"exc:\n{result.exception}"
            )
        assert result.exit_code == 0

    # test config
    for cmd in ["run", "integration"]:

        result = runner.invoke(
            cli, [cmd, str(path), "--config", str(path / "trilogy_dev.toml")]
        )
        if result.exception:
            raise AssertionError(
                f"Command '{cmd}' failed:\n"
                f"stdout:\n{result.stdout}\n"
                f"exc:\n{result.exception}"
            )
        assert result.exit_code == 0


def test_merge_config():
    base_config = DuckDBConfig(path="/base/path.db")
    override_config = DuckDBConfig(path="/override/path.db")

    merged = base_config.merge_config(override_config)

    assert merged.path == "/override/path.db"
    assert merged is base_config


def test_cli_merge_config():
    """Test that CLI conn_args merge with config file engine_config"""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        config_content = """
[engine]
dialect = "duckdb"

[engine.config]
path = ":memory:"
"""
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(config_content)

        test_script = tmppath / "test.preql"
        test_script.write_text("select 1 as value;")

        runner = CliRunner()
        result = runner.invoke(
            cli, ["run", str(test_script), "duckdb", "path=/tmp/override.db"]
        )

        if result.exception:
            raise AssertionError(
                f"Command failed:\nstdout:\n{result.stdout}\nexc:\n{result.exception}"
            )
        assert result.exit_code == 0


def test_config_types():

    test_cases = [
        (
            Dialects.DUCK_DB,
            """
[engine]
dialect = "duck_db"

[engine.config]
path = "/tmp/test.db"
""",
            DuckDBConfig,
            {"path": "/tmp/test.db"},
        ),
        (
            Dialects.POSTGRES,
            """
[engine]
dialect = "postgres"

[engine.config]
host = "localhost"
port = 5432
username = "user"
password = "pass"
database = "testdb"
""",
            PostgresConfig,
            {
                "host": "localhost",
                "port": 5432,
                "username": "user",
                "password": "pass",
                "database": "testdb",
            },
        ),
        (
            Dialects.PRESTO,
            """
[engine]
dialect = "presto"

[engine.config]
host = "localhost"
port = 8080
username = "user"
password = "pass"
catalog = "hive"
schema = "default"
""",
            PrestoConfig,
            {
                "host": "localhost",
                "port": 8080,
                "username": "user",
                "password": "pass",
                "catalog": "hive",
                "schema": "default",
            },
        ),
        (
            Dialects.SNOWFLAKE,
            """
[engine]
dialect = "snowflake"

[engine.config]
account = "test_account"
username = "user"
password = "pass"
database = "testdb"
schema = "public"
""",
            SnowflakeConfig,
            {
                "account": "test_account",
                "username": "user",
                "password": "pass",
                "database": "testdb",
                "schema": "public",
            },
        ),
        (
            Dialects.SQL_SERVER,
            """
[engine]
dialect = "sql_server"

[engine.config]
host = "localhost"
port = 1433
username = "sa"
password = "pass"
database = "master"
""",
            SQLServerConfig,
            {
                "host": "localhost",
                "port": 1433,
                "username": "sa",
                "password": "pass",
                "database": "master",
            },
        ),
        (
            Dialects.BIGQUERY,
            """
[engine]
dialect = "bigquery"

[engine.config]
project = "test-project"
""",
            BigQueryConfig,
            {"project": "test-project", "client": None},
        ),
    ]

    for dialect, toml_content, expected_config_class, expected_attrs in test_cases:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".toml", delete=False
        ) as tmp_file:
            tmp_file.write(toml_content)
            tmp_file_path = Path(tmp_file.name)

        try:
            # Load the config file
            config = load_config_file(tmp_file_path)

            # Verify the dialect is set correctly
            assert (
                config.engine_dialect == dialect
            ), f"Expected dialect {dialect}, got {config.engine_dialect}"

            # Verify the config object is of the correct type
            assert isinstance(config.engine_config, expected_config_class), (
                f"Expected config type {expected_config_class}, "
                f"got {type(config.engine_config)}"
            )

            # Verify all expected attributes are set correctly
            for attr_name, attr_value in expected_attrs.items():
                actual_value = getattr(config.engine_config, attr_name)
                assert (
                    actual_value == attr_value
                ), f"Expected {attr_name}={attr_value}, got {actual_value}"

        finally:
            # Clean up the temporary file
            tmp_file_path.unlink()
