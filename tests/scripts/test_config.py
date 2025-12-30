import os
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
from trilogy.execution.config import apply_env_vars, load_config_file, load_env_file
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


def test_load_env_file():
    """Test loading environment variables from a .env file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        env_file = tmppath / ".env"
        env_file.write_text(
            """
# Comment line
TRILOGY_TEST_VAR1=value1
TRILOGY_TEST_VAR2="quoted value"
TRILOGY_TEST_VAR3='single quoted'
TRILOGY_TEST_VAR4=value=with=equals

# Empty line above
TRILOGY_TEST_VAR5=
"""
        )

        env_vars = load_env_file(env_file)

        assert env_vars["TRILOGY_TEST_VAR1"] == "value1"
        assert env_vars["TRILOGY_TEST_VAR2"] == "quoted value"
        assert env_vars["TRILOGY_TEST_VAR3"] == "single quoted"
        assert env_vars["TRILOGY_TEST_VAR4"] == "value=with=equals"
        assert env_vars["TRILOGY_TEST_VAR5"] == ""


def test_load_env_file_not_found():
    """Test that loading a non-existent env file raises FileNotFoundError."""
    import pytest

    with pytest.raises(FileNotFoundError):
        load_env_file(Path("/nonexistent/.env"))


def test_apply_env_vars():
    """Test applying environment variables to os.environ."""
    test_key = "TRILOGY_TEST_APPLY_VAR"
    original_value = os.environ.get(test_key)

    try:
        apply_env_vars({test_key: "test_value"})
        assert os.environ[test_key] == "test_value"
    finally:
        if original_value is None:
            os.environ.pop(test_key, None)
        else:
            os.environ[test_key] = original_value


def test_config_env_file_single():
    """Test loading env_file from config as single string."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        # Create .env file
        env_file = tmppath / ".env.local"
        env_file.write_text("TRILOGY_CONFIG_TEST=from_env_file\n")

        config_content = """
[engine]
dialect = "duckdb"
env_file = ".env.local"
"""
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(config_content)

        config = load_config_file(config_file)

        assert len(config.env_files) == 1
        assert config.env_files[0] == tmppath / ".env.local"


def test_config_env_file_list():
    """Test loading env_file from config as list."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        config_content = """
[engine]
dialect = "duckdb"
env_file = [".env", ".env.local"]
"""
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(config_content)

        # Create the env files
        (tmppath / ".env").write_text("VAR1=value1\n")
        (tmppath / ".env.local").write_text("VAR2=value2\n")

        config = load_config_file(config_file)

        assert len(config.env_files) == 2
        assert config.env_files[0] == tmppath / ".env"
        assert config.env_files[1] == tmppath / ".env.local"


def test_cli_env_option():
    """Test --env CLI option sets environment variables."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        test_script = tmppath / "test.preql"
        test_script.write_text("select 1 as value;")

        test_key = "TRILOGY_CLI_ENV_TEST"
        original_value = os.environ.get(test_key)

        try:
            runner = CliRunner()
            result = runner.invoke(
                cli,
                [
                    "run",
                    str(test_script),
                    "duckdb",
                    "--env",
                    f"{test_key}=cli_value",
                ],
            )

            if result.exception:
                raise AssertionError(
                    f"Command failed:\nstdout:\n{result.stdout}\nexc:\n{result.exception}"
                )
            assert result.exit_code == 0
        finally:
            if original_value is None:
                os.environ.pop(test_key, None)
            else:
                os.environ[test_key] = original_value


def test_cli_env_multiple_options():
    """Test multiple --env CLI options."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        test_script = tmppath / "test.preql"
        test_script.write_text("select 1 as value;")

        test_keys = ["TRILOGY_MULTI_ENV_1", "TRILOGY_MULTI_ENV_2"]
        original_values = {k: os.environ.get(k) for k in test_keys}

        try:
            runner = CliRunner()
            result = runner.invoke(
                cli,
                [
                    "run",
                    str(test_script),
                    "duckdb",
                    "--env",
                    f"{test_keys[0]}=value1",
                    "--env",
                    f"{test_keys[1]}=value2",
                ],
            )

            if result.exception:
                raise AssertionError(
                    f"Command failed:\nstdout:\n{result.stdout}\nexc:\n{result.exception}"
                )
            assert result.exit_code == 0
        finally:
            for key in test_keys:
                if original_values[key] is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = original_values[key]


def test_env_file_and_cli_precedence():
    """Test that CLI --env takes precedence over env_file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        test_key = "TRILOGY_PRECEDENCE_TEST"
        original_value = os.environ.get(test_key)

        # Create .env file with initial value
        env_file = tmppath / ".env"
        env_file.write_text(f"{test_key}=from_file\n")

        config_content = """
[engine]
dialect = "duckdb"
env_file = ".env"
"""
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(config_content)

        test_script = tmppath / "test.preql"
        test_script.write_text("select 1 as value;")

        try:
            runner = CliRunner()
            result = runner.invoke(
                cli,
                [
                    "run",
                    str(test_script),
                    "--config",
                    str(config_file),
                    "--env",
                    f"{test_key}=from_cli",
                ],
            )

            if result.exception:
                raise AssertionError(
                    f"Command failed:\nstdout:\n{result.stdout}\nexc:\n{result.exception}"
                )
            assert result.exit_code == 0
            # CLI value should take precedence
            assert os.environ.get(test_key) == "from_cli"
        finally:
            if original_value is None:
                os.environ.pop(test_key, None)
            else:
                os.environ[test_key] = original_value
