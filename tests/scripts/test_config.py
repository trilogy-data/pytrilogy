import os
import tempfile
from pathlib import Path

from click.exceptions import Exit
from click.testing import CliRunner
from pytest import raises

from trilogy.ai.enums import Provider
from trilogy.dialect.config import (
    BigQueryConfig,
    DuckDBConfig,
    PostgresConfig,
    PrestoConfig,
    SnowflakeConfig,
    SQLiteConfig,
    SQLServerConfig,
)
from trilogy.dialect.enums import Dialects
from trilogy.execution.config import (
    DEFAULT_PARALLELISM,
    DEFAULT_STUDIO_URL,
    apply_env_vars,
    audit_config_file,
    load_config_file,
    load_env_file,
)
from trilogy.scripts.common import handle_execution_exception
from trilogy.scripts.trilogy import cli


def test_handle_execution_exception():
    with raises(Exit) as excinfo:
        handle_execution_exception(Exit(3))
    assert excinfo.value.exit_code == 3
    with raises(Exit) as excinfo:
        handle_execution_exception(ValueError("test error"))


def test_handle_execution_exception_labels_syntax_errors(capsys):
    """Syntax/validation errors are labelled `Syntax error:`; everything else
    stays `Unexpected error:`."""
    from trilogy.core.exceptions import InvalidSyntaxException

    for exc in (SyntaxError("bad having"), InvalidSyntaxException("Syntax [104]: …")):
        with raises(Exit):
            handle_execution_exception(exc)
        out = capsys.readouterr()
        combined = out.out + out.err
        assert "Syntax error:" in combined, combined
        assert "Unexpected error:" not in combined, combined

    with raises(Exit):
        handle_execution_exception(ValueError("boom"))
    combined = "".join(capsys.readouterr())
    assert "Unexpected error:" in combined, combined
    assert "Syntax error:" not in combined, combined


def test_handle_execution_exception_labels_resolution_errors(capsys):
    """A disconnected/unresolvable query is a fixable modeling mistake, labelled
    `Resolution error:` rather than `Unexpected error:`."""
    from trilogy.core.exceptions import (
        DisconnectedConceptsException,
        UnresolvableQueryException,
    )

    errs = (
        DisconnectedConceptsException("missing join", subgraphs=[["a"], ["b"]]),
        UnresolvableQueryException("no path"),
    )
    for exc in errs:
        with raises(Exit):
            handle_execution_exception(exc)
        combined = "".join(capsys.readouterr())
        assert "Resolution error:" in combined, combined
        assert "Unexpected error:" not in combined, combined


def test_handle_execution_exception_labels_recursion_errors(capsys):
    """A planner RecursionError is always a framework bug, not an opaque crash —
    labelled `Resolution error:` and stated plainly as a bug, not `Unexpected
    error: maximum recursion depth exceeded`."""
    with raises(Exit):
        handle_execution_exception(RecursionError("maximum recursion depth exceeded"))
    combined = "".join(capsys.readouterr())
    assert "Resolution error:" in combined, combined
    assert "Unexpected error:" not in combined, combined
    assert "this is a bug" in combined, combined


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
db_location = "configured.db"
"""
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(config_content)

        test_script = tmppath / "test.preql"
        test_script.write_text("select 1 as value;")
        configured_db_path = tmppath / "configured.db"
        db_path = tmppath / "override.db"

        runner = CliRunner()
        result = runner.invoke(
            cli, ["run", str(test_script), "duckdb", f"path={db_path.as_posix()}"]
        )

        if result.exception:
            raise AssertionError(
                f"Command failed:\nstdout:\n{result.stdout}\nexc:\n{result.exception}"
            )
        assert result.exit_code == 0
        assert db_path.exists()
        assert not configured_db_path.exists()


def test_config_staging_default():
    """Test that staging defaults to None path when not in config."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_file = tmppath / "trilogy.toml"
        config_file.write_text('[engine]\ndialect = "duckdb"\n')

        config = load_config_file(config_file)
        assert config.staging.path is None


def test_config_staging_local():
    """Test parsing local staging path from config."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(
            '[engine]\ndialect = "duckdb"\n\n[staging]\npath = "/tmp/my-staging"\n'
        )

        config = load_config_file(config_file)
        assert config.staging.path == "/tmp/my-staging"


def test_config_staging_gcs():
    """Test parsing GCS staging path from config."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(
            '[engine]\ndialect = "duckdb"\n\n[staging]\npath = "gs://bucket/prefix"\n'
        )

        config = load_config_file(config_file)
        assert config.staging.path == "gs://bucket/prefix"


def test_config_staging_s3():
    """Test parsing S3 staging path from config."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(
            '[engine]\ndialect = "duckdb"\n\n[staging]\npath = "s3://bucket/prefix"\n'
        )

        config = load_config_file(config_file)
        assert config.staging.path == "s3://bucket/prefix"


def test_config_types():

    test_cases = [
        (
            Dialects.DUCK_DB,
            """
[engine]
dialect = "duck_db"

[engine.config]
enable_spatial = true
""",
            DuckDBConfig,
            {"enable_spatial": True},
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
        (
            Dialects.SQLITE,
            """
[engine]
dialect = "sqlite"

[engine.config]
db_location = "test.sqlite"
""",
            SQLiteConfig,
            {},  # path checked separately since it resolves to absolute
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


def test_duckdb_path_resolution():
    """Test that DuckDB path in config is resolved relative to the toml file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(
            '[engine]\ndialect = "duck_db"\n\n[engine.config]\ndb_location = "my.duckdb"\n'
        )

        config = load_config_file(config_file)
        assert isinstance(config.engine_config, DuckDBConfig)
        expected = str((tmppath / "my.duckdb").resolve())
        assert config.engine_config.path == expected


def test_sqlite_path_resolution():
    """Test that SQLite path in config is resolved relative to the toml file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(
            '[engine]\ndialect = "sqlite"\n\n[engine.config]\ndb_location = "chinook.db"\n'
        )

        config = load_config_file(config_file)
        assert isinstance(config.engine_config, SQLiteConfig)
        expected = str((tmppath / "chinook.db").resolve())
        assert config.engine_config.path == expected


def test_sqlite_no_path():
    """Test that SQLite config without path defaults to in-memory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_file = tmppath / "trilogy.toml"
        config_file.write_text('[engine]\ndialect = "sqlite"\n')

        config = load_config_file(config_file)
        assert config.engine_config is None


def test_sqlite_remote_downloads_to_staging(tmp_path, monkeypatch):
    """Test that a remote SQLite db_location downloads to staging and opens read-only."""
    # Create a real sqlite db to serve as the "remote" content
    import sqlite3

    src_db = tmp_path / "source.db"
    conn = sqlite3.connect(str(src_db))
    conn.execute("CREATE TABLE t (id INTEGER)")
    conn.execute("INSERT INTO t VALUES (1)")
    conn.commit()
    conn.close()

    # Mock urlretrieve to copy the local db instead of hitting the network
    def fake_urlretrieve(url: str, dest: str) -> None:
        import shutil

        shutil.copy2(str(src_db), dest)

    monkeypatch.setattr(
        "trilogy.dialect.config.urllib.request.urlretrieve", fake_urlretrieve
    )

    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()

    config = SQLiteConfig(
        path="https://example.com/remote.db", staging_path=str(staging_dir)
    )

    # Downloaded to staging dir
    assert config.path is not None
    assert str(staging_dir) in config.path
    assert config._remote is True

    # Connection string is bare sqlite:// (creator handles the rest)
    assert config.connection_string() == "sqlite://"
    engine_args = config.create_engine_args()
    assert "creator" in engine_args

    # Actually queryable via sqlalchemy
    from sqlalchemy import create_engine, text

    engine = create_engine(config.connection_string(), **engine_args)
    with engine.connect() as c:
        result = c.execute(text("SELECT count(*) FROM t")).scalar()
        assert result == 1


def test_sqlite_remote_uses_system_temp_without_staging(tmp_path, monkeypatch):
    """Test that remote SQLite falls back to system temp when no staging_path."""
    import sqlite3

    src_db = tmp_path / "source.db"
    conn = sqlite3.connect(str(src_db))
    conn.execute("CREATE TABLE t (id INTEGER)")
    conn.close()

    def fake_urlretrieve(url: str, dest: str) -> None:
        import shutil

        shutil.copy2(str(src_db), dest)

    monkeypatch.setattr(
        "trilogy.dialect.config.urllib.request.urlretrieve", fake_urlretrieve
    )

    config = SQLiteConfig(path="gs://bucket/data.db")

    assert config._remote is True
    assert config.path is not None
    # Should be in system temp, not in any custom staging dir
    assert tempfile.gettempdir() in config.path


def test_sqlite_local_path_not_remote():
    """Test that local paths don't trigger remote behavior."""
    config = SQLiteConfig(path="/tmp/local.db")
    assert config._remote is False
    assert config.path == "/tmp/local.db"
    assert config.connection_string() == "sqlite:////tmp/local.db"
    assert config.create_connect_args() == {}


def test_duckdb_remote_path_passthrough():
    """Test that remote URIs are not resolved as local paths."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(
            '[engine]\ndialect = "duck_db"\n\n[engine.config]\ndb_location = "gs://my-bucket/data.duckdb"\n'
        )

        config = load_config_file(config_file)
        assert isinstance(config.engine_config, DuckDBConfig)
        assert config.engine_config.path == "gs://my-bucket/data.duckdb"


def test_load_env_file():
    """Test loading environment variables from a .env file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        env_file = tmppath / ".env"
        env_file.write_text("""
# Comment line
TRILOGY_TEST_VAR1=value1
TRILOGY_TEST_VAR2="quoted value"
TRILOGY_TEST_VAR3='single quoted'
TRILOGY_TEST_VAR4=value=with=equals

# Empty line above
TRILOGY_TEST_VAR5=
""")

        env_vars = load_env_file(env_file)

        assert env_vars["TRILOGY_TEST_VAR1"] == "value1"
        assert env_vars["TRILOGY_TEST_VAR2"] == "quoted value"
        assert env_vars["TRILOGY_TEST_VAR3"] == "single quoted"
        assert env_vars["TRILOGY_TEST_VAR4"] == "value=with=equals"
        assert env_vars["TRILOGY_TEST_VAR5"] == ""


def test_load_env_file_not_found():
    """Test that loading a non-existent env file returns None."""
    result = load_env_file(Path("/nonexistent/.env"))
    assert result is None


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
env_file = ".env.local"

[engine]
dialect = "duckdb"
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
env_file = [".env", ".env.local"]

[engine]
dialect = "duckdb"
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


def test_config_env_file_legacy_engine_section():
    """env_file under [engine] still works for backwards compat."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_content = """
[engine]
dialect = "duckdb"
env_file = ".env.legacy"
"""
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(config_content)
        (tmppath / ".env.legacy").write_text("X=1\n")

        config = load_config_file(config_file)
        assert config.env_files == [tmppath / ".env.legacy"]


def test_config_env_file_top_level_wins_over_engine():
    """Top-level env_file takes precedence over [engine].env_file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_content = """
env_file = ".env.top"

[engine]
dialect = "duckdb"
env_file = ".env.engine"
"""
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(config_content)

        config = load_config_file(config_file)
        assert config.env_files == [tmppath / ".env.top"]


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


def test_cli_env_file_option():
    """Test --env supports loading variables from a file path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        test_script = tmppath / "test.preql"
        test_script.write_text("select 1 as value;")

        env_file = tmppath / ".env.runtime"
        test_key = "TRILOGY_CLI_ENV_FILE_TEST"
        env_file.write_text(f"{test_key}=from_file\n")

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
                    str(env_file),
                ],
            )

            if result.exception:
                raise AssertionError(
                    f"Command failed:\nstdout:\n{result.stdout}\nexc:\n{result.exception}"
                )
            assert result.exit_code == 0
            assert os.environ.get(test_key) == "from_file"
        finally:
            if original_value is None:
                os.environ.pop(test_key, None)
            else:
                os.environ[test_key] = original_value


def test_cli_env_file_and_kv_order_precedence():
    """Test --env entries apply in order when mixing file paths and KEY=VALUE."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        test_script = tmppath / "test.preql"
        test_script.write_text("select 1 as value;")

        env_file = tmppath / ".env.runtime"
        test_key = "TRILOGY_CLI_ENV_MIXED_TEST"
        env_file.write_text(f"{test_key}=from_file\n")

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
                    str(env_file),
                    "--env",
                    f"{test_key}=from_kv",
                ],
            )

            if result.exception:
                raise AssertionError(
                    f"Command failed:\nstdout:\n{result.stdout}\nexc:\n{result.exception}"
                )
            assert result.exit_code == 0
            assert os.environ.get(test_key) == "from_kv"
        finally:
            if original_value is None:
                os.environ.pop(test_key, None)
            else:
                os.environ[test_key] = original_value


def test_cli_env_invalid_non_kv_and_missing_file():
    """Test --env error for values that are neither KEY=VALUE nor existing files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        test_script = tmppath / "test.preql"
        test_script.write_text("select 1 as value;")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "run",
                str(test_script),
                "duckdb",
                "--env",
                "not-a-kv-or-existing-file",
            ],
        )

        assert result.exit_code != 0
        assert "KEY=VALUE format" in result.output
        assert "path to an existing" in result.output


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
env_file = ".env"

[engine]
dialect = "duckdb"
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


def test_config_serve_defaults():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_file = tmppath / "trilogy.toml"
        config_file.write_text('[engine]\ndialect = "duckdb"\n')

        config = load_config_file(config_file)
        assert config.serve_studio_url == DEFAULT_STUDIO_URL
        assert config.serve_connection is None


def test_config_serve_studio_url_override():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_file = tmppath / "trilogy.toml"
        config_file.write_text('[serve]\nstudio_url = "https://studio.example.com/"\n')

        config = load_config_file(config_file)
        assert config.serve_studio_url == "https://studio.example.com/"


def test_config_serve_connection_parses_type_and_options():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(
            "[serve.connection]\n"
            'type = "snowflake"\n\n'
            "[serve.connection.options]\n"
            'account = "acme"\n'
            'warehouse = "wh"\n'
        )

        config = load_config_file(config_file)
        assert config.serve_connection is not None
        assert config.serve_connection.type == "snowflake"
        assert config.serve_connection.options == {
            "account": "acme",
            "warehouse": "wh",
        }


def test_config_serve_connection_coerces_non_string_options():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_file = tmppath / "trilogy.toml"
        config_file.write_text(
            "[serve.connection]\n"
            'type = "postgres"\n\n'
            "[serve.connection.options]\n"
            "port = 5432\n"
        )

        config = load_config_file(config_file)
        assert config.serve_connection is not None
        assert config.serve_connection.options == {"port": "5432"}


def test_config_serve_connection_missing_type_raises():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        config_file = tmppath / "trilogy.toml"
        config_file.write_text("[serve.connection.options]\n" 'account = "acme"\n')

        with raises(ValueError, match="type"):
            load_config_file(config_file)


def test_nonexistent_file_error_before_dialect_error():
    """Test that nonexistent file error is raised before dialect error."""
    runner = CliRunner()

    # Test with .preql extension
    result = runner.invoke(cli, ["run", "nonexistent.preql"])
    assert result.exit_code != 0
    assert "does not exist" in result.output
    assert "No dialect specified" not in result.output

    # Test with path separator
    result = runner.invoke(cli, ["run", "some/path/file"])
    assert result.exit_code != 0
    assert "does not exist" in result.output
    assert "No dialect specified" not in result.output

    # Test with .sql extension
    result = runner.invoke(cli, ["run", "nonexistent.sql"])
    assert result.exit_code != 0
    assert "does not exist" in result.output
    assert "No dialect specified" not in result.output


def test_agent_config_defaults_when_section_missing():
    with tempfile.TemporaryDirectory() as tmp:
        toml_path = Path(tmp) / "trilogy.toml"
        toml_path.write_text("")
        config = load_config_file(toml_path)
    assert config.agent.provider is None
    assert config.agent.model is None
    assert config.agent.api_key_env is None
    assert config.agent.max_iterations == 50
    assert config.agent.tool_output_limit == 8192


def test_agent_config_populated():
    content = """
[agent]
provider = "openai"
model = "gpt-x"
api_key_env = "MY_KEY"
max_iterations = 12
tool_output_limit = 1024
"""
    with tempfile.TemporaryDirectory() as tmp:
        toml_path = Path(tmp) / "trilogy.toml"
        toml_path.write_text(content)
        config = load_config_file(toml_path)
    assert config.agent.provider == Provider.OPENAI
    assert config.agent.model == "gpt-x"
    assert config.agent.api_key_env == "MY_KEY"
    assert config.agent.max_iterations == 12
    assert config.agent.tool_output_limit == 1024


def test_agent_config_invalid_provider_raises():
    content = '[agent]\nprovider = "claude"\n'
    with tempfile.TemporaryDirectory() as tmp:
        toml_path = Path(tmp) / "trilogy.toml"
        toml_path.write_text(content)
        with raises(ValueError, match="Unknown agent provider 'claude'"):
            load_config_file(toml_path)


def test_agent_config_provider_case_insensitive():
    content = '[agent]\nprovider = "ANTHROPIC"\n'
    with tempfile.TemporaryDirectory() as tmp:
        toml_path = Path(tmp) / "trilogy.toml"
        toml_path.write_text(content)
        config = load_config_file(toml_path)
    assert config.agent.provider == Provider.ANTHROPIC


# -----------------------------------------------------------------------------
# --parallelism CLI flag
# -----------------------------------------------------------------------------


def test_cli_parallelism_rejects_non_int():
    """Click should reject non-int --parallelism for unit/integration/run/refresh."""
    runner = CliRunner()
    for cmd in ["integration", "unit", "run", "refresh"]:
        result = runner.invoke(cli, [cmd, "model.preql", "--parallelism", "abc"])
        assert result.exit_code != 0, f"{cmd} should reject non-int parallelism"
        assert "not a valid integer" in result.output


def test_cli_parallelism_accepts_int(tmp_path, monkeypatch):
    """An int --parallelism flows into CLIRuntimeParams."""
    from trilogy.scripts import testing

    captured: dict = {}

    def fake_run_parallel_execution(**kwargs):
        captured["parallelism"] = kwargs["cli_params"].parallelism
        from trilogy.scripts.parallel_execution import ParallelExecutionSummary

        return ParallelExecutionSummary(
            total_scripts=0,
            successful=0,
            skipped=0,
            failed=0,
            total_duration=0.0,
            results=[],
        )

    monkeypatch.setattr(testing, "run_parallel_execution", fake_run_parallel_execution)
    test_file = tmp_path / "x.preql"
    test_file.write_text("select 1 as v;")

    runner = CliRunner()
    result = runner.invoke(
        cli, ["integration", str(test_file), "duckdb", "--parallelism", "7"]
    )
    assert result.exit_code == 0, result.output
    assert captured["parallelism"] == 7
    assert isinstance(captured["parallelism"], int)


# -----------------------------------------------------------------------------
# parallelism in trilogy.toml
# -----------------------------------------------------------------------------


def test_config_parallelism_under_engine_section(tmp_path):
    """[engine].parallelism is the canonical location and is applied."""
    toml_path = tmp_path / "trilogy.toml"
    toml_path.write_text('[engine]\ndialect = "duckdb"\nparallelism = 9\n')
    config = load_config_file(toml_path)
    assert config.parallelism == 9


def test_config_parallelism_top_level_fallback(tmp_path):
    """Top-level parallelism still works for prior installs."""
    toml_path = tmp_path / "trilogy.toml"
    toml_path.write_text('parallelism = 11\n[engine]\ndialect = "duckdb"\n')
    config = load_config_file(toml_path)
    assert config.parallelism == 11


def test_config_parallelism_engine_takes_precedence(tmp_path):
    """When both locations are set, [engine].parallelism wins."""
    toml_path = tmp_path / "trilogy.toml"
    toml_path.write_text(
        'parallelism = 1\n[engine]\ndialect = "duckdb"\nparallelism = 9\n'
    )
    config = load_config_file(toml_path)
    assert config.parallelism == 9


def test_config_parallelism_defaults_when_absent(tmp_path):
    toml_path = tmp_path / "trilogy.toml"
    toml_path.write_text('[engine]\ndialect = "duckdb"\n')
    config = load_config_file(toml_path)
    assert config.parallelism == DEFAULT_PARALLELISM


def test_config_parallelism_applies_through_cli(tmp_path, monkeypatch):
    """Config-file parallelism flows to the parallel-execution worker pool."""
    from trilogy.scripts import testing

    toml_path = tmp_path / "trilogy.toml"
    toml_path.write_text('[engine]\ndialect = "duckdb"\nparallelism = 6\n')

    test_file = tmp_path / "x.preql"
    test_file.write_text("select 1 as v;")

    captured: dict = {}

    def fake_run_parallel_execution(**kwargs):
        from trilogy.scripts.common import (
            merge_runtime_config,
            resolve_input_information,
        )
        from trilogy.scripts.parallel_execution import ParallelExecutionSummary

        cli_params = kwargs["cli_params"]
        _, _, _, _, file_config = resolve_input_information(
            cli_params.input, cli_params.config_path
        )
        _, parallelism = merge_runtime_config(cli_params, file_config)
        captured["parallelism"] = parallelism
        return ParallelExecutionSummary(
            total_scripts=0,
            successful=0,
            skipped=0,
            failed=0,
            total_duration=0.0,
            results=[],
        )

    monkeypatch.setattr(testing, "run_parallel_execution", fake_run_parallel_execution)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["integration", str(test_file), "duckdb", "--config", str(toml_path)],
    )
    assert result.exit_code == 0, result.output
    assert captured["parallelism"] == 6


def test_cli_parallelism_overrides_config(tmp_path, monkeypatch):
    """--parallelism takes precedence over the value in the config file."""
    from trilogy.scripts import testing

    toml_path = tmp_path / "trilogy.toml"
    toml_path.write_text('[engine]\ndialect = "duckdb"\nparallelism = 6\n')

    test_file = tmp_path / "x.preql"
    test_file.write_text("select 1 as v;")

    captured: dict = {}

    def fake_run_parallel_execution(**kwargs):
        from trilogy.scripts.common import (
            merge_runtime_config,
            resolve_input_information,
        )
        from trilogy.scripts.parallel_execution import ParallelExecutionSummary

        cli_params = kwargs["cli_params"]
        _, _, _, _, file_config = resolve_input_information(
            cli_params.input, cli_params.config_path
        )
        _, parallelism = merge_runtime_config(cli_params, file_config)
        captured["parallelism"] = parallelism
        return ParallelExecutionSummary(
            total_scripts=0,
            successful=0,
            skipped=0,
            failed=0,
            total_duration=0.0,
            results=[],
        )

    monkeypatch.setattr(testing, "run_parallel_execution", fake_run_parallel_execution)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "integration",
            str(test_file),
            "duckdb",
            "--config",
            str(toml_path),
            "--parallelism",
            "2",
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured["parallelism"] == 2


# -----------------------------------------------------------------------------
# trilogy.toml field audit
# -----------------------------------------------------------------------------


def test_audit_clean_config_produces_no_warnings(tmp_path):
    toml_path = tmp_path / "trilogy.toml"
    toml_path.write_text(
        "parallelism = 2\n"
        'env_file = ".env"\n\n'
        "[engine]\n"
        'dialect = "duckdb"\n'
        "parallelism = 3\n\n"
        "[engine.config]\n"
        "enable_spatial = true\n\n"
        "[setup]\n"
        "sql = []\n"
        "trilogy = []\n\n"
        "[staging]\n"
        'path = "/tmp/stg"\n\n'
        "[serve]\n"
        'studio_url = "https://example.com"\n\n'
        "[serve.connection]\n"
        'type = "duckdb"\n\n'
        "[serve.connection.options]\n"
        'arbitrary = "ok"\n\n'
        "[project]\n"
        'name = "x"\n\n'
        "[agent]\n"
        'provider = "openai"\n'
    )
    assert audit_config_file(toml_path) == []


def test_audit_warns_unknown_top_level_key(tmp_path):
    toml_path = tmp_path / "trilogy.toml"
    toml_path.write_text('typo_field = 1\n[engine]\ndialect = "duckdb"\n')
    warnings = audit_config_file(toml_path)
    assert len(warnings) == 1
    assert "typo_field" in warnings[0]
    assert "top-level" in warnings[0]


def test_audit_warns_unknown_section_key(tmp_path):
    toml_path = tmp_path / "trilogy.toml"
    toml_path.write_text('[engine]\ndialect = "duckdb"\nbogus = 1\n')
    warnings = audit_config_file(toml_path)
    assert len(warnings) == 1
    assert "bogus" in warnings[0]
    assert "[engine]" in warnings[0]


def test_audit_skips_engine_config_section(tmp_path):
    """Per-dialect config keys are not audited; the dialect dataclass enforces them."""
    toml_path = tmp_path / "trilogy.toml"
    toml_path.write_text(
        '[engine]\ndialect = "duckdb"\n\n'
        "[engine.config]\n"
        "enable_spatial = true\n"
        'unknown_to_duckdb = "but ignored by audit"\n'
    )
    assert audit_config_file(toml_path) == []


def test_audit_skips_serve_connection_options(tmp_path):
    toml_path = tmp_path / "trilogy.toml"
    toml_path.write_text(
        "[serve.connection]\n"
        'type = "snowflake"\n\n'
        "[serve.connection.options]\n"
        'anything_goes = "fine"\n'
    )
    assert audit_config_file(toml_path) == []


def test_audit_collects_multiple_warnings(tmp_path):
    toml_path = tmp_path / "trilogy.toml"
    toml_path.write_text(
        "typo_field = 1\n\n"
        "[engine]\n"
        'dialect = "duckdb"\n'
        'rogue = "x"\n\n'
        "[agent]\n"
        "nonsense = 1\n"
    )
    warnings = audit_config_file(toml_path)
    fields = sorted(w.split("'")[1] for w in warnings)
    assert fields == ["nonsense", "rogue", "typo_field"]


def test_integration_cli_warns_about_unknown_config_fields(tmp_path, monkeypatch):
    """Running `trilogy integration` surfaces audit warnings."""
    from trilogy.scripts import testing

    toml_path = tmp_path / "trilogy.toml"
    toml_path.write_text('[engine]\ndialect = "duckdb"\nbogus_key = 1\n')

    test_file = tmp_path / "x.preql"
    test_file.write_text("select 1 as v;")

    def fake_run_parallel_execution(**kwargs):
        from trilogy.scripts.parallel_execution import ParallelExecutionSummary

        return ParallelExecutionSummary(
            total_scripts=0,
            successful=0,
            skipped=0,
            failed=0,
            total_duration=0.0,
            results=[],
        )

    monkeypatch.setattr(testing, "run_parallel_execution", fake_run_parallel_execution)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["integration", str(test_file), "duckdb", "--config", str(toml_path)],
    )
    assert result.exit_code == 0, result.output
    assert "bogus_key" in result.output


def test_unit_cli_warns_about_unknown_config_fields(tmp_path, monkeypatch):
    """Running `trilogy unit` surfaces audit warnings."""
    from trilogy.scripts import testing

    toml_path = tmp_path / "trilogy.toml"
    toml_path.write_text('[engine]\ndialect = "duckdb"\n\n[agent]\nnonsense = 1\n')

    test_file = tmp_path / "x.preql"
    test_file.write_text("select 1 as v;")

    def fake_run_parallel_execution(**kwargs):
        from trilogy.scripts.parallel_execution import ParallelExecutionSummary

        return ParallelExecutionSummary(
            total_scripts=0,
            successful=0,
            skipped=0,
            failed=0,
            total_duration=0.0,
            results=[],
        )

    monkeypatch.setattr(testing, "run_parallel_execution", fake_run_parallel_execution)

    runner = CliRunner()
    result = runner.invoke(cli, ["unit", str(test_file), "--config", str(toml_path)])
    assert result.exit_code == 0, result.output
    assert "nonsense" in result.output


def test_integration_cli_no_warnings_for_clean_config(tmp_path, monkeypatch):
    """No spurious audit warnings when the config is clean."""
    from trilogy.scripts import testing

    toml_path = tmp_path / "trilogy.toml"
    toml_path.write_text('[engine]\ndialect = "duckdb"\nparallelism = 2\n')

    test_file = tmp_path / "x.preql"
    test_file.write_text("select 1 as v;")

    def fake_run_parallel_execution(**kwargs):
        from trilogy.scripts.parallel_execution import ParallelExecutionSummary

        return ParallelExecutionSummary(
            total_scripts=0,
            successful=0,
            skipped=0,
            failed=0,
            total_duration=0.0,
            results=[],
        )

    monkeypatch.setattr(testing, "run_parallel_execution", fake_run_parallel_execution)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["integration", str(test_file), "duckdb", "--config", str(toml_path)],
    )
    assert result.exit_code == 0, result.output
    assert "Unknown trilogy.toml" not in result.output
