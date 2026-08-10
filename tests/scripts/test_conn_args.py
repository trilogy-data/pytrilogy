"""Connection-argument parsing and validation for CLI commands.

Connection args must never be silently dropped: `key=value` tokens parse the
same as `key value` pairs, a dangling key errors, and unknown keys for a
dialect are a hard error against its allowed-kwargs list.
"""

from pathlib import Path

from click.testing import CliRunner
from pytest import mark, raises

from trilogy.core.exceptions import ConfigurationException
from trilogy.dialect.config import (
    BigQueryConfig,
    DuckDBConfig,
    MySQLConfig,
    PostgresConfig,
    PrestoConfig,
    SnowflakeConfig,
    SQLServerConfig,
)
from trilogy.dialect.enums import Dialects
from trilogy.execution.config import RuntimeConfig
from trilogy.scripts.common import (
    get_dialect_config,
    validate_required_connection_params,
)
from trilogy.scripts.environment import extra_to_kwargs
from trilogy.scripts.trilogy import cli


def _runtime_config() -> RuntimeConfig:
    return RuntimeConfig(startup_trilogy=[], startup_sql=[])


def test_extra_to_kwargs_space_separated_pairs():
    assert extra_to_kwargs(("path", "memory.duckdb")) == {"path": "memory.duckdb"}


def test_extra_to_kwargs_equals_form():
    assert extra_to_kwargs(("path=memory.duckdb",)) == {"path": "memory.duckdb"}


def test_extra_to_kwargs_mixed_forms():
    assert extra_to_kwargs(("path=x.db", "enable_gcs", "true", "port=5432")) == {
        "path": "x.db",
        "enable_gcs": True,
        "port": 5432,
    }


def test_extra_to_kwargs_strips_flag_prefix():
    assert extra_to_kwargs(("--path", "x.db", "--port=1")) == {
        "path": "x.db",
        "port": 1,
    }


def test_extra_to_kwargs_value_may_contain_equals():
    assert extra_to_kwargs(("password", "a=b")) == {"password": "a=b"}
    assert extra_to_kwargs(("password=a=b",)) == {"password": "a=b"}


def test_extra_to_kwargs_dangling_key_errors():
    with raises(ValueError, match="has no value"):
        extra_to_kwargs(("path",))
    with raises(ValueError, match="has no value"):
        extra_to_kwargs(("path=x.db", "enable_gcs"))


def test_validate_unknown_key_errors_with_valid_list():
    with raises(
        ConfigurationException, match="Unknown DuckDB.*bogus.*Valid parameters"
    ):
        validate_required_connection_params({"bogus": 1}, [], ["path"], "DuckDB")


def test_validate_missing_required_errors():
    with raises(ConfigurationException, match="Missing required Postgres.*host"):
        validate_required_connection_params(
            {"port": 5432}, ["host", "port"], [], "Postgres"
        )


def test_validate_passes_through_known_keys():
    conn = {"path": "x.db", "enable_gcs": True}
    assert (
        validate_required_connection_params(conn, [], ["path", "enable_gcs"], "DuckDB")
        == conn
    )


def test_get_dialect_config_duckdb_applies_path():
    conf = get_dialect_config(
        Dialects.DUCK_DB, {"path": "x.db"}, runtime_config=_runtime_config()
    )
    assert isinstance(conf, DuckDBConfig)
    assert conf.path == "x.db"


def test_get_dialect_config_duckdb_unknown_key_errors():
    with raises(ConfigurationException, match="Unknown DuckDB"):
        get_dialect_config(
            Dialects.DUCK_DB, {"bogus": 1}, runtime_config=_runtime_config()
        )


def test_get_dialect_config_mysql():
    conf = get_dialect_config(
        Dialects.MYSQL,
        {
            "host": "localhost",
            "port": 3307,
            "username": "user",
            "password": "password",
            "database": "analytics",
        },
        runtime_config=_runtime_config(),
    )
    assert isinstance(conf, MySQLConfig)
    assert conf.host == "localhost"
    assert conf.port == 3307
    assert conf.database == "analytics"


def test_get_dialect_config_mysql_validates_arguments():
    with raises(ConfigurationException, match="Missing required MySQL.*database"):
        get_dialect_config(
            Dialects.MYSQL,
            {
                "host": "localhost",
                "username": "user",
                "password": "password",
            },
            runtime_config=_runtime_config(),
        )
    with raises(ConfigurationException, match="Unknown MySQL.*bogus"):
        get_dialect_config(
            Dialects.MYSQL,
            {
                "host": "localhost",
                "username": "user",
                "password": "password",
                "database": "analytics",
                "bogus": "value",
            },
            runtime_config=_runtime_config(),
        )


def test_get_dialect_config_mysql_uses_file_config():
    runtime = _runtime_config()
    runtime.engine_config = MySQLConfig(
        host="localhost",
        username="user",
        password="password",
        database="analytics",
    )

    conf = get_dialect_config(Dialects.MYSQL, {}, runtime_config=runtime)

    assert isinstance(conf, MySQLConfig)
    assert conf.database == "analytics"


def test_get_dialect_config_ignores_file_config_for_other_dialect():
    """`trilogy unit` forces DuckDB; a BigQuery [engine.config] must not leak in."""
    runtime = _runtime_config()
    runtime.engine_config = BigQueryConfig(project="some-project")

    conf = get_dialect_config(Dialects.DUCK_DB, {}, runtime_config=runtime)

    assert isinstance(conf, DuckDBConfig)
    Dialects.DUCK_DB.default_engine(conf=conf).dispose()


def test_get_dialect_config_merges_matching_file_config():
    runtime = _runtime_config()
    runtime.engine_config = BigQueryConfig(project="some-project", staging_uri="gs://a")

    conf = get_dialect_config(
        Dialects.BIGQUERY, {"project": "cli-project"}, runtime_config=runtime
    )

    assert isinstance(conf, BigQueryConfig)
    assert conf.project == "cli-project"
    assert conf.staging_uri == "gs://a"


def test_default_engine_type_error_names_expected_config():
    with raises(TypeError, match="expected DuckDBConfig, got BigQueryConfig"):
        Dialects.DUCK_DB.default_engine(conf=BigQueryConfig())


@mark.parametrize(
    "dialect,stored,connection_string",
    [
        (
            Dialects.POSTGRES,
            PostgresConfig(
                host="h", port=5432, username="u", password="p", database="db"
            ),
            "postgresql://u:p@h:5432",
        ),
        (
            Dialects.SQL_SERVER,
            SQLServerConfig(
                host="h", port=1433, username="u", password="p", database="db"
            ),
            "sqlserver//u:p@h:1433",
        ),
        (
            Dialects.SNOWFLAKE,
            SnowflakeConfig(account="a", username="u", password="p", database="d"),
            "snowflake://u:p@a/d",
        ),
        (
            Dialects.PRESTO,
            PrestoConfig(host="h", port=8080, username="u", password="p", catalog="c"),
            "presto://u:p@h:8080/c",
        ),
    ],
)
def test_get_dialect_config_file_config_satisfies_required_params(
    dialect, stored, connection_string
):
    """A complete [engine.config] is enough on its own — required params are
    validated against the file config too, not only the CLI args."""
    runtime = _runtime_config()
    runtime.engine_config = stored

    conf = get_dialect_config(dialect, {}, runtime_config=runtime)

    assert conf.connection_string() == connection_string


def test_get_dialect_config_cli_args_win_over_file_config():
    runtime = _runtime_config()
    runtime.engine_config = PostgresConfig(
        host="h", port=5432, username="u", password="p", database="db"
    )

    conf = get_dialect_config(
        Dialects.POSTGRES, {"host": "cli-host"}, runtime_config=runtime
    )

    assert conf.host == "cli-host"
    assert conf.port == 5432


def test_get_dialect_config_file_config_for_other_dialect_does_not_seed():
    runtime = _runtime_config()
    runtime.engine_config = BigQueryConfig(project="some-project")

    with raises(ConfigurationException, match="Missing required Postgres"):
        get_dialect_config(Dialects.POSTGRES, {}, runtime_config=runtime)


def test_get_dialect_config_unsupported_dialect_with_args_errors():
    with raises(ConfigurationException, match="does not accept connection parameters"):
        get_dialect_config(
            Dialects.DATAFRAME, {"path": "x"}, runtime_config=_runtime_config()
        )


def test_get_dialect_config_trino_and_clickhouse_validated():
    with raises(ConfigurationException, match="Missing required Trino"):
        get_dialect_config(
            Dialects.TRINO, {"host": "h"}, runtime_config=_runtime_config()
        )
    with raises(ConfigurationException, match="Unknown ClickHouse"):
        get_dialect_config(
            Dialects.CLICKHOUSE, {"bogus": 1}, runtime_config=_runtime_config()
        )
    # no conn args -> defaults preserved (conf is None)
    assert (
        get_dialect_config(Dialects.TRINO, {}, runtime_config=_runtime_config()) is None
    )


def test_cli_conn_args_equals_form_applies(tmp_path: Path):
    """`path=<file>` must reach DuckDBConfig (was silently dropped by pairwise zip)."""
    script = tmp_path / "test.preql"
    script.write_text("select 1 as value;")
    db_path = tmp_path / "made.duckdb"

    result = CliRunner().invoke(
        cli, ["run", str(script), "duckdb", f"path={db_path.as_posix()}"]
    )
    assert result.exit_code == 0, result.output
    assert db_path.exists()


def test_cli_conn_args_pair_form_applies(tmp_path: Path):
    script = tmp_path / "test.preql"
    script.write_text("select 1 as value;")
    db_path = tmp_path / "made_pair.duckdb"

    result = CliRunner().invoke(
        cli, ["run", str(script), "duckdb", "path", db_path.as_posix()]
    )
    assert result.exit_code == 0, result.output
    assert db_path.exists()


def test_cli_conn_args_unknown_key_fails(tmp_path: Path):
    script = tmp_path / "test.preql"
    script.write_text("select 1 as value;")

    result = CliRunner().invoke(cli, ["run", str(script), "duckdb", "bogus_key", "x"])
    assert result.exit_code != 0
    assert "Unknown DuckDB connection parameters" in result.output
    assert "bogus_key" in result.output


def test_cli_conn_args_dangling_key_fails(tmp_path: Path):
    script = tmp_path / "test.preql"
    script.write_text("select 1 as value;")

    result = CliRunner().invoke(cli, ["run", str(script), "duckdb", "path"])
    assert result.exit_code != 0
    assert "has no value" in result.output
