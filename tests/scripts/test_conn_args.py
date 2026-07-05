"""Connection-argument parsing and validation for CLI commands.

Connection args must never be silently dropped: `key=value` tokens parse the
same as `key value` pairs, a dangling key errors, and unknown keys for a
dialect are a hard error against its allowed-kwargs list.
"""

from pathlib import Path

from click.testing import CliRunner
from pytest import raises

from trilogy.core.exceptions import ConfigurationException
from trilogy.dialect.config import DuckDBConfig
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
