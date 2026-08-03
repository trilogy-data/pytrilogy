import pytest

from trilogy.dialect import (
    BigQueryConfig,
    DuckDBConfig,
    SnowflakeConfig,
)
from trilogy.dialect.enums import Dialects
from trilogy.scripts.serve_helpers.connection_spec import (
    build_connection_spec,
    derive_engine_options,
    filter_connection_options,
    normalize_connection_type,
)
from trilogy.scripts.serve_helpers.models import StoreConnectionType


@pytest.mark.parametrize(
    "value,expected",
    [
        ("duck_db", StoreConnectionType.DUCKDB),
        ("duckdb", StoreConnectionType.DUCKDB),
        (Dialects.DUCK_DB, StoreConnectionType.DUCKDB),
        ("sqlite", StoreConnectionType.SQLITE),
        ("bigquery", StoreConnectionType.BIGQUERY),
        ("bigquery-oauth", StoreConnectionType.BIGQUERY),
        ("snowflake", StoreConnectionType.SNOWFLAKE),
        ("motherduck", StoreConnectionType.MOTHERDUCK),
        ("MotherDuck", StoreConnectionType.MOTHERDUCK),
    ],
)
def test_normalize_known_types(value, expected):
    assert normalize_connection_type(value) == expected


@pytest.mark.parametrize(
    "value",
    ["postgres", "presto", "trino", "sql_server", "mysql", "generic", "nonsense"],
)
def test_normalize_types_without_a_client_runtime(value):
    assert normalize_connection_type(value) is None


def test_filter_options_keeps_only_advertisable_keys():
    assert filter_connection_options(
        StoreConnectionType.SNOWFLAKE,
        {
            "account": "acme",
            "warehouse": "wh",
            "privateKey": "-----BEGIN PRIVATE KEY-----",
            "bogus": "x",
        },
    ) == {"account": "acme", "warehouse": "wh"}


def test_filter_options_drops_everything_for_optionless_types():
    assert (
        filter_connection_options(StoreConnectionType.MOTHERDUCK, {"token": "secret"})
        == {}
    )


def test_build_falls_back_to_serving_engine():
    spec = build_connection_spec(None, None, "duck_db")
    assert spec is not None
    assert spec.type == StoreConnectionType.DUCKDB
    assert spec.options == {}


def test_build_omits_connection_for_unadvertisable_engine():
    assert build_connection_spec(None, None, "postgres") is None
    assert build_connection_spec(None, None, "generic") is None


def test_build_configured_type_wins_over_engine():
    spec = build_connection_spec("bigquery", {"projectId": "p"}, "duck_db")
    assert spec is not None
    assert spec.type == StoreConnectionType.BIGQUERY
    assert spec.options == {"projectId": "p"}


def test_build_omits_connection_for_unadvertisable_configured_type(caplog):
    assert build_connection_spec("postgres", {"host": "db"}, "duck_db") is None
    assert "browse-only" in caplog.text


def test_build_filters_configured_options():
    spec = build_connection_spec(
        "snowflake", {"account": "acme", "password": "hunter2"}, "generic"
    )
    assert spec is not None
    assert spec.options == {"account": "acme"}


def test_derive_maps_bigquery_project_onto_project_id():
    assert derive_engine_options(
        StoreConnectionType.BIGQUERY, BigQueryConfig(project="preqldata")
    ) == {"projectId": "preqldata"}


def test_derive_skips_unset_and_server_only_fields():
    config = BigQueryConfig(staging_dataset="scratch", staging_uri="gs://bucket")
    assert derive_engine_options(StoreConnectionType.BIGQUERY, config) == {}
    assert derive_engine_options(StoreConnectionType.DUCKDB, DuckDBConfig()) == {}
    assert derive_engine_options(StoreConnectionType.BIGQUERY, None) == {}


def test_derive_never_carries_credentials():
    options = derive_engine_options(
        StoreConnectionType.SNOWFLAKE,
        SnowflakeConfig(
            account="acme",
            username="svc",
            password="hunter2",
            database="db",
            schema="public",
        ),
    )
    assert options == {
        "account": "acme",
        "username": "svc",
        "database": "db",
        "schema": "public",
    }


def test_derive_ignores_a_path_a_browser_client_cannot_open():
    assert (
        derive_engine_options(
            StoreConnectionType.DUCKDB, DuckDBConfig(path="/srv/local.db")
        )
        == {}
    )


def test_build_advertises_bigquery_project_from_engine_config():
    """`[engine] dialect = "bigquery"` + `[engine.config] project = ...` is
    enough to hand the client a usable connection — the validation case."""
    spec = build_connection_spec(None, None, "bigquery", BigQueryConfig("preqldata"))
    assert spec is not None
    assert spec.type == StoreConnectionType.BIGQUERY
    assert spec.options == {"projectId": "preqldata"}


def test_build_explicit_serve_connection_is_authoritative():
    """An explicit [serve.connection] replaces the engine-derived options
    wholesale, which is how a store advertises less than its engine implies."""
    spec = build_connection_spec(
        "bigquery", {}, "bigquery", BigQueryConfig("preqldata")
    )
    assert spec is not None
    assert spec.options == {}

    spec = build_connection_spec(
        "bigquery", {"projectId": "override"}, "bigquery", BigQueryConfig("preqldata")
    )
    assert spec is not None
    assert spec.options == {"projectId": "override"}
