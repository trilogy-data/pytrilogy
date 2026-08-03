"""Resolve the runtime connection advertised on /index.json.

The wire type is a client runtime name (`duckdb`, `bigquery`, ...), not a
`Dialects` member. Two reasons the two sets are not the same: `motherduck` is
a client connection with no dialect behind it, and trilogy supports engines
(postgres, presto, trino, sql_server, mysql, clickhouse) the client has no
constructor for. For those the store advertises **no** connection and the
client falls back to browse-only, which is a visible degradation — advertising
a runtime that fails on first query is not.

See docs/remote-store-contract.md in trilogy-studio-core.
"""

from trilogy.constants import logger
from trilogy.dialect.config import DialectConfig
from trilogy.dialect.enums import Dialects
from trilogy.scripts.serve_helpers.models import ConnectionSpec, StoreConnectionType

# Non-secret option keys the client reads, per type. Everything else is
# dropped: an unrecognized key is noise at best and a leaked secret at worst.
ALLOWED_CONNECTION_OPTIONS: dict[StoreConnectionType, frozenset[str]] = {
    StoreConnectionType.DUCKDB: frozenset(),
    StoreConnectionType.SQLITE: frozenset(),
    # The MotherDuck token is a secret, supplied per-user.
    StoreConnectionType.MOTHERDUCK: frozenset(),
    StoreConnectionType.BIGQUERY: frozenset({"projectId"}),
    # No `privateKey` — that's a secret.
    StoreConnectionType.SNOWFLAKE: frozenset(
        {"account", "username", "warehouse", "role", "database", "schema"}
    ),
}

_DIALECT_TO_STORE_TYPE: dict[Dialects, StoreConnectionType] = {
    Dialects.DUCK_DB: StoreConnectionType.DUCKDB,
    Dialects.SQLITE: StoreConnectionType.SQLITE,
    Dialects.BIGQUERY: StoreConnectionType.BIGQUERY,
    Dialects.SNOWFLAKE: StoreConnectionType.SNOWFLAKE,
}

# Spellings accepted in `[serve.connection] type`, beyond the dialect names
# `Dialects` itself resolves. OAuth vs service account is not a type
# distinction — the server always emits `bigquery` and the client picks the
# auth flow from per-user credential state.
_TYPE_ALIASES: dict[str, StoreConnectionType] = {
    "bigquery-oauth": StoreConnectionType.BIGQUERY,
    "bigquery_oauth": StoreConnectionType.BIGQUERY,
}

_STORE_TYPE_VALUES = {member.value for member in StoreConnectionType}

# Non-secret `[engine.config]` fields projected onto advertised wire options,
# per store type: {dialect config attribute: wire option key}. Deliberately
# narrow — only fields the client needs to build the same connection, and only
# ones that are never credentials. Everything else (paths, passwords, staging
# locations, client objects) stays server-side.
_ENGINE_CONFIG_OPTIONS: dict[StoreConnectionType, dict[str, str]] = {
    # A duckdb/sqlite `path` names a file on the server's disk; the client
    # opens its own database, so there is nothing useful to hand it.
    StoreConnectionType.DUCKDB: {},
    StoreConnectionType.SQLITE: {},
    # The MotherDuck token is the only field, and it is a secret.
    StoreConnectionType.MOTHERDUCK: {},
    StoreConnectionType.BIGQUERY: {"project": "projectId"},
    # SnowflakeConfig carries no warehouse/role; `password` never travels.
    StoreConnectionType.SNOWFLAKE: {
        "account": "account",
        "username": "username",
        "database": "database",
        "schema": "schema",
    },
}


def derive_engine_options(
    connection_type: StoreConnectionType, engine_config: DialectConfig | None
) -> dict[str, str]:
    """Non-secret wire options implied by `[engine.config]`.

    Values are advertised resolved: `${env:...}` in `[engine.config]` is
    interpolated for server-side execution, and the projection above is
    restricted to fields that are not credentials. A store that wants to
    publish less declares `[serve.connection]` explicitly.
    """
    if engine_config is None:
        return {}
    options: dict[str, str] = {}
    for attribute, wire_key in _ENGINE_CONFIG_OPTIONS[connection_type].items():
        value = getattr(engine_config, attribute, None)
        if value is None or value == "":
            continue
        options[wire_key] = str(value)
    return options


def normalize_connection_type(value: Dialects | str) -> StoreConnectionType | None:
    """Map a configured type or engine name onto a client runtime name."""
    if isinstance(value, Dialects):
        return _DIALECT_TO_STORE_TYPE.get(value)
    key = value.strip().lower()
    if key in _TYPE_ALIASES:
        return _TYPE_ALIASES[key]
    if key in _STORE_TYPE_VALUES:
        return StoreConnectionType(key)
    try:
        dialect = Dialects(key)
    except ValueError:
        return None
    return _DIALECT_TO_STORE_TYPE.get(dialect)


def filter_connection_options(
    connection_type: StoreConnectionType, options: dict[str, str]
) -> dict[str, str]:
    allowed = ALLOWED_CONNECTION_OPTIONS[connection_type]
    dropped = sorted(set(options) - allowed)
    if dropped:
        logger.warning(
            "Ignoring connection options not advertisable for %s: %s",
            connection_type,
            ", ".join(dropped),
        )
    return {key: value for key, value in options.items() if key in allowed}


def build_connection_spec(
    configured_type: Dialects | str | None,
    configured_options: dict[str, str] | None,
    engine: str,
    engine_config: DialectConfig | None = None,
) -> ConnectionSpec | None:
    """Connection to advertise, or None to leave the store browse-only.

    Explicit `[serve.connection]` wins and is authoritative: its options are
    the entire advertised set, which is also how a store publishes less than
    `[engine.config]` would imply. Otherwise the serving engine dialect is
    advertised, carrying the non-secret parts of `[engine.config]` (notably the
    BigQuery project) so a local `trilogy serve` runs queries without further
    config. `engine_config` must belong to `engine`; the caller drops it when
    the two disagree.
    """
    if configured_type:
        resolved = normalize_connection_type(configured_type)
        if resolved is None:
            logger.warning(
                "Connection type %r has no client runtime; serving browse-only "
                "(no connection advertised on /index.json).",
                configured_type,
            )
            return None
        return ConnectionSpec(
            type=resolved,
            options=filter_connection_options(resolved, configured_options or {}),
        )
    resolved = normalize_connection_type(engine)
    if resolved is None:
        return None
    return ConnectionSpec(
        type=resolved, options=derive_engine_options(resolved, engine_config)
    )
