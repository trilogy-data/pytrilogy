"""Driver-level cancellation of an in-flight statement.

A query timeout is only worth offering where something can actually stop the
query, so the executor asks here for a cancel entry point and refuses the
timeout outright when there is none — a timeout that leaves the warehouse
working is the failure it exists to prevent.

Dispatch is on the DBAPI connection rather than the dialect: a dialect layered
over another engine (``dataframe`` over duckdb) is then covered by its driver's
entry with no second registration. Every callable returned here is documented by
its driver as safe to call from another thread while the connection is busy, and
as a no-op on an idle connection — the timer fires blind, so both hold.
"""

from collections.abc import Callable
from typing import Any

from trilogy.engine import EngineConnection


def resolve_query_canceller(
    connection: EngineConnection,
) -> Callable[[], None] | None:
    """The driver call that aborts whatever is running on ``connection``."""
    raw = _dbapi_connection(connection)
    if raw is None:
        return None
    return _duckdb_canceller(raw) or _sqlite_canceller(raw) or _psycopg2_canceller(raw)


def _dbapi_connection(connection: EngineConnection) -> Any | None:
    from sqlalchemy import Connection as SQLAlchemyConnection

    if not isinstance(connection, SQLAlchemyConnection):
        return None
    return connection.connection.dbapi_connection


def _duckdb_canceller(raw: Any) -> Callable[[], None] | None:
    try:
        from duckdb_engine import ConnectionWrapper
    except ImportError:
        return None
    if not isinstance(raw, ConnectionWrapper):
        return None
    # duckdb_engine holds the DuckDBPyConnection in a name-mangled private
    # attribute and exposes no accessor for it; the wrapper itself has no
    # interrupt to forward to.
    return raw._ConnectionWrapper__c.interrupt


def _sqlite_canceller(raw: Any) -> Callable[[], None] | None:
    import sqlite3

    if not isinstance(raw, sqlite3.Connection):
        return None
    return raw.interrupt


def _psycopg2_canceller(raw: Any) -> Callable[[], None] | None:
    try:
        from psycopg2.extensions import connection as Psycopg2Connection
    except ImportError:
        return None
    if not isinstance(raw, Psycopg2Connection):
        return None
    return raw.cancel
