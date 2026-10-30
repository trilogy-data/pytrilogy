"""SQLAlchemy dialect for haybarn, Query.Farm's DuckDB build that can ``LOAD vgi``.

duckdb_engine binds the stock ``duckdb`` module: its DBAPI exception classes and
the rollback guard catch ``duckdb.*`` types, which a haybarn connection never
raises. Everything else (compilation, types, reflection) is shared, so this only
swaps those.
"""

from typing import Any, cast

import haybarn
from duckdb_engine import DBAPI, ConnectionWrapper, Dialect
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2

_NO_TRANSACTION = "no transaction is active"


class HaybarnDBAPI(DBAPI):
    Error = haybarn.Error
    TransactionException = haybarn.TransactionException
    ParserException = haybarn.ParserException


class HaybarnDialect(Dialect):
    driver = "haybarn"
    supports_statement_cache = True

    @staticmethod
    def dbapi(**kwargs: Any) -> type[DBAPI]:  # type: ignore[override]
        return HaybarnDBAPI

    def connect(self, *cargs: Any, **cparams: Any) -> Any:
        cparams.pop("preload_extensions", None)
        cparams.pop("register_filesystems", None)
        cparams["config"] = {
            **cparams.get("config", {}),
            **cparams.pop("url_config", {}),
        }
        # haybarn's connection is its own class with duckdb's interface.
        return ConnectionWrapper(cast(Any, haybarn.connect(*cargs, **cparams)))

    def do_rollback(self, connection: Any) -> None:
        try:
            PGDialect_psycopg2.do_rollback(self, connection)
        except haybarn.TransactionException as e:
            if _NO_TRANSACTION not in str(e):
                raise
