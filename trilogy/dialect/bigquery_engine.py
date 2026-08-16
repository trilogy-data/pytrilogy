"""Native google-cloud-bigquery adapter exposing trilogy's engine protocols.

Used instead of sqlalchemy-bigquery so a query job can carry
``tableDefinitions`` — per-job temporary external tables. That is the only way
to read a GCS object from BigQuery without creating a catalog entry, and
SQLAlchemy offers no way to pass job configuration through to the cursor.

BigQuery has no transactions outside a script, so begin/commit/rollback are
no-ops and the executor's implicit-transaction bookkeeping stays inert.
"""

from __future__ import annotations

import datetime
import decimal
import re
from typing import TYPE_CHECKING, Any

from trilogy.constants import logger
from trilogy.core.models.core import ListWrapper
from trilogy.core.models.environment import Environment
from trilogy.dialect.results import streamed_rows
from trilogy.engine import (
    EngineConnection,
    ExecutionEngine,
    NonTransactionalConnection,
    ResultProtocol,
    unescape_literal_colons,
)

if TYPE_CHECKING:
    from google.cloud import bigquery
    from sqlalchemy.sql.elements import TextClause

    from trilogy.core.statements.execute import ProcessedQueryPersist
    from trilogy.dialect.config import BigQueryConfig
    from trilogy.executor import Executor

LOGGER_PREFIX = "[BIGQUERY_ENGINE]"

# `:name` that the executor did not escape as `\:` (see escape_literal_colons),
# i.e. a real bind parameter rather than a colon inside a string literal.
BIND_MARKER = re.compile(r"(?<!\\):([A-Za-z_]\w*)")

# Runtime python type -> BigQuery parameter type. bool must precede int.
SCALAR_PARAMETER_TYPES: tuple[tuple[type, str], ...] = (
    (bool, "BOOL"),
    (int, "INT64"),
    (float, "FLOAT64"),
    (decimal.Decimal, "NUMERIC"),
    (str, "STRING"),
    (bytes, "BYTES"),
    (datetime.datetime, "DATETIME"),
    (datetime.date, "DATE"),
    (datetime.time, "TIME"),
)


def parameter_type(value: Any) -> str:
    """BigQuery parameter type for a runtime python value."""
    if isinstance(value, datetime.datetime):
        # tz-aware datetimes are TIMESTAMP; naive ones are DATETIME
        return "TIMESTAMP" if value.tzinfo is not None else "DATETIME"
    for python_type, bq_type in SCALAR_PARAMETER_TYPES:
        if isinstance(value, python_type):
            return bq_type
    raise ValueError(
        f"Cannot pass a {type(value).__name__} value as a BigQuery query parameter"
    )


def array_element_type(value: Any) -> str:
    """Element type for an array parameter, including when it is empty."""
    for element in value:
        if element is not None:
            return parameter_type(element)
    if isinstance(value, ListWrapper):
        # Local import: trilogy.dialect.bigquery imports this module, so the
        # dependency can only run in the other direction at call time.
        from trilogy.dialect.bigquery import DATATYPE_MAP

        mapped = DATATYPE_MAP.get(value.type)
        if mapped:
            return mapped
    return "STRING"


def query_parameter(name: str, value: Any) -> Any:
    from google.cloud import bigquery

    if isinstance(value, (list, tuple, ListWrapper)):
        return bigquery.ArrayQueryParameter(
            name, array_element_type(value), list(value)
        )
    if value is None:
        return bigquery.ScalarQueryParameter(name, "STRING", None)
    return bigquery.ScalarQueryParameter(name, parameter_type(value), value)


def to_bigquery_sql(
    statement: str | TextClause, parameters: dict | None
) -> tuple[str, list]:
    """Render a statement to BigQuery SQL plus its named query parameters.

    Bind markers are rewritten `:name` -> `@name` rather than inlined as
    literals: BigQuery escapes strings with backslashes (so SQLAlchemy's
    literal renderer would mis-quote them) and has no literal renderer for
    arrays at all.
    """
    from sqlalchemy.sql.elements import TextClause

    if not isinstance(statement, TextClause):
        return str(statement), []
    if not parameters:
        return unescape_literal_colons(statement.text), []

    used: set[str] = set()

    def rewrite(match: re.Match) -> str:
        name = match.group(1)
        if name not in parameters:
            return match.group(0)
        used.add(name)
        return f"@{name}"

    sql = unescape_literal_colons(BIND_MARKER.sub(rewrite, statement.text))
    return sql, [query_parameter(name, parameters[name]) for name in sorted(used)]


class BigQueryConnection(NonTransactionalConnection):
    """A single BigQuery client, plus any temp external tables to attach.

    ``external_tables`` is a name -> ExternalConfig registry populated by the
    dialect when it stages a python datasource. A definition is attached to a
    job only when its name appears in that statement's SQL, so unrelated
    statements are unaffected.
    """

    def __init__(self, client: bigquery.Client):
        self.client = client
        self.external_tables: dict[str, Any] = {}

    def register_external_table(self, name: str, config: Any) -> None:
        self.external_tables[name] = config

    def _referenced_tables(self, sql: str) -> dict[str, Any]:
        return {
            name: config for name, config in self.external_tables.items() if name in sql
        }

    def query_job_config(self, sql: str, query_parameters: list | None = None) -> Any:
        """A job config carrying whatever external tables ``sql`` names.

        Always a config, unlike ``_job_config`` — a caller that adds
        destination settings needs something to add them to."""
        from google.cloud import bigquery

        # QueryJobConfig rejects an explicit None for table_definitions
        config = bigquery.QueryJobConfig(query_parameters=query_parameters or [])
        referenced = self._referenced_tables(sql)
        if referenced:
            config.table_definitions = referenced
        return config

    def _job_config(self, sql: str, query_parameters: list) -> Any | None:
        if not self._referenced_tables(sql) and not query_parameters:
            return None
        return self.query_job_config(sql, query_parameters)

    def execute(self, statement: Any, parameters: Any | None = None) -> ResultProtocol:
        sql, query_parameters = to_bigquery_sql(statement, parameters)
        job = self.client.query(sql, job_config=self._job_config(sql, query_parameters))
        # Blocks until the query finishes, so a failed query still raises from
        # here (inside the executor's retry wrapper). The RowIterator it returns
        # then pages server-side, and is bound to its own job rather than to a
        # shared cursor — so it stays valid once this connection runs the next
        # statement, and rows can be wrapped as they are consumed.
        rows = job.result()
        # DDL/DML jobs report an empty schema and yield no rows.
        # A BigQuery Row is not tuple-equal, which SQLAlchemy Row consumers
        # rely on, so streamed_rows re-wraps the values.
        return streamed_rows(
            [field.name for field in rows.schema],
            (row.values() for row in rows),
            "BigQueryRow",
        )

    def close(self) -> None:
        self.external_tables.clear()


class BigQueryEngine(ExecutionEngine):
    def __init__(self, config: BigQueryConfig):
        self.config = config
        self._connection: BigQueryConnection | None = None

    def _bigquery_connection(self) -> BigQueryConnection:
        if self._connection is None:
            self._connection = BigQueryConnection(self.config.resolve_client())
        return self._connection

    def connect(self) -> EngineConnection:
        return self._bigquery_connection()

    def execute_persist(
        self, query: ProcessedQueryPersist, executor: Executor
    ) -> ResultProtocol | None:
        """Satisfies ``SupportsNativePersist``: a partitioned APPEND becomes
        per-slice copy jobs instead of DML. Everything else declines to None and
        runs the dialect's SQL — see ``bigquery_persist``."""
        if not self.config.native_partition_swap:
            return None
        from trilogy.dialect.bigquery_persist import execute_partition_swap

        # Read `project` only after the connection exists: resolving the client
        # is what completes it (see `BigQueryConfig.resolve_client`), and the
        # swap declines a `dataset.table` address without one.
        connection = self._bigquery_connection()
        return execute_partition_swap(query, executor, connection, self.config.project)

    def setup(self, env: Environment, connection: Any) -> None:
        return None

    def dispose(self, close: bool = True) -> None:
        if close and self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.debug("%s disposed connection", LOGGER_PREFIX)
