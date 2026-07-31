"""Stage python datasource output into GCS and expose it to BigQuery.

BigQuery cannot run a local process, so each python datasource is streamed to a
parquet object under the staging URI. There are two ways to make BigQuery read
that object, and which one applies is decided by whether a staging dataset is
configured:

**Temp table definitions (default).** The object is attached to the query job
as a ``tableDefinitions`` entry and referenced by a bare name. Nothing is
written to the catalog, there is no DDL round-trip, and no dataset permissions
are needed. Requires the native ``BigQueryEngine`` — SQLAlchemy cannot pass job
configuration through. Objects are namespaced per executor and deleted on
teardown, since nothing outlives the session.

**External tables (``staging_dataset`` set).** ``CREATE OR REPLACE EXTERNAL
TABLE`` points a catalog entry at a stable object path. Costs an extra job per
script, but the table is queryable outside trilogy and survives the session, so
the object is deliberately *not* cleaned up — see the lifecycle guidance in
docs/bigquery_python_datasources.md.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from trilogy.constants import logger
from trilogy.core.models.datasource import Address
from trilogy.dialect.python_source import (
    ParquetStreamWriter,
    normalize_object_uri,
    open_uri_sink,
    staged_object_name,
    stream_script,
)

LOGGER_PREFIX = "[BIGQUERY_STAGING]"

STAGING_TABLE_PREFIX = "trilogy_py_"

EXTERNAL_TABLE_DDL = """CREATE OR REPLACE EXTERNAL TABLE {table}
OPTIONS (
    format = 'PARQUET',
    uris = ['{uri}'],
    description = 'Trilogy python datasource staged from {script}'
)"""


def delete_uri(uri: str) -> None:
    from pyarrow import fs as pafs

    filesystem, path = pafs.FileSystem.from_uri(normalize_object_uri(uri))
    filesystem.delete_file(path)


@dataclass
class BigQueryPythonStaging:
    """Resolves and materializes GCS-backed staging for python scripts."""

    root_uri: str
    dataset: str | None = None
    project: str | None = None
    # Namespaces staged objects for the temp-definition mode, so teardown can
    # only ever delete objects this executor wrote.
    instance_id: str | None = None
    table_prefix: str = STAGING_TABLE_PREFIX
    # script location -> staged object URI. A script is staged once per
    # executor, not once per statement that references it.
    staged: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.root_uri = normalize_object_uri(self.root_uri).rstrip("/") + "/"

    @property
    def uses_external_tables(self) -> bool:
        return bool(self.dataset)

    @property
    def object_root(self) -> str:
        """Where staged objects live.

        External tables point at a stable path so a re-run refreshes in place;
        temp definitions get a per-executor prefix that teardown can clear.
        """
        if self.uses_external_tables or not self.instance_id:
            return self.root_uri
        return f"{self.root_uri}{self.instance_id}/"

    @property
    def qualified_dataset(self) -> str:
        if self.project and self.dataset and "." not in self.dataset:
            return f"{self.project}.{self.dataset}"
        return self.dataset or ""

    def table_name(self, address: Address) -> str:
        return staged_object_name(address.location, prefix=self.table_prefix)

    def table_reference(self, address: Address) -> str:
        name = self.table_name(address)
        if self.uses_external_tables:
            return f"`{self.qualified_dataset}.{name}`"
        return f"`{name}`"

    def object_uri(self, address: Address) -> str:
        return f"{self.object_root}{self.table_name(address)}.parquet"

    def stage(self, address: Address, force: bool = False) -> str | None:
        """Stream the script's Arrow output to a parquet object in GCS.

        Returns the object URI written, or None if it was already staged.
        """
        if not force and address.location in self.staged:
            return None
        uri = self.object_uri(address)
        rows = stream_script(
            address.location, "", ParquetStreamWriter(open_uri_sink(uri))
        )
        self.staged[address.location] = uri
        logger.info(
            "%s staged %s rows from %s to %s",
            LOGGER_PREFIX,
            rows,
            address.location,
            uri,
        )
        return uri

    def external_table_ddl(self, address: Address, uri: str) -> str:
        return EXTERNAL_TABLE_DDL.format(
            table=self.table_reference(address),
            uri=uri,
            script=address.location.replace("'", "\\'"),
        )

    def external_config(self, uri: str) -> Any:
        from google.cloud import bigquery

        config = bigquery.ExternalConfig("PARQUET")
        config.source_uris = [uri]
        return config

    def materialize(
        self, address: Address, run_sql: Callable[[str], Any], force: bool = False
    ) -> str | None:
        """Stage the script and point an external table at it."""
        uri = self.stage(address, force=force)
        if uri is None:
            return None
        run_sql(self.external_table_ddl(address, uri))
        return uri

    def cleanup(self) -> list[str]:
        """Best-effort delete of the objects this instance staged.

        Skipped in external-table mode, where deleting the object would leave a
        dangling table. Never raises: an object we fail to delete — or one left
        behind when the process dies before teardown — is caught by the
        bucket's lifecycle rule, which is the documented backstop.
        """
        if self.uses_external_tables:
            return []
        deleted: list[str] = []
        for uri in list(self.staged.values()):
            try:
                delete_uri(uri)
                deleted.append(uri)
            except Exception as e:
                logger.warning("%s could not delete %s: %s", LOGGER_PREFIX, uri, e)
        self.staged.clear()
        return deleted
