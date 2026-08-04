"""Replace BigQuery partitions through the jobs API instead of with DML.

A partitioned APPEND has to replace exactly the slices its select produced. The
portable SQL for that stages the rows, deletes the keys they cover and inserts
(``BaseDialect.generate_partitioned_insert_statements``) — three passes over
the target, none atomic with the others.

BigQuery can do the same thing as metadata. A destination table id may carry a
**partition decorator** (``dataset.table$20240103``), and writing to one with
``WRITE_TRUNCATE`` replaces that partition alone, atomically. So:

1. one query job stages the select into a table partitioned like the target;
2. ``INFORMATION_SCHEMA.PARTITIONS`` names the slices it produced — metadata,
   no scan, and it spells the NULL slice ``__NULL__``, which is *also* that
   slice's decorator, so no value formatting is involved anywhere here;
3. one **copy** job per slice, ``staging$id`` -> ``target$id``. A copy job moves
   no bytes through a query engine: it is free, consumes no slots, and is
   atomic per partition.

The target is read once instead of three times, the delete is not billed at
all, and an interrupted run leaves each slice either wholly old or wholly new.

Declining is always safe — the executor runs the dialect's SQL instead — so
every check below returns None rather than guessing. What this does NOT do is
decide *which* slices to write: that is the select's job, exactly as in SQL.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from trilogy.constants import logger
from trilogy.core.enums import PersistMode
from trilogy.dialect.results import BufferedResult

if TYPE_CHECKING:
    from google.cloud import bigquery

    from trilogy.core.statements.execute import ProcessedQueryPersist
    from trilogy.dialect.bigquery_engine import BigQueryConnection
    from trilogy.engine import ResultProtocol
    from trilogy.executor import Executor

LOGGER_PREFIX = "[BIGQUERY_PERSIST]"

#: Copy jobs submitted before waiting on any. They are free and short, but a
#: backfill legitimately covers thousands of slices and BigQuery should not be
#: handed all of them in one breath.
MAX_INFLIGHT_COPY_JOBS = 32

#: Rows BigQuery has not yet assigned to a partition (the streaming buffer).
#: A query job cannot produce them — and they have no decorator, so they must
#: not be quietly skipped if they ever appear.
UNPARTITIONED = "__UNPARTITIONED__"

STAGING_PREFIX = "trilogy_swap_"
#: Backstop for a process killed between the staging write and the drop, so a
#: crash cannot leave the staged copy billing storage forever.
STAGING_TTL = timedelta(hours=6)


@dataclass(frozen=True)
class TableName:
    project: str
    dataset: str
    table: str

    def qualified(self, table: str | None = None) -> str:
        return f"{self.project}.{self.dataset}.{table if table else self.table}"


@dataclass(frozen=True)
class SwapTarget:
    name: TableName
    partitioning: Any


def parse_table_name(location: str, default_project: str | None) -> TableName | None:
    """Split an address into its parts, or None if it does not name a table.

    Accepts the legacy ``project:dataset.table`` spelling alongside the
    standard one. A bare ``table`` cannot be resolved here — a copy job needs a
    dataset — so it declines and the SQL path handles it."""
    parts = location.replace(":", ".").split(".")
    if len(parts) == 3:
        return TableName(parts[0], parts[1], parts[2])
    if len(parts) == 2 and default_project:
        return TableName(default_project, parts[0], parts[1])
    return None


def swap_target(
    query: ProcessedQueryPersist,
    client: bigquery.Client,
    default_project: str | None,
) -> SwapTarget | None:
    """The table this module can swap partitions on, or None to decline.

    Each check rules out a case where the decorator write would be *wrong*, not
    merely unoptimized: a table that is not physically partitioned has no
    decorators at all; one partitioned on a different column would file the
    rows under the wrong slice; and ingestion-time partitioning (``field``
    unset) is keyed on load time rather than on any column we write.

    Granularity is deliberately not restricted. The staging table copies the
    target's ``type_``, so the partition ids the two tables use are the same by
    construction — whether that is DAY, HOUR, MONTH or YEAR.
    """
    from google.api_core.exceptions import NotFound

    if query.persist_mode != PersistMode.APPEND or len(query.partition_by) != 1:
        return None
    name = parse_table_name(query.output_to.address.location, default_project)
    if name is None:
        return None
    try:
        table = client.get_table(name.qualified())
    except NotFound:
        # The SQL path raises its own, more recognizable error for a missing
        # target; do not pre-empt it with one phrased around partitioning.
        return None
    partitioning = table.time_partitioning
    if partitioning is None or partitioning.field != query.partition_by[0]:
        logger.debug(
            "%s %s is not time-partitioned on %s; using SQL",
            LOGGER_PREFIX,
            name.qualified(),
            query.partition_by[0],
        )
        return None
    return SwapTarget(name=name, partitioning=partitioning)


def execute_partition_swap(
    query: ProcessedQueryPersist,
    executor: Executor,
    connection: BigQueryConnection,
    default_project: str | None,
) -> ResultProtocol | None:
    """Perform a partitioned APPEND as staged write plus per-slice copy jobs."""
    from google.cloud import bigquery
    from sqlalchemy import text

    from trilogy.dialect.bigquery_engine import to_bigquery_sql

    client = connection.client
    target = swap_target(query, client, default_project)
    if target is None:
        return None

    staging = f"{STAGING_PREFIX}{target.name.table}_{uuid4().hex[:8]}"
    # The same preparation the normal query path applies: a rendered select can
    # carry bind markers, and BigQuery spells them `@name` rather than `:name`.
    prepared, params = executor.prepare_sql(
        executor.generator.render_select_only(query), query.local_concepts
    )
    sql, query_parameters = to_bigquery_sql(text(prepared), params)
    # Carries whatever per-job external tables the SQL names, so a persist
    # reading a staged python datasource resolves here exactly as it does on
    # the normal query path.
    config = connection.query_job_config(sql, query_parameters)
    config.destination = target.name.qualified(staging)
    config.write_disposition = "WRITE_TRUNCATE"
    config.time_partitioning = bigquery.TimePartitioning(
        type_=target.partitioning.type_, field=target.partitioning.field
    )
    try:
        client.query(sql, job_config=config).result()
        _set_staging_ttl(client, target.name, staging)
        ids = _partition_ids(client, target.name, staging)
        if not ids:
            # An empty select covers no slices, so it replaces none — the same
            # guarantee the staged DELETE gives by matching no staged keys.
            logger.info(
                "%s %s produced no rows; no partition replaced",
                LOGGER_PREFIX,
                target.name.qualified(),
            )
        else:
            _copy_partitions(client, target.name, staging, ids)
    finally:
        client.delete_table(target.name.qualified(staging), not_found_ok=True)
    return BufferedResult([], [])


def _set_staging_ttl(client: bigquery.Client, target: TableName, staging: str) -> None:
    from google.cloud import bigquery

    table = bigquery.Table(target.qualified(staging))
    table.expires = datetime.now(timezone.utc) + STAGING_TTL
    client.update_table(table, ["expires"])


def _partition_ids(
    client: bigquery.Client, target: TableName, staging: str
) -> list[str]:
    """The slices the staged rows landed in, read from metadata.

    The literal is safe to inline: ``staging`` is built here from a prefix, the
    target's own name and a hex suffix."""
    rows = client.query(
        f"SELECT partition_id FROM `{target.project}.{target.dataset}`"
        ".INFORMATION_SCHEMA.PARTITIONS"
        f" WHERE table_name = '{staging}' AND total_rows > 0"
    ).result()
    ids = [row[0] for row in rows]
    if UNPARTITIONED in ids:
        raise ValueError(
            f"Staged rows for {target.qualified()} are unpartitioned, which has"
            " no partition decorator to swap through. This should be"
            " unreachable for a query-job write; rerun with"
            " native_partition_swap disabled to fall back to SQL."
        )
    return ids


def _copy_partitions(
    client: bigquery.Client, target: TableName, staging: str, ids: Sequence[str]
) -> None:
    from google.cloud import bigquery

    dataset = bigquery.DatasetReference(target.project, target.dataset)
    config = bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE")
    logger.info(
        "%s replacing %d partition(s) of %s by copy job",
        LOGGER_PREFIX,
        len(ids),
        target.qualified(),
    )
    for batch in _batches(ids, MAX_INFLIGHT_COPY_JOBS):
        # Submitted as a group, then awaited: the jobs are independent and each
        # one is a metadata operation, so serializing them would be pure
        # latency. A TableReference is built directly rather than parsed from a
        # string, because the decorator is not valid in a parsed table id.
        jobs = [
            client.copy_table(
                dataset.table(f"{staging}${partition_id}"),
                dataset.table(f"{target.table}${partition_id}"),
                job_config=config,
            )
            for partition_id in batch
        ]
        for job in jobs:
            job.result()


def _batches(values: Sequence[str], size: int) -> Iterator[Sequence[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]
