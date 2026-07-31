"""Verifies a declared `partition by` reaches BigQuery's table metadata.

Rendering the clause is not proof: an unpartitioned table accepts the same DDL
minus the clause and every subsequent load still succeeds, just with a full
scan per incremental DELETE. The check that catches that is the table's own
metadata — is_partitioning_column, and one PARTITIONS row per loaded day.

Needs a dataset the credentials may create and drop tables in
(TRILOGY_BIGQUERY_TEST_DATASET).
"""

from collections.abc import Generator
from uuid import uuid4

import pytest

from trilogy import Dialects, Executor

pytestmark = pytest.mark.bigquery_execution

MODEL = """
key id int;
property id.created_at date;
property id.amount float;

datasource facts (
    id: id,
    created_at: created_at,
    amount: amount,
)
grain (id)
address `{table}`
partition by created_at;

create or replace datasources facts;
"""


@pytest.fixture(scope="module")
def live_executor(bq_write_dataset) -> Generator[Executor, None, None]:
    try:
        executor = Dialects.BIGQUERY.default_executor()
    except Exception as e:
        pytest.skip(f"BigQuery not available: {e}")
    yield executor
    executor.close()


def test_created_table_carries_partitioning(live_executor, bq_write_dataset):
    name = f"trilogy_partition_{uuid4().hex[:8]}"
    table = f"{bq_write_dataset}.{name}"
    try:
        live_executor.execute_text(MODEL.format(table=table))

        partitioning = live_executor.execute_raw_sql(
            f"select column_name from `{bq_write_dataset}`.INFORMATION_SCHEMA.COLUMNS "
            f"where table_name = '{name}' and is_partitioning_column = 'YES'"
        ).fetchall()
        assert [r[0] for r in partitioning] == ["created_at"]

        live_executor.execute_raw_sql(
            f"insert into `{table}` values "
            "(1, date '2024-01-01', 1.5), (2, date '2024-01-02', 2.5)"
        )
        partitions = live_executor.execute_raw_sql(
            f"select partition_id from `{bq_write_dataset}`.INFORMATION_SCHEMA.PARTITIONS "
            f"where table_name = '{name}'"
        ).fetchall()
        assert {r[0] for r in partitions} == {"20240101", "20240102"}
    finally:
        live_executor.execute_raw_sql(f"drop table if exists `{table}`")


def test_partition_ddl_is_accepted_for_each_supported_type(
    live_executor, bq_write_dataset
):
    """A rendered PARTITION BY expression BigQuery rejects would fail here."""
    for datatype in ("date", "datetime", "timestamp"):
        name = f"trilogy_partition_{datatype}_{uuid4().hex[:8]}"
        table = f"{bq_write_dataset}.{name}"
        model = MODEL.replace(
            "property id.created_at date;", f"property id.created_at {datatype};"
        )
        try:
            live_executor.execute_text(model.format(table=table))
            partitioning = live_executor.execute_raw_sql(
                f"select column_name from `{bq_write_dataset}`.INFORMATION_SCHEMA.COLUMNS "
                f"where table_name = '{name}' and is_partitioning_column = 'YES'"
            ).fetchall()
            assert [r[0] for r in partitioning] == ["created_at"], datatype
        finally:
            live_executor.execute_raw_sql(f"drop table if exists `{table}`")


def test_incremental_delete_prunes_to_one_partition(live_executor, bq_write_dataset):
    """The point of partitioning here: the per-partition DELETE an append emits
    reads one day, not the whole table."""
    name = f"trilogy_partition_{uuid4().hex[:8]}"
    table = f"{bq_write_dataset}.{name}"
    try:
        live_executor.execute_text(MODEL.format(table=table))
        live_executor.execute_raw_sql(
            f"insert into `{table}` values "
            "(1, date '2024-01-01', 1.5), (2, date '2024-01-02', 2.5)"
        )
        live_executor.execute_raw_sql(
            f"delete from `{table}` where created_at = date '2024-01-01'"
        )
        remaining = live_executor.execute_raw_sql(
            f"select id from `{table}` order by id"
        ).fetchall()
        assert [r[0] for r in remaining] == [2]

        partitions = live_executor.execute_raw_sql(
            f"select partition_id from `{bq_write_dataset}`.INFORMATION_SCHEMA.PARTITIONS "
            f"where table_name = '{name}' and total_rows > 0"
        ).fetchall()
        assert {r[0] for r in partitions} == {"20240102"}
    finally:
        live_executor.execute_raw_sql(f"drop table if exists `{table}`")
