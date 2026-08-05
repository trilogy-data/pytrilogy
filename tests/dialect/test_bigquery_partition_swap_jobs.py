"""What the native partition swap asks BigQuery to do.

Real job-config objects against a fake client, so the decorators, dispositions
and cleanup are checked without credentials. This cannot show that BigQuery
accepts the jobs — that is what
`tests/engine/bigquery/test_bigquery_partition_swap.py` is for — but it pins
every choice this module makes on the way there.
"""

import pytest

from trilogy import Dialects, Environment, Executor
from trilogy.dialect.bigquery_engine import BigQueryConnection
from trilogy.dialect.bigquery_persist import execute_partition_swap
from trilogy.parser import parse

pytest.importorskip("google.cloud.bigquery")

MODEL = """
key id int;
property id.created_at date;

root datasource source_facts (
    id: id,
    created_at: created_at,
)
grain (id)
address raw_facts;

datasource facts (
    id: id,
    created_at: created_at,
)
grain (id)
address my_ds.my_facts
partition by created_at;

append into facts by created_at from select id, created_at;
"""


class FakeJob:
    def __init__(self, rows=()):
        self.rows = rows

    def result(self):
        return self.rows


class FakeTable:
    def __init__(self, field="created_at", type_="DAY"):
        from google.cloud import bigquery

        self.time_partitioning = bigquery.TimePartitioning(type_=type_, field=field)


class FakeClient:
    def __init__(self, partition_ids=("20240101", "20240102"), copy_error=None):
        self.partition_ids = partition_ids
        self.copy_error = copy_error
        self.queries: list = []
        self.copies: list = []
        self.updated: list = []
        self.deleted: list = []

    def get_table(self, name):
        return FakeTable()

    def query(self, sql, job_config=None):
        self.queries.append((sql, job_config))
        if "INFORMATION_SCHEMA" in sql:
            return FakeJob([(pid,) for pid in self.partition_ids])
        return FakeJob()

    def update_table(self, table, fields):
        self.updated.append((table, fields))
        return table

    def copy_table(self, source, destination, job_config=None):
        self.copies.append((source, destination, job_config))
        if self.copy_error:
            raise self.copy_error
        return FakeJob()

    def delete_table(self, name, not_found_ok=False):
        self.deleted.append(name)


class FakeEngine:
    """Enough of an engine for a real Executor — no credentials involved."""

    def __init__(self, connection):
        self.connection = connection

    def connect(self):
        return self.connection

    def setup(self, env, connection):
        return None

    def dispose(self, close: bool = True):
        return None


def _swap(client, model: str = MODEL):
    env = Environment()
    _, statements = parse(model, env)
    connection = BigQueryConnection(client)
    executor = Executor(
        dialect=Dialects.BIGQUERY, engine=FakeEngine(connection), environment=env
    )
    (processed,) = executor.generator.generate_queries(env, [statements[-1]])
    return execute_partition_swap(processed, executor, connection, "proj")


def _staging_name(client) -> str:
    return client.queries[0][1].destination.table_id


def test_the_staging_write_is_partitioned_like_the_target():
    """The partition ids of the two tables have to line up, which is what makes
    `staging$id -> target$id` correct without formatting any value."""
    client = FakeClient()
    _swap(client)
    sql, config = client.queries[0]
    assert "INSERT" not in sql
    assert config.write_disposition == "WRITE_TRUNCATE"
    assert config.time_partitioning.field == "created_at"
    assert config.time_partitioning.type_ == "DAY"
    assert config.destination.dataset_id == "my_ds"
    assert config.destination.project == "proj"
    assert config.destination.table_id.startswith("trilogy_swap_my_facts_")


def test_staging_granularity_follows_the_target():
    """Hourly/monthly targets are addressed by their own id shape; nothing here
    assumes days."""
    client = FakeClient()
    client.get_table = lambda name: FakeTable(type_="MONTH")
    _swap(client)
    assert client.queries[0][1].time_partitioning.type_ == "MONTH"


def test_partitions_are_read_from_metadata_for_the_staging_table():
    client = FakeClient()
    _swap(client)
    sql, _ = client.queries[1]
    assert "INFORMATION_SCHEMA.PARTITIONS" in sql
    assert "`proj.my_ds`" in sql
    assert f"table_name = '{_staging_name(client)}'" in sql
    assert "total_rows > 0" in sql


def test_one_copy_job_per_slice_truncates_that_partition_alone():
    client = FakeClient()
    _swap(client)
    staging = _staging_name(client)
    # Sorted, not positional: submission is concurrent, so arrival order is not
    # a property of the code under test.
    assert sorted(
        (source.table_id, destination.table_id)
        for source, destination, _ in client.copies
    ) == [
        (f"{staging}$20240101", "my_facts$20240101"),
        (f"{staging}$20240102", "my_facts$20240102"),
    ]
    assert all(
        config.write_disposition == "WRITE_TRUNCATE" for _, _, config in client.copies
    )


def test_the_null_slice_is_replaced_by_dml_not_a_copy_job():
    """BigQuery reports `__NULL__` in INFORMATION_SCHEMA but rejects it as a
    decorator (`Invalid date partitioned partition key: __NULL__`), so it is the
    one slice with no copy job. It must still be replaced, and atomically."""
    client = FakeClient(partition_ids=("20240101", "__NULL__"))
    _swap(client)
    assert [d.table_id for _, d, _ in client.copies] == ["my_facts$20240101"]
    dml = client.queries[-1][0]
    assert dml.startswith("BEGIN TRANSACTION;")
    assert "DELETE FROM `proj.my_ds.my_facts` WHERE `created_at` IS NULL" in dml
    assert "WHERE `created_at` IS NULL" in dml.split("INSERT INTO")[1]
    assert dml.rstrip().endswith("COMMIT TRANSACTION;")


def test_a_null_only_select_still_replaces_that_slice():
    client = FakeClient(partition_ids=("__NULL__",))
    _swap(client)
    assert client.copies == []
    assert client.queries[-1][0].startswith("BEGIN TRANSACTION;")


def test_an_empty_select_copies_nothing():
    client = FakeClient(partition_ids=())
    assert _swap(client) is not None
    assert client.copies == []


def test_unpartitioned_rows_raise_rather_than_being_skipped():
    """They have no decorator, so copying the rest would silently drop them."""
    client = FakeClient(partition_ids=("20240101", "__UNPARTITIONED__"))
    with pytest.raises(ValueError, match="unpartitioned"):
        _swap(client)
    assert client.copies == []


def test_staging_is_dropped_and_given_a_ttl():
    client = FakeClient()
    _swap(client)
    assert client.deleted == [f"proj.my_ds.{_staging_name(client)}"]
    table, fields = client.updated[0]
    assert fields == ["expires"]
    assert table.expires is not None


COLON_MODEL = (
    MODEL.replace(
        "property id.created_at date;",
        "property id.created_at date;\nproperty id.label string;",
    )
    .replace(
        "    created_at: created_at,\n)\ngrain (id)\naddress raw_facts;",
        "    created_at: created_at,\n    label: label,\n)\ngrain (id)\naddress raw_facts;",
    )
    .replace(
        "from select id, created_at;",
        "from select id, created_at where label = 'http://a:b';",
    )
)


def test_the_staged_select_goes_through_the_normal_sql_preparation():
    """A colon inside a string literal is escaped on the way to SQLAlchemy and
    has to be unescaped before BigQuery sees it — the same handling every other
    statement gets, which is why this reuses the executor's preparation rather
    than rendering straight into a job."""
    client = FakeClient()
    _swap(client, COLON_MODEL)
    sql, _ = client.queries[0]
    assert "'http://a:b'" in sql
    assert "\\:" not in sql


def test_staging_is_dropped_when_a_copy_fails():
    client = FakeClient(copy_error=RuntimeError("boom"))
    with pytest.raises(RuntimeError):
        _swap(client)
    assert client.deleted == [f"proj.my_ds.{_staging_name(client)}"]
