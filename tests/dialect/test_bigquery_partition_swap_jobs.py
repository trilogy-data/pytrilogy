"""What the native partition swap asks BigQuery to do.

Real job-config objects against a fake client, so the decorators, dispositions
and cleanup are checked without credentials. This cannot show that BigQuery
accepts the jobs — that is what
`tests/engine/bigquery/test_bigquery_partition_swap.py` is for — but it pins
every choice this module makes on the way there.
"""

from datetime import datetime, timedelta, timezone

import pytest

from trilogy import Dialects, Environment, Executor
from trilogy.dialect.bigquery_engine import BigQueryConnection
from trilogy.dialect.bigquery_persist import STAGING_TTL, execute_partition_swap
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

#: The same shape, but with the datasource's declared column names *different*
#: from its concept names — which is the ordinary case, and the one MODEL above
#: cannot see. A select names its output columns after concepts
#: (``created_at``), a datasource after its own columns (``event_date``), and
#: only a positional INSERT into a table shaped like the target reconciles the
#: two.
ALIASED_MODEL = """
key id int;
property id.created_at date;

root datasource source_facts (
    id: id,
    created_at: created_at,
)
grain (id)
address raw_facts;

datasource facts (
    fact_id: id,
    event_date: created_at,
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
    #: Every real client has one, from ADC or set explicitly.
    project = "from-the-client"

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


def _sql_containing(client, needle: str) -> str:
    """The one statement matching ``needle``. By content, not by index: the
    number of statements this module issues is not the property under test."""
    matches = [sql for sql, _ in client.queries if needle in sql]
    assert len(matches) == 1, f"expected exactly one {needle!r}, got {len(matches)}"
    return matches[0]


def _config_for(client, sql: str):
    return next(config for issued, config in client.queries if issued is sql)


def _staging_name(client) -> str:
    create = _sql_containing(client, "CREATE TABLE")
    return create.split("`")[1].split(".")[-1]


def test_the_staging_table_is_created_from_the_target():
    """`LIKE` is what gives the staged rows the target's column names, its
    partitioning and its granularity at once — so the partition ids of the two
    tables line up, which is what makes `staging$id -> target$id` correct
    without formatting any value."""
    client = FakeClient()
    _swap(client)
    create = _sql_containing(client, "CREATE TABLE")
    assert create.startswith("CREATE TABLE `proj.my_ds.trilogy_swap_my_facts_")
    assert " LIKE `proj.my_ds.my_facts` " in create


def test_the_staged_write_is_a_positional_insert_not_a_destination_write():
    """A destination write names the staged columns after the select's
    *concepts*, so it stages `created_at` where a datasource may declare
    `event_date` — nothing a copy job can move and nothing a partition spec can
    name. The INSERT carries no column list because the mapping is positional,
    exactly as on the SQL path."""
    client = FakeClient()
    _swap(client)
    insert = _sql_containing(client, "INSERT INTO")
    assert insert.lstrip().startswith(
        "INSERT INTO `proj`.`my_ds`.`trilogy_swap_my_facts_"
    )
    config = _config_for(client, insert)
    assert config.destination is None
    assert config.time_partitioning is None


def test_declared_column_names_are_never_asked_of_the_select():
    """The regression: with aliases that differ from concept names, nothing may
    depend on the select producing the target's columns. It does not — the
    staging table brings them, and the rows land positionally."""
    client = FakeClient()
    # The target is partitioned on the *declared column*, which is what
    # `swap_target` matches against — the divergence this test is about.
    client.get_table = lambda name: FakeTable(field="event_date")
    _swap(client, model=ALIASED_MODEL)
    create = _sql_containing(client, "CREATE TABLE")
    assert " LIKE `proj.my_ds.my_facts` " in create
    insert = _sql_containing(client, "INSERT INTO")
    # The old code set this from the *target's* column name against a schema
    # named after concepts: "The field specified for partitioning cannot be
    # found in the schema".
    assert _config_for(client, insert).time_partitioning is None
    assert "event_date" not in insert


@pytest.mark.parametrize(
    "project,expected",
    [(None, "from-the-client"), ("explicit", "explicit")],
    ids=["adopted", "explicit-wins"],
)
def test_a_supplied_client_supplies_the_project(project, expected):
    """`dataset.table` only becomes a copy-job address once something completes
    it with a project, and a supplied client is the one that knows. Until it was
    adopted the swap declined silently for every 2-part address — including the
    one this suite's live counterpart writes to. Adopting is a fallback, not an
    override: an explicit project is what a user wrote."""
    from trilogy.dialect.bigquery_engine import BigQueryEngine
    from trilogy.dialect.config import BigQueryConfig

    engine = BigQueryEngine(BigQueryConfig(client=FakeClient(), project=project))
    engine.connect()
    assert engine.config.project == expected


def test_the_sqlalchemy_path_completes_the_project_the_same_way():
    """The same resolution, one seam: without it the escape-hatch path built a
    `bigquery://None` URL from a perfectly good client."""
    from trilogy.dialect.config import BigQueryConfig

    config = BigQueryConfig(client=FakeClient())
    assert config.create_connect_args() == {"client": config.client}
    assert config.connection_string().startswith("bigquery://from-the-client?")


def test_partitions_are_read_from_metadata_for_the_staging_table():
    client = FakeClient()
    _swap(client)
    sql = _sql_containing(client, "INFORMATION_SCHEMA.PARTITIONS")
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


def test_staging_is_dropped_and_expires_on_its_own():
    """The expiry is the backstop for a process killed before the drop, so it
    is set by the CREATE rather than by a follow-up — a table that exists
    without one, however briefly, is the case the backstop is for."""
    client = FakeClient()
    _swap(client)
    assert client.deleted == [f"proj.my_ds.{_staging_name(client)}"]
    create = _sql_containing(client, "CREATE TABLE")
    assert "OPTIONS(expiration_timestamp = TIMESTAMP '" in create
    expires = datetime.fromisoformat(create.split("TIMESTAMP '")[1].split("'")[0])
    assert timedelta(hours=5) < expires - datetime.now(timezone.utc) <= STAGING_TTL
    assert client.updated == []


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
    sql = _sql_containing(client, "INSERT INTO")
    assert "'http://a:b'" in sql
    assert "\\:" not in sql


def test_staging_is_dropped_when_a_copy_fails():
    client = FakeClient(copy_error=RuntimeError("boom"))
    with pytest.raises(RuntimeError):
        _swap(client)
    assert client.deleted == [f"proj.my_ds.{_staging_name(client)}"]
