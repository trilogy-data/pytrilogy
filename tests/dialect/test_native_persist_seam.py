"""An engine may perform a persist through its own API instead of running SQL.

The seam is the processed statement itself, so there is no second description
of the write to keep in step. What has to hold is the handshake: the engine
gets first refusal, declining falls back to SQL with nothing lost, and sources
are prepared either way.
"""

from datetime import date

import pytest

from trilogy import Dialects, Environment
from trilogy.core.enums import PersistMode
from trilogy.dialect.bigquery_persist import TableName, parse_table_name, swap_target
from trilogy.dialect.results import BufferedResult
from trilogy.engine import SupportsNativePersist
from trilogy.parser import parse

MODEL = """
key id int;
property id.created_at date;

root datasource source_facts (
    id: id,
    created_at: created_at,
)
grain (id)
query '''
SELECT 1 as id, DATE '2024-01-01' as created_at
UNION ALL SELECT 2, DATE '2024-01-02'
''';

datasource facts (
    id: id,
    created_at: created_at,
)
grain (id)
address my_facts
partition by created_at;

CREATE IF NOT EXISTS DATASOURCE facts;
"""

APPEND = "append into facts by created_at from select id, created_at;"


class RecordingEngine:
    """Wraps a real engine and answers ``execute_persist`` with ``result``."""

    def __init__(self, inner, result):
        self.inner = inner
        self.result = result
        self.seen: list = []

    def connect(self):
        return self.inner.connect()

    def setup(self, env, connection):
        return self.inner.setup(env, connection)

    def dispose(self, close: bool = True):
        return self.inner.dispose(close)

    def execute_persist(self, query, executor):
        self.seen.append(query)
        return self.result


def _executor_with(result):
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(MODEL)
    engine = RecordingEngine(executor.engine, result)
    executor.engine = engine
    return executor, engine


def _rows(executor) -> list:
    return executor.execute_raw_sql(
        "SELECT id, created_at FROM my_facts ORDER BY id"
    ).fetchall()


def test_recording_engine_satisfies_the_protocol():
    assert isinstance(RecordingEngine(None, None), SupportsNativePersist)


def test_an_engine_without_the_method_is_not_offered_the_write():
    executor = Dialects.DUCK_DB.default_executor()
    assert not isinstance(executor.engine, SupportsNativePersist)


def test_the_engine_gets_first_refusal_and_suppresses_the_sql():
    executor, engine = _executor_with(BufferedResult([], []))
    executor.execute_text(APPEND)
    assert len(engine.seen) == 1
    assert engine.seen[0].persist_mode == PersistMode.APPEND
    assert engine.seen[0].partition_by == ["created_at"]
    # The native writer claimed it, so no SQL ran and the table stays empty.
    assert _rows(executor) == []


def test_declining_falls_back_to_the_dialect_sql():
    executor, engine = _executor_with(None)
    executor.execute_text(APPEND)
    assert len(engine.seen) == 1
    assert [(r[0], r[1]) for r in _rows(executor)] == [
        (1, date(2024, 1, 1)),
        (2, date(2024, 1, 2)),
    ]


def test_declining_leaves_the_repeated_append_idempotent():
    """The fallback is the whole staged replace, not just its insert."""
    executor, _ = _executor_with(None)
    executor.execute_text(APPEND)
    first = _rows(executor)
    executor.execute_text(APPEND)
    assert _rows(executor) == first


def test_render_insert_into_writes_somewhere_other_than_the_target():
    """What a native writer runs instead of the persist's own INSERT: the same
    select, landed positionally in a staging table of the writer's choosing."""
    env = Environment()
    _, statements = parse(MODEL + APPEND, env)
    renderer = Dialects.BIGQUERY.default_renderer()
    (processed,) = renderer.generate_queries(env, [statements[-1]])
    rendered = renderer.render_insert_into(processed, "proj.ds.staged")
    assert rendered.lstrip().startswith("INSERT INTO `proj`.`ds`.`staged` \nSELECT")
    assert "CREATE" not in rendered
    assert "created_at" in rendered
    # No column list: the mapping from select to table is positional.
    assert "`staged` (" not in rendered


class FakeTable:
    def __init__(self, partitioning):
        self.time_partitioning = partitioning


class FakePartitioning:
    def __init__(self, field, type_="DAY"):
        self.field = field
        self.type_ = type_


class FakeClient:
    def __init__(self, table):
        self.table = table

    def get_table(self, name):
        if self.table is None:
            from google.api_core.exceptions import NotFound

            raise NotFound(name)
        return self.table


# A copy job needs a dataset, so the swap tests address one. Never executed —
# these only reach the render + decline logic.
BQ_MODEL = """
key id int;
property id.created_at date;
property id.region date;

root datasource source_facts (
    id: id,
    created_at: created_at,
    region: region,
)
grain (id)
address raw_facts;

datasource facts (
    id: id,
    created_at: created_at,
    region: region,
)
grain (id)
address my_ds.my_facts
partition by {keys};
"""

BQ_APPEND = "append into facts by {keys} from select id, created_at, region;"


def _persist(keys: str = "created_at"):
    env = Environment()
    _, statements = parse(BQ_MODEL.format(keys=keys) + BQ_APPEND.format(keys=keys), env)
    renderer = Dialects.BIGQUERY.default_renderer()
    (processed,) = renderer.generate_queries(env, [statements[-1]])
    return processed


@pytest.mark.parametrize(
    "location,project,expected",
    [
        ("proj.ds.tbl", None, TableName("proj", "ds", "tbl")),
        ("ds.tbl", "proj", TableName("proj", "ds", "tbl")),
        ("proj:ds.tbl", None, TableName("proj", "ds", "tbl")),
        ("ds.tbl", None, None),
        ("tbl", "proj", None),
    ],
)
def test_parse_table_name(location, project, expected):
    assert parse_table_name(location, project) == expected


def test_swap_target_accepts_a_matching_partitioned_table():
    pytest.importorskip("google.cloud.bigquery")
    client = FakeClient(FakeTable(FakePartitioning("created_at")))
    target = swap_target(_persist(), client, "proj")
    assert target is not None
    assert target.name == TableName("proj", "my_ds", "my_facts")


@pytest.mark.parametrize(
    "partitioning",
    [None, FakePartitioning("loaded_at"), FakePartitioning(None)],
    ids=["unpartitioned", "different-column", "ingestion-time"],
)
def test_swap_target_declines_a_table_it_cannot_address(partitioning):
    """Each of these would write rows into the wrong slice, or into a table
    with no decorators at all."""
    pytest.importorskip("google.cloud.bigquery")
    client = FakeClient(FakeTable(partitioning))
    assert swap_target(_persist(), client, "proj") is None


def test_swap_target_declines_a_missing_table():
    """The SQL path raises its own, more recognizable error for this."""
    pytest.importorskip("google.cloud.bigquery")
    assert swap_target(_persist(), FakeClient(None), "proj") is None


def test_swap_target_declines_a_multi_column_partition_key():
    """BigQuery partitions on one column, so a second key has no decorator to
    address it by."""
    pytest.importorskip("google.cloud.bigquery")
    client = FakeClient(FakeTable(FakePartitioning("created_at")))
    assert swap_target(_persist("created_at, region"), client, "proj") is None
