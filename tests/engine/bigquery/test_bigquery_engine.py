import datetime
import decimal

import pytest
from sqlalchemy import text

from trilogy import Dialects
from trilogy.core.models.core import DataType, ListWrapper
from trilogy.dialect.bigquery_engine import (
    BigQueryConnection,
    BigQueryEngine,
    array_element_type,
    parameter_type,
    query_parameter,
    to_bigquery_sql,
)
from trilogy.dialect.config import BigQueryConfig, DuckDBConfig
from trilogy.engine import escape_literal_colons


def external_config(uri: str = "gs://b/p/x.parquet"):
    from google.cloud import bigquery

    config = bigquery.ExternalConfig("PARQUET")
    config.source_uris = [uri]
    return config


class FakeJob:
    def __init__(self, rows, schema):
        self.rows = FakeRowIterator(rows, schema)

    def result(self):
        return self.rows


class FakeField:
    def __init__(self, name: str):
        self.name = name


class FakeRow:
    def __init__(self, values: tuple):
        self._values = values

    def values(self) -> tuple:
        return self._values


class FakeRowIterator:
    """Pages like a real RowIterator: records what has actually been pulled."""

    def __init__(self, rows, schema):
        self._rows = rows
        self.schema = schema
        self.pulled: list[object] = []

    def __iter__(self):
        for row in self._rows:
            self.pulled.append(row)
            yield row


class FakeClient:
    """Captures the SQL and job config the connection would send."""

    def __init__(self, rows=None, schema=None):
        self.rows = rows or []
        self.schema = schema or []
        self.calls: list[tuple[str, object]] = []
        self.jobs: list[FakeJob] = []

    def query(self, sql, job_config=None):
        self.calls.append((sql, job_config))
        job = FakeJob(self.rows, self.schema)
        self.jobs.append(job)
        return job


def test_parameter_type_covers_runtime_values():
    assert parameter_type(True) == "BOOL"
    # bool must win over int
    assert parameter_type(1) == "INT64"
    assert parameter_type(1.5) == "FLOAT64"
    assert parameter_type(decimal.Decimal("1.5")) == "NUMERIC"
    assert parameter_type("x") == "STRING"
    assert parameter_type(b"x") == "BYTES"
    assert parameter_type(datetime.date(2024, 1, 1)) == "DATE"
    assert parameter_type(datetime.datetime(2024, 1, 1)) == "DATETIME"
    assert (
        parameter_type(datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc))
        == "TIMESTAMP"
    )


def test_parameter_type_rejects_unsupported_values():
    with pytest.raises(ValueError, match="BigQuery query parameter"):
        parameter_type({"a": 1})


def test_array_element_type_infers_from_values():
    assert array_element_type([1, 2, 3]) == "INT64"
    assert array_element_type([None, "a"]) == "STRING"


def test_array_element_type_falls_back_to_declared_type_when_empty():
    assert array_element_type(ListWrapper([], type=DataType.INTEGER)) == "INT64"
    assert array_element_type([]) == "STRING"


def test_query_parameter_builds_array_and_scalar():
    array = query_parameter("nums", ListWrapper([1, 2], type=DataType.INTEGER))
    assert array.array_type == "INT64"
    assert array.values == [1, 2]

    scalar = query_parameter("name", "trilogy")
    assert scalar.type_ == "STRING"
    assert scalar.value == "trilogy"

    empty = query_parameter("nothing", None)
    assert empty.value is None


def test_to_bigquery_sql_rewrites_bind_markers():
    sql, params = to_bigquery_sql(
        text("select :answer as a, :name as b"), {"answer": 42, "name": "x"}
    )
    assert sql == "select @answer as a, @name as b"
    assert [p.name for p in params] == ["answer", "name"]


def test_to_bigquery_sql_leaves_escaped_colons_alone():
    """A colon inside a string literal is escaped by the executor, not a param."""
    raw = escape_literal_colons("select 'http(?:s)?' as pattern, :n as n")
    sql, params = to_bigquery_sql(text(raw), {"n": 1})

    assert sql == "select 'http(?:s)?' as pattern, @n as n"
    assert [p.name for p in params] == ["n"]


def test_to_bigquery_sql_skips_unreferenced_parameters():
    sql, params = to_bigquery_sql(text("select :a"), {"a": 1, "unused": 2})
    assert sql == "select @a"
    assert [p.name for p in params] == ["a"]


def test_to_bigquery_sql_passes_plain_strings_through():
    assert to_bigquery_sql("select 1", None) == ("select 1", [])


def test_execute_returns_tuple_comparable_rows():
    client = FakeClient(
        rows=[FakeRow((1, "a")), FakeRow((2, "b"))],
        schema=[FakeField("n"), FakeField("label")],
    )
    result = BigQueryConnection(client).execute(text("select 1"))

    assert result.keys() == ["n", "label"]
    rows = result.fetchall()
    # SQLAlchemy Row semantics: tuple equality, index and attribute access
    assert rows[0] == (1, "a")
    assert rows[0][1] == "a"
    assert rows[1].label == "b"


def test_execute_does_not_read_the_result_set_up_front():
    """Rows page from the job as they are consumed, so an unbounded select does
    not have to fit in memory."""
    client = FakeClient(
        rows=[FakeRow((1, "a")), FakeRow((2, "b")), FakeRow((3, "c"))],
        schema=[FakeField("n"), FakeField("label")],
    )
    result = BigQueryConnection(client).execute(text("select 1"))
    pulled = client.jobs[0].rows.pulled

    assert result.keys() == ["n", "label"]
    assert pulled == []
    assert result.fetchone() == (1, "a")
    assert len(pulled) == 1
    assert result.fetchall() == [(2, "b"), (3, "c")]


def test_execute_buffers_nothing_for_ddl():
    result = BigQueryConnection(FakeClient()).execute(text("create table x"))
    assert result.keys() == []
    assert result.fetchall() == []


def test_execute_attaches_only_referenced_external_tables():
    client = FakeClient()
    connection = BigQueryConnection(client)
    connection.register_external_table("trilogy_py_used", external_config())
    connection.register_external_table("trilogy_py_other", external_config())

    connection.execute(text("select * from `trilogy_py_used`"))

    _, job_config = client.calls[0]
    assert list(job_config.table_definitions) == ["trilogy_py_used"]


def test_execute_sends_no_job_config_when_nothing_to_attach():
    client = FakeClient()
    BigQueryConnection(client).execute(text("select 1"))
    assert client.calls[0][1] is None


def test_execute_combines_parameters_and_external_tables():
    client = FakeClient()
    connection = BigQueryConnection(client)
    connection.register_external_table("trilogy_py_used", external_config())

    connection.execute(text("select :n from `trilogy_py_used`"), {"n": 1})

    sql, job_config = client.calls[0]
    assert "@n" in sql
    assert list(job_config.table_definitions) == ["trilogy_py_used"]
    assert [p.name for p in job_config.query_parameters] == ["n"]


def test_connection_has_no_transaction_semantics():
    connection = BigQueryConnection(FakeClient())
    assert connection.in_transaction() is False
    assert connection.get_transaction() is None
    connection.begin()
    connection.commit()
    connection.rollback()


def test_engine_reuses_one_connection_and_disposes_it():
    engine = BigQueryEngine(BigQueryConfig(client=FakeClient(), project="p"))
    connection = engine.connect()

    assert engine.connect() is connection
    engine.dispose()
    assert engine.connect() is not connection


def test_bigquery_defaults_to_the_native_engine():
    """The native engine is the default; it builds its client lazily, so this
    resolves with no credentials present."""
    engine = Dialects.BIGQUERY.default_engine()
    assert isinstance(engine, BigQueryEngine)
    assert engine.config.client is None


def test_bigquery_honours_an_explicit_native_config():
    conf = BigQueryConfig(client=FakeClient(), project="p")
    engine = Dialects.BIGQUERY.default_engine(conf=conf)
    assert isinstance(engine, BigQueryEngine)
    assert engine.config is conf


def test_use_sqlalchemy_routes_to_the_sqlalchemy_engine():
    conf = BigQueryConfig(client=FakeClient(), project="p", use_sqlalchemy=True)
    captured: list[object] = []

    def factory(config, config_type):
        captured.append(config)
        return "sqlalchemy-engine"

    assert (
        Dialects.BIGQUERY.default_engine(conf=conf, _engine_factory=factory)
        == "sqlalchemy-engine"
    )
    assert captured == [conf]


def test_bigquery_rejects_a_foreign_dialect_config():
    with pytest.raises(TypeError, match="expected BigQueryConfig"):
        Dialects.BIGQUERY.default_engine(conf=DuckDBConfig())


def test_engine_close_clears_registered_external_tables():
    engine = BigQueryEngine(BigQueryConfig(client=FakeClient(), project="p"))
    connection = engine.connect()
    assert isinstance(connection, BigQueryConnection)
    connection.register_external_table("trilogy_py_x", external_config())

    engine.dispose()
    assert connection.external_tables == {}
