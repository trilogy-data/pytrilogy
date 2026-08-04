"""A partitioned append against live BigQuery, both ways.

The native path replaces each slice with a copy job onto a partition decorator;
the SQL path stages, deletes the covered keys and inserts. They must agree, and
the only thing that shows they do is running the same write both ways against a
real table and comparing rows — a rendered string cannot tell you that a
decorator addressed the slice you meant, that the NULL slice was reachable, or
that a neighbouring partition survived.

Needs a dataset the credentials may create and drop tables in
(TRILOGY_BIGQUERY_TEST_DATASET).
"""

from collections.abc import Generator
from datetime import date
from uuid import uuid4

import pytest

from trilogy import Dialects, Executor
from trilogy.dialect.config import BigQueryConfig

pytestmark = pytest.mark.bigquery_execution

MODEL = """
key id int;
property id.created_at {datatype};
property id.label string;

root datasource source_facts (
    id: id,
    created_at: created_at,
    label: label,
)
grain (id)
query '''{source}''';

datasource facts (
    id: id,
    created_at: created_at,
    label: label,
)
grain (id)
address `{table}`
partition by created_at;

create or replace datasources facts;
"""

APPEND = "append into facts by created_at from select id, created_at, label;"

#: One row per slice, plus the NULL slice, per partitionable type. `label` is
#: what a replace has to overwrite: rerunning the append with a new label must
#: leave one row per id, carrying the new value.
SOURCES = {
    "date": """
SELECT 1 as id, DATE '2024-01-01' as created_at, '{label}' as label
UNION ALL SELECT 2, DATE '2024-01-02', '{label}'
UNION ALL SELECT 3, CAST(NULL AS DATE), '{label}'
""",
    "datetime": """
SELECT 1 as id, DATETIME '2024-01-01 06:00:00' as created_at, '{label}' as label
UNION ALL SELECT 2, DATETIME '2024-01-02 06:00:00', '{label}'
UNION ALL SELECT 3, CAST(NULL AS DATETIME), '{label}'
""",
    "timestamp": """
SELECT 1 as id, TIMESTAMP '2024-01-01 06:00:00 UTC' as created_at, '{label}' as label
UNION ALL SELECT 2, TIMESTAMP '2024-01-02 06:00:00 UTC', '{label}'
UNION ALL SELECT 3, CAST(NULL AS TIMESTAMP), '{label}'
""",
}


def _executor(native: bool) -> Executor:
    try:
        return Dialects.BIGQUERY.default_executor(
            conf=BigQueryConfig(native_partition_swap=native)
        )
    except Exception as e:  # pragma: no cover - credentials-dependent
        pytest.skip(f"BigQuery not available: {e}")


@pytest.fixture(scope="module")
def native_executor() -> Generator[Executor, None, None]:
    executor = _executor(native=True)
    yield executor
    executor.close()


@pytest.fixture(scope="module")
def sql_executor() -> Generator[Executor, None, None]:
    executor = _executor(native=False)
    yield executor
    executor.close()


def _build(executor: Executor, table: str, datatype: str, label: str) -> None:
    executor.execute_text(
        MODEL.format(
            table=table, datatype=datatype, source=SOURCES[datatype].format(label=label)
        )
    )


def _rows(executor: Executor, table: str) -> list[tuple]:
    return [
        tuple(row)
        for row in executor.execute_raw_sql(
            f"SELECT id, created_at, label FROM `{table}` ORDER BY id"
        ).fetchall()
    ]


def _partition_ids(executor: Executor, dataset: str, name: str) -> set[str]:
    rows = executor.execute_raw_sql(
        f"SELECT partition_id FROM `{dataset}`.INFORMATION_SCHEMA.PARTITIONS"
        f" WHERE table_name = '{name}' AND total_rows > 0"
    ).fetchall()
    return {row[0] for row in rows}


@pytest.fixture
def table(bq_write_dataset) -> Generator[tuple[str, str], None, None]:
    name = f"trilogy_swap_test_{uuid4().hex[:8]}"
    yield f"{bq_write_dataset}.{name}", name
    executor = _executor(native=False)
    executor.execute_raw_sql(f"drop table if exists `{bq_write_dataset}.{name}`")
    executor.close()


@pytest.mark.parametrize("datatype", list(SOURCES))
def test_append_is_idempotent_for_every_partition_type(
    native_executor, bq_write_dataset, table, datatype
):
    """Rerunning one append must replace its slices rather than duplicate them,
    and must carry the new values through."""
    qualified, _ = table
    _build(native_executor, qualified, datatype, "first")
    native_executor.execute_text(APPEND)
    first = _rows(native_executor, qualified)
    assert [row[0] for row in first] == [1, 2, 3]
    assert {row[2] for row in first} == {"first"}

    _build(native_executor, qualified, datatype, "second")
    native_executor.execute_text(APPEND)
    second = _rows(native_executor, qualified)
    assert [row[0] for row in second] == [1, 2, 3]
    assert {row[2] for row in second} == {"second"}
    assert [row[1] for row in second] == [row[1] for row in first]


@pytest.mark.parametrize("datatype", list(SOURCES))
def test_the_null_slice_is_written_and_replaced(
    native_executor, bq_write_dataset, table, datatype
):
    """`__NULL__` is a real partition and its own decorator. The scripted loop
    this replaced could not reach it at all."""
    qualified, name = table
    _build(native_executor, qualified, datatype, "first")
    native_executor.execute_text(APPEND)
    assert "__NULL__" in _partition_ids(native_executor, bq_write_dataset, name)

    _build(native_executor, qualified, datatype, "second")
    native_executor.execute_text(APPEND)
    null_rows = [row for row in _rows(native_executor, qualified) if row[1] is None]
    assert len(null_rows) == 1
    assert null_rows[0][2] == "second"


def test_only_the_covered_partitions_are_replaced(
    native_executor, bq_write_dataset, table
):
    """The guarantee that makes this a *partitioned* append: a slice the select
    does not produce is not touched, and one it does produce is fully replaced
    rather than added to."""
    qualified, _ = table
    _build(native_executor, qualified, "date", "first")
    native_executor.execute_text(APPEND)
    # A neighbour no select below covers, and a stale row inside one they do.
    native_executor.execute_raw_sql(
        f"INSERT INTO `{qualified}` VALUES"
        " (99, DATE '2023-06-01', 'untouched'), (98, DATE '2024-01-01', 'stale')"
    )

    _build(native_executor, qualified, "date", "second")
    native_executor.execute_text(APPEND)
    rows = _rows(native_executor, qualified)
    assert (99, date(2023, 6, 1), "untouched") in rows
    assert 98 not in [row[0] for row in rows]
    assert sorted(row[0] for row in rows) == [1, 2, 3, 99]


@pytest.mark.parametrize("datatype", list(SOURCES))
def test_native_and_sql_paths_write_the_same_rows(
    native_executor, sql_executor, bq_write_dataset, datatype
):
    """The parity check. Same model, same select, two write implementations —
    including the NULL slice and a second run over the same slices."""
    names = {
        path: f"trilogy_swap_parity_{path}_{uuid4().hex[:8]}"
        for path in ("native", "sql")
    }
    tables = {path: f"{bq_write_dataset}.{name}" for path, name in names.items()}
    executors = {"native": native_executor, "sql": sql_executor}
    try:
        results = {}
        for path, executor in executors.items():
            for label in ("first", "second"):
                _build(executor, tables[path], datatype, label)
                executor.execute_text(APPEND)
            results[path] = _rows(executor, tables[path])
        assert results["native"] == results["sql"]
        assert len(results["native"]) == 3

        assert _partition_ids(
            native_executor, bq_write_dataset, names["native"]
        ) == _partition_ids(sql_executor, bq_write_dataset, names["sql"])
    finally:
        for qualified in tables.values():
            native_executor.execute_raw_sql(f"drop table if exists `{qualified}`")


def test_an_empty_select_replaces_nothing(native_executor, bq_write_dataset, table):
    """No slice is covered, so none is cleared — the twin of the staged DELETE
    matching no staged keys."""
    qualified, _ = table
    _build(native_executor, qualified, "date", "first")
    native_executor.execute_text(APPEND)
    before = _rows(native_executor, qualified)

    native_executor.execute_text(
        "append into facts by created_at from"
        " select id, created_at, label where id > 1000;"
    )
    assert _rows(native_executor, qualified) == before


def test_the_native_path_claims_a_partitioned_append(
    native_executor, bq_write_dataset, table
):
    """Rows alone cannot tell the two implementations apart, so assert the
    handshake directly: the engine returns a result for this shape (it handled
    it) and None for an unpartitioned overwrite (the SQL runs instead)."""
    from trilogy.core.statements.execute import ProcessedQueryPersist

    qualified, _ = table
    _build(native_executor, qualified, "date", "first")
    processed = native_executor.parse_text(APPEND)[-1]
    assert isinstance(processed, ProcessedQueryPersist)
    assert (
        native_executor.engine.execute_persist(processed, native_executor) is not None
    )
    assert len(_rows(native_executor, qualified)) == 3
