"""A partitioned append against live BigQuery, both ways.

The native path replaces each slice with a copy job onto a partition decorator;
the SQL path stages, deletes the covered keys and inserts. They must agree, and
the only thing that shows they do is running the same write both ways against a
real table and comparing rows — a rendered string cannot tell you that a
decorator addressed the slice you meant, that the null slice was reachable, or
that a neighbouring partition survived.

Two things this harness must not do, both of which silently make the tests pass
for the wrong reason:

- **Re-run the create between appends.** ``create or replace datasources``
  truncates, so a second one turns "append twice" into "truncate, then append"
  and the idempotency assertion becomes vacuous. The target is created exactly
  once per table; only the model's source rows are redefined.
- **Share an Environment across parametrized cases.** The model redefines the
  same concepts with a different datatype each time, so a reused environment
  carries the previous case's types.

Needs a dataset the credentials may create and drop tables in
(TRILOGY_BIGQUERY_TEST_DATASET).
"""

from collections.abc import Generator
from datetime import date
from uuid import uuid4

import pytest

from trilogy import Dialects, Environment, Executor
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
    {columns}
)
grain (id)
address `{table}`
partition by created_at;
"""

CREATE = "create or replace datasources facts;"
APPEND = "append into facts by created_at from select id, created_at, label;"

#: How the target datasource declares its columns, and the physical names that
#: gives the table. The declared names are the only ones the target has, and a
#: select names its outputs after *concepts* — so a layout whose two halves
#: differ is the ordinary case, and the one that catches anything asking the
#: select for the target's columns.
LAYOUTS = {
    "concept-named": (
        "id: id,\n    created_at: created_at,\n    label: label,",
        ("id", "created_at", "label"),
    ),
    "declared-named": (
        "fact_id: id,\n    event_date: created_at,\n    row_label: label,",
        ("fact_id", "event_date", "row_label"),
    ),
}

#: One row per slice plus the null slice. `label` is what a replace has to
#: overwrite: rerunning the append with a new label must leave one row per id
#: carrying the new value.
#:
#: Every type BigQuery can partition on is covered, DATETIME included — the
#: dialect's `SUPPORTED_PARTITION_KEY_TYPES` is the DDL's partition-expression
#: map, so a type the CREATE can partition on must survive an append too.
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


@pytest.fixture(scope="module")
def bq_client():
    """One authenticated client for the module; environments stay per-test."""
    try:
        from google.auth import default
        from google.cloud import bigquery

        credentials, project = default()
        return bigquery.Client(credentials=credentials, project=project)
    except Exception as e:  # pragma: no cover - credentials-dependent
        pytest.skip(f"BigQuery not available: {e}")


@pytest.fixture
def executors(bq_client) -> Generator:
    built: list[Executor] = []

    def make(native: bool) -> Executor:
        executor = Dialects.BIGQUERY.default_executor(
            environment=Environment(),
            conf=BigQueryConfig(client=bq_client, native_partition_swap=native),
        )
        built.append(executor)
        return executor

    yield make
    for executor in built:
        executor.close()


@pytest.fixture
def table(bq_write_dataset, bq_client) -> Generator[tuple[str, str], None, None]:
    name = f"trilogy_swap_{uuid4().hex[:8]}"
    yield f"{bq_write_dataset}.{name}", name
    bq_client.delete_table(f"{bq_write_dataset}.{name}", not_found_ok=True)


def _define(
    executor: Executor,
    table: str,
    datatype: str,
    label: str,
    layout: str = "concept-named",
) -> None:
    """Redefine the model — including the source rows — without touching the
    target table."""
    executor.execute_text(
        MODEL.format(
            table=table,
            datatype=datatype,
            source=SOURCES[datatype].format(label=label),
            columns=LAYOUTS[layout][0],
        )
    )


def _seed(
    executor: Executor,
    table: str,
    datatype: str,
    label: str,
    layout: str = "concept-named",
) -> None:
    _define(executor, table, datatype, label, layout)
    executor.execute_text(CREATE)


def _rows(executor: Executor, table: str, layout: str = "concept-named") -> list[tuple]:
    key, *rest = LAYOUTS[layout][1]
    return [
        tuple(row)
        for row in executor.execute_raw_sql(
            f"SELECT {', '.join([key, *rest])} FROM `{table}` ORDER BY {key}"
        ).fetchall()
    ]


def _partition_ids(executor: Executor, dataset: str, name: str) -> set[str]:
    rows = executor.execute_raw_sql(
        f"SELECT partition_id FROM `{dataset}`.INFORMATION_SCHEMA.PARTITIONS"
        f" WHERE table_name = '{name}' AND total_rows > 0"
    ).fetchall()
    return {row[0] for row in rows}


@pytest.mark.parametrize("datatype", list(SOURCES))
def test_append_is_idempotent_for_every_partition_type(executors, table, datatype):
    """Rerunning one append replaces its slices rather than duplicating them,
    and carries the new values through. The table is created once."""
    executor = executors(native=True)
    qualified, _ = table
    _seed(executor, qualified, datatype, "first")
    executor.execute_text(APPEND)
    first = _rows(executor, qualified)
    assert [row[0] for row in first] == [1, 2, 3]
    assert {row[2] for row in first} == {"first"}

    _define(executor, qualified, datatype, "second")
    executor.execute_text(APPEND)
    second = _rows(executor, qualified)
    assert [row[0] for row in second] == [1, 2, 3]
    assert {row[2] for row in second} == {"second"}
    assert [row[1] for row in second] == [row[1] for row in first]


@pytest.mark.parametrize("datatype", list(SOURCES))
def test_the_null_slice_is_written_and_replaced(
    executors, bq_write_dataset, table, datatype
):
    """BigQuery reports the null slice as `__NULL__` but rejects that as a
    decorator, so it is the one slice the native path replaces with DML."""
    executor = executors(native=True)
    qualified, name = table
    _seed(executor, qualified, datatype, "first")
    executor.execute_text(APPEND)
    assert "__NULL__" in _partition_ids(executor, bq_write_dataset, name)

    _define(executor, qualified, datatype, "second")
    executor.execute_text(APPEND)
    null_rows = [row for row in _rows(executor, qualified) if row[1] is None]
    assert len(null_rows) == 1
    assert null_rows[0][2] == "second"


def test_only_the_covered_partitions_are_replaced(executors, table):
    """The guarantee that makes this a *partitioned* append: a slice the select
    does not produce is untouched, and one it does produce is fully replaced
    rather than added to."""
    executor = executors(native=True)
    qualified, _ = table
    _seed(executor, qualified, "date", "first")
    executor.execute_text(APPEND)
    # A neighbour the select never covers, and a stale row inside one it does.
    executor.execute_raw_sql(
        f"INSERT INTO `{qualified}` VALUES"
        " (99, DATE '2023-06-01', 'untouched'), (98, DATE '2024-01-01', 'stale')"
    )

    _define(executor, qualified, "date", "second")
    executor.execute_text(APPEND)
    rows = _rows(executor, qualified)
    assert (99, date(2023, 6, 1), "untouched") in rows
    assert sorted(row[0] for row in rows) == [1, 2, 3, 99]


@pytest.mark.parametrize("datatype", list(SOURCES))
def test_native_and_sql_paths_write_the_same_rows(
    executors, bq_write_dataset, datatype
):
    """The parity check. Same model, same select, two write implementations —
    including the null slice and a second run over the same slices."""
    results: dict[str, list[tuple]] = {}
    partitions: dict[str, set[str]] = {}
    tables: dict[str, tuple[Executor, str]] = {}
    try:
        for path, native in (("native", True), ("sql", False)):
            executor = executors(native=native)
            name = f"trilogy_parity_{path}_{uuid4().hex[:8]}"
            qualified = f"{bq_write_dataset}.{name}"
            tables[path] = (executor, qualified)
            _seed(executor, qualified, datatype, "first")
            executor.execute_text(APPEND)
            _define(executor, qualified, datatype, "second")
            executor.execute_text(APPEND)
            results[path] = _rows(executor, qualified)
            partitions[path] = _partition_ids(executor, bq_write_dataset, name)
        assert results["native"] == results["sql"]
        assert len(results["native"]) == 3
        assert partitions["native"] == partitions["sql"]
    finally:
        for executor, qualified in tables.values():
            executor.execute_raw_sql(f"drop table if exists `{qualified}`")


def test_declared_column_names_survive_the_swap(executors, bq_write_dataset, table):
    """The target's columns are named by the datasource, not by the concepts the
    select outputs. Nothing on the write path may ask the select for them: the
    staging table brings the names and the rows land positionally. Staging them
    under concept names instead fails as "The field specified for partitioning
    cannot be found in the schema" — or, worse, lands them in the wrong column.
    """
    executor = executors(native=True)
    qualified, name = table
    _seed(executor, qualified, "date", "first", layout="declared-named")
    executor.execute_text(APPEND)
    first = _rows(executor, qualified, layout="declared-named")
    assert [row[0] for row in first] == [1, 2, 3]
    assert [row[1] for row in first] == [date(2024, 1, 1), date(2024, 1, 2), None]

    _define(executor, qualified, "date", "second", layout="declared-named")
    executor.execute_text(APPEND)
    second = _rows(executor, qualified, layout="declared-named")
    assert {row[2] for row in second} == {"second"}
    assert [row[1] for row in second] == [row[1] for row in first]
    assert "__NULL__" in _partition_ids(executor, bq_write_dataset, name)


def test_an_empty_select_replaces_nothing(executors, table):
    """No slice is covered, so none is cleared — the twin of the staged DELETE
    matching no staged keys."""
    executor = executors(native=True)
    qualified, _ = table
    _seed(executor, qualified, "date", "first")
    executor.execute_text(APPEND)
    before = _rows(executor, qualified)

    executor.execute_text(
        "append into facts by created_at from"
        " select id, created_at, label where id > 1000;"
    )
    assert _rows(executor, qualified) == before


def test_the_native_path_claims_a_partitioned_append(executors, table):
    """Rows alone cannot tell the implementations apart, so assert the handshake
    directly: the engine returns a result for this shape, meaning it handled the
    write itself rather than declining to SQL."""
    from trilogy.core.statements.execute import ProcessedQueryPersist

    executor = executors(native=True)
    qualified, _ = table
    _seed(executor, qualified, "date", "first")
    processed = executor.parse_text(APPEND)[-1]
    assert isinstance(processed, ProcessedQueryPersist)
    assert executor.engine.execute_persist(processed, executor) is not None
    assert len(_rows(executor, qualified)) == 3
