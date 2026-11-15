"""Smoke tests against a real Databricks SQL warehouse.

Skipped unless DATABRICKS_HOST, DATABRICKS_HTTP_PATH and DATABRICKS_TOKEN are
set. Tables are created in TRILOGY_DATABRICKS_CATALOG/SCHEMA (default
workspace.default) and dropped afterwards.
"""

import os
import uuid
from collections.abc import Generator
from datetime import date

import pytest

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DatabricksConfig

pytestmark = pytest.mark.databricks_server

CATALOG = os.getenv("TRILOGY_DATABRICKS_CATALOG", "workspace")
SCHEMA = os.getenv("TRILOGY_DATABRICKS_SCHEMA", "default")
SUFFIX = uuid.uuid4().hex[:8]
SALES = f"trilogy_sales_{SUFFIX}"
DAILY = f"trilogy_daily_{SUFFIX}"

MODEL = f"""
key id int;
property id.region string;
property id.amount float;
property id.sold date;
property id.tags list<string>;
datasource sales (id: id, region: region, amount: amount, sold: sold, tags: tags)
grain (id)
address {SALES};
"""


def _configured() -> bool:
    names = ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")
    return all(
        os.getenv(n) and not os.getenv(n, "").startswith("vault:") for n in names
    )


@pytest.fixture(scope="module")
def executor() -> Generator[Executor, None, None]:
    if not _configured():
        pytest.skip("DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN not set")
    exec = Dialects.DATABRICKS.default_executor(
        environment=Environment(),
        conf=DatabricksConfig(catalog=CATALOG, schema=SCHEMA),
    )
    exec.execute_raw_sql(f"""CREATE TABLE {SALES} AS
        SELECT id, region, CAST(amount AS DOUBLE) AS amount, sold, tags FROM VALUES
        (1, 'east', 10.0, DATE'2024-01-01', array('a', 'b')),
        (2, 'east', 5.0, DATE'2024-01-02', array('c')),
        (3, 'west', 7.5, DATE'2024-01-02', array()),
        (4, NULL, 1.0, DATE'2024-02-01', array('d'))
        AS t(id, region, amount, sold, tags)""")
    exec.parse_text(MODEL)
    try:
        yield exec
    finally:
        for table in (SALES, DAILY):
            exec.execute_raw_sql(f"DROP TABLE IF EXISTS {table}")
        exec.close()


def _rows(executor: Executor, query: str) -> list[tuple]:
    return [tuple(r) for r in executor.execute_text(query)[-1].fetchall()]


def test_aggregate_by_group(executor: Executor):
    assert _rows(
        executor, "select region, sum(amount) as total order by region asc;"
    ) == [("east", 15.0), ("west", 7.5), (None, 1.0)]


def test_rollup(executor: Executor):
    rows = _rows(
        executor,
        "select region, sum(amount) as total by rollup (region) order by total desc;",
    )
    assert rows[0] == (None, 23.5)


def test_qualify_window(executor: Executor):
    assert (
        _rows(
            executor,
            """auto amount_rank <- rank id over region by amount desc;
        select region, id where amount_rank = 1 and region is not null
        order by region asc;""",
        )
        == [("east", 1), ("west", 3)]
    )


def test_unnest_and_index(executor: Executor):
    assert _rows(
        executor, "select id, unnest(tags) as tag where id = 1 order by tag asc;"
    ) == [(1, "a"), (1, "b")]
    assert _rows(
        executor, "select id, tags[1] as first where id <= 3 order by id asc;"
    ) == [
        (1, "a"),
        (2, "c"),
        (3, None),
    ]


def test_array_agg_of_only_nulls_is_null(executor: Executor):
    assert _rows(executor, "select array_agg(region) as regions where id = 4;") == [
        (None,)
    ]


def test_dates(executor: Executor):
    assert (
        _rows(
            executor,
            """select id, date_trunc(sold, month) as m, date_diff(sold, '2024-03-01'::date, month) as n,
        format_time(sold, '%Y/%m') as f where id = 4;""",
        )
        == [(4, date(2024, 2, 1), 1, "2024/02")]
    )


def test_complex_constants(executor: Executor):
    assert (
        _rows(
            executor,
            """const arr <- [1, 2, 3];
        const m <- {'a': 1};
        select len(arr) as n, m['a'] as a, "it's \\\\d" as s;""",
        )
        == [(3, 1, "it's \\\\d")]
    )


def test_table_schema(executor: Executor):
    columns = {
        c.column_name: c for c in executor.generator.get_table_schema(executor, SALES)
    }
    assert set(columns) == {"id", "region", "amount", "sold", "tags"}


def test_persist_and_partitioned_append(executor: Executor):
    executor.parse_text(f"""
auto daily_total <- sum(amount) by sold;
datasource daily (sold: sold, total: daily_total)
grain (sold)
address {DAILY}
incremental by sold
partition by sold;
""")
    executor.execute_text("create if not exists datasource daily;")
    executor.execute_text("append daily where sold <= '2024-01-02'::date;")
    executor.execute_text("append daily where sold = '2024-01-02'::date;")
    assert executor.execute_raw_sql(
        f"select sold, total from {DAILY} order by sold"
    ).fetchall() == [(date(2024, 1, 1), 10.0), (date(2024, 1, 2), 12.5)]


def test_table_primary_keys(executor: Executor):
    pk_table = f"trilogy_pk_{SUFFIX}"
    executor.execute_raw_sql(
        f"CREATE TABLE {pk_table} (a BIGINT NOT NULL, b BIGINT NOT NULL, c STRING,"
        f" CONSTRAINT {pk_table}_pk PRIMARY KEY (b, a))"
    )
    try:
        dialect = executor.generator
        assert dialect.get_table_primary_keys(executor, pk_table) == ["b", "a"]
        assert dialect.get_table_primary_keys(executor, SALES) == []
    finally:
        executor.execute_raw_sql(f"DROP TABLE IF EXISTS {pk_table}")
