from datetime import UTC, datetime

import pytest

from trilogy.core.enums import DatePart
from trilogy.core.functions import get_date_trunc_output
from trilogy.core.models.core import DataType, arg_to_datatype
from trilogy.core.validation.datasource import type_check
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.dialect.clickhouse import ClickhouseDialect
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.dialect.mysql import MySQLDialect
from trilogy.dialect.postgres import PostgresDialect
from trilogy.dialect.presto import PrestoDialect, TrinoDialect
from trilogy.dialect.snowflake import SnowflakeDialect

AWARE = datetime(2024, 1, 15, 22, 30, tzinfo=UTC)
NAIVE = datetime(2024, 1, 15, 22, 30)


@pytest.mark.parametrize(
    "dialect, naive, aware",
    [
        (DuckDBDialect(), "datetime", "timestamptz"),
        (PostgresDialect(), "timestamp", "timestamptz"),
        (SnowflakeDialect(), "TIMESTAMP_NTZ", "TIMESTAMP_TZ"),
        (PrestoDialect(), "timestamp", "timestamp with time zone"),
        (TrinoDialect(), "timestamp", "timestamp with time zone"),
        (BigqueryDialect(), "DATETIME", "TIMESTAMP"),
        (ClickhouseDialect(), "DateTime64(3)", "DateTime64(3, 'UTC')"),
        (MySQLDialect(), "DATETIME", "TIMESTAMP"),
    ],
)
def test_ddl_keeps_awareness(dialect, naive, aware):
    assert dialect.render_expr(DataType.DATETIME) == naive
    assert dialect.render_expr(DataType.TIMESTAMP) == aware


@pytest.mark.parametrize(
    "dialect",
    [
        DuckDBDialect(),
        PostgresDialect(),
        SnowflakeDialect(),
        TrinoDialect(),
        BigqueryDialect(),
        ClickhouseDialect(),
        MySQLDialect(),
    ],
)
def test_written_type_reads_back_as_declared(dialect):
    for declared in (DataType.DATETIME, DataType.TIMESTAMP):
        assert dialect.normalize_db_type(dialect.render_expr(declared)) == declared


def test_aware_literal_renders_aware():
    assert (
        DuckDBDialect().render_expr(AWARE) == "timestamptz '2024-01-15 22:30:00+00:00'"
    )
    assert (
        BigqueryDialect().render_expr(AWARE) == "TIMESTAMP '2024-01-15 22:30:00+00:00'"
    )
    assert DuckDBDialect().render_expr(NAIVE) == "datetime '2024-01-15 22:30:00'"


def test_duckdb_timestamp_function_is_aware():
    from trilogy import Dialects

    sql = Dialects.DUCK_DB.default_executor().generate_sql(
        "const x <- timestamp('2024-01-15 22:30:00+00'); select x;"
    )[-1]
    assert "as timestamptz" in sql


def test_python_value_types_by_awareness():
    assert arg_to_datatype(AWARE) == DataType.TIMESTAMP
    assert arg_to_datatype(NAIVE) == DataType.DATETIME


def test_type_check_distinguishes_awareness():
    assert type_check(AWARE, DataType.TIMESTAMP)
    assert type_check(NAIVE, DataType.DATETIME)
    assert not type_check(NAIVE, DataType.TIMESTAMP)
    assert not type_check(AWARE, DataType.DATETIME)


def test_sub_day_trunc_keeps_awareness():
    assert get_date_trunc_output([AWARE, DatePart.HOUR]) == DataType.TIMESTAMP
    assert get_date_trunc_output([NAIVE, DatePart.HOUR]) == DataType.DATETIME


def test_mock_timestamps_are_aware():
    from trilogy.dialect.mock import mock_timestamps

    assert all(v.tzinfo for v in mock_timestamps(5, True))
    assert all(v.tzinfo for v in mock_timestamps(5, False))
