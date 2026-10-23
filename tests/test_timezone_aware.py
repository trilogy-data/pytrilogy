from datetime import UTC, datetime

import pytest

from trilogy.core.enums import DatePart, FunctionType
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


@pytest.mark.parametrize(
    "dialect, naive, aware",
    [
        (
            DuckDBDialect(),
            "datetime '2024-01-15 22:30:00'",
            "timestamptz '2024-01-15 22:30:00+00:00'",
        ),
        (
            PostgresDialect(),
            "timestamp '2024-01-15 22:30:00'",
            "timestamptz '2024-01-15 22:30:00+00:00'",
        ),
        (
            BigqueryDialect(),
            "DATETIME '2024-01-15 22:30:00'",
            "TIMESTAMP '2024-01-15 22:30:00+00:00'",
        ),
        (
            TrinoDialect(),
            "timestamp '2024-01-15 22:30:00'",
            "timestamp '2024-01-15 22:30:00+00:00'",
        ),
        (
            ClickhouseDialect(),
            "toDateTime64('2024-01-15 22:30:00', 3)",
            "parseDateTime64BestEffort('2024-01-15 22:30:00+00:00', 3, 'UTC')",
        ),
    ],
)
def test_literal_keeps_awareness(dialect, naive, aware):
    assert dialect.render_expr(NAIVE) == naive
    assert dialect.render_expr(AWARE) == aware


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


@pytest.mark.parametrize(
    "dialect, naive, aware",
    [
        (TrinoDialect(), "LOCALTIMESTAMP", "CURRENT_TIMESTAMP"),
        (PostgresDialect(), "LOCALTIMESTAMP", "CURRENT_TIMESTAMP"),
        (SnowflakeDialect(), "LOCALTIMESTAMP()::TIMESTAMP_NTZ", "CURRENT_TIMESTAMP()"),
    ],
)
def test_current_time_keeps_awareness(dialect, naive, aware):
    assert dialect.FUNCTION_MAP[FunctionType.CURRENT_DATETIME]([], []) == naive
    assert dialect.FUNCTION_MAP[FunctionType.CURRENT_TIMESTAMP]([], []) == aware
