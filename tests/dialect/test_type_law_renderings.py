"""Renderings behind formal/Trilogy/Types.lean on dialects with no engine in
CI; tests/core/test_type_materialization_laws.py runs the laws on DuckDB and
SQLite."""

import pytest

from trilogy.core.enums import FunctionType
from trilogy.core.models.core import DataType
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.dialect.clickhouse import ClickhouseDialect
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.dialect.mysql import MySQLDialect
from trilogy.dialect.postgres import PostgresDialect
from trilogy.dialect.presto import PrestoDialect
from trilogy.dialect.snowflake import SnowflakeDialect
from trilogy.dialect.sql_server import SqlServerDialect
from trilogy.dialect.sqlite import SQLiteDialect


@pytest.mark.parametrize(
    "dialect, rendered",
    [
        (BaseDialect, "float"),
        (PostgresDialect, "float"),
        (SqlServerDialect, "float"),
        (SnowflakeDialect, "float"),
        (DuckDBDialect, "double"),
        (MySQLDialect, "DOUBLE"),
        (PrestoDialect, "double"),
        (BigqueryDialect, "FLOAT64"),
        (ClickhouseDialect, "Float64"),
        (SQLiteDialect, "REAL"),
    ],
)
def test_float_is_eight_bytes(dialect, rendered: str) -> None:
    assert dialect.DATATYPE_MAP[DataType.FLOAT] == rendered


@pytest.mark.parametrize(
    "dialect, rendered",
    [
        (BaseDialect, "power(x, y)"),
        (PostgresDialect, "power(x, y)"),
        (PrestoDialect, "power(x, y)"),
        (SQLiteDialect, "power(x, y)"),
        (SqlServerDialect, "POWER(CAST(x AS FLOAT), y)"),
    ],
)
def test_power_is_fractional(dialect, rendered: str) -> None:
    assert dialect.FUNCTION_MAP[FunctionType.POWER](["x", "y"], []) == rendered
