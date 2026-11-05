"""Renderings behind formal/Trilogy/Types.lean on dialects with no engine in
CI; tests/core/test_type_materialization_laws.py runs the laws on DuckDB and
SQLite."""

import pytest

from trilogy.core.enums import FunctionType
from trilogy.core.models.core import (
    DataType,
    NumericType,
    TraitDataType,
    merge_computed_types,
)
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


def test_computed_merge_keeps_a_float_trait() -> None:
    money = TraitDataType(type=DataType.FLOAT, traits=["usd"])
    merged = merge_computed_types([NumericType(precision=12, scale=2), money])
    assert merged == money and merged.traits == ["usd"]


def test_duckdb_sub_day_truncation_keeps_its_type() -> None:
    render = DuckDBDialect.FUNCTION_MAP[FunctionType.DATE_TRUNCATE]
    assert render(["t", "hour"], [DataType.TIMESTAMP]) == "date_trunc('hour', t)"
    assert render(["t", "month"], [DataType.TIMESTAMP]) == (
        "cast(date_trunc('month', t) as date)"
    )
