"""Renderings behind formal/Trilogy/Scalars.lean on dialects with no engine in
CI; tests/core/test_scalar_laws.py runs the laws on DuckDB and SQLite."""

import sqlite3

import pytest

from trilogy.core.enums import FunctionType
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment
from trilogy.dialect.base import BaseDialect, render_present_extreme
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.dialect.mysql import MySQLDialect
from trilogy.dialect.postgres import PostgresDialect
from trilogy.dialect.presto import PrestoDialect
from trilogy.dialect.snowflake import SnowflakeDialect
from trilogy.dialect.sql_server import SqlServerDialect
from trilogy.dialect.sqlite import SQLiteDialect

F = FunctionType


def render(
    dialect: type[BaseDialect], fn: FunctionType, args: list, types: list
) -> str:
    return dialect.FUNCTION_MAP[fn](args, types)


@pytest.mark.parametrize(
    "dialect, ten, other",
    [
        (BaseDialect, "log10(x)", "(ln(x) / nullif(ln(b), 0))"),
        (PostgresDialect, "log10(x)", "(ln(x) / nullif(ln(b), 0))"),
        (MySQLDialect, "log10(x)", "(ln(x) / nullif(ln(b), 0))"),
        (BigqueryDialect, "log10(x)", "(ln(x) / nullif(ln(b), 0))"),
        (PrestoDialect, "log10(x)", "(ln(x) / nullif(ln(b), 0))"),
        (SnowflakeDialect, "(LN(x) / nullif(LN(10), 0))", "(LN(x) / nullif(LN(b), 0))"),
        (
            SqlServerDialect,
            "(LOG(x) / nullif(LOG(10), 0))",
            "(LOG(x) / nullif(LOG(b), 0))",
        ),
    ],
)
def test_log_base_is_second(dialect, ten: str, other: str) -> None:
    def guarded(s: str) -> str:
        return s.replace("(x)", "(CASE WHEN x > 0 THEN x END)").replace(
            "(b)", "(CASE WHEN b > 0 THEN b END)"
        )

    assert render(dialect, F.LOG, ["x", "10"], []) == guarded(ten)
    assert render(dialect, F.LOG, ["x", "b"], []) == guarded(other)


@pytest.mark.parametrize(
    "dialect, name",
    [
        (BigqueryDialect, "GREATEST"),
        (SnowflakeDialect, "GREATEST"),
        (MySQLDialect, "GREATEST"),
        (PrestoDialect, "greatest"),
    ],
)
def test_strict_greatest_falls_back(dialect, name: str) -> None:
    assert render(dialect, F.GREATEST, ["a", "b"], []) == (
        f"{name}(coalesce(a, b), coalesce(b, a))"
    )


@pytest.mark.parametrize("dialect", [BaseDialect, PostgresDialect, SqlServerDialect])
def test_null_skipping_greatest_is_native(dialect) -> None:
    assert render(dialect, F.GREATEST, ["a", "b"], []) == "greatest(a,b)"


def test_fallback_is_exact_under_a_strict_extreme() -> None:
    """`max(a, b, ...)` on SQLite is NULL when any argument is, as BigQuery's,
    Snowflake's, MySQL's and Presto's `greatest` are."""
    values = [None, 1, 2]
    connection = sqlite3.connect(":memory:")
    for a in values:
        for b in values:
            for c in values:
                args = [repr(v) if v is not None else "NULL" for v in (a, b, c)]
                present = [v for v in (a, b, c) if v is not None]
                for fn, name in ((max, "max"), (min, "min")):
                    sql = render_present_extreme(name)(args, [])
                    (got,) = connection.execute(f"select {sql}").fetchone()
                    assert got == (fn(present) if present else None), sql


@pytest.mark.parametrize(
    "dialect, plain",
    [
        (DuckDBDialect, "date_add(d, (1) * INTERVAL 1 day)"),
        (PostgresDialect, "(d + (1) * INTERVAL '1 day')"),
    ],
)
def test_day_offset_of_a_date_casts_back(dialect, plain: str) -> None:
    types = [DataType.DATE, DataType.DATE_PART, DataType.INTEGER]
    rendered = render(dialect, F.DATE_ADD, ["d", "day", "1"], types)
    assert rendered.lower() == f"cast({plain} as date)".lower()
    stamp = [DataType.DATETIME, DataType.DATE_PART, DataType.INTEGER]
    assert render(dialect, F.DATE_ADD, ["d", "day", "1"], stamp) == plain
    assert render(dialect, F.DATE_ADD, ["d", "hour", "1"], types) == plain.replace(
        "day", "hour"
    )


def test_log_accepts_a_fractional_base() -> None:
    env = Environment()
    env.parse("key x float; auto l <- log(x, 2.5); auto t <- log(x, 10.0);")
    assert env.concepts["l"].datatype == DataType.FLOAT
    assert render(BaseDialect, F.LOG, ["x", "10.0"], []) == (
        "log10(CASE WHEN x > 0 THEN x END)"
    )


def test_sqlite_offset_rejects_a_non_calendar_unit() -> None:
    with pytest.raises(ValueError, match="day_of_week"):
        render(SQLiteDialect, F.DATE_ADD, ["d", "day_of_week", "1"], [])
