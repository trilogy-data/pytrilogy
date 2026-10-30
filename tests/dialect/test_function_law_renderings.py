"""Renderings behind formal/Trilogy/Functions.lean on dialects with no engine
in CI; tests/core/test_function_laws.py runs the laws on DuckDB and SQLite."""

import pytest

from trilogy.core.enums import FunctionType
from trilogy.core.models.core import DataType
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.dialect.clickhouse import ClickhouseDialect
from trilogy.dialect.mysql import MySQLDialect
from trilogy.dialect.postgres import PostgresDialect
from trilogy.dialect.presto import PrestoDialect
from trilogy.dialect.snowflake import SnowflakeDialect
from trilogy.dialect.sql_server import SqlServerDialect
from trilogy.dialect.sqlite import SQLiteDialect

F = FunctionType
INT = DataType.INTEGER
DT = DataType.DATETIME
PARTS = ["year", "quarter", "month", "week", "day", "hour", "minute", "second"]


def render(
    dialect: type[BaseDialect], fn: FunctionType, args: list, types: list
) -> str:
    return dialect.FUNCTION_MAP[fn](args, types)


@pytest.mark.parametrize(
    "dialect, float_type",
    [
        (SQLiteDialect, "REAL"),
        (PostgresDialect, "double precision"),
        (SqlServerDialect, "float"),
        (PrestoDialect, "double"),
    ],
)
def test_integer_quotient_casts_the_dividend(dialect, float_type: str) -> None:
    assert render(dialect, F.DIVIDE, ["a", "b"], [INT, INT]) == (
        f"cast(a as {float_type}) / b"
    )
    assert render(dialect, F.DIVIDE, ["a", "b"], [DataType.FLOAT, INT]) == "a / b"


@pytest.mark.parametrize(
    "dialect, expected",
    [
        (SQLiteDialect, "cast(round(x) as INTEGER)"),
        (PostgresDialect, "cast((x)::numeric as INTEGER)"),
        (SqlServerDialect, "cast(round(x, 0) as INTEGER)"),
    ],
)
def test_fraction_cast_rounds(dialect, expected: str) -> None:
    assert render(dialect, F.CAST, ["x", "INTEGER"], [DataType.DOUBLE, INT]) == expected
    assert render(
        dialect, F.CAST, ["x", "TEXT"], [DataType.DOUBLE, DataType.STRING]
    ) == ("cast(x as TEXT)")


def test_postgres_date_diff_truncates_both_sides() -> None:
    out = {
        p: render(PostgresDialect, F.DATE_DIFF, ["a", "b", p], [DT, DT]) for p in PARTS
    }
    assert out["day"] == "((b)::date - (a)::date)"
    assert out["week"] == "(((b)::date - (a)::date) / 7)"
    assert "date_part('quarter', b)" in out["quarter"]
    assert (
        "date_trunc('hour', (b)::timestamp)" in out["hour"] and "/ 3600" in out["hour"]
    )
    assert "/ 1 AS BIGINT" in out["second"]
    with pytest.raises(NotImplementedError):
        render(PostgresDialect, F.DATE_DIFF, ["a", "b", "day_of_week"], [DT, DT])


def test_mysql_date_diff_counts_fields() -> None:
    out = {p: render(MySQLDialect, F.DATE_DIFF, ["a", "b", p], [DT, DT]) for p in PARTS}
    assert out["year"] == "(YEAR(b) - YEAR(a))"
    assert out["quarter"] == "((YEAR(b) - YEAR(a)) * 4 + QUARTER(b) - QUARTER(a))"
    assert out["month"] == "((YEAR(b) - YEAR(a)) * 12 + MONTH(b) - MONTH(a))"
    assert out["week"] == "(DATEDIFF(b, a) DIV 7)"
    assert out["minute"] == (
        "TIMESTAMPDIFF(MINUTE, DATE_FORMAT(a, '%Y-%m-%d %H:%i:00'),"
        " DATE_FORMAT(b, '%Y-%m-%d %H:%i:00'))"
    )
    with pytest.raises(NotImplementedError):
        render(MySQLDialect, F.DATE_DIFF, ["a", "b", "day_of_week"], [DT, DT])


def test_sql_server_date_diff() -> None:
    assert render(SqlServerDialect, F.DATE_DIFF, ["a", "b", "day"], [DT, DT]) == (
        "DATEDIFF(day, a, b)"
    )
    assert render(SqlServerDialect, F.DATE_DIFF, ["a", "b", "week"], [DT, DT]) == (
        "(DATEDIFF(day, a, b) / 7)"
    )


def test_sqlite_date_diff_rejects_unknown_part() -> None:
    with pytest.raises(ValueError):
        render(SQLiteDialect, F.DATE_DIFF, ["a", "b", "day_of_week"], [DT, DT])


@pytest.mark.parametrize(
    "dialect, day_of_week, week",
    [
        (PostgresDialect, "(date_part('dow', x) + 1)", "date_part('week', x)"),
        (MySQLDialect, "DAYOFWEEK(x)", "WEEK(x, 3)"),
        (BigqueryDialect, "EXTRACT(DAYOFWEEK from x)", "EXTRACT(ISOWEEK from x)"),
        (SnowflakeDialect, "(DAYOFWEEKISO(x) % 7 + 1)", "WEEKISO(x)"),
        (ClickhouseDialect, "(toDayOfWeek(x) % 7 + 1)", "toISOWeek(x)"),
        (PrestoDialect, "(day_of_week(x) % 7 + 1)", "EXTRACT(WEEK from x)"),
        (
            SqlServerDialect,
            "(((DATEPART(weekday, x) + @@DATEFIRST - 2) % 7 + 1) % 7 + 1)",
            "DATEPART(iso_week, x)",
        ),
    ],
)
def test_day_of_week_is_sunday_first_and_week_is_iso(
    dialect, day_of_week: str, week: str
) -> None:
    assert render(dialect, F.DAY_OF_WEEK, ["x"], [DT]) == day_of_week
    assert render(dialect, F.WEEK, ["x"], [DT]) == week


@pytest.mark.parametrize(
    "dialect",
    [
        PostgresDialect,
        MySQLDialect,
        BigqueryDialect,
        SnowflakeDialect,
        ClickhouseDialect,
    ],
)
def test_date_part_agrees_with_the_function(dialect) -> None:
    for name, fn in [("day_of_week", F.DAY_OF_WEEK), ("week", F.WEEK)]:
        part = render(dialect, F.DATE_PART, ["x", name], [DT])
        assert part.upper() == render(dialect, fn, ["x"], [DT]).upper()
    assert "MONTH" in render(dialect, F.DATE_PART, ["x", "month"], [DT]).upper()
