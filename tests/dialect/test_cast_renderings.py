"""Renderings behind formal/Trilogy/Casts.lean on dialects with no engine in
CI; tests/core/test_cast_laws.py runs the laws on DuckDB, SQLite and
ClickHouse."""

import duckdb
import pytest

from tests.dialect.test_string_function_renderings import _select
from trilogy import Dialects
from trilogy.core.casts import FALSE_TEXT, TRUE_TEXT, text_to_bool
from trilogy.dialect.common import bool_from_text, text_from_bool

RENDERED = [
    (Dialects.POSTGRES, "v::string", 'cast("s"."v" as text)'),
    (Dialects.SQL_SERVER, "v::string", 'cast("s"."v" as NVARCHAR(MAX))'),
    (Dialects.SQL_SERVER, "i::bool", 'cast("s"."i" as BIT)'),
    (Dialects.MYSQL, "i::int", "CAST(`s`.`i` AS SIGNED)"),
    (Dialects.MYSQL, "v::string", "CAST(`s`.`v` AS CHAR)"),
    (Dialects.MYSQL, "i::bool", "(`s`.`i` <> 0)"),
    (
        Dialects.MYSQL,
        "(i > 0)::string",
        "CASE (`s`.`i` > 0) WHEN 1 THEN 'true' WHEN 0 THEN 'false' END",
    ),
    (
        Dialects.SQL_SERVER,
        "(i > 0)::string",
        """CASE ("s"."i" > 0) WHEN 1 THEN 'true' WHEN 0 THEN 'false' END""",
    ),
    (Dialects.BIGQUERY, "v::bool", bool_from_text("`s`.`v`")),
    (Dialects.PRESTO, "v::bool", bool_from_text('"s"."v"')),
    (Dialects.MYSQL, "v::bool", bool_from_text("`s`.`v`")),
    (
        Dialects.SQL_SERVER,
        "v::bool",
        bool_from_text('"s"."v"', "CAST(1 AS bit)", "CAST(0 AS bit)"),
    ),
    (Dialects.POSTGRES, "v::bool", 'cast("s"."v" as bool)'),
    (Dialects.SNOWFLAKE, "sqrt(i)", 'sqrt(CASE WHEN "s"."i" >= 0 THEN "s"."i" END)'),
    (
        Dialects.SQL_SERVER,
        "log(i, 2)",
        """(LOG(CASE WHEN "s"."i" > 0 THEN "s"."i" END) / nullif(LOG(2), 0))""",
    ),
    (
        Dialects.SNOWFLAKE,
        "log(i, 10)",
        """(LN(CASE WHEN "s"."i" > 0 THEN "s"."i" END) / nullif(LN(10), 0))""",
    ),
    (Dialects.POSTGRES, "sqrt(4)", "sqrt(4)"),
]


@pytest.mark.parametrize("dialect, expr, expected", RENDERED)
def test_rendering(dialect: Dialects, expr: str, expected: str) -> None:
    assert _select(dialect, expr) == expected


@pytest.mark.parametrize("text", [*TRUE_TEXT, *FALSE_TEXT, "TRUE", "No", "", "on"])
def test_text_bool_case_is_the_reference(text: str) -> None:
    sql = bool_from_text("v")
    got = duckdb.sql(f"select {sql} from (select '{text}' as v)").fetchall()[0][0]
    try:
        want: bool | None = text_to_bool(text)
    except ValueError:
        want = None
    assert got is want


@pytest.mark.parametrize("value", [True, False, None])
def test_bool_text_case(value: bool | None) -> None:
    literal = "NULL" if value is None else str(int(value))
    got = duckdb.sql(f"select {text_from_bool(literal)}").fetchall()[0][0]
    assert got == (None if value is None else str(value).lower())
