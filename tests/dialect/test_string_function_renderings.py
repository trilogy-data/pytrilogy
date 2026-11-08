"""Renderings behind formal/Trilogy/Strings.lean on dialects with no engine in
CI; tests/core/test_string_function_laws.py runs the laws on DuckDB, SQLite and
ClickHouse."""

import duckdb
import pytest

from tests.core.test_string_function_laws import substr
from trilogy import Dialects, Environment
from trilogy.constants import Rendering
from trilogy.dialect.common import render_window_substring
from trilogy.parser import parse
from trilogy.render import get_dialect_generator

MODEL = """
key id int;
property id.v string;
property id.i int;
datasource s (id: id, v: ?v, i: ?i) grain (id) address s;
"""


def _select(dialect: Dialects, expr: str) -> str:
    env = Environment()
    _, statements = parse(MODEL + f"select id, {expr} as x;", env)
    renderer = get_dialect_generator(dialect, rendering=Rendering(parameters=False))
    sql = renderer.compile_statements(*renderer.generate_queries(env, statements))
    body = " ".join(sql[-1].split())
    selected = body[body.index(", ") + 2 : body.upper().index(" FROM ")]
    return selected[: selected.upper().rindex(" AS ")]


RENDERED = [
    (Dialects.MYSQL, "len(v)", "CHAR_LENGTH(`s`.`v`)"),
    (Dialects.SQL_SERVER, "len(v)", """(LEN("s"."v" + 'x') - 1)"""),
    (Dialects.POSTGRES, "len(v)", 'length("s"."v")'),
    (
        Dialects.SQL_SERVER,
        "strpos(v, 'a')",
        (
            """CASE WHEN "s"."v" IS NULL THEN NULL WHEN DATALENGTH('a') = 0"""
            """ THEN 1 ELSE CHARINDEX('a', "s"."v") END"""
        ),
    ),
    (Dialects.SNOWFLAKE, "strpos(v, 'a')", """POSITION('a', "s"."v")"""),
    (Dialects.BIGQUERY, "trim(v)", "TRIM(`s`.`v`, ' ')"),
    (Dialects.BIGQUERY, "ltrim(v)", "LTRIM(`s`.`v`, ' ')"),
    (Dialects.BIGQUERY, "rtrim(v)", "RTRIM(`s`.`v`, ' ')"),
    (
        Dialects.POSTGRES,
        "regexp_replace(v, 'a', 'b')",
        """REGEXP_REPLACE("s"."v",'a', 'b', 'g')""",
    ),
    (Dialects.POSTGRES, "substring(v, 2, 3)", 'SUBSTRING("s"."v",2,3)'),
    (Dialects.BIGQUERY, "substring(v, 2, 3)", "SUBSTRING(`s`.`v`,2,3)"),
]


@pytest.mark.parametrize("dialect, expr, expected", RENDERED)
def test_rendering(dialect: Dialects, expr: str, expected: str) -> None:
    assert _select(dialect, expr) == expected


@pytest.mark.parametrize(
    "dialect, size",
    [
        (Dialects.POSTGRES, 'length("s"."v")'),
        (Dialects.MYSQL, "CHAR_LENGTH(`s`.`v`)"),
        (Dialects.SQL_SERVER, """(LEN("s"."v" + 'x') - 1)"""),
        (Dialects.SNOWFLAKE, 'length("s"."v")'),
        (Dialects.PRESTO, 'length("s"."v")'),
        (Dialects.TRINO, 'length("s"."v")'),
        (Dialects.BIGQUERY, "length(`s`.`v`)"),
    ],
)
def test_other_positions_are_clamped(dialect: Dialects, size: str) -> None:
    sql = _select(dialect, "substring(v, -2, 3)")
    assert sql.startswith("SUBSTRING("), sql
    assert f"CASE WHEN ({size} + -1) < 1 THEN 1" in sql, sql
    assert "CASE WHEN" in _select(dialect, "substring(v, i, 3)")


def _arg(value: int | None, literal: bool) -> str:
    if value is None:
        return "NULL::int"
    return str(value) if literal else f"(select {value}::int)"


def test_the_clamped_window_is_the_reference() -> None:
    """`clamp_exact`: once start >= 1 and count >= 0 every engine agrees, so
    the clamped arguments are run on DuckDB's own SUBSTRING."""
    connection = duckdb.connect()
    values: list[int | None] = [*range(-8, 9), None]
    for s in ("", "abcdé"):
        for start in values:
            for count in values:
                want = None if None in (start, count) else substr(s, start, count)  # type: ignore[arg-type]
                for literal in (True, False):
                    sql = render_window_substring(
                        "SUBSTRING",
                        "length({})",
                        [f"'{s}'", _arg(start, literal), _arg(count, literal)],
                    )
                    (got,) = connection.execute(f"select {sql}").fetchone()  # type: ignore[misc]
                    assert got == want, (s, start, count, sql)
