"""Renderings behind formal/Trilogy/StringMatch.lean on dialects with no
engine in CI; tests/core/test_string_match_laws.py runs the laws on DuckDB,
SQLite and ClickHouse."""

import pytest

from trilogy import Dialects, Environment
from trilogy.constants import Rendering
from trilogy.core.enums import FunctionType
from trilogy.core.models.core import DataType
from trilogy.engine import escape_literal_colons
from trilogy.parser import parse
from trilogy.render import get_dialect_generator

MODEL = """
key id int;
property id.v string;
property id.p string;
datasource s (id: id, v: ?v, p: ?p) grain (id) address s;
"""


def _where(dialect: Dialects, condition: str) -> str:
    env = Environment()
    _, statements = parse(MODEL + f"select id where {condition};", env)
    renderer = get_dialect_generator(dialect, rendering=Rendering(parameters=False))
    sql = renderer.compile_statements(*renderer.generate_queries(env, statements))
    body = " ".join(sql[-1].split())
    return body[body.upper().index("WHERE ") + 6 :]


LIKE = {
    Dialects.POSTGRES: """"s"."v" like 'a\\%%' ESCAPE '\\'""",
    Dialects.SNOWFLAKE: """"s"."v" like 'a\\\\%%' ESCAPE '\\\\'""",
    Dialects.MYSQL: """`s`.`v` like 'a\\\\%%' ESCAPE '\\\\'""",
    Dialects.PRESTO: """"s"."v" like 'a\\%%' ESCAPE '\\'""",
    Dialects.TRINO: """"s"."v" like 'a\\%%' ESCAPE '\\'""",
    Dialects.SQL_SERVER: """"s"."v" like 'a\\%\\[%' ESCAPE '\\'""",
    Dialects.BIGQUERY: """`s`.`v` like 'a\\\\%%'""",
}


@pytest.mark.parametrize("dialect, expected", list(LIKE.items()))
def test_like_escapes_with_a_backslash(dialect: Dialects, expected: str) -> None:
    pattern = "a\\%[%" if dialect == Dialects.SQL_SERVER else "a\\%%"
    assert _where(dialect, f"v like '{pattern}'") == expected


ILIKE = {
    Dialects.POSTGRES: """"s"."v" ilike 'a%' ESCAPE '\\'""",
    Dialects.SNOWFLAKE: """"s"."v" ilike 'a%' ESCAPE '\\\\'""",
    Dialects.BIGQUERY: """(LOWER(`s`.`v`) LIKE LOWER('a%'))""",
    Dialects.SQL_SERVER: """(LOWER("s"."v") LIKE LOWER('a%') ESCAPE '\\')""",
    Dialects.PRESTO: """(LOWER("s"."v") LIKE LOWER('a%') ESCAPE '\\')""",
    Dialects.TRINO: """(NOT LOWER("s"."v") LIKE LOWER('a%') ESCAPE '\\')""",
    Dialects.MYSQL: """(LOWER(`s`.`v`) LIKE LOWER('a%') ESCAPE '\\\\')""",
}


@pytest.mark.parametrize("dialect, expected", list(ILIKE.items()))
def test_ilike_folds_case_where_the_engine_has_no_ilike(
    dialect: Dialects, expected: str
) -> None:
    op = "not ilike" if dialect == Dialects.TRINO else "ilike"
    assert _where(dialect, f"v {op} 'a%'") == expected


CONTAINS = {
    Dialects.POSTGRES: """(strpos(lower("s"."v"), lower('B')) > 0) = True""",
    Dialects.PRESTO: """(strpos(lower("s"."v"), lower('B')) > 0) = True""",
    Dialects.SNOWFLAKE: """CONTAINS(LOWER("s"."v"), LOWER('B')) = True""",
    Dialects.MYSQL: """(LOCATE(LOWER('B'), LOWER(`s`.`v`)) > 0) = True""",
    Dialects.SQL_SERVER: (
        """(LOWER("s"."v") LIKE '%' + REPLACE(REPLACE(REPLACE(REPLACE("""
        """LOWER('B'), '\\', '\\\\'), '%', '\\%'), '_', '\\_'), '[', '\\[')"""
        """ + '%' ESCAPE '\\') = True"""
    ),
}


@pytest.mark.parametrize("dialect, expected", list(CONTAINS.items()))
def test_contains_is_a_folded_substring(dialect: Dialects, expected: str) -> None:
    assert _where(dialect, "contains(v, 'B')") == expected


def test_snowflake_array_contains_is_membership() -> None:
    render = Dialects.SNOWFLAKE.default_renderer().FUNCTION_MAP[FunctionType.CONTAINS]
    assert render(["xs", "x"], [DataType.ARRAY]) == "ARRAY_CONTAINS(x::variant, xs)"


@pytest.mark.parametrize(
    "dialect, expected",
    [
        (Dialects.POSTGRES, """"s"."v" like "s"."p" ESCAPE '\\'"""),
        (
            Dialects.CLICKHOUSE,
            """`s`.`v` like replaceRegexpAll(`s`.`p`, '\\\\\\\\([^%_\\\\\\\\])', '\\\\1')""",
        ),
    ],
)
def test_a_computed_pattern(dialect: Dialects, expected: str) -> None:
    assert _where(dialect, "v like p") == expected


def test_a_standard_literal_ends_at_its_quote() -> None:
    sql = "select x like 'a%' ESCAPE '\\', :p, 'b:c'"
    assert escape_literal_colons(sql, backslash_escapes=False) == (
        "select x like 'a%' ESCAPE '\\', :p, 'b\\:c'"
    )
    # read with backslash escapes, `'\'` opens a literal that swallows `:p`
    assert ", :p," not in escape_literal_colons(sql)


def test_chdb_parameters_double_their_backslashes() -> None:
    from trilogy.dialect.clickhouse_chdb import _escape_backslashes

    assert _escape_backslashes(["a\\b", ("c\\",), 1]) == ["a\\\\b", ("c\\\\",), 1]


def test_raw_sql_binds_after_a_backslash_literal() -> None:
    executor = Dialects.SQLITE.default_executor(environment=Environment())
    got = executor.execute_raw_sql(
        "select 'a%' like 'a\\%' ESCAPE '\\', :p", {"p": 3}
    ).fetchall()
    assert [tuple(r) for r in got] == [(1, 3)]
