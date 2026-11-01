"""A HAVING naming a SELECT alias (formal/Trilogy/Rewrite.lean, `alias_shadowed`).

A FROM column of the alias's name binds before the alias, so `having c > 3`
beside a table column `c` filtered on that column and returned no rows. The
alias is kept only where every column in scope is a CTE output, so over a
table the aggregate is restated."""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.core.query_processor import process_query
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.dialect.mysql import MySQLDialect

MODEL = """
key id int;
property id.g int;
property id.x int;
datasource t (id: id, c: g, x: x) grain (id) address t;
"""

TABLE = "CREATE TABLE t AS SELECT * FROM (VALUES (1, 1, 5), (2, 1, 7), (3, 2, 1)) v(id, c, x);"


@pytest.mark.parametrize("alias", ["c", "s"])
def test_having_reads_the_aggregate(alias: str) -> None:
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_raw_sql(TABLE)
    executor.parse_text(MODEL)
    rows = executor.execute_text(f"select g, sum(x) -> {alias} having {alias} > 3;")
    assert rows[-1].fetchall() == [(1, 12)]


def _sql(dialect: BaseDialect, text: str) -> str:
    env = Environment()
    env.parse(MODEL)
    _, statements = env.parse(text)
    return dialect.compile_statement(process_query(env, statements[-1]))


@pytest.mark.parametrize("dialect", [DuckDBDialect, MySQLDialect])
def test_having_over_a_table_inlines_the_aggregate(
    dialect: type[BaseDialect],
) -> None:
    sql = _sql(dialect(), "select g, sum(x) -> c having c > 3;")
    assert "HAVING\n    sum(" in sql, sql
