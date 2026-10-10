"""MySQL has no FULL OUTER JOIN; TPC-DS is the corpus that actually produces them.

Pins the two real outcomes on this corpus: a query whose FULL joins share one
key lowers to a spine, and a query whose FULL joins key off different concepts
raises a typed error instead of emitting SQL MySQL cannot run.
"""

from pathlib import Path

import pytest

from trilogy.core.models.author import Comment
from trilogy.core.models.environment import Environment
from trilogy.core.optimizations.full_join_lowering import UnsupportedFullJoinError
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.dialect.mysql import MySQLDialect
from trilogy.parser import parse_text

WORKING_PATH = Path(__file__).parent


def _statements(name: str):
    path = WORKING_PATH / name
    env = Environment(working_path=WORKING_PATH)
    _, raw = parse_text(path.read_text(encoding="utf-8"), env)
    return env, [s for s in raw if not isinstance(s, Comment)]


def _sql(dialect, env, statements) -> str:
    return dialect.compile_statement(dialect.generate_queries(env, statements)[0])


@pytest.mark.parametrize("name", ["adhoc02.preql", "query97-one.preql"])
def test_query_emits_full_join_on_duckdb(name: str):
    env, statements = _statements(name)
    assert "FULL JOIN" in _sql(DuckDBDialect(), env, statements).upper()


def test_shared_key_full_join_lowers_to_spine():
    env, statements = _statements("adhoc02.preql")

    sql = _sql(MySQLDialect(), env, statements)

    assert "FULL JOIN" not in sql.upper(), sql
    assert "`_spine" in sql, sql


def test_unlowerable_shape_raises_with_remediation():
    # q97's FULL join reads its keys off two relations (the item dimension
    # and the store channel's customer), so no single spine replaces them.
    env, statements = _statements("query97-one.preql")

    with pytest.raises(UnsupportedFullJoinError) as excinfo:
        _sql(MySQLDialect(), env, statements)

    message = str(excinfo.value)
    assert "more than one relation" in message, message
    assert "To resolve:" in message, message
    assert "native FULL JOIN support" in message, message
