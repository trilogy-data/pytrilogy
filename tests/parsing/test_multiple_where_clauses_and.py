"""Two `where` clauses in one select (a common agent shape: pre-`select` row
filters plus a post-join `where`) are AND-combined into a single clause rather
than raising. Post-aggregation filtering is `having`, so a second `where` can
only mean "more input-row filters" — ANDing them is the unambiguous intent and
avoids a wasted agent retry."""

from __future__ import annotations

import pytest

from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

_MODEL = """
key id int;
property id.a int;
property id.b int;
datasource t (id: id, a: a, b: b)
grain (id)
query '''select 1 as id, 1 as a, 2 as b
         union all select 2, 1, 9
         union all select 3, 5, 2''';
"""


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_two_where_clauses_parse(backend):
    backend("import t as t;\nwhere t.a = 1\nselect t.a, t.b\nwhere t.b = 2\nlimit 5;")


def test_two_where_clauses_and_combine_executes():
    from trilogy import Dialects
    from trilogy.core.models.environment import Environment

    engine = Dialects.DUCK_DB.default_executor(environment=Environment())
    engine.execute_text(_MODEL)
    text = "where a = 1\nselect id\nwhere b = 2;"
    sql = engine.generate_sql(text)[-1]
    # both predicates survive (AND-combined, not the last one winning)
    assert "= 1" in sql and "= 2" in sql, sql
    # only id 1 satisfies a=1 AND b=2
    rows = engine.execute_text(text)[-1].fetchall()
    assert [tuple(r) for r in rows] == [(1,)], rows
