"""Every emitted CTE is read by something. A raw base datasource renders
inline in FROM, so the sub-CTE minted for it beside an existence feeder is
named by nothing; a query-backed table is not inlineable, which is what left
it in the SQL as a dead `WITH` member."""

import re

from trilogy import Dialects
from trilogy.core.models.environment import Environment

_MODEL = """
key oid int;
property oid.cat string;
property oid.amt float;
datasource orders (o: oid, c: cat, a: amt) grain (oid)
query '''select 1 o, 'a' c, 10.0 a union all select 2 o, 'a' c, 30.0 a
    union all select 3 o, 'b' c, 5.0 a''';

with rs as where amt > 6 select oid as k, cat;
"""


def _unreferenced_ctes(sql: str) -> list[str]:
    names = re.findall(r"^(\w+) as \(\s*$", sql, re.MULTILINE)
    return [name for name in names if len(re.findall(rf"\b{name}\b", sql)) < 2]


def test_existence_only_filter_over_query_backed_table_emits_no_dead_cte():
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    query = _MODEL + "select oid where oid in rs.k;"
    sql = executor.generate_sql(query)[-1]
    assert _unreferenced_ctes(sql) == [], sql
    rows = [tuple(r) for r in executor.execute_text(query)[-1].fetchall()]
    assert sorted(rows) == [(1,), (2,)]
