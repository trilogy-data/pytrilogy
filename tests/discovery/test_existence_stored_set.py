"""A membership reads a stored set as a root: `big` is bound as a column
beside a `_raw` no table binds, so it has no derive-from-base plan."""

import pytest

from trilogy import Dialects

MODEL = """
key id int;
property id.x int;
key y int;
key _raw int;
auto big <- _raw * 10;

datasource facts (id, x, y)
grain (id)
query '''select * from (values (1, 10, 10), (2, 20, 30), (3, 30, 50)) as t(id, x, y)''';

datasource bigs (big)
grain (big)
query '''select * from (values (10), (30)) as t(big)''';
"""

QUERIES = {
    "where_in": ("where y in big select id order by id asc;", [(1,), (2,)]),
    "where_not_in": ("where y not in big select id order by id asc;", [(3,)]),
    "filter_nested": ("select sum(x ? y in big) -> kept;", [(30,)]),
    "derived_membership": (
        "auto flag <- y in big; select id, flag order by id asc;",
        [(1, True), (2, True), (3, False)],
    ),
    "filtered_projection": (
        "select id, x ? y in big -> kx order by id asc;",
        [(1, 10), (2, 20), (3, None)],
    ),
}


@pytest.mark.parametrize("query", sorted(QUERIES))
def test_membership_reads_stored_set(query: str):
    text, expected = QUERIES[query]
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MODEL)
    sql = executor.generate_sql(text)[-1]
    assert "_raw" not in sql, sql
    assert [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()] == expected
