"""A keyless select may compute the aggregate its WHERE gates on with an
explicit `by`: the WHERE reads every row at that grain, the select the rows
the WHERE kept. The two were refused as indistinguishable (the 48 rejected
queries of expressions 650,000-800,000); only the bare spelling, a HAVING
written as a WHERE, still is.

Rows checked by hand against `test_oracle_sweep_laws.ROWS` (six trees, four
in city A; heights 3, NULL, 5, NULL, 7, 2)."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize
from trilogy.core.exceptions import InvalidSyntaxException

CASES = [
    # expressions 652494
    (
        (
            "select avg(coalesce(height, 0)) as v0, count(tree_id) by * as v1,"
            " max(height) by * as v2 where (count(tree_id) by * - sum(height) by"
            " city, species = 1 or max(height) by city, species - sum(height) by *"
            " > 2);"
        ),
        [(5, 1, 5)],
    ),
    (
        "select count(tree_id) by * as n where count(tree_id) by * > 5 and city = 'A';",
        [(4,)],
    ),
    (
        "select count(tree_id) by * as n where count(tree_id) by * > 7 and city = 'A';",
        [],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query, expected", CASES)
def test_where_by_beside_select(query: str, expected: list[tuple], model: str):
    ex = executor(model)
    sql = ex.generate_sql(query)[-1]
    assert normalize(ex.execute_raw_sql(sql).fetchall()) == normalize(expected), sql


def test_bare_where_aggregate_beside_select_is_refused():
    with pytest.raises(InvalidSyntaxException, match="use `having n > ...`"):
        executor("base").generate_sql(
            "select count(tree_id) as n where count(tree_id) > 5 and city = 'A';"
        )
