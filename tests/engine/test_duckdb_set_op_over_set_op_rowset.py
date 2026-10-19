"""A set-op TVF whose arm reads another set-op rowset.

Every set op hydrated its arms under the same `__tvf_arm_{i}` alias scope, so
the outer op's arm columns rebound the inner rowset's arm columns to
themselves and the build recursed ("query could not be planned").
"""

import pytest

from trilogy import Dialects, Environment

MODEL = """
key a_id int;
property a_id.a_cust int;
datasource fa (id: a_id, c: a_cust) grain (a_id)
query '''select 1 id, 1 c union all select 2, 2 union all select 3, 3''';

key b_id int;
property b_id.b_cust int;
datasource fb (id: b_id, c: b_cust) grain (b_id)
query '''select 1 id, 1 c union all select 2, 3 union all select 3, 4''';
"""

INTERSECT = (
    "with sc as intersect((select a_cust as csk), (select b_cust as csk)) -> (csk);"
)
UNION = "with sc as union((select a_cust as csk), (select b_cust as csk)) -> (csk);"

CASES = {
    "union_over_intersect": (
        INTERSECT
        + "with u as union((select sc.csk as csk), (select a_cust as csk)) -> (csk);"
        + "select count(u.csk) as n;",
        [(3,)],
    ),
    "except_over_intersect": (
        INTERSECT
        + "with u as except((select a_cust as csk), (select sc.csk as csk)) -> (csk);"
        + "select u.csk order by u.csk asc;",
        [(2,)],
    ),
    "intersect_of_intersect": (
        INTERSECT
        + "with u as intersect((select sc.csk as csk), (select sc.csk as csk)) -> (csk);"
        + "select u.csk order by u.csk asc;",
        [(1,), (3,)],
    ),
    "union_over_union": (
        UNION
        + "with u as union((select sc.csk as csk), (select a_cust as csk)) -> (csk);"
        + "select count(u.csk) as n;",
        [(4,)],
    ),
}


@pytest.mark.parametrize("case", sorted(CASES))
def test_set_op_arm_reads_set_op_rowset(case):
    query, expected = CASES[case]
    env = Environment()
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    assert executor.execute_query(query).fetchall() == expected
