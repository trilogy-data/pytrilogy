"""A rowset used as a `subset join` key set must not fan the anchor's rows.

A `union(...)` stacks its arms with UNION ALL, so a key two arms share
appears twice; a plain rowset read for one column repeats it on its finer
grain. Joined onto the anchor key, either repeat fanned the anchor rows and
`count(key)` counted a customer once per repeat (q35).

Fixture: four customers; customer 1 is in both union arms and has two orders.
"""

import pytest

from trilogy import Dialects, Environment

MODEL = """
key cust_id int;
property cust_id.gender string;
datasource customers (id: cust_id, g: gender) grain (cust_id)
query '''select 1 id, 'F' g union all select 2, 'F' union all select 3, 'M' union all select 4, 'M' ''';

key ws_id int;
property ws_id.ws_cust int;
datasource web_sales (id: ws_id, c: ws_cust) grain (ws_id)
query '''select 1 id, 1 c union all select 2, 2 union all select 3, 3''';

key cs_id int;
property cs_id.cs_cust int;
datasource catalog_sales (id: cs_id, c: cs_cust) grain (cs_id)
query '''select 1 id, 1 c union all select 2, 3''';

key o_id int;
property o_id.o_amt int;
datasource orders (id: o_id, c: cust_id, a: o_amt) grain (o_id)
query '''select 1 id, 1 c, 10 a union all select 2, 1, 5 union all select 3, 3, 7 union all select 4, 4, 1''';
"""

COUNT_BY_GENDER = """
where other.csk is not null
select gender, count(cust_id) as n
subset join other.csk = cust_id
order by gender asc;
"""

CASES = {
    "union_of_foreign_keys": (
        "with other as union((select ws_cust as csk), (select cs_cust as csk)) -> (csk);",
        [("F", 2), ("M", 1)],
    ),
    "union_of_one_key": (
        (
            "with other as union((where o_amt > 5 select cust_id as csk), "
            "(where o_amt < 8 select cust_id as csk)) -> (csk);"
        ),
        [("F", 1), ("M", 2)],
    ),
    "rowset_finer_than_key": (
        "with other as select cust_id as csk, o_id as oid;",
        [("F", 1), ("M", 2)],
    ),
}


@pytest.mark.parametrize("case", sorted(CASES))
def test_subset_join_key_set_counts_distinct_customers(case):
    rowset, expected = CASES[case]
    env = Environment()
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    assert executor.execute_query(rowset + COUNT_BY_GENDER).fetchall() == expected


def test_union_rowset_projection_keeps_stacked_rows():
    env = Environment()
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    rows = executor.execute_query(
        "with other as union((select ws_cust as csk), (select cs_cust as csk)) -> (csk);"
        " select other.csk order by other.csk asc;"
    ).fetchall()
    assert rows == [(1,), (1,), (2,), (3,), (3,)]
