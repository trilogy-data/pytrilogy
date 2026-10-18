"""Counting a rowset column that passes a key through is counting the key.

`count(key)` counts distinct keys; a rowset finer than the key (or a union
stacking arms that share a value) repeats it, and the count read those
repeats as rows. A column computed from a key is not the key and still
counts rows.

Fixture: four customers; orders repeat customer 1; customers 1 and 3 are in
both union arms.
"""

import pytest

from trilogy import Dialects, Environment

MODEL = """
key cust_id int;
datasource customers (id: cust_id) grain (cust_id)
query '''select 1 id union all select 2 union all select 3 union all select 4''';

key o_id int;
datasource orders (id: o_id, c: cust_id) grain (o_id)
query '''select 1 id, 1 c union all select 2, 1 union all select 3, 3 union all select 4, 4''';

key ws_id int;
property ws_id.ws_cust int;
datasource web_sales (id: ws_id, c: ws_cust) grain (ws_id)
query '''select 1 id, 1 c union all select 2, 2 union all select 3, 3''';

key cs_id int;
property cs_id.cs_cust int;
datasource catalog_sales (id: cs_id, c: cs_cust) grain (cs_id)
query '''select 1 id, 1 c union all select 2, 3''';
"""

UNION = (
    "with other as union((select ws_cust as csk), (select cs_cust as csk)) -> (csk);"
)

CASES = {
    "finer_than_key": ("with other as select cust_id as csk, o_id as oid;", 3),
    "key_alone": ("with other as select cust_id as csk;", 4),
    "computed_from_key": (
        "with other as select cust_id + 0 as csk, o_id as oid;",
        4,
    ),
    "union_arms": (UNION, 3),
}


@pytest.mark.parametrize("case", sorted(CASES))
def test_count_rowset_key(case):
    rowset, expected = CASES[case]
    env = Environment()
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    rows = executor.execute_query(rowset + " select count(other.csk) as n;")
    assert rows.fetchall() == [(expected,)]
