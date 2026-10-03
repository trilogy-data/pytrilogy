"""Facts that each bind `~cust_id` beside a complete customer table are summed
per customer before they meet: joined row by row first, the intermediate grows
as the product of each customer's fact rows."""

import pytest

from trilogy import Dialects

MODEL = """
key cust_id int;
key order_id int;
property order_id.amount int;
key return_id int;
property return_id.refund int;
key visit_id int;
property visit_id.dur int;

datasource custs (cid: cust_id) grain (cust_id)
query '''select range as cid from range(4)''';
datasource orders (oid: order_id, cid: ~cust_id, amt: amount) grain (order_id)
query '''select range as oid, range % 2 as cid, 1 as amt from range(6)''';
datasource returns (rid: return_id, cid: ~cust_id, r: refund) grain (return_id)
query '''select range as rid, range % 3 as cid, 2 as r from range(6)''';
datasource visits (vid: visit_id, cid: ~cust_id, d: dur) grain (visit_id)
query '''select range as vid, 1 as cid, 3 as d from range(2)''';
"""

TWO = "select cust_id, sum(amount) as amt, sum(refund) as ref order by cust_id asc;"
THREE = (
    "select cust_id, sum(amount) as amt, sum(refund) as ref, sum(dur) as d"
    " order by cust_id asc;"
)


@pytest.mark.parametrize(
    "query, expected",
    [
        (TWO, [(0, 3, 4), (1, 3, 4), (2, None, 4), (3, None, None)]),
        (
            THREE,
            [(0, 3, 4, None), (1, 3, 4, 6), (2, None, 4, None), (3, None, None, None)],
        ),
        (
            "where amt > 0 " + TWO,
            [(0, 3, 4), (1, 3, 4)],
        ),
    ],
)
def test_facts_are_summed_before_they_meet(query: str, expected: list[tuple]):
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(MODEL)
    sql = executor.generate_sql(query)[-1]
    assert "FULL JOIN" not in sql, sql
    assert [tuple(r) for r in executor.execute_text(query)[-1].fetchall()] == expected
