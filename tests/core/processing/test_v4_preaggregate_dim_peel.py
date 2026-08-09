import re
from datetime import date

from trilogy import Dialects, Environment

_MODEL = """
key order_id int;
property order_id.order_date date;
property order_id.ship_priority int;

key line_no int;
property <line_no, order_id>.ship_date date;
property <line_no, order_id>.amount int;

datasource orders (
    id: order_id,
    od: order_date,
    sp: ship_priority,
)
grain (order_id)
query '''select 1 id, '1995-03-01'::date od, 0 sp''';

datasource lines (
    line: line_no,
    oid: order_id,
    sd: ship_date,
    amt: amount,
)
grain (line_no, order_id)
query '''
select 1 line, 1 oid, '1995-03-20'::date sd, 10 amt union all
select 2 line, 1 oid, '1995-03-21'::date sd, 20 amt
''';
"""

_QUERY = """
where order_date < '1995-03-15'::date and ship_date > '1995-03-15'::date
select order_id, sum(amount) -> total, order_date, ship_priority;
"""


def test_preaggregate_dimension_peel_duplicates_filter_on_both_scans():
    env = Environment()
    env, _ = env.parse(_MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = executor.generate_sql(_QUERY)[-1]
    rows = executor.execute_text(_QUERY)[-1].fetchall()

    assert [tuple(row) for row in rows] == [(1, 30, date(1995, 3, 1), 0)]
    assert len(re.findall(r"\bWITH\b|,\s*\w+\s+as\s*\(", sql, re.IGNORECASE)) == 2
    # once per scan, and no redundant third copy on the join that reunites them
    assert sql.count("date '1995-03-15'") == 2
    assert "GROUP BY\n    1,\n    2,\n    3" not in sql
