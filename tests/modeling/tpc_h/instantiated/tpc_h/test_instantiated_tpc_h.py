import re
from pathlib import Path

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

working_path = Path(__file__).parent


def test_adhoc07():
    env = Environment(working_path=working_path)
    with open(working_path / "adhoc07.preql") as f:
        text = f.read()

    engine: Executor = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])

    results = engine.generate_sql(text)[-1]
    assert re.search(
        r'INNER JOIN "(\w+)" on "order_orders"\."o_custkey" = "\1"\."order_customer_id"',
        results,
    ), results


ADHOC07_ORACLE = """select o_orderkey, o_custkey,
sum(l_extendedprice * (1 - l_discount)) from lineitem
join orders on l_orderkey = o_orderkey
join customer on o_custkey = c_custkey
join nation on c_nationkey = n_nationkey
where upper(n_name) = 'INDONESIA' group by 1, 2"""


def test_adhoc07_rows_are_the_nations_orders():
    """A customer with no order pads `order.id` NULL in every plan side; that
    NULL names no order, so it never pairs the order lookup's padded rows
    (each customer with no order, of any nation) to the summed ones."""
    env = Environment(working_path=working_path)
    engine = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
    engine.execute_raw_sql("INSTALL tpch; LOAD tpch; CALL dbgen(sf=0.01)")
    text = (working_path / "adhoc07.preql").read_text().replace("limit 20", "")
    rows = engine.execute_raw_sql(engine.generate_sql(text)[-1]).fetchall()
    expected = engine.execute_raw_sql(ADHOC07_ORACLE).fetchall()
    assert sorted(rows) == sorted(expected)
