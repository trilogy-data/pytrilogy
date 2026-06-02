"""Self-contained repro of the join-promotion bug blocking TPC-DS q05.

Two measures on two datasources joined by a key. Aggregating them together is a
correct LEFT OUTER JOIN; adding a per-measure FILTER promotes the datasource join
to INNER JOIN, dropping rows present on only one side -> undercount.

Data: 3 sales (orders 1,2,3 = 100/200/300); 1 return (order 1 = 10).
Correct: sales=600, returns=10."""

from trilogy import Dialects, Environment

MODEL = """
key order_id int;
property order_id.sale_amount float?;
property order_id.return_amount float?;
property order_id.sale_flag int?;
property order_id.return_flag int?;

datasource sales_src (
    order_id: order_id,
    sale_amount: sale_amount,
    sale_flag: sale_flag,
)
grain (order_id)
query '''select 1 as order_id, 100.0 as sale_amount, 1 as sale_flag
   union all select 2, 200.0, 1
   union all select 3, 300.0, 1''';

datasource returns_src (
    order_id: ~order_id,
    return_amount: return_amount,
    return_flag: return_flag,
)
grain (order_id)
query '''select 1 as order_id, 10.0 as return_amount, 1 as return_flag''';
"""


def case(label, select, expect):
    env = Environment(working_path=".")
    env.parse(MODEL)
    ex = Dialects.DUCK_DB.default_executor(environment=env)
    sql = ex.generate_sql(select)[-1]
    rows = [tuple(float(x) for x in r) for r in ex.execute_raw_sql(sql).fetchall()]
    join = next((ln.strip() for ln in sql.splitlines() if "JOIN" in ln.upper()), "(no join)")
    flag = "OK " if rows == expect else "BUG"
    print(f"[{flag}] {label}\n      expect {expect}  got {rows}\n      {join}")


case("1. two measures, no filter",
     "select sum(sale_amount) as sales, sum(return_amount) as returns;",
     [(600.0, 10.0)])

case("2. two measures, filter on a direct property of each side",
     "select sum(sale_amount ? sale_flag = 1) as sales, "
     "sum(return_amount ? return_flag = 1) as returns;",
     [(600.0, 10.0)])

case("3. filter on the RETURN side only",
     "select sum(sale_amount) as sales, "
     "sum(return_amount ? return_flag = 1) as returns;",
     [(600.0, 10.0)])
