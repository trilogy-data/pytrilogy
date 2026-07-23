"""Repro: union join on a window expression key (v3 vs v4)."""

import sys
from pathlib import Path

from trilogy import Dialects
from trilogy.constants import CONFIG
from trilogy.core.models.environment import Environment

ORDERS = """key oid int;
property oid.amt float;
property oid.ckey int;
datasource orders (oid: oid, amt: amt, ck: ckey)
grain (oid) address orders_tbl;
"""

CUSTOMERS = """key cid int;
property cid.region string;
property cid.rnk int;
datasource customers (cid: cid, reg: region, rk: rnk)
grain (cid) address customers_tbl;
"""

QUERY = """import orders as orders;
import customers as customers;
WHERE customers.region is not null
SELECT customers.region, orders.amt
union join rank orders.oid order by orders.amt desc = customers.rnk ORDER BY orders.amt desc;
"""


def run(use_v4: bool, root: Path) -> None:
    CONFIG.use_v4_discovery = use_v4
    label = "v4" if use_v4 else "v3"
    eng = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=root))
    eng.execute_raw_sql("create table orders_tbl (oid int, amt double, ck int)")
    eng.execute_raw_sql(
        "insert into orders_tbl values (1,10.0,100),(2,20.0,200),(3,30.0,100)"
    )
    eng.execute_raw_sql("create table customers_tbl (cid int, reg varchar, rk int)")
    eng.execute_raw_sql(
        "insert into customers_tbl values (100,'east',1),(200,'west',2)"
    )
    try:
        stmts = eng.parse_text(QUERY)
        sql = eng.generator.compile_statement(stmts[-1])
        print(f"=== {label} SQL ===")
        print(sql)
        rows = [tuple(r) for r in eng.execute_text(QUERY)[-1].fetchall()]
        print(f"=== {label} rows: {rows}")
    except Exception as e:
        print(f"=== {label} FAILED: {type(e).__name__}: {e}")


if __name__ == "__main__":
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
    root = root.resolve()
    (root / "orders.preql").write_text(ORDERS)
    (root / "customers.preql").write_text(CUSTOMERS)
    which = sys.argv[2] if len(sys.argv) > 2 else "both"
    if which in ("v3", "both"):
        run(False, root)
    if which in ("v4", "both"):
        run(True, root)
