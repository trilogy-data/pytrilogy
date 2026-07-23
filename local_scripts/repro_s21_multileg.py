import sys
from pathlib import Path

from trilogy import Dialects, Environment, Executor
from trilogy.constants import CONFIG

sys.path.insert(0, "tests/join_matrix")
from test_multileg_scoped_join_null_identity import (  # noqa: E402
    CATALOG_SALES,
    CUSTOMER,
    HAVING,
    HEADER,
    ITEM,
    JOINS,
    MEASURES,
    STORE_RETURNS,
    STORE_SALES,
)

CONFIG.use_v4_discovery = sys.argv[1] == "v4"
if "noopt" in sys.argv:
    for f in CONFIG.optimizations.__dataclass_fields__:
        if isinstance(getattr(CONFIG.optimizations, f), bool):
            setattr(CONFIG.optimizations, f, False)

QUERIES = {
    "a": "select\n  ss.item.item_sk,\n  ss.store,\n" + MEASURES + JOINS + HAVING + ";",
    "b": "where ss.customer.customer_sk is not null\n"
    "select\n  ss.item.item_sk,\n  ss.store,\n" + MEASURES + JOINS + HAVING + ";",
    "c": "select\n  ss.item.item_sk,\n  ss.store,\n  ss.customer.customer_sk,\n"
    + MEASURES
    + JOINS
    + HAVING
    + ";",
    # no having
    "d": "select\n  ss.item.item_sk,\n  ss.store,\n" + MEASURES + JOINS + ";",
    # only ss + sr leg
    "e": "select\n  ss.item.item_sk,\n  ss.store,\n"
    "  sum(ss.profit) as ss_profit,\n  sum(sr.loss) as sr_loss,\n"
    "union join ss.ticket = sr.ticket and ss.item.item_sk = sr.item.item_sk"
    " and ss.customer.customer_sk = sr.customer.customer_sk\n;",
    # only ss + cs leg
    "f": "select\n  ss.item.item_sk,\n  ss.store,\n"
    "  sum(ss.profit) as ss_profit,\n  sum(cs.cprofit) as cs_profit,\n"
    "union join ss.customer.customer_sk = cs.customer.customer_sk"
    " and ss.item.item_sk = cs.item.item_sk\n;",
    # both legs, no having
    "g": "select\n  ss.item.item_sk,\n  ss.store,\n"
    "  sum(ss.profit) as ss_profit,\n" + JOINS + ";",
    # sr leg only, ss measure only
    "h": "select\n  ss.item.item_sk,\n  ss.store,\n"
    "  sum(ss.profit) as ss_profit,\n"
    "union join ss.ticket = sr.ticket and ss.item.item_sk = sr.item.item_sk"
    " and ss.customer.customer_sk = sr.customer.customer_sk\n;",
    # cs leg only, ss measure only
    "i": "select\n  ss.item.item_sk,\n  ss.store,\n"
    "  sum(ss.profit) as ss_profit,\n"
    "union join ss.customer.customer_sk = cs.customer.customer_sk"
    " and ss.item.item_sk = cs.item.item_sk\n;",
    # sr leg on ticket only
    "j": "select\n  ss.item.item_sk,\n  ss.store,\n"
    "  sum(ss.profit) as ss_profit,\n"
    "union join ss.ticket = sr.ticket\n;",
    # no joins at all
    "k": "select\n  ss.item.item_sk,\n  ss.store,\n  sum(ss.profit) as ss_profit;",
}

key = sys.argv[2] if len(sys.argv) > 2 else "a"
tmp = Path("local_scripts/_s21_model")
tmp.mkdir(exist_ok=True)
(tmp / "item.preql").write_text(ITEM)
(tmp / "customer.preql").write_text(CUSTOMER)
(tmp / "store_sales.preql").write_text(STORE_SALES)
(tmp / "store_returns.preql").write_text(STORE_RETURNS)
(tmp / "catalog_sales.preql").write_text(CATALOG_SALES)

engine: Executor = Dialects.DUCK_DB.default_executor(
    environment=Environment(working_path=tmp)
)
statements = engine.parse_text(HEADER + QUERIES[key])
sql = engine.generate_sql(statements[-1])[-1]
print(sql)
print("---- rows ----")
for r in engine.execute_raw_sql(sql).fetchall():
    print(tuple(r))
