"""Isolate the dropped `~?` guest under a group by a dimension property:
vary the property's nullability and whether the `~?` key is in the fact grain."""

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

sys.path.insert(0, str(REPO))
from trilogy import Dialects, Environment

MODEL = """
key item_sk int;
property item_sk.item_desc string DESCNULL;

datasource items (i_sk: item_sk, i_desc: item_desc)
grain (item_sk)
query '''select 10 as i_sk, 'alpha' as i_desc union all select 20, 'beta' union all select 40, 'gamma' ''';

key order_number int;
property order_number.quantity int;

datasource sales (o_num: order_number, i_sk: ~?item_sk, qty: quantity)
grain (GRAIN)
query '''select 1 as o_num, 10 as i_sk, 5 as qty
union all select 2, 20, 15
union all select 5, null, 7''';
"""

QUERIES = [
    "select item_desc, count(order_number) as total order by item_desc asc nulls last;",
    "select item_desc as d, count(order_number) as total order by d asc nulls last;",
    "select item_desc, sum(quantity) as total order by item_desc asc nulls last;",
    "select item_desc, order_number order by order_number asc nulls last;",
    "select item_sk, count(order_number) as total order by item_sk asc nulls last;",
]

for descnull in ["?", ""]:
    for grain in ["order_number, item_sk", "order_number"]:
        print(f"\n##### desc string{descnull}, grain ({grain})")
        env = Environment()
        env.parse(MODEL.replace("DESCNULL", descnull).replace("GRAIN", grain))
        ex = Dialects.DUCK_DB.default_executor(environment=env)
        for q in QUERIES:
            try:
                print(ex.execute_query(q).fetchall(), "<=", q)
            except Exception as e:
                print("ERROR", type(e).__name__, str(e)[:160], "<=", q)
