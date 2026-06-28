"""Regression for TPC-DS q37.

A WHERE membership (`inv.item.id in cs.item.id`) compares one import's shared
dimension key against another import's copy of the *same* physical table. Two
optimizations run in sequence:

1. ``InlineDatasource`` folds the existence-set scan (`cs.item.id` over the
   ``items`` table) into its consumer as an inlined datasource.
2. ``PredicatePushdown`` then pushes the membership into the inventory base CTE
   that materializes ``inv.item.id``.

The promotion of the existence source onto the base CTE only carried regular
dependency CTEs, not the already-inlined datasource, so the pushed subselect
rendered against a phantom CTE (`... in (select cs_item_items."cs_item_id" from
cs_item_items ...)`) that was never emitted -> DuckDB CatalogException. The fix
propagates the inlined datasource alongside the source-map entry so the pushed
subselect renders `from items as cs_item_items`.
"""

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

ITEMS = """
key id int;
property id.price float;
property id.mfid int;
datasource items (id: id, p: price, m: mfid) grain (id) address items;
"""
INVENTORY = """
key inv_row int;
property inv_row.qoh int;
datasource inventory (r: inv_row, q: qoh, it: item.id) grain (inv_row) address inventory;
"""
CATALOG = """
key cs_row int;
datasource catalog (r: cs_row, it: item.id) grain (cs_row) address catalog;
"""

QUERY = """where inv.item.price between 60 and 100
  and inv.item.mfid in (100)
  and inv.qoh between 100 and 500
  and inv.item.id in cs.item.id
select inv.item.id, inv.item.price order by inv.item.id;"""


def _executor() -> Executor:
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.execute_raw_sql(
        "create table items as select * from "
        "(values (1,50.0,100),(2,80.0,100),(3,90.0,200)) t(id,p,m)"
    )
    executor.execute_raw_sql(
        "create table inventory as select * from "
        "(values (1,150,1),(2,300,2),(3,600,3)) t(r,q,it)"
    )
    executor.execute_raw_sql(
        "create table catalog as select * from (values (10,1),(11,2)) t(r,it)"
    )
    item_env, _ = Environment().parse(ITEMS)
    inv_env = Environment()
    inv_env.add_import("item", item_env)
    inv_env.parse(INVENTORY)
    cs_env = Environment()
    cs_env.add_import("item", item_env)
    cs_env.parse(CATALOG)
    executor.environment.add_import("inv", inv_env)
    executor.environment.add_import("cs", cs_env)
    return executor


def test_cross_import_membership_inlined_existence_renders_and_executes():
    executor = _executor()
    sql = executor.generate_sql(QUERY)[-1]
    # the pushed-down existence subselect must read from the real table, not a
    # phantom CTE named after the inlined datasource's source key
    assert "from cs_item_items" not in sql
    assert "from items as cs_item_items" in sql
    rows = [
        (int(r[0]), float(r[1])) for r in executor.execute_text(QUERY)[0].fetchall()
    ]
    # item 1 fails the price filter (50<60); item 3 fails qoh (600) and mfid;
    # only item 2 survives every filter and is present in catalog
    assert rows == [(2, 80.0)]
