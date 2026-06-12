"""A multi-key query-scoped INNER join must be commutative in operand order.

Regression for the bug where `inner join A.k = B.k` and `inner join B.k = A.k`
diverged when (1) there were two scoped-join keys and (2) one key was a
*transitive* attribute reached through a dimension (here `week_seq`, a property
of each fact's own date dim). One operand order dropped the dimension datasource
that bridged the second fact to `week_seq` — `resolve_subgraphs` treated it as a
concept-subset of the other fact's date dim — silently severing the week join
and fanning the result out.

The fan-out only shows when a fact has rows in a key bucket where the other fact
does NOT: item 1 is sold in week 200 but has inventory only in week 100, so it
must appear for week 100 only.
"""

from pathlib import Path

import pytest

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

DATE = """
key date_id int;
property date_id.week_seq int;
property date_id.year int;
datasource dates (did: date_id, wk: week_seq, yr: year)
grain (date_id) address dates_tbl;
"""
ITEM = """
key item_id int;
property item_id.desc string;
datasource items (iid: item_id, d: desc) grain (item_id) address items_tbl;
"""
WAREHOUSE = """
key wh_id int;
property wh_id.name string;
datasource warehouses (wid: wh_id, nm: name) grain (wh_id) address wh_tbl;
"""
SALES = """
import item as item;
import date as sold_date;
key order_id int;
property order_id.qty int;
datasource sales (oid: order_id, iid: item.item_id, did: sold_date.date_id, q: qty)
grain (order_id) address sales_tbl;
"""
INVENTORY = """
import item as item;
import date as inv_date;
import warehouse as warehouse;
property <item.item_id, inv_date.date_id, warehouse.wh_id>.qoh int;
datasource inventory (iid: item.item_id, did: inv_date.date_id, wid: warehouse.wh_id, q: qoh)
grain (item.item_id, inv_date.date_id, warehouse.wh_id) address inv_tbl;
"""

QUERY = (
    "import sales as cs;\n"
    "import inventory as inv;\n"
    "where cs.sold_date.year = 1999 and inv.qoh < cs.qty\n"
    "select cs.item.desc as item_desc, inv.warehouse.name as wh, "
    "cs.sold_date.week_seq as wk, count(cs.order_id) as t\n"
    "inner join cs.item.item_id = inv.item.item_id\n"
    "inner join cs.sold_date.week_seq = inv.inv_date.week_seq\n"
    "order by 1,2,3;"
)


def _flip(text: str) -> str:
    return text.replace(
        "cs.item.item_id = inv.item.item_id", "inv.item.item_id = cs.item.item_id"
    ).replace(
        "cs.sold_date.week_seq = inv.inv_date.week_seq",
        "inv.inv_date.week_seq = cs.sold_date.week_seq",
    )


@pytest.fixture
def models(tmp_path: Path) -> Path:
    (tmp_path / "date.preql").write_text(DATE)
    (tmp_path / "item.preql").write_text(ITEM)
    (tmp_path / "warehouse.preql").write_text(WAREHOUSE)
    (tmp_path / "sales.preql").write_text(SALES)
    (tmp_path / "inventory.preql").write_text(INVENTORY)
    return tmp_path


@pytest.fixture
def engine(models: Path) -> Executor:
    env = Environment(working_path=models)
    eng = Dialects.DUCK_DB.default_executor(environment=env)
    eng.execute_raw_sql("create table dates_tbl(did int, wk int, yr int)")
    eng.execute_raw_sql(
        "insert into dates_tbl values (10,100,1999),(11,100,1999),(20,200,1999),(21,200,1999)"
    )
    eng.execute_raw_sql("create table items_tbl(iid int, d varchar)")
    eng.execute_raw_sql("insert into items_tbl values (1,'X'),(2,'Y')")
    eng.execute_raw_sql("create table wh_tbl(wid int, nm varchar)")
    eng.execute_raw_sql("insert into wh_tbl values (1,'W1')")
    eng.execute_raw_sql("create table sales_tbl(oid int, iid int, did int, q int)")
    eng.execute_raw_sql("insert into sales_tbl values (1,1,10,5),(2,1,20,7),(3,2,10,9)")
    eng.execute_raw_sql("create table inv_tbl(iid int, did int, wid int, q int)")
    # item 1 has inventory ONLY in week 100 (days 10,11); none in week 200.
    eng.execute_raw_sql("insert into inv_tbl values (1,10,1,3),(1,11,1,4),(2,10,1,2)")
    return eng


def _run(engine: Executor, models: Path, text: str) -> list[tuple]:
    engine.environment = Environment(working_path=models)
    return [tuple(r) for r in engine.execute_text(text)[-1].fetchall()]


# item 1 sold in weeks 100 and 200 but stocked only in week 100 -> week 100 only;
# item 2 sold and stocked in week 100. Week-200 sales of item 1 must NOT appear.
EXPECTED = [("X", "W1", 100, 1), ("Y", "W1", 100, 1)]


def test_scoped_join_operand_order_commutative(engine: Executor, models: Path) -> None:
    forward = _run(engine, models, QUERY)
    flipped = _run(engine, models, _flip(QUERY))
    assert forward == EXPECTED
    assert flipped == EXPECTED


# An INNER global `merge` (no `~`) of a transitive attribute (week_seq) across two
# dims must collapse at build time exactly like the scoped INNER join, otherwise
# inventory joins on item alone and fans out. (`~` merges are LEFT enrichment and
# stay on the pseudonym path — not exercised here.)
MERGE_QUERY = (
    "import sales as cs;\n"
    "import inventory as inv;\n"
    "merge cs.item.item_id into inv.item.item_id;\n"
    "merge cs.sold_date.week_seq into inv.inv_date.week_seq;\n"
    "where cs.sold_date.year = 1999 and inv.qoh < cs.qty\n"
    "select cs.item.desc as item_desc, inv.warehouse.name as wh, "
    "inv.inv_date.week_seq as wk, count(cs.order_id) as t\n"
    "order by 1,2,3;"
)


def test_global_inner_merge_bridge_matches_scoped_join(
    engine: Executor, models: Path
) -> None:
    rows = _run(engine, models, MERGE_QUERY)
    assert rows == EXPECTED
