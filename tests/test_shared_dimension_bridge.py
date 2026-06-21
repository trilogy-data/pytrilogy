"""Regression: a dimension datasource that uniquely bridges a fact to a shared
attribute must not be pruned as a concept-subset of another datasource.

PURE setup — no `merge`, no scoped `join`. Two facts reach ONE shared `week_seq`
concept through two DIFFERENT date dimensions (a diamond):

         week_seq
        /        \\
   sold_dates   inv_dates
       |            |
     sales      inventory      (sales and inventory also share item_id)

Correctly relating the facts requires joining on item_id AND week_seq. The
subset-pruning in `resolve_subgraphs` compared only *requested* concepts, so
`inv_dates` (which surfaces only `week_seq`) looked like a subset of `sold_dates`
(which also surfaces `year`/`sold_date_id`) and was dropped — even though only
`inv_dates` bridges `inventory` to `week_seq`. Dropping it joined the facts on
item alone and fanned the result out.

The fan-out shows because item 1 is sold in week 200 but stocked only in week
100, so it must appear for week 100 only.
"""

from pathlib import Path

import pytest

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

MODEL = """
key week_seq int;

key sold_date_id int;
property sold_date_id.year int;
datasource sold_dates (sdid: sold_date_id, wsk: week_seq, yr: year)
grain (sold_date_id) address sold_dates_tbl;

key inv_date_id int;
datasource inv_dates (idid: inv_date_id, wsk: week_seq)
grain (inv_date_id) address inv_dates_tbl;

key item_id int;
property item_id.desc string;
datasource items (iid: item_id, d: desc) grain (item_id) address items_tbl;

key wh_id int;
property wh_id.name string;
datasource warehouses (wid: wh_id, nm: name) grain (wh_id) address wh_tbl;

key order_id int;
property order_id.qty int;
property order_id.sold_date_id int;
datasource sales (oid: order_id, iid: item_id, sdid: sold_date_id, q: qty)
grain (order_id) address sales_tbl;

property <item_id, inv_date_id, wh_id>.qoh int;
datasource inventory (iid: item_id, idid: inv_date_id, wid: wh_id, q: qoh)
grain (item_id, inv_date_id, wh_id) address inv_tbl;
"""

QUERY = (
    "where sold_date_id.year = 1999 and qoh < qty\n"
    "select item_id.desc as item_desc, wh_id.name as wh, week_seq as wk, "
    "count(order_id) as t\n"
    "order by 1,2,3;"
)


@pytest.fixture
def engine(tmp_path: Path) -> Executor:
    (tmp_path / "model.preql").write_text(MODEL)
    env = Environment(working_path=tmp_path)
    eng = Dialects.DUCK_DB.default_executor(environment=env)
    eng.execute_raw_sql("create table sold_dates_tbl(sdid int, wsk int, yr int)")
    eng.execute_raw_sql(
        "insert into sold_dates_tbl values (10,100,1999),(11,100,1999),(20,200,1999),(21,200,1999)"
    )
    eng.execute_raw_sql("create table inv_dates_tbl(idid int, wsk int)")
    eng.execute_raw_sql("insert into inv_dates_tbl values (30,100),(31,100),(40,200)")
    eng.execute_raw_sql("create table items_tbl(iid int, d varchar)")
    eng.execute_raw_sql("insert into items_tbl values (1,'X'),(2,'Y')")
    eng.execute_raw_sql("create table wh_tbl(wid int, nm varchar)")
    eng.execute_raw_sql("insert into wh_tbl values (1,'W1')")
    eng.execute_raw_sql("create table sales_tbl(oid int, iid int, sdid int, q int)")
    # item 1 sold in week 100 (sdid 10) and week 200 (sdid 20); item 2 in week 100
    eng.execute_raw_sql("insert into sales_tbl values (1,1,10,5),(2,1,20,7),(3,2,10,9)")
    eng.execute_raw_sql("create table inv_tbl(iid int, idid int, wid int, q int)")
    # item 1 stocked ONLY in week 100 (idid 30,31); item 2 week 100 (idid 30)
    eng.execute_raw_sql("insert into inv_tbl values (1,30,1,3),(1,31,1,4),(2,30,1,2)")
    return eng


# item 1 sold in week 200 but never stocked that week -> week 100 only.
EXPECTED = [("X", "W1", 100, 1), ("Y", "W1", 100, 1)]


def test_shared_attribute_bridge_not_pruned(engine: Executor) -> None:
    rows = [tuple(r) for r in engine.execute_text(MODEL + "\n" + QUERY)[-1].fetchall()]
    assert rows == EXPECTED
