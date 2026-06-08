"""Parity: a query-scoped `join` must produce outcomes identical to the
equivalent global `merge`. The join clause is handled at build time (more
performant) but is intended to be semantically equivalent to merge, which goes
through the established author-level pseudonym path. Each case below pairs a
`join` form with the equivalent `merge` form and asserts identical results.
"""

from pathlib import Path

import pytest

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

ITEM = """
key item_id int;
property item_id.brand int;
property item_id.category string;
datasource items (iid: item_id, br: brand, cat: category)
grain (item_id) address items_tbl;
"""

CUSTOMER = """
key customer_id int;
property customer_id.name string;
datasource customers (cid: customer_id, nm: name)
grain (customer_id) address customers_tbl;
"""

SALES = """
import item as item;
import customer as customer;
key sale_id int;
property sale_id.year int;
property sale_id.qty int;
datasource sales (sid: sale_id, iid: item.item_id, cid: customer.customer_id, yr: year, q: qty)
grain (sale_id) address sales_tbl;
"""


@pytest.fixture
def models(tmp_path: Path) -> Path:
    (tmp_path / "item.preql").write_text(ITEM)
    (tmp_path / "customer.preql").write_text(CUSTOMER)
    (tmp_path / "sales.preql").write_text(SALES)
    return tmp_path


@pytest.fixture
def engine(models: Path) -> Executor:
    env = Environment(working_path=models)
    eng = Dialects.DUCK_DB.default_executor(environment=env)
    eng.execute_raw_sql("create table items_tbl (iid int, br int, cat varchar)")
    eng.execute_raw_sql("insert into items_tbl values (1,10,'A'),(2,20,'A'),(3,30,'B')")
    eng.execute_raw_sql("create table customers_tbl (cid int, nm varchar)")
    eng.execute_raw_sql("insert into customers_tbl values (100,'alice'),(200,'bob')")
    eng.execute_raw_sql(
        "create table sales_tbl (sid int, iid int, cid int, yr int, q int)"
    )
    eng.execute_raw_sql(
        "insert into sales_tbl values "
        "(1,1,100,2001,5),(2,1,100,2002,7),(3,2,200,2001,3),"
        "(4,2,200,2002,9),(5,3,100,2002,4),(6,1,200,2001,2),(7,2,100,2002,6)"
    )
    return eng


def _run(engine: Executor, models: Path, text: str):
    # fresh env per query so a global `merge` in one case doesn't leak
    engine.environment = Environment(working_path=models)
    return [tuple(r) for r in engine.execute_text(text)[-1].fetchall()]


# Each case: (join_form, merge_form, expected_rows). join and merge must each
# equal expected (so "both wrong" can't masquerade as parity).
PARITY_CASES: dict[str, tuple[str, str, list[tuple]]] = {
    # year-over-year on a shared base dim, two INDEPENDENT rowsets
    "yoy_independent_rowsets": (
        """
import sales as sales;
rowset c <- where sales.year = 2002 select sales.item.brand as brand, sum(sales.qty) as c_qty;
rowset p <- where sales.year = 2001 select sales.item.brand as brand, sum(sales.qty) as p_qty;
inner join c.brand = p.brand
select c.brand, c.c_qty, p.p_qty order by c.brand asc;
""",
        """
import sales as sales;
rowset c <- where sales.year = 2002 select sales.item.brand as brand, sum(sales.qty) as c_qty;
rowset p <- where sales.year = 2001 select sales.item.brand as brand, sum(sales.qty) as p_qty;
merge c.brand into ~p.brand;
select c.brand, c.c_qty, p.p_qty order by c.brand asc;
""",
        [(10, 7, 7), (20, 15, 3)],
    ),
    # rowset FK joined to a freshly imported dimension, selecting dim attrs
    "rowset_fk_to_dim": (
        """
import sales as sales;
import customer as customer;
rowset bought <- where sales.year = 2002 select sales.customer.customer_id, sum(sales.qty) as qty;
inner join bought.sales.customer.customer_id = customer.customer_id
select customer.name, bought.qty order by customer.name asc;
""",
        """
import sales as sales;
import customer as customer;
rowset bought <- where sales.year = 2002 select sales.customer.customer_id, sum(sales.qty) as qty;
merge bought.sales.customer.customer_id into ~customer.customer_id;
select customer.name, bought.qty order by customer.name asc;
""",
        [("alice", 17), ("bob", 9)],
    ),
    # year-over-year where both rowsets derive from one shared `deduped` parent.
    # Deeper rowset-derived-twice limitation that affects BOTH join and merge
    # (the merge reference also emits FULL JOIN 1=1); xfail until the shared
    # rowset source is split into two branches. Same expected as the
    # independent-rowset case.
    "yoy_shared_parent": (
        """
import sales as sales;
rowset deduped <- where sales.year in (2001, 2002) select sales.item.brand, sales.year, sum(sales.qty) as qty;
rowset c <- where deduped.sales.year = 2002 select deduped.sales.item.brand as brand, sum(deduped.qty) as c_qty;
rowset p <- where deduped.sales.year = 2001 select deduped.sales.item.brand as brand, sum(deduped.qty) as p_qty;
inner join c.brand = p.brand
select c.brand, c.c_qty, p.p_qty order by c.brand asc;
""",
        """
import sales as sales;
rowset deduped <- where sales.year in (2001, 2002) select sales.item.brand, sales.year, sum(sales.qty) as qty;
rowset c <- where deduped.sales.year = 2002 select deduped.sales.item.brand as brand, sum(deduped.qty) as c_qty;
rowset p <- where deduped.sales.year = 2001 select deduped.sales.item.brand as brand, sum(deduped.qty) as p_qty;
merge c.brand into ~p.brand;
select c.brand, c.c_qty, p.p_qty order by c.brand asc;
""",
        [(10, 7, 7), (20, 15, 3)],
    ),
}

# `yoy_shared_parent` derives both rowsets from a third rowset, which nests an
# aggregate over an aggregate — a deeper limitation that fails the `merge`
# reference identically, so it stays xfail until rowset-derived-twice is split.
XFAIL_CASES = {"yoy_shared_parent"}


@pytest.mark.parametrize("name", list(PARITY_CASES))
def test_join_merge_parity(engine: Executor, models: Path, name: str):
    if name in XFAIL_CASES:
        pytest.xfail(
            "rowset-derived-twice: shared rowset source not split; affects merge too"
        )
    join_form, merge_form, expected = PARITY_CASES[name]
    expected_rows = [tuple(r) for r in expected]
    merge_rows = _run(engine, models, merge_form)
    assert merge_rows == expected_rows, f"{name}: merge (reference) wrong: {merge_rows}"
    join_rows = _run(engine, models, join_form)
    assert join_rows == expected_rows, f"{name}: join wrong: {join_rows}"
    assert join_rows == merge_rows
