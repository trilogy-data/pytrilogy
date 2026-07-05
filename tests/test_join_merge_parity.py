"""Parity: a query-scoped `join` must produce outcomes identical to the
equivalent global `merge`. The join clause is handled at build time (more
performant) and global merge statements now feed the same build-time machinery.
Each case below pairs a `join` form with the equivalent `merge` form and asserts
identical results.
"""

from pathlib import Path

import pytest

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.parsing.exceptions import ParseError
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

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
    # year-over-year on a shared base dim, two INDEPENDENT rowsets. A non-partial
    # `merge` is a FULL identity join, so the equivalent join form is `full join`
    # (brand 30 exists only in 2002 and is kept with a NULL prior year).
    "yoy_independent_rowsets": (
        """
import sales as sales;
rowset c <- where sales.year = 2002 select sales.item.brand as brand, sum(sales.qty) as c_qty;
rowset p <- where sales.year = 2001 select sales.item.brand as brand, sum(sales.qty) as p_qty;
full join c.brand = p.brand
select c.brand, c.c_qty, p.p_qty order by c.brand asc;
""",
        """
import sales as sales;
rowset c <- where sales.year = 2002 select sales.item.brand as brand, sum(sales.qty) as c_qty;
rowset p <- where sales.year = 2001 select sales.item.brand as brand, sum(sales.qty) as p_qty;
merge c.brand into p.brand;
select c.brand, c.c_qty, p.p_qty order by c.brand asc;
""",
        [(10, 7, 7), (20, 15, 3), (30, 4, None)],
    ),
    # partial merge (`merge x into ~y`) declares c.brand ⊆ p.brand. Both
    # rowsets are FILTERED, so the subset can't be proven against the 2001
    # side and the preserving render stands: the 2002-only brand 30 survives
    # with a NULL p_qty — pins that merge~ and `left join` share the
    # declaration machinery and both render row-preserving.
    "partial_merge_left_anchor": (
        """
import sales as sales;
rowset c <- where sales.year = 2002 select sales.item.brand as brand, sum(sales.qty) as c_qty;
rowset p <- where sales.year = 2001 select sales.item.brand as brand, sum(sales.qty) as p_qty;
left join p.brand = c.brand
select p.brand, p.p_qty, c.c_qty order by p.brand asc;
""",
        """
import sales as sales;
rowset c <- where sales.year = 2002 select sales.item.brand as brand, sum(sales.qty) as c_qty;
rowset p <- where sales.year = 2001 select sales.item.brand as brand, sum(sales.qty) as p_qty;
merge c.brand into ~p.brand;
select p.brand, p.p_qty, c.c_qty order by p.brand asc;
""",
        [(10, 7, 7), (20, 3, 15), (30, None, 4)],
    ),
    # ... and the explicit `is not null` idiom restores the old anchored rows.
    "partial_merge_left_anchor_filtered": (
        """
import sales as sales;
rowset c <- where sales.year = 2002 select sales.item.brand as brand, sum(sales.qty) as c_qty;
rowset p <- where sales.year = 2001 select sales.item.brand as brand, sum(sales.qty) as p_qty;
where p.p_qty is not null
left join p.brand = c.brand
select p.brand, p.p_qty, c.c_qty order by p.brand asc;
""",
        """
import sales as sales;
rowset c <- where sales.year = 2002 select sales.item.brand as brand, sum(sales.qty) as c_qty;
rowset p <- where sales.year = 2001 select sales.item.brand as brand, sum(sales.qty) as p_qty;
merge c.brand into ~p.brand;
where p.p_qty is not null
select p.brand, p.p_qty, c.c_qty order by p.brand asc;
""",
        [(10, 7, 7), (20, 3, 15)],
    ),
    # merge-as-imputation: the target is an UNBOUND property, only sourceable
    # through the source's derivation. Historically merge-only (the collapse
    # wired the pseudonyms); the join form must resolve identically.
    "derived_into_unbound_property": (
        """
import sales as sales;
property sales.sale_id.imputed_qty int;
auto doubled <- sales.qty * 2;
full join doubled = sales.imputed_qty
select sales.sale_id, sales.imputed_qty order by sales.sale_id asc;
""",
        """
import sales as sales;
property sales.sale_id.imputed_qty int;
auto doubled <- sales.qty * 2;
merge doubled into sales.imputed_qty;
select sales.sale_id, sales.imputed_qty order by sales.sale_id asc;
""",
        [(1, 10), (2, 14), (3, 6), (4, 18), (5, 8), (6, 4), (7, 12)],
    ),
    # rowset FK joined to a freshly imported dimension, selecting dim attrs
    "rowset_fk_to_dim": (
        """
import sales as sales;
import customer as customer;
rowset bought <- where sales.year = 2002 select sales.customer.customer_id, sum(sales.qty) as qty;
where customer.name is not null
left join bought.sales.customer.customer_id = customer.customer_id
select customer.name, bought.qty order by customer.name asc;
""",
        """
import sales as sales;
import customer as customer;
rowset bought <- where sales.year = 2002 select sales.customer.customer_id, sum(sales.qty) as qty;
merge bought.sales.customer.customer_id into customer.customer_id;
select customer.name, bought.qty order by customer.name asc;
""",
        [("alice", 17), ("bob", 9)],
    ),
}


@pytest.mark.parametrize("name", list(PARITY_CASES))
def test_join_merge_parity(engine: Executor, models: Path, name: str):
    join_form, merge_form, expected = PARITY_CASES[name]
    expected_rows = [tuple(r) for r in expected]
    merge_rows = _run(engine, models, merge_form)
    assert merge_rows == expected_rows, f"{name}: merge (reference) wrong: {merge_rows}"
    join_rows = _run(engine, models, join_form)
    assert join_rows == expected_rows, f"{name}: join wrong: {join_rows}"
    assert join_rows == merge_rows


# The query-scoped join may sit BEFORE `select` (legacy) or RIGHT AFTER the
# select list (preferred, SQL-like). Both positions must parse on both backends
# and execute identically. The post form carries a trailing comma before the
# join to exercise the select-list/join-keyword disambiguation.
_POS_HEAD = """
import sales as sales;
rowset c <- where sales.year = 2002 select sales.item.brand as brand, sum(sales.qty) as c_qty;
rowset p <- where sales.year = 2001 select sales.item.brand as brand, sum(sales.qty) as p_qty;
"""
JOIN_PRE = (
    _POS_HEAD
    + "where p.p_qty is not null\nleft join c.brand = p.brand\n"
    + "select c.brand, c.c_qty, p.p_qty order by c.brand asc;\n"
)
JOIN_POST = (
    _POS_HEAD
    + "select c.brand, c.c_qty, p.p_qty,\nleft join c.brand = p.brand\n"
    + "where p.p_qty is not null\norder by c.brand asc;\n"
)


@pytest.mark.parametrize("backend", [parse_lark, parse_pest])
def test_post_select_join_parses_on_both_backends(backend):
    backend(JOIN_POST)


def test_post_select_join_position_matches_pre(engine: Executor, models: Path):
    expected = [(10, 7, 7), (20, 15, 3)]
    pre = _run(engine, models, JOIN_PRE)
    post = _run(engine, models, JOIN_POST)
    assert pre == expected, f"pre-select join wrong: {pre}"
    assert post == expected, f"post-select join wrong: {post}"
    assert pre == post


# Scoped LEFT / FULL joins on a *derived* key (not a rowset, not a root column —
# here a basic transform on each side from a different datasource): each side
# keeps its own derivation and the join relates the two computed columns. The
# collapse-only path used to discard the source's derivation, leaving the merged
# concept unsourceable from the source side ("No remaining priority concepts").
# Regression for that.
_DERIVED_ORDERS = """
key oid int;
property oid.amt int;
datasource orders (o: oid, a: amt) grain (oid) address orders_tbl;
"""
_DERIVED_COSTS = """
key cid int;
property cid.cost int;
datasource costs (c: cid, k: cost) grain (cid) address costs_tbl;
"""
_DERIVED_HEAD = """
import d_orders as o;
import d_costs as c;
auto da <- o.amt + 1;
auto db <- c.cost + 1;
"""
DERIVED_LEFT_JOIN = _DERIVED_HEAD + """
left join da = db
select da, sum(o.oid) as n_orders, sum(c.cid) as n_costs order by da asc;
"""

DERIVED_LEFT_MERGE = _DERIVED_HEAD + """
merge db into ~da;
select da, sum(o.oid) as n_orders, sum(c.cid) as n_costs order by da asc;
"""


@pytest.fixture
def derived_left_engine(tmp_path: Path) -> Executor:
    (tmp_path / "d_orders.preql").write_text(_DERIVED_ORDERS)
    (tmp_path / "d_costs.preql").write_text(_DERIVED_COSTS)
    env = Environment(working_path=tmp_path)
    eng = Dialects.DUCK_DB.default_executor(environment=env)
    eng.execute_raw_sql("create table orders_tbl (o int, a int)")
    # da values 10, 20, 99 -- da=99 (order 3) has NO matching cost.
    eng.execute_raw_sql("insert into orders_tbl values (1,9),(2,19),(3,98)")
    eng.execute_raw_sql("create table costs_tbl (c int, k int)")
    eng.execute_raw_sql("insert into costs_tbl values (10,9),(20,19),(40,39)")
    return eng


def test_scoped_left_join_on_nonrowset_derived_key(
    derived_left_engine: Executor, tmp_path: Path
):
    # LEFT preserves the left (da) side: da=99 stays with n_costs NULL. A plain
    # collapse would silently render INNER and drop it (the derived key has no
    # datasource binding to carry partiality). Regression for that.
    expected = [(10, 1, 10), (20, 2, 20), (99, 3, None)]
    rows = _run(derived_left_engine, tmp_path, DERIVED_LEFT_JOIN)
    assert rows == expected, f"left join wrong: {rows}"
    merge_rows = _run(derived_left_engine, tmp_path, DERIVED_LEFT_MERGE)
    assert merge_rows == expected, f"partial merge wrong: {merge_rows}"
    sql = derived_left_engine.generate_sql(DERIVED_LEFT_JOIN)[-1]
    assert "LEFT OUTER JOIN" in sql, f"expected a LEFT OUTER JOIN, sql:\n{sql}"


DERIVED_FULL_JOIN = _DERIVED_HEAD + """
full join da = db
select da, sum(o.oid) as n_orders, sum(c.cid) as n_costs order by da asc;
"""


@pytest.fixture
def derived_full_engine(tmp_path: Path) -> Executor:
    (tmp_path / "d_orders.preql").write_text(_DERIVED_ORDERS)
    (tmp_path / "d_costs.preql").write_text(_DERIVED_COSTS)
    env = Environment(working_path=tmp_path)
    eng = Dialects.DUCK_DB.default_executor(environment=env)
    eng.execute_raw_sql("create table orders_tbl (o int, a int)")
    # da values 10, 20, 99 -- da=99 (order 3) has NO matching cost.
    eng.execute_raw_sql("insert into orders_tbl values (1,9),(2,19),(3,98)")
    eng.execute_raw_sql("create table costs_tbl (c int, k int)")
    # db values 10, 20, 41 -- db=41 (cost 40) has NO matching order.
    eng.execute_raw_sql("insert into costs_tbl values (10,9),(20,19),(40,40)")
    return eng


def test_scoped_full_join_on_nonrowset_derived_key(
    derived_full_engine: Executor, tmp_path: Path
):
    # FULL spans both sides on a *derived* key (da vs db are distinct columns
    # that collapse to one canonical output). Matched rows (10, 20), the
    # orders-only row (da=99, n_costs NULL) and the costs-only row (da=41 via
    # the both-sides coalesce, n_orders NULL) must all survive. A plain collapse
    # dead-ended ("No remaining priority concepts"); the first attempt at the
    # coalesce dropped da on the orders-only row -> (None, 3, None).
    expected = [(10, 1, 10), (20, 2, 20), (41, None, 40), (99, 3, None)]
    rows = _run(derived_full_engine, tmp_path, DERIVED_FULL_JOIN)
    assert rows == expected, f"full join wrong: {rows}"
    sql = derived_full_engine.generate_sql(DERIVED_FULL_JOIN)[-1]
    assert "FULL JOIN" in sql.upper(), f"expected a FULL JOIN, sql:\n{sql}"
    assert "coalesce" in sql.lower(), f"expected coalesce of both keys, sql:\n{sql}"


# --- N-way (2/3/4) and mixed-type scoped joins on a *derived* key. Four
# independent models, each contributing a derived bucket key (val+1), related on
# the shared bucket value. Buckets per source:
#   a {10,20,30,40,50} (dup at 10), b {10,20,40,50}, c {10,20,50}, d {10,20,99}.
# Per-source sums of the id column at each bucket: a 10->8 else id; b/c/d 1:1.
_MULTI_HEAD = """
import ma as a; import mb as b; import mc as c; import md as d;
auto ka <- a.a_val + 1;
auto kb <- b.b_val + 1;
auto kc <- c.c_val + 1;
auto kd <- d.d_val + 1;
"""


@pytest.fixture
def multi_engine(tmp_path: Path) -> Executor:
    for mod, (k, v) in {
        "ma": ("a_id", "a_val"),
        "mb": ("b_id", "b_val"),
        "mc": ("c_id", "c_val"),
        "md": ("d_id", "d_val"),
    }.items():
        (tmp_path / f"{mod}.preql").write_text(
            f"key {k} int;\nproperty {k}.{v} int;\n"
            f"datasource src (i: {k}, x: {v}) grain ({k}) address {mod}_tbl;\n"
        )
    eng = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    eng.execute_raw_sql("create table ma_tbl (i int, x int)")
    eng.execute_raw_sql(
        "insert into ma_tbl values (1,9),(7,9),(2,19),(3,29),(4,39),(5,49)"
    )
    eng.execute_raw_sql("create table mb_tbl (i int, x int)")
    eng.execute_raw_sql("insert into mb_tbl values (1,9),(2,19),(4,39),(5,49)")
    eng.execute_raw_sql("create table mc_tbl (i int, x int)")
    eng.execute_raw_sql("insert into mc_tbl values (1,9),(2,19),(5,49)")
    eng.execute_raw_sql("create table md_tbl (i int, x int)")
    eng.execute_raw_sql("insert into md_tbl values (1,9),(2,19),(9,98)")
    return eng


# N-way (2/3/4) scoped LEFT join + `where <b> is not null` narrows to the
# intersection of all sources' buckets (2way {10,20,40,50} -> 3way {10,20,50} ->
# 4way {10,20}).
MULTI_LEFT_CASES: dict[str, tuple[str, str, list[tuple]]] = {
    "2way": (
        "where b.b_id is not null\nleft join ka = kb",
        "select ka, sum(a.a_id) as na, sum(b.b_id) as nb order by ka asc;",
        [(10, 8, 1), (20, 2, 2), (40, 4, 4), (50, 5, 5)],
    ),
    "3way": (
        "where b.b_id is not null and c.c_id is not null\n"
        "left join ka = kb\nleft join ka = kc",
        "select ka, sum(a.a_id) as na, sum(b.b_id) as nb, "
        "sum(c.c_id) as nc order by ka asc;",
        [(10, 8, 1, 1), (20, 2, 2, 2), (50, 5, 5, 5)],
    ),
    "4way": (
        "where b.b_id is not null and c.c_id is not null and d.d_id is not null\n"
        "left join ka = kb\nleft join ka = kc\nleft join ka = kd",
        "select ka, sum(a.a_id) as na, sum(b.b_id) as nb, "
        "sum(c.c_id) as nc, sum(d.d_id) as nd order by ka asc;",
        [(10, 8, 1, 1, 1), (20, 2, 2, 2, 2)],
    ),
}


@pytest.mark.parametrize("name", list(MULTI_LEFT_CASES))
def test_multi_way_left_join_intersection(
    multi_engine: Executor, tmp_path: Path, name: str
):
    join_clause, select, expected = MULTI_LEFT_CASES[name]
    expected = [tuple(r) for r in expected]
    join_rows = _run(multi_engine, tmp_path, _MULTI_HEAD + join_clause + "\n" + select)
    assert join_rows == expected, f"{name}: join wrong: {join_rows}"


# Chained-equality `a = b = c` collapses all keys into ONE equivalence group
# with one join type. It must match the equivalent pairwise form exactly.
def test_chained_equality_join_matches_pairwise(multi_engine: Executor, tmp_path: Path):
    select = (
        "select ka, sum(a.a_id) as na, sum(b.b_id) as nb, "
        "sum(c.c_id) as nc order by ka asc;"
    )
    chained = _run(
        multi_engine, tmp_path, _MULTI_HEAD + "left join ka = kb = kc\n" + select
    )
    pairwise = _run(
        multi_engine,
        tmp_path,
        _MULTI_HEAD + "left join ka = kb\nleft join ka = kc\n" + select,
    )
    assert chained == pairwise, f"chained != pairwise: {chained} vs {pairwise}"
    assert chained == [
        (10, 8, 1, 1),
        (20, 2, 2, 2),
        (30, 3, None, None),
        (40, 4, 4, None),
        (50, 5, 5, 5),
    ]


def test_chained_full_join_all_buckets(multi_engine: Executor, tmp_path: Path):
    # an all-FULL group (legal: one type) over three keys -> union of every
    # bucket, each source NULL where it lacks the bucket.
    rows = _run(
        multi_engine,
        tmp_path,
        _MULTI_HEAD + "full join ka = kb = kc\n"
        "select ka, sum(a.a_id) as na, sum(b.b_id) as nb, "
        "sum(c.c_id) as nc order by ka asc;",
    )
    assert rows == [
        (10, 8, 1, 1),
        (20, 2, 2, 2),
        (30, 3, None, None),
        (40, 4, 4, None),
        (50, 5, 5, 5),
    ], f"chained full wrong: {rows}"


# FULL may NOT be mixed with another type on the same group (ambiguous) — see
# the rejection test below.


@pytest.mark.parametrize(
    "clauses",
    [
        "left join ka = kb\nfull join ka = kc",
    ],
)
def test_full_mixed_with_other_type_in_one_group_rejected(
    multi_engine: Executor, tmp_path: Path, clauses: str
):
    # FULL polluting an INNER/LEFT group is ambiguous -> rejected at parse time.
    select = "select ka, sum(a.a_id) as na order by ka asc;"
    with pytest.raises(ParseError, match="FULL/UNION join cannot be mixed"):
        _run(multi_engine, tmp_path, _MULTI_HEAD + clauses + "\n" + select)
