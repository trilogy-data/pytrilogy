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
    # year-over-year where both rowsets derive from one shared `base` parent
    # (a row-grain passthrough rowset), each aggregating it per year and joined
    # on brand. The join key is sourced *through* the intermediate rowset
    # (`base.sales.item.brand`), which used to collapse the rowset node grain to
    # Abstract and degenerate the outer join to FULL JOIN 1=1. Same expected as
    # the independent-rowset case.
    "yoy_shared_parent": (
        """
import sales as sales;
rowset base <- where sales.year in (2001, 2002) select sales.sale_id, sales.item.brand, sales.year, sales.qty;
rowset c <- where base.sales.year = 2002 select base.sales.item.brand as brand, sum(base.sales.qty) as c_qty;
rowset p <- where base.sales.year = 2001 select base.sales.item.brand as brand, sum(base.sales.qty) as p_qty;
inner join c.brand = p.brand
select c.brand, c.c_qty, p.p_qty order by c.brand asc;
""",
        """
import sales as sales;
rowset base <- where sales.year in (2001, 2002) select sales.sale_id, sales.item.brand, sales.year, sales.qty;
rowset c <- where base.sales.year = 2002 select base.sales.item.brand as brand, sum(base.sales.qty) as c_qty;
rowset p <- where base.sales.year = 2001 select base.sales.item.brand as brand, sum(base.sales.qty) as p_qty;
merge c.brand into ~p.brand;
select c.brand, c.c_qty, p.p_qty order by c.brand asc;
""",
        [(10, 7, 7), (20, 15, 3)],
    ),
    # Same shared-parent shape but the parent itself aggregates, so the child
    # rowsets re-aggregate an aggregate (`sum(deduped.qty)` over
    # `deduped.qty = sum(...)`). That lowers to `sum(sum(...))`, invalid SQL in
    # every dialect; it fails the `merge` reference identically. Orthogonal to
    # the grain-collapse bug fixed above — xfail until nested aggregates split.
    "yoy_shared_parent_nested_agg": (
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

# `yoy_shared_parent_nested_agg` re-aggregates an aggregate (sum(sum(...))) — a
# deeper limitation that fails the `merge` reference identically, so it stays
# xfail until nested aggregates are split into separate grouping passes.
XFAIL_CASES = {"yoy_shared_parent_nested_agg"}


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
    + "inner join c.brand = p.brand\nselect c.brand, c.c_qty, p.p_qty order by c.brand asc;\n"
)
JOIN_POST = (
    _POS_HEAD
    + "select c.brand, c.c_qty, p.p_qty,\ninner join c.brand = p.brand\norder by c.brand asc;\n"
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


# A scoped INNER join on a *derived* key (not a rowset, not a root column —
# here a basic transform on each side from a different datasource) must resolve
# exactly like the global `merge`: each side keeps its own derivation and the
# join relates the two computed columns. The collapse-only path used to discard
# the source's derivation, leaving the merged concept unsourceable from the
# source side ("No remaining priority concepts"). Regression for that.
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
DERIVED_JOIN = _DERIVED_HEAD + """
inner join da = db
select da, sum(o.oid) as n_orders, sum(c.cid) as n_costs order by da asc;
"""
DERIVED_MERGE = _DERIVED_HEAD + """
merge da into ~db;
select da, sum(o.oid) as n_orders, sum(c.cid) as n_costs order by da asc;
"""


@pytest.fixture
def derived_engine(tmp_path: Path) -> Executor:
    (tmp_path / "d_orders.preql").write_text(_DERIVED_ORDERS)
    (tmp_path / "d_costs.preql").write_text(_DERIVED_COSTS)
    env = Environment(working_path=tmp_path)
    eng = Dialects.DUCK_DB.default_executor(environment=env)
    eng.execute_raw_sql("create table orders_tbl (o int, a int)")
    eng.execute_raw_sql("insert into orders_tbl values (1,9),(2,9),(3,19)")
    eng.execute_raw_sql("create table costs_tbl (c int, k int)")
    eng.execute_raw_sql("insert into costs_tbl values (10,9),(20,19),(30,29)")
    return eng


def test_scoped_join_on_nonrowset_derived_key(derived_engine: Executor, tmp_path: Path):
    # da=10 -> orders {1,2}, costs {10}; da=20 -> orders {3}, costs {20}.
    expected = [(10, 3, 10), (20, 3, 20)]
    merge_rows = _run(derived_engine, tmp_path, DERIVED_MERGE)
    assert merge_rows == expected, f"merge (reference) wrong: {merge_rows}"
    join_rows = _run(derived_engine, tmp_path, DERIVED_JOIN)
    assert join_rows == expected, f"join wrong: {join_rows}"
    assert join_rows == merge_rows


DERIVED_LEFT_JOIN = _DERIVED_HEAD + """
left join da = db
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


# join_clause, merge_clause, select, expected. Pure-INNER N-way has a `merge`
# reference (the canonical key is complete on every side -> INNER-shaped), so
# join and merge must both equal expected.
MULTI_INNER_CASES: dict[str, tuple[str, str, str, list[tuple]]] = {
    "2way": (
        "inner join ka = kb",
        "merge ka into ~kb;",
        "select ka, sum(a.a_id) as na, sum(b.b_id) as nb order by ka asc;",
        [(10, 8, 1), (20, 2, 2), (40, 4, 4), (50, 5, 5)],
    ),
    "3way": (
        "inner join ka = kb\ninner join ka = kc",
        "merge ka into ~kb;\nmerge ka into ~kc;",
        "select ka, sum(a.a_id) as na, sum(b.b_id) as nb, "
        "sum(c.c_id) as nc order by ka asc;",
        [(10, 8, 1, 1), (20, 2, 2, 2), (50, 5, 5, 5)],
    ),
    "4way": (
        "inner join ka = kb\ninner join ka = kc\ninner join ka = kd",
        "merge ka into ~kb;\nmerge ka into ~kc;\nmerge ka into ~kd;",
        "select ka, sum(a.a_id) as na, sum(b.b_id) as nb, "
        "sum(c.c_id) as nc, sum(d.d_id) as nd order by ka asc;",
        [(10, 8, 1, 1, 1), (20, 2, 2, 2, 2)],
    ),
}


@pytest.mark.parametrize("name", list(MULTI_INNER_CASES))
def test_multi_way_inner_join_merge_parity(
    multi_engine: Executor, tmp_path: Path, name: str
):
    # N-way INNER narrows to the intersection of all sources' buckets
    # (2way {10,20,40,50} -> 3way {10,20,50} -> 4way {10,20}); each must match
    # the equivalent chained global `merge`.
    join_clause, merge_clause, select, expected = MULTI_INNER_CASES[name]
    expected = [tuple(r) for r in expected]
    merge_rows = _run(
        multi_engine, tmp_path, _MULTI_HEAD + merge_clause + "\n" + select
    )
    assert merge_rows == expected, f"{name}: merge (reference) wrong: {merge_rows}"
    join_rows = _run(multi_engine, tmp_path, _MULTI_HEAD + join_clause + "\n" + select)
    assert join_rows == expected, f"{name}: join wrong: {join_rows}"
    assert join_rows == merge_rows


# Chained-equality `a = b = c` collapses all keys into ONE equivalence group
# with one join type. It must match the equivalent pairwise form exactly.
def test_chained_equality_join_matches_pairwise(multi_engine: Executor, tmp_path: Path):
    select = (
        "select ka, sum(a.a_id) as na, sum(b.b_id) as nb, "
        "sum(c.c_id) as nc order by ka asc;"
    )
    chained = _run(
        multi_engine, tmp_path, _MULTI_HEAD + "inner join ka = kb = kc\n" + select
    )
    pairwise = _run(
        multi_engine,
        tmp_path,
        _MULTI_HEAD + "inner join ka = kb\ninner join ka = kc\n" + select,
    )
    assert chained == pairwise, f"chained != pairwise: {chained} vs {pairwise}"
    assert chained == [(10, 8, 1, 1), (20, 2, 2, 2), (50, 5, 5, 5)]


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


# Mixed join types. INNER and LEFT mix freely on one key group (a shared anchor
# with required + optional sources is well-defined; INNER drops non-matching
# buckets, LEFT keeps the inner result with NULLs). FULL may NOT be mixed with
# another type on the same group (ambiguous) — see the rejection test below.
# `merge` can't express LEFT, so this is a hand-computed expectation.
def test_mixed_inner_left_on_one_group(multi_engine: Executor, tmp_path: Path):
    # INNER drops a-only bucket 30; LEFT keeps the a∩b result with nc NULL at 40.
    expected = [(10, 8, 1, 1), (20, 2, 2, 2), (40, 4, 4, None), (50, 5, 5, 5)]
    select = (
        "select ka, sum(a.a_id) as na, sum(b.b_id) as nb, "
        "sum(c.c_id) as nc order by ka asc;"
    )
    forward = _run(
        multi_engine,
        tmp_path,
        _MULTI_HEAD + "inner join ka = kb\nleft join ka = kc\n" + select,
    )
    assert forward == expected, f"inner+left wrong: {forward}"
    # well-defined -> independent of clause order.
    swapped = _run(
        multi_engine,
        tmp_path,
        _MULTI_HEAD + "left join ka = kc\ninner join ka = kb\n" + select,
    )
    assert swapped == expected, f"order changed result: {swapped}"


@pytest.mark.parametrize(
    "clauses",
    [
        "inner join ka = kb\nfull join ka = kc",
        "left join ka = kb\nfull join ka = kc",
        "full join ka = kb\ninner join kb = kc",
    ],
)
def test_full_mixed_with_other_type_in_one_group_rejected(
    multi_engine: Executor, tmp_path: Path, clauses: str
):
    # FULL polluting an INNER/LEFT group is ambiguous -> rejected at parse time.
    select = "select ka, sum(a.a_id) as na order by ka asc;"
    with pytest.raises(ParseError, match="FULL join cannot be mixed"):
        _run(multi_engine, tmp_path, _MULTI_HEAD + clauses + "\n" + select)


# DISJOINT key groups may use different join types: a central fact with two
# derived FK keys, one INNER-joined to a required dim, one FULL-joined to an
# optional dim. The INNER group must still filter (its rows are NOT preserved by
# the FULL on the *other* group), and the FULL group's one-sided rows must
# survive without fanning out. The fan-out bug: the multi-measure re-assembly
# joined on the composite (km1, km2) with non-null-safe `=`, so an m2-only row
# (km1 NULL-injected by the disjoint FULL) split into spurious all-NULL copies.
_DISJOINT_FACT = """
key fid int;
property fid.v1 int;
property fid.v2 int;
datasource src (i: fid, a: v1, b: v2) grain (fid) address f_tbl;
"""
_DISJOINT_DIM = """
key {key} int;
property {key}.{val} int;
datasource src (i: {key}, a: {val}) grain ({key}) address {tbl};
"""
_DISJOINT_HEAD = """
import mf as f; import m1 as m1; import m2 as m2;
auto kf1 <- f.v1 + 1;
auto kf2 <- f.v2 + 1;
auto km1 <- m1.w1 + 1;
auto km2 <- m2.w2 + 1;
"""


@pytest.fixture
def disjoint_engine(tmp_path: Path) -> Executor:
    (tmp_path / "mf.preql").write_text(_DISJOINT_FACT)
    (tmp_path / "m1.preql").write_text(
        _DISJOINT_DIM.format(key="m1id", val="w1", tbl="m1_tbl")
    )
    (tmp_path / "m2.preql").write_text(
        _DISJOINT_DIM.format(key="m2id", val="w2", tbl="m2_tbl")
    )
    eng = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    eng.execute_raw_sql("create table f_tbl (i int, a int, b int)")
    # fid 1: kf1=10 (in m1), kf2=10 (in m2); fid 2: kf1=10 (in m1), kf2=50 (NOT
    # in m2); fid 3: kf1=99 (NOT in m1), kf2=10 -> dropped by the INNER.
    eng.execute_raw_sql("insert into f_tbl values (1,9,9),(2,9,49),(3,98,9)")
    eng.execute_raw_sql("create table m1_tbl (i int, a int)")
    eng.execute_raw_sql("insert into m1_tbl values (1,9)")  # km1 = {10}
    eng.execute_raw_sql("create table m2_tbl (i int, a int)")
    eng.execute_raw_sql("insert into m2_tbl values (1,9),(5,19)")  # km2 = {10, 20}
    return eng


def test_disjoint_inner_and_full_groups(disjoint_engine: Executor, tmp_path: Path):
    query = _DISJOINT_HEAD + (
        "inner join kf1 = km1\nfull join kf2 = km2\n"
        "select kf1, kf2, sum(f.fid) as nf, sum(m1.m1id) as nm1, "
        "sum(m2.m2id) as nm2 order by kf2 asc, kf1 asc;"
    )
    rows = _run(disjoint_engine, tmp_path, query)
    assert rows == [
        (10, 10, 1, 1, 1),  # fully matched
        (None, 20, None, None, 5),  # m2-only (FULL), no fan-out
        (10, 50, 2, 1, None),  # f matched m1, no m2 match (FULL keeps it)
    ], f"disjoint inner+full wrong: {rows}"
    # the disjoint FULL null-injects the INNER group's key -> assembly must use
    # null-safe equality, else the m2-only row fans into spurious all-NULL copies.
    sql = disjoint_engine.generate_sql(query)[-1]
    assert (
        "IS NOT DISTINCT FROM" in sql.upper()
    ), f"expected null-safe join, sql:\n{sql}"
