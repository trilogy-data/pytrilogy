"""Side-qualified null tests on ROOT (datasource-bound) scoped-join key
members: the presence-probe contract of test_subset_presence_probe.py /
test_coalescing_presence_matrix.py, without rowsets (the TPC-DS q84 shape).

A ROOT member's binding is substituted onto the group canonical at build
time, so both sides' datasources carry the SAME address and a null test on
the member would read whichever side sourcing picks — pre-fix the filter
silently no-op'd and the member's datasource vanished from the plan
(evals/tpcds_agent/bug_q84_subset_join_presence_filter.md). Null tests now
bind to a probe pinned to a datasource that physically carries the member's
authored column (gen_presence_probe_node, side identity recovered via
BuildColumnAssignment.origin_address).

The superset/anchor side of a subset join stays a genuine no-op: the
declaration says every subset value matches it, and a lying declaration is an
author error (docs/subset_union_join_design.md), caught by opt-in domain
validation — not engine heroics. q84's authored form filtered the anchor of a
lying subset declaration; the fix there is authoring the correct direction,
which lands on the subset-side cells below.
"""

from pathlib import Path

from tests.join_matrix.harness import sort_rows
from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

# customers 1..3; orders only for customers 1 and 2 (customer 1 twice: the
# probe must not fan out the customer grain).
MODEL = """
key cust_id int;
property cust_id.cname string;
datasource customers (i: cust_id, n: cname) grain (cust_id)
query '''
select 1 i, 'A' n union all select 2 i, 'B' n union all select 3 i, 'C' n
''';

key order_id int;
property order_id.ord_cust int;
property order_id.amt int;
datasource orders (o: order_id, c: ord_cust, a: amt) grain (order_id)
query '''
select 100 o, 1 c, 10 a union all select 101 o, 1 c, 15 a
union all select 102 o, 2 c, 20 a
''';
"""


def _run(tmp_path: Path, query: str) -> list[tuple]:
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    statements = engine.parse_text(MODEL + query)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    return sort_rows([tuple(r) for r in engine.execute_raw_sql(sql).fetchall()])


# ord_cust ⊆ cust_id: customers is the anchor, orders the subset side.
SUBSET = "select cname subset join ord_cust = cust_id "

ALL_CUSTOMERS = [("A",), ("B",), ("C",)]
WITH_ORDERS = [("A",), ("B",)]
WITHOUT_ORDERS = [("C",)]


def test_subset_no_filter_row_preserving(tmp_path: Path):
    assert _run(tmp_path, SUBSET + ";") == sort_rows(ALL_CUSTOMERS)


def test_subset_side_not_null_intersects(tmp_path: Path):
    # The q59 idiom on a ROOT member: pre-fix the filter vanished and all
    # customers came back.
    assert _run(tmp_path, "where ord_cust is not null " + SUBSET + ";") == sort_rows(
        WITH_ORDERS
    )


def test_subset_side_is_null_anti_joins(tmp_path: Path):
    assert _run(tmp_path, "where ord_cust is null " + SUBSET + ";") == sort_rows(
        WITHOUT_ORDERS
    )


def test_subset_anchor_not_null_is_noop(tmp_path: Path):
    # Anchor side: trusted by declaration, never probed.
    assert _run(tmp_path, "where cust_id is not null " + SUBSET + ";") == sort_rows(
        ALL_CUSTOMERS
    )


def test_subset_both_endpoints_not_null_intersects(tmp_path: Path):
    # The q84 authored predicate pair, correct declaration direction.
    assert _run(
        tmp_path,
        "where ord_cust is not null and cust_id is not null " + SUBSET + ";",
    ) == sort_rows(WITH_ORDERS)


def test_subset_projected_member_stays_group_axis(tmp_path: Path):
    # Projecting the member renders the coalesced group axis; only the null
    # test binds per-side.
    rows = _run(
        tmp_path,
        "where ord_cust is not null "
        "select cust_id, cname subset join ord_cust = cust_id;",
    )
    assert rows == sort_rows([(1, "A"), (2, "B")])


def test_subset_filter_with_subset_side_measure(tmp_path: Path):
    rows = _run(
        tmp_path,
        "where ord_cust is not null "
        "select cname, sum(amt) as total subset join ord_cust = cust_id;",
    )
    assert rows == sort_rows([("A", 25), ("B", 20)])


# Member bound in BOTH its defining dimension (PK, spans the whole domain)
# and a sparse fact (FK): the presence probe must read the FK carrier, not
# the dimension — probing the dimension is a tautology (the exact TPC-DS q84
# shape: ss.return_customer_demographic.sk is defined by
# customer_demographics but the question is about store_returns).
DOUBLE_BINDING_MODEL = """
key r_demo int;
datasource r_demos (k: r_demo) grain (r_demo)
query '''
select 1 k union all select 2 k union all select 3 k
''';

key ret_id int;
datasource rets (o: ret_id, d: ?r_demo) grain (ret_id)
query '''
select 100 o, 1 d
''';

key c_demo int;
datasource c_demos (k: c_demo) grain (c_demo)
query '''
select 1 k union all select 2 k union all select 3 k
''';

key cust_id int;
property cust_id.cname string;
datasource customers (i: cust_id, d: c_demo, n: cname) grain (cust_id)
query '''
select 1 i, 1 d, 'A' n union all select 2 i, 2 d, 'B' n
''';
"""


def test_subset_member_dimension_and_fact_bindings_probe_fact(tmp_path: Path):
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    statements = engine.parse_text(
        DOUBLE_BINDING_MODEL
        + "where r_demo is not null select cname subset join r_demo = c_demo;"
    )
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    rows = sort_rows([tuple(r) for r in engine.execute_raw_sql(sql).fetchall()])
    # only customer A's demo appears in the returns FACT; the dimension holds
    # every demo, so a dimension-pinned probe would keep B too
    assert rows == [("A",)]
    assert "rets" in sql


# Coalescing (union) relation between two ROOT keys in separate fact tables:
# the q97 presence-count shape without rowsets.
UNION_MODEL = """
key sid int;
property sid.s_cust int;
datasource store_fact (r: sid, c: s_cust) grain (sid)
query '''
select 1 r, 1 c union all select 2 r, 2 c
''';

key cid int;
property cid.c_cust int;
datasource catalog_fact (r: cid, c: c_cust) grain (cid)
query '''
select 1 r, 1 c union all select 2 r, 3 c
''';
"""


def _run_union(tmp_path: Path, query: str) -> list[tuple]:
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    statements = engine.parse_text(UNION_MODEL + query)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    return sort_rows([tuple(r) for r in engine.execute_raw_sql(sql).fetchall()])


def test_union_presence_counts(tmp_path: Path):
    rows = _run_union(
        tmp_path,
        "select "
        "sum(case when s_cust is not null and c_cust is null then 1 else 0 end)"
        " as store_only, "
        "sum(case when s_cust is null and c_cust is not null then 1 else 0 end)"
        " as catalog_only, "
        "sum(case when s_cust is not null and c_cust is not null then 1 else 0 end)"
        " as both_channels "
        "union join s_cust = c_cust;",
    )
    assert rows == [(1, 1, 1)]


def test_union_where_side_is_null(tmp_path: Path):
    # catalog-only customers (anti-join via the store-side probe)
    rows = _run_union(
        tmp_path,
        "where s_cust is null select c_cust, cid union join s_cust = c_cust;",
    )
    assert rows == [(3, 2)]


def test_union_bare_axis_projection_unions_domains(tmp_path: Path):
    # Pre-fix this single-sourced one member's table as the unified axis.
    # Every member binding is stamped partial (union = neither domain contains
    # the other) and gen_coalescing_axis_node assembles the mandatory coalesce
    # of every member side.
    rows = _run_union(tmp_path, "select c_cust union join s_cust = c_cust;")
    assert rows == [(1,), (2,), (3,)]


def test_union_where_side_is_null_bare_axis(tmp_path: Path):
    # Anti-join over the complete axis: the store-side probe filters the
    # coalesced domain, keeping catalog-only customers. Also pins the
    # predicate-pushdown null-extension guard: `probe IS NULL` must never push
    # below the FULL join that null-extends it.
    rows = _run_union(
        tmp_path,
        "where s_cust is null select c_cust union join s_cust = c_cust;",
    )
    assert rows == [(3,)]


def test_union_where_side_not_null(tmp_path: Path):
    rows = _run_union(
        tmp_path,
        "where s_cust is not null select c_cust, s_cust" " union join s_cust = c_cust;",
    )
    assert rows == sort_rows([(1, 1), (2, 2)])


# Mixed relation: ROOT member against a ROWSET key. The ROOT member's null
# test must pin to its datasource; the ROWSET member keeps the rowset path.
# Rowset spans every customer so `ord_cust ⊆ bc_id` is an honest declaration
# (orders exist for customers 1 and 2 only).
MIXED_MODEL = MODEL + """
rowset big_customers <- select cust_id as bc_id, cname as bc_name;
"""


def _run_mixed(tmp_path: Path, query: str) -> list[tuple]:
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    statements = engine.parse_text(MIXED_MODEL + query)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    return sort_rows([tuple(r) for r in engine.execute_raw_sql(sql).fetchall()])


def test_mixed_root_member_not_null_against_rowset_anchor(tmp_path: Path):
    # ord_cust (ROOT, subset side) ⊆ big_customers.bc_id (rowset anchor):
    # requiring an order-side match keeps customers 1 and 2.
    rows = _run_mixed(
        tmp_path,
        "where ord_cust is not null "
        "select big_customers.bc_name subset join ord_cust = big_customers.bc_id;",
    )
    assert rows == sort_rows([("A",), ("B",)])


def test_mixed_rowset_member_not_null_against_root_anchor(tmp_path: Path):
    # big_customers.bc_id (rowset, subset side) ⊆ cust_id (root anchor); the
    # rowset spans every customer, so its presence test keeps all three.
    rows = _run_mixed(
        tmp_path,
        "where big_customers.bc_id is not null "
        "select cname subset join big_customers.bc_id = cust_id;",
    )
    assert rows == sort_rows(ALL_CUSTOMERS)
