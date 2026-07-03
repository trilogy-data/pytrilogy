from pathlib import Path

import pytest

from trilogy import Dialects, Executor
from trilogy.core.enums import JoinType, Modifier
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.environment import Environment
from trilogy.core.query_processor import process_query
from trilogy.core.statements.author import SelectStatement
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.parsing.exceptions import ParseError
from trilogy.parsing.parse_engine_v2 import parse_text

ORDERS = """key customer_id int;
property customer_id.order_amount float;

datasource orders (
    cid: customer_id,
    amt: order_amount
)
grain (customer_id)
address orders_tbl;
"""

CUSTOMERS = """key customer_id int;
property customer_id.region string;

datasource customers (
    cid: customer_id,
    reg: region
)
grain (customer_id)
address customers_tbl;
"""


SHIPMENTS = """key customer_id int;
property customer_id.ship_count int;

datasource shipments (
    cid: customer_id,
    sc: ship_count
)
grain (customer_id)
address shipments_tbl;
"""


@pytest.fixture
def models(tmp_path: Path) -> Path:
    (tmp_path / "orders.preql").write_text(ORDERS)
    (tmp_path / "customers.preql").write_text(CUSTOMERS)
    return tmp_path


@pytest.fixture
def multi_models(tmp_path: Path) -> Path:
    (tmp_path / "orders.preql").write_text(ORDERS)
    (tmp_path / "customers.preql").write_text(CUSTOMERS)
    (tmp_path / "shipments.preql").write_text(SHIPMENTS)
    return tmp_path


def _select(jointype: str) -> str:
    return f"""
import orders as orders;
import customers as customers;

{jointype} JOIN orders.customer_id = customers.customer_id
SELECT
    customers.region,
    sum(orders.order_amount) -> total_amount
ORDER BY customers.region asc;
"""


def test_left_join_marks_partial(models: Path):
    _, parsed = parse_text(_select("LEFT"), root=models)
    join = parsed[-1].join_clauses[0]
    assert join.join_type is JoinType.LEFT_OUTER
    assert join.modifiers == [Modifier.PARTIAL]


def test_left_join_renders_left_outer(models: Path):
    env, parsed = parse_text(_select("LEFT"), root=models)
    sql = DuckDBDialect().compile_statement(process_query(env, parsed[-1]))
    assert "LEFT OUTER JOIN" in sql


def test_scoped_join_does_not_mutate_global_environment(models: Path):
    env, parsed = parse_text(_select("LEFT"), root=models)
    src = env.concepts["orders.customer_id"]
    before = set(src.pseudonyms)
    process_query(env, parsed[-1])
    after = set(env.concepts["orders.customer_id"].pseudonyms)
    assert before == after
    # the anchor key likewise gains no scoped pseudonym in the global env
    assert "orders.customer_id" not in env.concepts["customers.customer_id"].pseudonyms


def test_scoped_join_leaves_global_objects_untouched(models: Path):
    # the build-level merge never replaces or adds concept/datasource objects on
    # the author env, and registers no scoped alias.
    env, parsed = parse_text(_select("LEFT"), root=models)
    concepts_before = {k: id(v) for k, v in env.concepts.items()}
    datasources_before = {k: id(v) for k, v in env.datasources.items()}
    alias_before = set(env.alias_origin_lookup)
    process_query(env, parsed[-1])
    assert {k: id(v) for k, v in env.concepts.items()} == concepts_before
    assert {k: id(v) for k, v in env.datasources.items()} == datasources_before
    assert set(env.alias_origin_lookup) == alias_before


def test_scoped_join_not_visible_in_global_pseudonyms(models: Path):
    from trilogy.core.models.build import get_canonical_pseudonyms

    env, parsed = parse_text(_select("LEFT"), root=models)
    process_query(env, parsed[-1])
    pseudonyms = get_canonical_pseudonyms(env)
    assert "customers.customer_id" not in pseudonyms.get("orders.customer_id", set())
    assert "orders.customer_id" not in pseudonyms.get("customers.customer_id", set())


def test_scoped_join_round_trips_through_render(models: Path):
    from trilogy.parsing.render import render_query

    _, parsed = parse_text(_select("LEFT"), root=models)
    rendered = render_query(parsed[-1])
    assert "left join orders.customer_id = customers.customer_id" in rendered
    text = """
import orders as orders;
import customers as customers;

""" + rendered
    _, reparsed = parse_text(text, root=models)
    join = reparsed[-1].join_clauses[0]
    assert join.join_type is JoinType.LEFT_OUTER
    assert join.source_address == "orders.customer_id"


@pytest.mark.parametrize("jointype", ["RIGHT", "CROSS"])
def test_unsupported_join_types_rejected(models: Path, jointype: str):
    with pytest.raises(ParseError, match="not supported in query-scoped joins"):
        parse_text(_select(jointype), root=models)


def test_full_join_key_is_not_partial(models: Path):
    # FULL no longer hijacks the partial flag — the key stays complete and the
    # FULL JOIN is driven by the scoped_full_join_keys registry instead.
    _, parsed = parse_text(_select("FULL"), root=models)
    join = parsed[-1].join_clauses[0]
    assert join.join_type is JoinType.FULL
    assert join.modifiers == []


def test_full_join_registers_canonical_key(models: Path):
    # the build advertises the FULL join's canonical key in the registry that
    # join resolution consults to emit a FULL JOIN.
    env = _env_for(models, "import orders as orders;\nimport customers as customers;")
    be = env.materialize_for_select(
        scoped_joins=[("orders.customer_id", "customers.customer_id", JoinType.FULL)]
    )
    assert be.scoped_full_join_keys == {"customers.customer_id"}
    # and the key is bound complete (NOT partial) on both datasources
    for ds_name in ("orders.orders", "customers.customers"):
        binding = next(
            c
            for c in be.datasources[ds_name].columns
            if c.concept.address == "customers.customer_id"
        )
        assert Modifier.PARTIAL not in binding.modifiers


def test_full_join_renders_full_join(models: Path):
    env, parsed = parse_text(_select("FULL"), root=models)
    sql = DuckDBDialect().compile_statement(process_query(env, parsed[-1]))
    assert "FULL JOIN" in sql.upper()
    assert "orders_tbl" in sql and "customers_tbl" in sql


def test_full_join_round_trips_through_render(models: Path):
    from trilogy.parsing.render import render_query

    _, parsed = parse_text(_select("FULL"), root=models)
    rendered = render_query(parsed[-1])
    assert "full join orders.customer_id = customers.customer_id" in rendered
    text = "import orders as orders;\nimport customers as customers;\n" + rendered
    _, reparsed = parse_text(text, root=models)
    assert reparsed[-1].join_clauses[0].join_type is JoinType.FULL


def test_literal_self_join_rejected(models: Path):
    # joining a concept to itself collapses to 1=1; reject with a clear error.
    # (Distinct concepts that share a base — e.g. two rowset outputs — are a
    # legitimate join and must still be allowed.)
    text = """
import orders as orders;
import customers as customers;

LEFT JOIN orders.customer_id = orders.customer_id
SELECT sum(orders.order_amount) -> total;
"""
    with pytest.raises(ParseError, match="itself"):
        parse_text(text, root=models)


TWO_KEY = """key item_id int;
key cust_id int;
property item_id.amt float;

datasource sales (iid: item_id, cid: cust_id, a: amt)
grain (item_id, cust_id)
address sales_tbl;
"""


@pytest.fixture
def two_key_models(tmp_path: Path) -> Path:
    (tmp_path / "base.preql").write_text(TWO_KEY)
    return tmp_path


_TWO_ROWSETS = (
    "import base as base;\n"
    "with web_agg as select base.item_id, base.cust_id, sum(base.amt) as total;\n"
    "with cat_agg as select base.item_id, base.cust_id, sum(base.amt) as total;\n"
)


def test_chained_composite_key_rejected(two_key_models: Path):
    # Folding a COMPOSITE key into one `=` chain (`a.i = b.i = a.c = b.c`) repeats
    # each source twice. Adjacency pairing would emit a cross-concept garbage pair
    # (`cat_agg.item = web_agg.cust`) that downstream silently drops, collapsing the
    # FULL join toward a cross product (q78). Reject it at parse time instead.
    text = _TWO_ROWSETS + (
        "FULL JOIN web_agg.base.item_id = cat_agg.base.item_id"
        " = web_agg.base.cust_id = cat_agg.base.cust_id\n"
        "SELECT web_agg.base.item_id, web_agg.total, cat_agg.total;"
    )
    with pytest.raises(ParseError, match="repeats source"):
        parse_text(text, root=two_key_models)


def test_composite_key_via_and_pairs_correctly(two_key_models: Path):
    # The supported composite-key form: `and` splits into two same-concept groups,
    # each pairing cleanly with no cross-concept garbage.
    text = _TWO_ROWSETS + (
        "FULL JOIN web_agg.base.item_id = cat_agg.base.item_id"
        " and web_agg.base.cust_id = cat_agg.base.cust_id\n"
        "SELECT web_agg.base.item_id, web_agg.total, cat_agg.total;"
    )
    _, parsed = parse_text(text, root=two_key_models)
    pairs = {(j.source_address, j.target_address) for j in parsed[-1].join_clauses}
    assert pairs == {
        ("web_agg.base.item_id", "cat_agg.base.item_id"),
        ("web_agg.base.cust_id", "cat_agg.base.cust_id"),
    }


def test_three_source_single_key_chain_allowed(multi_models: Path):
    # A valid `=` chain binds ONE key across DISTINCT sources — must not be flagged.
    text = """
import orders as orders;
import customers as customers;
import shipments as shipments;

LEFT JOIN orders.customer_id = customers.customer_id = shipments.customer_id
SELECT customers.region, sum(orders.order_amount) -> amt;
"""
    _, parsed = parse_text(text, root=multi_models)
    pairs = {(j.source_address, j.target_address) for j in parsed[-1].join_clauses}
    assert pairs == {
        ("orders.customer_id", "customers.customer_id"),
        ("customers.customer_id", "shipments.customer_id"),
    }


def test_scoped_join_column_binding_origin_stamp():
    """A scoped join folding one binding onto another's canonical address must
    thread the authored ORIGIN forward on the column (the same-address
    narrowing arbitration reads it; physical datasource identity does not
    discriminate when one table binds several relation endpoints)."""
    from trilogy.parser import parse

    env, _ = parse(
        """key aid int;
key bid int;
property aid.av float;
datasource a_src (x: aid, v: av) grain (aid) address a_tbl;
datasource b_src (y: bid) grain (bid) address b_tbl;
"""
    )
    build_env = env.materialize_for_select(
        scoped_joins=[("local.aid", "local.bid", JoinType.LEFT_OUTER)]
    )
    b_cols = {
        (c.origin_address, c.concept.address)
        for c in build_env.datasources["local.b_src"].columns
    }
    assert ("local.bid", "local.aid") in b_cols, b_cols
    a_cols = {
        (c.origin_address, c.concept.address)
        for c in build_env.datasources["local.a_src"].columns
    }
    assert all(origin is None for origin, _ in a_cols), a_cols


def test_scoped_join_canonical_collapse():
    from trilogy.core.domain_graph import DomainGraph, EdgeScope

    def graph(*joins):
        return DomainGraph.from_scoped_joins([(j, EdgeScope.STATEMENT) for j in joins])

    # chained FULL joins collapse transitively onto the final target
    g = graph(("a", "b", JoinType.FULL), ("b", "c", JoinType.FULL))
    assert g.canonical_map() == {"a": "c", "b": "c"}
    # LEFT (SQL `a LEFT JOIN b`): preserve the source `a`, so `b` collapses onto
    # `a` (a is canonical) and `b` is the partial/subset side.
    g = graph(("a", "b", JoinType.LEFT_OUTER))
    assert g.canonical_map() == {"b": "a"} and g.subset_sources() == {"b"}
    # FULL marks NEITHER partial — its canonical key is complete and the FULL JOIN
    # is driven by the scoped_full_join_keys registry, not the partial flag. The
    # source still collapses onto the canonical target (union-find).
    g = graph(("a", "b", JoinType.FULL))
    assert g.canonical_map() == {"a": "b"} and g.subset_sources() == set()


DIM_ITEM = """key item_id int;
property item_id.item_name string;

datasource ditems (iid: item_id, nm: item_name)
grain (item_id)
address ditems_tbl;
"""

SALES = """import dim_item as item;

key sale_id int;
property sale_id.amt float;

datasource s (sid: sale_id, iid: item.item_id, a: amt)
grain (sale_id)
address sales_tbl;
"""

CATALOG = """import dim_item as item;

key cat_id int;

datasource c (cid: cat_id, iid: item.item_id)
grain (cat_id)
address catalog_tbl;
"""


@pytest.fixture
def nested_models(tmp_path: Path) -> Path:
    (tmp_path / "dim_item.preql").write_text(DIM_ITEM)
    (tmp_path / "sales.preql").write_text(SALES)
    (tmp_path / "catalog.preql").write_text(CATALOG)
    return tmp_path


def test_join_resolves_nested_namespace_key(nested_models: Path):
    # nested-namespace keys (`catalog.item.item_id`) resolve as fully-addressed
    # concepts; direction is positional (left = source, right = anchor).
    text = """
import sales as sales;
import catalog as catalog;

LEFT JOIN catalog.item.item_id = sales.item.item_id
SELECT sales.item.item_id, sum(sales.amt) -> total;
"""
    _, parsed = parse_text(text, root=nested_models)
    join = parsed[-1].join_clauses[0]
    assert join.source_address == "catalog.item.item_id"
    assert join.target_address == "sales.item.item_id"


def test_join_key_must_exist(models: Path):
    from trilogy.core.exceptions import UndefinedConceptException

    text = """
import orders as orders;
import customers as customers;

LEFT JOIN orders.nonexistent = customers.customer_id
SELECT customers.region;
"""
    with pytest.raises(UndefinedConceptException, match="orders.nonexistent"):
        parse_text(text, root=models)


@pytest.mark.parametrize(
    "tail",
    [
        "left join orders.customer_id = customers.customer_id\nand orders.order_amount > 0\nselect customers.region;",
        "left join orders.customer_id = customers.customer_id\nwhere orders.order_amount > 0\nselect customers.region;",
    ],
)
def test_filter_after_join_gives_helpful_error(models: Path, tail: str):
    # filters placed after a join clause must point the author back to WHERE,
    # not surface the internal "expected JOIN_TYPE" token.
    from trilogy.core.exceptions import InvalidSyntaxException

    text = "import orders as orders;\nimport customers as customers;\n" + tail
    with pytest.raises(InvalidSyntaxException, match="Filter input rows in `where`"):
        parse_text(text, root=models)


def test_multiple_join_clauses_blend_three_models(multi_models: Path):
    text = """
import orders as orders;
import customers as customers;
import shipments as shipments;

LEFT JOIN orders.customer_id = customers.customer_id
LEFT JOIN shipments.customer_id = customers.customer_id
SELECT
    customers.region,
    sum(orders.order_amount) -> amt,
    sum(shipments.ship_count) -> ships;
"""
    env, parsed = parse_text(text, root=multi_models)
    assert len(parsed[-1].join_clauses) == 2
    sql = DuckDBDialect().compile_statement(process_query(env, parsed[-1]))
    assert "orders_tbl" in sql
    assert "customers_tbl" in sql
    assert "shipments_tbl" in sql


def _final_select(sql: str) -> str:
    # the final SELECT projection (after the last SELECT, up to its FROM).
    tail = sql[sql.rfind("SELECT") :]
    return tail[: tail.find("FROM")]


def test_projected_join_source_renders(models: Path):
    # Selecting the *source* side of a scoped join (which collapses onto the
    # target) must still render under the written name — not silently drop.
    text = """
import orders as orders;
import customers as customers;

LEFT JOIN orders.customer_id = customers.customer_id
SELECT
    orders.customer_id,
    sum(orders.order_amount) -> amt;
"""
    env, parsed = parse_text(text, root=models)
    sql = DuckDBDialect().compile_statement(process_query(env, parsed[-1]))
    final = _final_select(sql)
    assert 'as "orders_customer_id"' in final
    assert 'as "amt"' in final


def test_projected_join_target_renders(models: Path):
    # the mirror of the above: selecting the target side also renders.
    text = """
import orders as orders;
import customers as customers;

LEFT JOIN orders.customer_id = customers.customer_id
SELECT
    customers.customer_id,
    sum(orders.order_amount) -> amt;
"""
    env, parsed = parse_text(text, root=models)
    sql = DuckDBDialect().compile_statement(process_query(env, parsed[-1]))
    final = _final_select(sql)
    assert 'as "customers_customer_id"' in final
    assert 'as "amt"' in final


def test_aggregate_grouped_by_merged_key(models: Path):
    # the output grain IS the merged key. The build canonicalizes
    # customers.customer_id and orders.customer_id into one (pure pseudonym
    # substitution, no author rewrite cascade), so the anchor key is satisfied
    # straight from the orders source.
    text = """
import orders as orders;
import customers as customers;

LEFT JOIN orders.customer_id = customers.customer_id
SELECT
    customers.customer_id,
    sum(orders.order_amount) -> amt;
"""
    env, parsed = parse_text(text, root=models)
    sql = DuckDBDialect().compile_statement(process_query(env, parsed[-1]))
    # the merged anchor key resolves (sourced via the orders datasource)
    assert "customers_customer_id" in sql
    assert "orders_tbl" in sql


# --- multi-way join collapse (bug: a 3+ equivalence group selecting a
# source-side attribute must collapse like a global merge, not surface
# AmbiguousRelationshipResolution) ----------------------------------------

DIM = """key dim_id int;
property dim_id.attr string;

datasource dims (did: dim_id, a: attr)
grain (dim_id)
address dim_tbl;
"""

FACT1 = """import dim as d;

key k1 int;
property k1.m1 float;

datasource f1t (kk: k1, did: d.dim_id, m: m1)
grain (k1)
address f1_tbl;
"""

FACT_N = """import dim as d;

key {key} int;

datasource {tbl} (kk: {key}, did: d.dim_id)
grain ({key})
address {tbl}_tbl;
"""


@pytest.fixture
def three_fact_models(tmp_path: Path) -> Path:
    (tmp_path / "dim.preql").write_text(DIM)
    (tmp_path / "f1.preql").write_text(FACT1)
    (tmp_path / "f2.preql").write_text(FACT_N.format(key="k2", tbl="f2t"))
    (tmp_path / "f3.preql").write_text(FACT_N.format(key="k3", tbl="f3t"))
    return tmp_path


def test_three_way_join_collapses_like_merge(three_fact_models: Path):
    # Three facts each import their OWN copy of `dim`; joining all three dim
    # keys into one equivalence group and selecting a SOURCE-side attribute
    # (`f1.d.attr`) must resolve, exactly as the equivalent global `merge`
    # does. The pseudonym-only scoped merge left three distinct dim concepts,
    # so the planner saw three join paths and raised
    # AmbiguousRelationshipResolutionException.
    join_text = """
import f1 as f1;
import f2 as f2;
import f3 as f3;

left join f1.d.dim_id = f2.d.dim_id
left join f1.d.dim_id = f3.d.dim_id
select f1.d.attr as attr, sum(f1.m1) as total;
"""
    env, parsed = parse_text(join_text, root=three_fact_models)
    # must RESOLVE (no AmbiguousRelationshipResolution) and join the single
    # collapsed dim onto f1 — the same outcome as the global-merge form.
    join_sql = DuckDBDialect().compile_statement(process_query(env, parsed[-1]))
    assert "dim_tbl" in join_sql and "f1_tbl" in join_sql
    # f2/f3 contribute no output and are pruned — proof the keys collapsed to one.
    assert "f2_tbl" not in join_sql and "f3_tbl" not in join_sql


# --- build-environment contract: what materialize_for_select produces for a
# given (env, scoped_joins) input. These pin the build-time merge behavior
# directly, independent of downstream query resolution. -------------------


def _env_for(root: Path, imports: str):
    # parse a trivial select so the env is populated with the imported concepts
    env, _ = parse_text(imports + "\nSELECT 1 -> one;", root=root)
    return env


def test_buildenv_left_join_marks_datasource_binding_partial(models: Path):
    env = _env_for(models, "import orders as orders;\nimport customers as customers;")
    be = env.materialize_for_select(
        scoped_joins=[
            ("orders.customer_id", "customers.customer_id", JoinType.LEFT_OUTER)
        ]
    )
    # SQL `orders LEFT JOIN customers`: orders (the source/left operand) is the
    # preserved anchor, so customers collapses onto `orders.customer_id` (the
    # canonical) and the customers binding of it is partial; orders stays complete.
    assert be.concepts["customers.customer_id"].address == "orders.customer_id"
    customers_binding = next(
        c
        for c in be.datasources["customers.customers"].columns
        if c.concept.address == "orders.customer_id"
    )
    assert Modifier.PARTIAL in customers_binding.modifiers
    assert not customers_binding.is_complete
    orders_binding = next(
        c
        for c in be.datasources["orders.orders"].columns
        if c.concept.address == "orders.customer_id"
    )
    assert Modifier.PARTIAL not in orders_binding.modifiers


# --- end-to-end FULL JOIN execution: row-level correctness + datasource
# selection. The scoped-join machinery marks a FULL-join key partial on both
# sides and exempts it from the partial-source gate (it coalesces -> complete);
# these guard that the exemption stays narrow and the executed numbers are right.
# ----------------------------------------------------------------------------

# customers.customer_id is bound by a COMPLETE source (all rows) and a PARTIAL
# source (only region='east'). A FULL join key must resolve from the COMPLETE
# source, not the partial one — otherwise the coalesce silently drops customers.
CUSTOMERS_DUAL_SOURCE = """key customer_id int;
property customer_id.region string;

datasource customers (cid: customer_id, reg: region)
grain (customer_id) address customers_tbl;

partial datasource customers_east (cid: customer_id, reg: region)
grain (customer_id) complete where region = 'east'
address customers_east_tbl;
"""

# vip_flag is available ONLY from a partial source and is NOT a join key, so the
# FULL-join-key exemption must NOT cover it — the gate must still reject it.
ORDERS_PARTIAL_FLAG = """key customer_id int;
property customer_id.order_amount float;
property customer_id.vip_flag int;

datasource orders (cid: customer_id, amt: order_amount)
grain (customer_id) address orders_tbl;

partial datasource orders_vip (cid: customer_id, f: vip_flag)
grain (customer_id) complete where vip_flag = 1
address orders_vip_tbl;
"""

# Two row-grain facts that each carry a composite (k1,k2) key plus a measure at
# the true row grain — the well-formed shape for multi-key joins.
LEFT_ROWS = """key lrow int;
property lrow.k1 int;
property lrow.k2 int;
property lrow.m1 float;

datasource leftt (r: lrow, a: k1, b: k2, m: m1)
grain (lrow) address left_tbl;
"""

RIGHT_ROWS = """key rrow int;
property rrow.k1 int;
property rrow.k2 int;
property rrow.m2 float;

datasource rightt (r: rrow, a: k1, b: k2, m: m2)
grain (rrow) address right_tbl;
"""


def _exec_rows(eng: Executor, models: Path, text: str) -> list[tuple]:
    eng.environment = Environment(working_path=models)
    return [tuple(r) for r in eng.execute_text(text)[-1].fetchall()]


@pytest.fixture
def dual_source_engine(tmp_path: Path) -> Executor:
    (tmp_path / "customers.preql").write_text(CUSTOMERS_DUAL_SOURCE)
    (tmp_path / "orders.preql").write_text(ORDERS_PARTIAL_FLAG)
    eng = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    eng.execute_raw_sql("create table customers_tbl (cid int, reg varchar)")
    eng.execute_raw_sql(
        "insert into customers_tbl values (1,'east'),(2,'west'),(3,'south')"
    )
    eng.execute_raw_sql("create table customers_east_tbl (cid int, reg varchar)")
    eng.execute_raw_sql("insert into customers_east_tbl values (1,'east')")
    eng.execute_raw_sql("create table orders_tbl (cid int, amt float)")
    eng.execute_raw_sql("insert into orders_tbl values (1,100.0),(2,200.0),(4,400.0)")
    eng.execute_raw_sql("create table orders_vip_tbl (cid int, f int)")
    eng.execute_raw_sql("insert into orders_vip_tbl values (1,1)")
    return eng


def test_full_join_key_resolves_from_complete_not_partial_source(
    dual_source_engine: Executor, tmp_path: Path
):
    # the FULL-join key is exempt from the partial-source gate; verify discovery
    # still picks the COMPLETE customers source (so no customer rows are dropped),
    # not the partial east-only one.
    text = """
import customers as customers;
import orders as orders;

FULL JOIN orders.customer_id = customers.customer_id
SELECT customers.customer_id, customers.region, sum(orders.order_amount) -> total
ORDER BY customers.customer_id asc;
"""
    sql = dual_source_engine.generate_sql(text)[-1]
    assert "customers_tbl" in sql
    assert "customers_east_tbl" not in sql
    rows = _exec_rows(dual_source_engine, tmp_path, text)
    # full outer: all customers (1,2,3) and the orders-only customer (4) survive.
    assert rows == [
        (1, "east", 100.0),
        (2, "west", 200.0),
        (3, "south", None),
        (4, None, 400.0),
    ]


def test_full_join_exemption_does_not_cover_partial_only_non_key(tmp_path: Path):
    # the exemption is keyed to FULL-join canonical keys only; a partial-only
    # NON-key concept must still trip the unresolvable gate.
    (tmp_path / "customers.preql").write_text(CUSTOMERS_DUAL_SOURCE)
    (tmp_path / "orders.preql").write_text(ORDERS_PARTIAL_FLAG)
    text = """
import customers as customers;
import orders as orders;

FULL JOIN orders.customer_id = customers.customer_id
SELECT customers.customer_id, customers.region, orders.vip_flag;
"""
    env, parsed = parse_text(text, root=tmp_path)
    with pytest.raises(UnresolvableQueryException, match="orders.vip_flag"):
        process_query(env, parsed[-1])


@pytest.fixture
def two_key_engine(tmp_path: Path) -> Executor:
    (tmp_path / "left.preql").write_text(LEFT_ROWS)
    (tmp_path / "right.preql").write_text(RIGHT_ROWS)
    eng = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    eng.execute_raw_sql("create table left_tbl (r int, a int, b int, m float)")
    eng.execute_raw_sql(
        "insert into left_tbl values (101,1,1,10.0),(102,1,2,5.0),(103,2,2,20.0)"
    )
    eng.execute_raw_sql("create table right_tbl (r int, a int, b int, m float)")
    eng.execute_raw_sql(
        "insert into right_tbl values (201,1,1,100.0),(202,1,2,50.0),(203,3,3,300.0)"
    )
    return eng


def test_full_join_two_keys_single_join(two_key_engine: Executor, tmp_path: Path):
    # two FULL-join clauses between the SAME pair of sources collapse to ONE
    # composite-key FULL JOIN — each table is scanned once, no redundant re-join.
    text = """
import left as l;
import right as r;

FULL JOIN l.k1 = r.k1
FULL JOIN l.k2 = r.k2
SELECT r.k1, r.k2, sum(l.m1) -> sm1, sum(r.m2) -> sm2
ORDER BY r.k1 asc, r.k2 asc;
"""
    sql = two_key_engine.generate_sql(text)[-1]
    assert sql.upper().count("FULL JOIN") == 1
    assert sql.count("left_tbl") == 1 and sql.count("right_tbl") == 1
    rows = _exec_rows(two_key_engine, tmp_path, text)
    assert rows == [
        (1, 1, 10.0, 100.0),
        (1, 2, 5.0, 50.0),
        (2, 2, 20.0, None),
        (3, 3, None, 300.0),
    ]


def test_full_join_two_keys_coarser_output_grain(
    two_key_engine: Executor, tmp_path: Path
):
    # joined on (k1,k2) but the output grain is k1 only: the per-side sums roll up
    # to k1 and the full join still preserves left-only (k1=2) and right-only
    # (k1=3) groups.
    text = """
import left as l;
import right as r;

FULL JOIN l.k1 = r.k1
FULL JOIN l.k2 = r.k2
SELECT r.k1, sum(l.m1) -> sm1, sum(r.m2) -> sm2
ORDER BY r.k1 asc;
"""
    rows = _exec_rows(two_key_engine, tmp_path, text)
    assert rows == [(1, 15.0, 150.0), (2, 20.0, None), (3, None, 300.0)]


def test_full_join_three_sources(tmp_path: Path):
    for nm, tbl in [("a", "a_tbl"), ("b", "b_tbl"), ("c", "c_tbl")]:
        (tmp_path / f"{nm}.preql").write_text(
            f"key kid int;\nproperty kid.m_{nm} float;\n"
            f"datasource {nm}t (k: kid, m: m_{nm}) grain (kid) address {tbl};\n"
        )
    eng = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    eng.execute_raw_sql("create table a_tbl (k int, m float)")
    eng.execute_raw_sql("insert into a_tbl values (1,10.0),(2,20.0)")
    eng.execute_raw_sql("create table b_tbl (k int, m float)")
    eng.execute_raw_sql("insert into b_tbl values (2,200.0),(3,300.0)")
    eng.execute_raw_sql("create table c_tbl (k int, m float)")
    eng.execute_raw_sql("insert into c_tbl values (3,3000.0),(4,4000.0)")
    text = """
import a as a;
import b as b;
import c as c;

FULL JOIN a.kid = b.kid
FULL JOIN c.kid = b.kid
SELECT b.kid, sum(a.m_a) -> sa, sum(b.m_b) -> sb, sum(c.m_c) -> sc
ORDER BY b.kid asc;
"""
    rows = _exec_rows(eng, tmp_path, text)
    # every key (1..4) from the union of the three sources survives, padded NULL.
    assert rows == [
        (1, 10.0, None, None),
        (2, 20.0, 200.0, None),
        (3, None, 300.0, 3000.0),
        (4, None, None, 4000.0),
    ]


# --- OUTER joins between two rowsets that share a base (the yoy shape FULL JOIN
# was built for). An OUTER scoped-join key is partial on every arm; it must NOT
# be enriched back to the complete base dimension (that re-completes the key and
# collapses the cross-rowset join to a 1=1 cross product). INNER is unaffected:
# it joins on the shared base as before.
# ----------------------------------------------------------------------------

SALES_YEARLY = """key sale_id int;
property sale_id.brand int;
property sale_id.cls int;
property sale_id.year int;
property sale_id.amt float;

datasource sales (sid: sale_id, br: brand, cl: cls, yr: year, a: amt)
grain (sale_id) address sales_tbl;
"""


@pytest.fixture
def yearly_engine(tmp_path: Path) -> Executor:
    (tmp_path / "sales.preql").write_text(SALES_YEARLY)
    eng = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    eng.execute_raw_sql(
        "create table sales_tbl (sid int, br int, cl int, yr int, a float)"
    )
    # brand 1 in both years; brand 2 only 2001; brand 3 only 2002.
    eng.execute_raw_sql(
        "insert into sales_tbl values "
        "(1,1,1,2001,10.0),(2,1,1,2002,15.0),(3,2,2,2001,20.0),"
        "(4,3,3,2002,30.0),(5,1,1,2001,5.0)"
    )
    return eng


_YEARLY_ROWSETS = """
import sales as sales;
rowset y01 <- where sales.year = 2001
select sales.brand as brand, sum(sales.amt) as amt_01;
rowset y02 <- where sales.year = 2002
select sales.brand as brand, sum(sales.amt) as amt_02;
"""


@pytest.mark.parametrize(
    "jointype,expected",
    [
        # LEFT declares y02.brand ⊆ y01.brand; both rowsets are filtered, so
        # the subset can't be proven against the filtered 2001 side and the
        # preserving render stands — brand 3 (2002-only) survives NULL-padded.
        # Row restriction is an explicit filter (`where y01.amt_01 is not
        # null`), never a silent join drop.
        ("LEFT", [(1, 15.0, 15.0), (2, 20.0, None), (3, None, 30.0)]),
        # FULL: every brand from either year, padded NULL on the missing side.
        ("FULL", [(1, 15.0, 15.0), (2, 20.0, None), (3, None, 30.0)]),
    ],
)
def test_rowset_outer_join_shared_base_no_fanout(
    yearly_engine: Executor, tmp_path: Path, jointype: str, expected: list
):
    text = (
        _YEARLY_ROWSETS + f"\n{jointype} JOIN y01.brand = y02.brand\n"
        "SELECT y02.brand, y01.amt_01, y02.amt_02 ORDER BY y02.brand asc;\n"
    )
    sql = yearly_engine.generate_sql(text)[-1]
    # each year-rowset is scanned exactly once: no base-dimension re-enrichment.
    assert sql.count("sales_tbl") == 2
    rows = _exec_rows(yearly_engine, tmp_path, text)
    assert rows == expected


def test_rowset_full_join_shared_base_multi_key(
    yearly_engine: Executor, tmp_path: Path
):
    # two FULL-join clauses (brand, cls) between two shared-base rowsets collapse
    # to ONE composite-key FULL JOIN with no fan-out.
    text = """
import sales as sales;
rowset y01 <- where sales.year = 2001
select sales.brand as brand, sales.cls as cls, sum(sales.amt) as amt_01;
rowset y02 <- where sales.year = 2002
select sales.brand as brand, sales.cls as cls, sum(sales.amt) as amt_02;

FULL JOIN y01.brand = y02.brand
FULL JOIN y01.cls = y02.cls
SELECT y02.brand, y02.cls, y01.amt_01, y02.amt_02
ORDER BY y02.brand asc;
"""
    sql = yearly_engine.generate_sql(text)[-1]
    assert sql.upper().count("FULL JOIN") == 1
    assert sql.count("sales_tbl") == 2
    rows = _exec_rows(yearly_engine, tmp_path, text)
    assert rows == [
        (1, 1, 15.0, 15.0),
        (2, 2, 20.0, None),
        (3, 3, None, 30.0),
    ]


# --- two FACTS full-joined on a shared dimension key, with the dimension itself
# in scope. Confirms the outer-join-key refinement (rowset-only) does NOT block
# the dimension from being pulled in: selecting a dim attribute anchors EVERY
# dimension row, so the result is all customers (even those with no sale either
# side), with each fact's measure LEFT-padded. The dual case — selecting only the
# join key — has no complete dimension anchor, so it is the union of the two
# facts' customers (the genuine FULL-join-of-facts population).
# ----------------------------------------------------------------------------

CUSTOMER_DIM = """key customer_id int;
property customer_id.name string;

datasource customers (cid: customer_id, nm: name)
grain (customer_id) address customers_tbl;
"""

STORE_FACT = """import customer as customer;

key store_sale_id int;
property store_sale_id.store_amt float;

datasource store_sales (sid: store_sale_id, cid: customer.customer_id, amt: store_amt)
grain (store_sale_id) address store_tbl;
"""

WEB_FACT = """import customer as customer;

key web_sale_id int;
property web_sale_id.web_amt float;

datasource web_sales (wid: web_sale_id, cid: customer.customer_id, amt: web_amt)
grain (web_sale_id) address web_tbl;
"""


@pytest.fixture
def two_fact_engine(tmp_path: Path) -> Executor:
    (tmp_path / "customer.preql").write_text(CUSTOMER_DIM)
    (tmp_path / "store.preql").write_text(STORE_FACT)
    (tmp_path / "web.preql").write_text(WEB_FACT)
    eng = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    eng.execute_raw_sql("create table customers_tbl (cid int, nm varchar)")
    # Dave (4) has no sale on either side.
    eng.execute_raw_sql(
        "insert into customers_tbl values (1,'Alice'),(2,'Bob'),(3,'Carol'),(4,'Dave')"
    )
    eng.execute_raw_sql("create table store_tbl (sid int, cid int, amt float)")
    eng.execute_raw_sql("insert into store_tbl values (10,1,100.0),(11,2,200.0)")
    eng.execute_raw_sql("create table web_tbl (wid int, cid int, amt float)")
    eng.execute_raw_sql("insert into web_tbl values (20,2,20.0),(21,3,30.0)")
    return eng


def test_two_fact_full_join_with_dim_attr_keeps_all_customers(
    two_fact_engine: Executor, tmp_path: Path
):
    # selecting a customer-dim attribute pulls the complete dim in, so EVERY
    # customer survives — including Dave, who has no store or web sale.
    text = """
import store as store;
import web as web;

FULL JOIN store.customer.customer_id = web.customer.customer_id
SELECT
    store.customer.customer_id,
    store.customer.name,
    sum(store.store_amt) -> s_total,
    sum(web.web_amt) -> w_total
ORDER BY store.customer.customer_id asc;
"""
    sql = two_fact_engine.generate_sql(text)[-1]
    assert "customers_tbl" in sql  # the dimension IS pulled in
    rows = _exec_rows(two_fact_engine, tmp_path, text)
    assert rows == [
        (1, "Alice", 100.0, None),
        (2, "Bob", 200.0, 20.0),
        (3, "Carol", None, 30.0),
        (4, "Dave", None, None),
    ]


def test_two_fact_full_join_key_only_is_union_of_facts(
    two_fact_engine: Executor, tmp_path: Path
):
    # without any dim attribute there is no complete anchor, so the result is the
    # FULL-join-of-facts population: customers with a store OR web sale (no Dave).
    text = """
import store as store;
import web as web;

FULL JOIN store.customer.customer_id = web.customer.customer_id
SELECT
    store.customer.customer_id,
    sum(store.store_amt) -> s_total,
    sum(web.web_amt) -> w_total
ORDER BY store.customer.customer_id asc;
"""
    sql = two_fact_engine.generate_sql(text)[-1]
    assert "customers_tbl" not in sql  # no dim needed -> not pulled in
    rows = _exec_rows(two_fact_engine, tmp_path, text)
    assert rows == [
        (1, 100.0, None),
        (2, 200.0, 20.0),
        (3, None, 30.0),
    ]


def test_rowset_full_join_to_complete_dimension_key_yields_all_rows(
    two_fact_engine: Executor, tmp_path: Path
):
    # A rowset emits a PARTIAL customer key (only customers with a web sale).
    # FULL-joining it to the COMPLETE customer_id dimension key must, by FULL-join
    # semantics, surface every customer — the dimension is the complete side, so
    # its rows are preserved even where the rowset has no match. This is the case
    # the LEFT-only enrichment guard must NOT suppress: the dimension is a genuine
    # second source, not the rowset's own base.
    text = """
import customer as customer;
import web as web;

rowset web_rs <- select web.customer.customer_id as wc, sum(web.web_amt) as web_total;

FULL JOIN web_rs.wc = customer.customer_id
SELECT customer.customer_id, customer.name, web_rs.web_total
ORDER BY customer.customer_id asc;
"""
    sql = two_fact_engine.generate_sql(text)[-1]
    assert "customers_tbl" in sql  # the complete dimension IS sourced
    rows = _exec_rows(two_fact_engine, tmp_path, text)
    # web sales exist for customers 2 and 3; 1 and 4 survive with a NULL total.
    assert rows == [
        (1, "Alice", None),
        (2, "Bob", 20.0),
        (3, "Carol", 30.0),
        (4, "Dave", None),
    ]


# --- `and` sugar: `JOIN a = b and c = d` is exactly two stacked join clauses of
# the same type (two DISTINCT equivalence groups), saving a repeated prefix. The
# canonical render is the split (stacked) form. These pin parity between the two
# spellings, across both grammar backends, and that rendering normalizes to split.
# ----------------------------------------------------------------------------


def _join_tuples(stmt: SelectStatement) -> list[tuple]:
    return [
        (j.join_type, j.source_address, j.target_address) for j in stmt.join_clauses
    ]


def _parse_with_backend(text: str, root: Path, backend):
    from trilogy.constants import CONFIG
    from trilogy.parsing.parse_engine_v2 import clear_parse_cache

    prior = CONFIG.parser_backend
    CONFIG.parser_backend = backend
    clear_parse_cache()
    try:
        return parse_text(text, root=root)
    finally:
        CONFIG.parser_backend = prior
        clear_parse_cache()


@pytest.mark.parametrize("jointype", ["LEFT", "FULL"])
def test_and_sugar_matches_stacked_clauses(multi_models: Path, jointype: str):
    sugar = f"""
import orders as orders;
import customers as customers;
import shipments as shipments;

{jointype} JOIN orders.customer_id = customers.customer_id and shipments.customer_id = customers.customer_id
SELECT customers.region, sum(orders.order_amount) -> amt, sum(shipments.ship_count) -> ships;
"""
    stacked = f"""
import orders as orders;
import customers as customers;
import shipments as shipments;

{jointype} JOIN orders.customer_id = customers.customer_id
{jointype} JOIN shipments.customer_id = customers.customer_id
SELECT customers.region, sum(orders.order_amount) -> amt, sum(shipments.ship_count) -> ships;
"""
    _, sugar_parsed = parse_text(sugar, root=multi_models)
    _, stacked_parsed = parse_text(stacked, root=multi_models)
    assert _join_tuples(sugar_parsed[-1]) == _join_tuples(stacked_parsed[-1])
    assert len(sugar_parsed[-1].join_clauses) == 2


def test_and_sugar_two_groups_matches_stacked(two_key_engine: Executor, tmp_path: Path):
    # `a = b and c = d` is two distinct one-pair groups, identical to two stacked
    # FULL JOIN clauses (which collapse to one composite-key FULL JOIN either way).
    sugar = """
import left as l;
import right as r;

FULL JOIN l.k1 = r.k1 and l.k2 = r.k2
SELECT r.k1, r.k2, sum(l.m1) -> sm1, sum(r.m2) -> sm2
ORDER BY r.k1 asc, r.k2 asc;
"""
    stacked = """
import left as l;
import right as r;

FULL JOIN l.k1 = r.k1
FULL JOIN l.k2 = r.k2
SELECT r.k1, r.k2, sum(l.m1) -> sm1, sum(r.m2) -> sm2
ORDER BY r.k1 asc, r.k2 asc;
"""
    _, sugar_parsed = parse_text(sugar, root=tmp_path)
    _, stacked_parsed = parse_text(stacked, root=tmp_path)
    assert _join_tuples(sugar_parsed[-1]) == _join_tuples(stacked_parsed[-1])
    # and they execute to the same rows (one composite-key FULL JOIN either way).
    assert _exec_rows(two_key_engine, tmp_path, sugar) == _exec_rows(
        two_key_engine, tmp_path, stacked
    )


def test_and_sugar_combines_with_chained_group(tmp_path: Path):
    # a `=`-chain and an `and`-separated group compose: `a = b = c and p = q` is
    # one 3-key group (two adjacent pairs) plus a separate one-pair group, exactly
    # equivalent to a chained clause stacked with a second clause.
    for nm in ["a", "b", "c", "p", "q"]:
        (tmp_path / f"{nm}.preql").write_text(
            f"key {nm}_id int;\nproperty {nm}_id.m_{nm} float;\n"
            f"datasource {nm}t (k: {nm}_id, m: m_{nm}) grain ({nm}_id) address {nm}_tbl;\n"
        )
    imports = "".join(f"import {nm} as {nm};\n" for nm in ["a", "b", "c", "p", "q"])
    sugar = (
        imports + "LEFT JOIN a.a_id = b.b_id = c.c_id and p.p_id = q.q_id\n"
        "SELECT a.a_id, sum(a.m_a) -> sm;"
    )
    stacked = (
        imports + "LEFT JOIN a.a_id = b.b_id = c.c_id\n"
        "LEFT JOIN p.p_id = q.q_id\n"
        "SELECT a.a_id, sum(a.m_a) -> sm;"
    )
    _, sugar_parsed = parse_text(sugar, root=tmp_path)
    _, stacked_parsed = parse_text(stacked, root=tmp_path)
    assert _join_tuples(sugar_parsed[-1]) == _join_tuples(stacked_parsed[-1])
    assert len(sugar_parsed[-1].join_clauses) == 3


def test_and_sugar_parity_across_backends(multi_models: Path):
    from trilogy.constants import ParserBackend

    text = """
import orders as orders;
import customers as customers;
import shipments as shipments;

LEFT JOIN orders.customer_id = customers.customer_id and shipments.customer_id = customers.customer_id
SELECT customers.region, sum(orders.order_amount) -> amt, sum(shipments.ship_count) -> ships;
"""
    _, lark_parsed = _parse_with_backend(text, multi_models, ParserBackend.LARK)
    _, pest_parsed = _parse_with_backend(text, multi_models, ParserBackend.PEST)
    assert _join_tuples(lark_parsed[-1]) == _join_tuples(pest_parsed[-1])


def test_and_sugar_renders_as_split_joins(multi_models: Path):
    from trilogy.parsing.render import render_query

    text = """
import orders as orders;
import customers as customers;
import shipments as shipments;

LEFT JOIN orders.customer_id = customers.customer_id and shipments.customer_id = customers.customer_id
SELECT customers.region, sum(orders.order_amount) -> amt;
"""
    _, parsed = parse_text(text, root=multi_models)
    rendered = render_query(parsed[-1])
    # canonical form is split: one `left join` line per group, no `and`.
    assert "left join orders.customer_id = customers.customer_id" in rendered
    assert "left join shipments.customer_id = customers.customer_id" in rendered
    assert " and " not in rendered.split("select")[0]
    reimport = (
        "import orders as orders;\nimport customers as customers;\n"
        "import shipments as shipments;\n"
    )
    _, reparsed = parse_text(reimport + rendered, root=multi_models)
    assert _join_tuples(reparsed[-1]) == _join_tuples(parsed[-1])


def test_and_sugar_execution_parity(multi_models: Path):
    eng = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=multi_models)
    )
    eng.execute_raw_sql("create table orders_tbl (cid int, amt float)")
    eng.execute_raw_sql("insert into orders_tbl values (1,100.0),(2,200.0)")
    eng.execute_raw_sql("create table customers_tbl (cid int, reg varchar)")
    eng.execute_raw_sql("insert into customers_tbl values (1,'east'),(2,'west')")
    eng.execute_raw_sql("create table shipments_tbl (cid int, sc int)")
    eng.execute_raw_sql("insert into shipments_tbl values (1,5),(2,7)")
    sugar = """
import orders as orders;
import customers as customers;
import shipments as shipments;

LEFT JOIN orders.customer_id = customers.customer_id and shipments.customer_id = customers.customer_id
SELECT customers.region, sum(orders.order_amount) -> amt, sum(shipments.ship_count) -> ships
ORDER BY customers.region asc;
"""
    stacked = sugar.replace(
        "LEFT JOIN orders.customer_id = customers.customer_id and shipments.customer_id = customers.customer_id",
        "LEFT JOIN orders.customer_id = customers.customer_id\n"
        "LEFT JOIN shipments.customer_id = customers.customer_id",
    )
    assert _exec_rows(eng, multi_models, sugar) == _exec_rows(
        eng, multi_models, stacked
    )
