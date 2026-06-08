from pathlib import Path

import pytest

from trilogy.core.enums import JoinType, Modifier
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


def test_inner_join_parses_to_select_join(models: Path):
    _, parsed = parse_text(_select("INNER"), root=models)
    stmt = parsed[-1]
    assert isinstance(stmt, SelectStatement)
    assert len(stmt.join_clauses) == 1
    join = stmt.join_clauses[0]
    assert join.join_type is JoinType.INNER
    # source is the joined-in model's key; target is the anchor key
    assert join.source_address == "orders.customer_id"
    assert join.target_address == "customers.customer_id"
    assert join.modifiers == []


def test_left_join_marks_partial(models: Path):
    _, parsed = parse_text(_select("LEFT"), root=models)
    join = parsed[-1].join_clauses[0]
    assert join.join_type is JoinType.LEFT_OUTER
    assert join.modifiers == [Modifier.PARTIAL]


def test_inner_join_blends_both_models(models: Path):
    env, parsed = parse_text(_select("INNER"), root=models)
    sql = DuckDBDialect().compile_statement(process_query(env, parsed[-1]))
    assert "INNER JOIN" in sql
    assert "orders_tbl" in sql and "customers_tbl" in sql


def test_left_join_renders_left_outer(models: Path):
    env, parsed = parse_text(_select("LEFT"), root=models)
    sql = DuckDBDialect().compile_statement(process_query(env, parsed[-1]))
    assert "LEFT OUTER JOIN" in sql


def test_scoped_join_does_not_mutate_global_environment(models: Path):
    env, parsed = parse_text(_select("INNER"), root=models)
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
    env, parsed = parse_text(_select("INNER"), root=models)
    concepts_before = {k: id(v) for k, v in env.concepts.items()}
    datasources_before = {k: id(v) for k, v in env.datasources.items()}
    alias_before = set(env.alias_origin_lookup)
    process_query(env, parsed[-1])
    assert {k: id(v) for k, v in env.concepts.items()} == concepts_before
    assert {k: id(v) for k, v in env.datasources.items()} == datasources_before
    assert set(env.alias_origin_lookup) == alias_before


def test_scoped_join_not_visible_in_global_pseudonyms(models: Path):
    from trilogy.core.models.build import get_canonical_pseudonyms

    env, parsed = parse_text(_select("INNER"), root=models)
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


@pytest.mark.parametrize("jointype", ["FULL", "RIGHT", "CROSS"])
def test_unsupported_join_types_rejected(models: Path, jointype: str):
    with pytest.raises(ParseError, match="not yet supported"):
        parse_text(_select(jointype), root=models)


def test_literal_self_join_rejected(models: Path):
    # joining a concept to itself collapses to 1=1; reject with a clear error.
    # (Distinct concepts that share a base — e.g. two rowset outputs — are a
    # legitimate join and must still be allowed.)
    text = """
import orders as orders;
import customers as customers;

INNER JOIN orders.customer_id = orders.customer_id
SELECT sum(orders.order_amount) -> total;
"""
    with pytest.raises(ParseError, match="itself"):
        parse_text(text, root=models)


def test_build_scoped_merge_index():
    from trilogy.core.models.build import _build_scoped_merge_index

    merge_map, partial = _build_scoped_merge_index([("a", "b", [])])
    assert merge_map == {"a": "b"} and partial == set()
    # chained joins collapse transitively onto the final target
    merge_map, _ = _build_scoped_merge_index([("a", "b", []), ("b", "c", [])])
    assert merge_map == {"a": "c", "b": "c"}
    # PARTIAL (left) source is tracked for nullable binding
    _, partial = _build_scoped_merge_index([("a", "b", [Modifier.PARTIAL])])
    assert partial == {"a"}


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

INNER JOIN catalog.item.item_id = sales.item.item_id
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

INNER JOIN orders.nonexistent = customers.customer_id
SELECT customers.region;
"""
    with pytest.raises(UndefinedConceptException, match="orders.nonexistent"):
        parse_text(text, root=models)


@pytest.mark.parametrize(
    "tail",
    [
        "inner join orders.customer_id = customers.customer_id\nand orders.order_amount > 0\nselect customers.region;",
        "inner join orders.customer_id = customers.customer_id\nwhere orders.order_amount > 0\nselect customers.region;",
    ],
)
def test_filter_after_join_gives_helpful_error(models: Path, tail: str):
    # filters placed after a join clause must point the author back to WHERE,
    # not surface the internal "expected JOIN_TYPE" token.
    from trilogy.core.exceptions import InvalidSyntaxException

    text = "import orders as orders;\nimport customers as customers;\n" + tail
    with pytest.raises(InvalidSyntaxException, match="may only be followed"):
        parse_text(text, root=models)


def test_multiple_join_clauses_blend_three_models(multi_models: Path):
    text = """
import orders as orders;
import customers as customers;
import shipments as shipments;

INNER JOIN orders.customer_id = customers.customer_id
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

INNER JOIN orders.customer_id = customers.customer_id
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

INNER JOIN orders.customer_id = customers.customer_id
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
    # substitution, no with_merge cascade), so the anchor key is satisfied
    # straight from the orders source.
    text = """
import orders as orders;
import customers as customers;

INNER JOIN orders.customer_id = customers.customer_id
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

inner join f1.d.dim_id = f2.d.dim_id
inner join f1.d.dim_id = f3.d.dim_id
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


def test_buildenv_collapses_source_concept_to_target(models: Path):
    env = _env_for(models, "import orders as orders;\nimport customers as customers;")
    be = env.materialize_for_select(
        scoped_joins=[("orders.customer_id", "customers.customer_id", [])]
    )
    # source resolves to the target concept; target is unchanged
    assert be.concepts["orders.customer_id"].address == "customers.customer_id"
    assert be.concepts["customers.customer_id"].address == "customers.customer_id"
    # the global author env is untouched
    assert env.concepts["orders.customer_id"].address == "orders.customer_id"


def test_buildenv_multiway_collapses_all_sources_to_one_canonical(
    three_fact_models: Path,
):
    env = _env_for(
        three_fact_models,
        "import f1 as f1;\nimport f2 as f2;\nimport f3 as f3;",
    )
    be = env.materialize_for_select(
        scoped_joins=[
            ("f1.d.dim_id", "f2.d.dim_id", []),
            ("f1.d.dim_id", "f3.d.dim_id", []),
        ]
    )
    # union-find: both sources collapse to the final target (f3), one canonical key
    assert be.concepts["f1.d.dim_id"].address == "f3.d.dim_id"
    assert be.concepts["f2.d.dim_id"].address == "f3.d.dim_id"
    # a dependent's grain collapses too (the whole point — no N distinct keys)
    attr_grain = be.concepts["f1.d.attr"].grain.components
    assert "f3.d.dim_id" in attr_grain
    assert "f1.d.dim_id" not in attr_grain and "f2.d.dim_id" not in attr_grain


def test_buildenv_left_join_marks_datasource_binding_partial(models: Path):
    env = _env_for(models, "import orders as orders;\nimport customers as customers;")
    be = env.materialize_for_select(
        scoped_joins=[
            ("orders.customer_id", "customers.customer_id", [Modifier.PARTIAL])
        ]
    )
    # the orders datasource bound the source key; after a LEFT merge its binding
    # for the (now target) concept must be partial -> drives a LEFT-OUTER join.
    orders_ds = be.datasources["orders.orders"]
    binding = next(
        c for c in orders_ds.columns if c.concept.address == "customers.customer_id"
    )
    assert Modifier.PARTIAL in binding.modifiers
    assert not binding.is_complete


def test_buildenv_inner_join_binding_not_partial(models: Path):
    env = _env_for(models, "import orders as orders;\nimport customers as customers;")
    be = env.materialize_for_select(
        scoped_joins=[("orders.customer_id", "customers.customer_id", [])]
    )
    orders_ds = be.datasources["orders.orders"]
    binding = next(
        c for c in orders_ds.columns if c.concept.address == "customers.customer_id"
    )
    assert Modifier.PARTIAL not in binding.modifiers
