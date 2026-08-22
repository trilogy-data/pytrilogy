"""Graph-time election of `~` extension-span owners (docs/extent_ownership.md).

These assert the DECISION, not a rendered shape: which group is licensed to
manufacture a span's extension rows, and that every other group is told not to.
The row-level consequences live in tests/engine/test_duckdb_partial_key_assembly
and tests/engine/test_duckdb_partial_fk_field_report.
"""

from tests.engine.test_duckdb_partial_fk_field_report import MODEL
from trilogy import Environment
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.build import Factory, get_canonical_pseudonyms
from trilogy.core.processing.concept_strategies_v4 import V4History
from trilogy.core.processing.concept_strategies_v4 import (
    search_concepts as search_concepts_v4,
)
from trilogy.core.processing.nodes import History
from trilogy.core.processing.v4_helper.constants import FINAL_NODE_ID
from trilogy.core.processing.v4_helper.extent_ownership import (
    demanded_extension_spans,
    licensed_extension_spans,
)
from trilogy.parser import parse_text

_SIMPLE = """
key user_id int;
property user_id.state string;
key product_id int;
property product_id.brand string;
key order_id int;
property order_id.amount int;
key item_id int;
property item_id.qty int;

auto total_qty <- sum(qty);

datasource users (user_id: user_id, state: state)
grain (user_id) address users;

datasource products (product_id: product_id, brand: brand)
grain (product_id) address products;

datasource orders (order_id: order_id, user_id: ~user_id, amount: amount)
grain (order_id) address orders;

datasource items (
    item_id: item_id,
    order_id: order_id,
    product_id: ~product_id,
    user_id: ~user_id,
    qty: qty,
)
grain (item_id) address items;
"""


def _plan(model: str, query: str):
    env = Environment()
    env, _ = env.parse(model)
    _, parsed = parse_text(query, env)
    statement = parsed[-1]
    history = History(base_environment=env)
    history.build_caches.pseudonym_map = get_canonical_pseudonyms(env)
    factory = Factory(
        environment=env,
        build_cache=history.build_caches.build_cache,
        canonical_build_cache=history.build_caches.canonical_build_cache,
        grain_build_cache=history.build_caches.grain_build_cache,
        pseudonym_map=history.build_caches.pseudonym_map,
    )
    built = factory.build(statement.as_lineage(env))
    build_env = env.materialize_for_select(
        built.local_concepts,
        build_cache=history.build_caches.build_cache,
        pseudonym_map=factory.pseudonym_map,
        grain_build_cache=factory.grain_build_cache,
        canonical_build_cache=history.build_caches.canonical_build_cache,
        datasource_build_cache=history.build_caches.datasource_build_cache,
    )
    info = search_concepts_v4(
        list(built.output_components),
        V4History(base_environment=env, build_caches=history.build_caches),
        build_env,
        0,
        generate_graph(build_env),
        conditions=[],
    )
    return info, build_env


def _ownership(model: str, query: str):
    info, _ = _plan(model, query)
    return info, info.group_attrs[FINAL_NODE_ID].extent_ownership


def test_every_span_routes_to_one_owner():
    info, ownership = _ownership(
        MODEL,
        "select order_id, item_id, user_id, product_id, total_revenue,"
        " total_quantity, total_cost;",
    )
    assert ownership is not None
    assert ownership.spans == frozenset({"local.user_id", "local.product_id"})
    assert len(set(ownership.owner_by_span.values())) == 1
    owner = ownership.owner_by_span["local.user_id"]
    assert "local.user_id" in info.group_attrs[owner].primary_members

    # every other group is told not to manufacture either family
    others = [gid for gid in info.group_attrs if gid not in (owner, FINAL_NODE_ID)]
    assert others
    for gid in others:
        assert ownership.suppressed_for(gid) == ownership.spans


def test_dimension_attribute_demands_its_key_span():
    """A dimension attribute in the output demands its key's extension rows,
    even though the key itself is never projected. Reading demand off the merge
    grain instead would sweep in join axes nobody asks extension rows of."""
    info, build_env = _plan(_SIMPLE, "select state, brand;")
    licensed = licensed_extension_spans(build_env)
    assert licensed == frozenset({"local.user_id", "local.product_id"})
    assert demanded_extension_spans(info.group_attrs, licensed, build_env) == frozenset(
        {"local.user_id", "local.product_id"}
    )


def test_span_nobody_projects_is_not_demanded():
    info, build_env = _plan(_SIMPLE, "select order_id, total_qty;")
    assert (
        demanded_extension_spans(
            info.group_attrs, licensed_extension_spans(build_env), build_env
        )
        == frozenset()
    )
    ownership = info.group_attrs[FINAL_NODE_ID].extent_ownership
    assert ownership is not None
    assert ownership.spans == frozenset()


def test_span_no_group_delivers_stays_unmanaged():
    """Demand is not enough: a key nothing exposes cannot be routed, and
    suppressing what has no owner would delete its extension rows outright.
    The `select state, brand` plan reaches both keys only through joins."""
    _, ownership = _ownership(_SIMPLE, "select state, brand;")
    assert ownership is not None
    assert ownership.spans == frozenset()
    assert ownership.owner_by_span == {}
