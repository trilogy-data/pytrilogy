import pytest

from trilogy import Environment
from trilogy.core.exceptions import AmbiguousRelationshipResolutionException
from trilogy.core.processing.model_ambiguity import (
    build_key_graph,
    connector_sets_from,
    sweep_model,
    validate_relation_paths,
)

# Two incomparable bridge chains between store and product: via orders
# (connector order_id) and via warehouse (connector wh_id).
AMBIGUOUS_MODEL = """
key order_id int;
key store_id int;
key product_id int;
key wh_id int;
property store_id.store_name string;

datasource orders (order_id: order_id, store_id: store_id)
grain (order_id)
address orders;

datasource order_products (order_id: order_id, product_id: product_id)
grain (order_id, product_id)
address order_products;

datasource stores (store_id: store_id, store_name: store_name)
grain (store_id)
address stores;

datasource store_warehouse (store_id: store_id, wh_id: wh_id)
grain (store_id, wh_id)
address store_warehouse;

datasource inventory (wh_id: wh_id, product_id: product_id)
grain (wh_id, product_id)
address inventory;
"""

# One datasource hop on a composite key set: alternatives per shared key would
# be a false flag — the hop joins on ALL shared keys at once.
SNOWFLAKE_MODEL = """
key symbol int;
key city string;
key state string;
key iso_code string;

datasource symbols (symbol: symbol, city: city, state: state)
grain (symbol)
address symbols;

datasource cities (city: city, state: state, iso_code: iso_code)
grain (city, state)
address cities;
"""


def _benv(model: str):
    env = Environment()
    env.parse(model)
    return env.materialize_for_select()


def _concepts(benv, *addresses):
    return [benv.concepts[address] for address in addresses]


def test_sweep_flags_incomparable_bridge_paths():
    benv = _benv(AMBIGUOUS_MODEL)
    pairs = {(p.left, p.right): p.alternatives for p in sweep_model(benv)}
    assert ("local.product_id", "local.store_id") in pairs
    assert set(pairs[("local.product_id", "local.store_id")]) == {
        frozenset({"local.order_id"}),
        frozenset({"local.wh_id"}),
    }


def test_validate_raises_for_unpinned_ambiguous_pair():
    benv = _benv(AMBIGUOUS_MODEL)
    with pytest.raises(AmbiguousRelationshipResolutionException) as err:
        validate_relation_paths(
            benv, _concepts(benv, "local.store_id", "local.product_id")
        )
    assert err.value.parents == [{"local.order_id"}, {"local.wh_id"}]


def test_request_carrying_a_connector_pins_the_path():
    benv = _benv(AMBIGUOUS_MODEL)
    validate_relation_paths(
        benv, _concepts(benv, "local.store_id", "local.product_id", "local.wh_id")
    )


def test_colocated_pair_is_clean():
    benv = _benv(AMBIGUOUS_MODEL)
    validate_relation_paths(benv, _concepts(benv, "local.store_id", "local.wh_id"))


def test_composite_snowflake_hop_is_one_path():
    benv = _benv(SNOWFLAKE_MODEL)
    assert sweep_model(benv) == []
    kg = build_key_graph(benv)
    assert connector_sets_from(kg, "local.symbol")["local.iso_code"] == [
        frozenset({"local.city", "local.state"})
    ]


def test_properties_are_not_path_endpoints():
    benv = _benv(AMBIGUOUS_MODEL)
    kg = build_key_graph(benv)
    assert "local.store_name" not in kg.rep
