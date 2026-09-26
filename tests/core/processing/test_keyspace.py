"""The keyspace: a plan's row universe (docs/keyspace_phase_plan.md).

Asserts the REGIONS, never a rendered shape. The row-level consequences are
pinned by tests/engine/test_derived_key_domain.py.
"""

from tests.core.processing.test_extent_ownership import _SIMPLE, _plan
from tests.engine.test_derived_key_domain import (
    _ACTIVITY,
    _DERIVED,
    _NULLABLE_FK,
    _PARTIAL_PROPERTY_SOURCE,
)
from tests.engine.test_duckdb_partial_fk_field_report import MODEL as FIELD_REPORT
from tests.engine.test_duckdb_rowset_null_group_rejoin import (
    KEYLESS_CASES,
    NESTED_ROWSET_QUERY,
    ROWSET_QUERY,
    UNSOLD_MODEL,
)
from trilogy import Dialects
from trilogy.core.processing import partial_bridging
from trilogy.core.processing.v4_helper.constants import FINAL_NODE_ID
from trilogy.core.processing.v4_helper.keyspace import build_keyspace
from trilogy.core.processing.v4_helper.models import Keyspace
from trilogy.core.processing.v4_node_generators import rowset_witness

CUSTOMER = "local.customer_id"
ORDER = "local.order_id"
USER = "local.user_id"
PRODUCT = "local.product_id"
ITEM = "local.item_id"


def _keyspace(model: str, query: str) -> Keyspace:
    info, _ = _plan(model, query)
    return info.keyspace


class _Capture:
    def __init__(self) -> None:
        self.seen: list[Keyspace] = []

    def __call__(self, *args, **kwargs) -> Keyspace:
        self.seen.append(build_keyspace(*args, **kwargs))
        return self.seen[-1]


def _planned_keyspace(monkeypatch, model: str, query: str) -> Keyspace:
    """Through the full statement path, so the WHERE reaches the plan."""
    capture = _Capture()
    monkeypatch.setattr(rowset_witness, "build_keyspace", capture)
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(model)
    executor.generate_sql(query)
    return capture.seen[0]


def _heal_keyspace(monkeypatch, model: str, query: str) -> Keyspace:
    """What pin-heal asks: the statement's bindings as authored."""
    capture = _Capture()
    monkeypatch.setattr(partial_bridging, "build_keyspace", capture)
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(model)
    executor.generate_sql(query)
    return capture.seen[0]


def _cells(keyspace: Keyspace) -> set[frozenset[str]]:
    return {r.present for r in keyspace.regions}


def test_demanded_partial_key_adds_its_extension_region():
    keyspace = _keyspace(_DERIVED, "select customer_id, status;")
    assert _cells(keyspace) == {frozenset({CUSTOMER, ORDER}), frozenset({CUSTOMER})}
    (extension,) = keyspace.extensions
    assert extension.spans == frozenset({CUSTOMER})
    assert extension.sources == frozenset({"customers"})


def test_concept_is_defined_only_where_its_keys_are_present():
    keyspace = _keyspace(_DERIVED, "select customer_id, name, status, label;")
    (extension,) = keyspace.extensions
    assert keyspace.defined_on("local.name", extension)
    assert not keyspace.defined_on("local.status", extension)
    assert not keyspace.defined_on("local.label", extension)
    assert [r.present for r in keyspace.absent_regions("local.status")] == [
        frozenset({CUSTOMER})
    ]


def test_aggregate_by_the_span_is_defined_on_the_extension_region():
    keyspace = _keyspace(_DERIVED + _ACTIVITY, "select customer_id, status, activity;")
    (extension,) = keyspace.extensions
    assert keyspace.defined_on("local.activity", extension)
    assert not keyspace.defined_on("local.status", extension)


def test_join_axis_only_key_adds_no_region():
    keyspace = _keyspace(_DERIVED, "select status, count(order_id) as n;")
    assert _cells(keyspace) == {frozenset({ORDER})}


def test_span_demanded_through_a_member_it_determines():
    keyspace = _keyspace(_DERIVED, "select name, status;")
    assert _cells(keyspace) == {frozenset({CUSTOMER, ORDER}), frozenset({CUSTOMER})}
    assert keyspace.output_demanded_spans == frozenset({CUSTOMER})


def test_span_demanded_only_as_an_aggregate_argument():
    keyspace = _keyspace(_DERIVED, "select status, count(customer_id) as customers;")
    assert keyspace.demanded_spans == frozenset({CUSTOMER})
    assert keyspace.output_demanded_spans == frozenset()


def test_extension_families_never_cross_pair():
    keyspace = _keyspace(_SIMPLE, "select state, brand;")
    assert _cells(keyspace) == {
        frozenset({USER, PRODUCT}),
        frozenset({USER}),
        frozenset({PRODUCT}),
    }
    assert keyspace.demanded_spans == frozenset({USER, PRODUCT})


def test_completely_bound_key_is_absorbed_by_the_finer_source():
    keyspace = _keyspace(_SIMPLE, "select order_id, total_qty;")
    assert _cells(keyspace) == {frozenset({ORDER, ITEM})}


def test_source_binding_its_own_grain_partially_is_the_same_region():
    keyspace = _keyspace(_PARTIAL_PROPERTY_SOURCE, "select order_id, is_returned;")
    assert _cells(keyspace) == {frozenset({ORDER, ITEM})}
    assert keyspace.absent_regions("local.is_returned") == ()


def test_nullable_key_is_a_value_on_one_region():
    keyspace = _keyspace(_NULLABLE_FK, "select order_id, customer_label;")
    assert _cells(keyspace) == {frozenset({ORDER, CUSTOMER})}


def test_rollup_subtotal_is_not_a_region():
    flat = _keyspace(_DERIVED, "select customer_id, sum(amount) as total;")
    rolled = _keyspace(
        _DERIVED, "select customer_id, sum(amount) as total by rollup (customer_id);"
    )
    assert _cells(rolled) == _cells(flat)


def test_where_null_rejecting_an_absent_concept_empties_the_region(monkeypatch):
    keyspace = _heal_keyspace(
        monkeypatch, _DERIVED, "select customer_id, status where status = 'delivered';"
    )
    (extension,) = keyspace.extensions
    assert extension.emptied_by == frozenset({"local.status"})
    assert keyspace.demanded_spans == frozenset()
    # a merge below the WHERE still sees the dead region's padding
    assert keyspace.in_play_spans == frozenset({CUSTOMER})


def test_where_between_on_an_absent_concept_empties_the_region(monkeypatch):
    keyspace = _heal_keyspace(
        monkeypatch,
        _DERIVED,
        "select customer_id, status where amount between 1 and 9;",
    )
    (extension,) = keyspace.extensions
    assert extension.emptied_by == frozenset({"local.amount"})
    assert keyspace.binding_is_complete("orders", CUSTOMER)


def test_where_over_a_present_concept_leaves_the_region_live(monkeypatch):
    keyspace = _planned_keyspace(
        monkeypatch, _DERIVED, "select customer_id, status where name = 'cat';"
    )
    assert keyspace.demanded_spans == frozenset({CUSTOMER})


def test_field_report_has_one_region_per_family():
    keyspace = _keyspace(
        FIELD_REPORT,
        "select order_id, item_id, user_id, product_id, total_revenue,"
        " total_quantity, total_cost;",
    )
    assert keyspace.demanded_spans == frozenset({USER, PRODUCT})
    assert {r.present for r in keyspace.extensions} == {
        frozenset({USER}),
        frozenset({PRODUCT}),
    }


def test_needed_partial_source_beside_a_complete_one_is_a_completion():
    keyspace = _keyspace(_PARTIAL_PROPERTY_SOURCE, "select order_id, is_returned;")
    (base,) = keyspace.regions
    assert base.completes == frozenset({ORDER, ITEM})
    # the election's question: whose unmatched members carry an output
    assert keyspace.output_demanded_spans == frozenset({ORDER})


def test_unneeded_partial_source_demands_nothing():
    keyspace = _keyspace(_PARTIAL_PROPERTY_SOURCE, "select order_id, qty;")
    (base,) = keyspace.regions
    assert base.completes == frozenset()
    assert keyspace.output_demanded_spans == frozenset()


_MERGED_PARTIAL = """
key customer_id int;
property customer_id.name string;
key order_id int;
key order_customer_id int;

datasource customers (customer_id: customer_id, name: name)
grain (customer_id) address customers;

datasource orders (order_id: order_id, customer_id: ~order_customer_id)
grain (order_id) address orders;

merge order_customer_id into customer_id;
"""


def test_partial_binding_survives_a_merge_onto_its_target():
    keyspace = _keyspace(_MERGED_PARTIAL, "select name, order_id;")
    (extension,) = keyspace.extensions
    assert extension.present == frozenset({CUSTOMER})
    assert extension.spans == frozenset({"local.order_customer_id"})


def test_election_routes_the_spans_the_keyspace_demands():
    info, _ = _plan(
        FIELD_REPORT,
        "select order_id, item_id, user_id, product_id, total_revenue,"
        " total_quantity, total_cost;",
    )
    ownership = info.group_attrs[FINAL_NODE_ID].extent_ownership
    assert ownership.spans == info.keyspace.output_demanded_spans


def test_span_no_read_source_binds_is_not_routed():
    """`select user_id, state` reads `users` alone. `orders` binding `~user_id`
    somewhere in the model demands nothing of this statement."""
    info, _ = _plan(_SIMPLE, "select user_id, state;")
    assert len(info.keyspace.regions) == 1
    assert info.group_attrs[FINAL_NODE_ID].extent_ownership.spans == frozenset()


_GRAINLESS_DIMENSION = """
key customer_id int;
key nation_id int;
property nation_id.nation_name string;
key order_id int;

datasource nations (nation_id: nation_id, nation_name: nation_name)
address nations;

datasource customers (customer_id: customer_id, nation_id: nation_id)
address customers;

datasource orders (order_id: order_id, customer_id: ~customer_id)
grain (order_id) address orders;
"""


def test_source_without_a_grain_is_identified_by_its_own_key():
    """`customers` declares no grain; `nation_id` identifies `nations`, so
    `customers` is one row per customer and an order reaches its nation."""
    keyspace = _keyspace(_GRAINLESS_DIMENSION, "select order_id, nation_name;")
    (extension,) = keyspace.extensions
    assert extension.present == frozenset({"local.nation_id"})
    assert extension.spans == frozenset({CUSTOMER})


_PROPERTY_AS_GRAIN = """
key id int;
property id.region string;
property id.amount int;

datasource fact (id: id, region: ~?region, amount: amount)
grain (id) address fact;

datasource region_dim (region: region)
grain (region) address region_dim;
"""


def test_property_identifying_a_source_is_an_entity():
    keyspace = _keyspace(_PROPERTY_AS_GRAIN, "select region, sum(amount) as total;")
    (extension,) = keyspace.extensions
    assert extension.present == frozenset({"local.region"})
    assert keyspace.output_demanded_spans == frozenset({"local.region"})


def test_row_computed_key_lives_on_its_arguments_entity():
    keyspace = _keyspace(
        _DERIVED + "auto name_key <- upper(name);",
        "select name_key, count(order_id) as n;",
    )
    assert keyspace.keys_by_address["local.name_key"] == frozenset({CUSTOMER})
    assert keyspace.output_demanded_spans == frozenset({CUSTOMER})


_GENERATED_DOMAIN = """
key orid int;
auto orid_2 <- unnest([1, 2, 3, 4, 5]);
property orid.val int;

datasource orders (orid: ~orid, val: val)
grain (orid) address orders;

merge orid into ~orid_2;
"""


def test_generated_key_is_the_complete_side_of_a_partial_merge():
    keyspace = _keyspace(_GENERATED_DOMAIN, "select orid_2, val;")
    (base,) = keyspace.regions
    assert base.completes == frozenset({"local.orid"})
    assert keyspace.output_demanded_spans == frozenset({"local.orid"})


_BRIDGED = """
key engine_id int;
property engine_id.engine_group string;
key stage_id int;
key vehicle_id int;
key launch_id int;

datasource engines (engine_id: engine_id, engine_group: engine_group)
grain (engine_id) address engines;

datasource stages (stage_id: stage_id, vehicle_id: vehicle_id, engine_id: ~engine_id)
grain (stage_id) address stages;

datasource launches (launch_id: launch_id, vehicle_id: vehicle_id)
grain (launch_id) address launches;
"""


def test_fan_out_bridge_keeps_the_unmatched_members_a_region():
    """No lookup leads from a launch to its engines (a vehicle has many
    stages), so no single source carries both; `stages` is the bridge."""
    keyspace = _keyspace(_BRIDGED, "select engine_group, count(launch_id) as n;")
    (extension,) = keyspace.extensions
    assert extension.present == frozenset({"local.engine_id"})
    assert extension.spans == frozenset({"local.engine_id"})


_PEER_PARTIALS = """
key order_id int;
property order_id.web_id int;
property order_id.store_id int;

datasource web_orders (order_id: ~order_id, web_id: web_id)
grain (order_id) address web_orders;

datasource store_orders (order_id: ~order_id, store_id: store_id)
grain (order_id) address store_orders;
"""


def test_partial_sources_with_no_complete_one_do_not_complete_each_other():
    """Two partial bindings have no defined relationship: the full set of a
    key is a complete source, never the partial ones completing each other."""
    keyspace = _keyspace(_PEER_PARTIALS, "select order_id, web_id, store_id;")
    (base,) = keyspace.regions
    assert base.completes == frozenset()
    assert keyspace.in_play_spans == frozenset()


_ROWSET = _DERIVED + """
rowset delivered <- select customer_id, order_id where delivery_date is not null;
"""


def test_rowset_key_is_its_own_entity():
    """A customer no order references is not a row of the rowset."""
    keyspace = _keyspace(_ROWSET, "select delivered.customer_id, delivered.order_id;")
    assert keyspace.output_demanded_spans == frozenset()


def _plan_keyspaces(monkeypatch, model: str, query: str) -> list[Keyspace]:
    """Every keyspace the statement builds: witnesses, bodies and the plan."""
    capture = _Capture()
    monkeypatch.setattr(rowset_witness, "build_keyspace", capture)
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(model)
    executor.generate_sql(query)
    return capture.seen


def test_rowset_witness_spells_the_body_padding_by_the_handle(monkeypatch):
    """The body of `s` pads for `local.item_sk`; the plan reading `s.sk` names
    that padding by the handle, so a merge above the boundary attributes it to
    the region it holds rather than to a span of another plan."""
    seen = _plan_keyspaces(monkeypatch, UNSOLD_MODEL, ROWSET_QUERY)
    outer = next(k for k in seen if "s.d" in k.outputs)
    assert outer.in_play_spans == frozenset({"s.sk"})
    assert outer.witnessed == {"local.item_sk": "s.sk", "local._s_sk": "s.sk"}


def test_key_no_handle_spells_is_spelled_by_what_carries_it(monkeypatch):
    """`select order_number as o, item_desc as d, quantity as q` exposes no
    item key; the reader identifies the item's rows by `s.d` alone, so the
    region and its padding are spelled by it."""
    seen = _plan_keyspaces(monkeypatch, UNSOLD_MODEL, KEYLESS_CASES[0][0])
    outer = next(k for k in seen if "s.d" in k.outputs)
    assert outer.describe() == "{s.d, s.o} | {s.d} ~['s.d']"
    assert outer.keys_by_address["s.d"] == frozenset({"s.d"})
    assert outer.witnessed == {"local.item_sk": "s.d"}


def test_rowset_over_a_rowset_is_a_region_of_the_reader(monkeypatch):
    """`s` reads `t.sk` as `sk2`, and the alias is the canonical spelling of
    that entity in `s`'s body, so `t`'s witness is read through it; spelled in
    `t`'s handles alone it matched no entity, `s` had one region, and the
    count ran over the padding. The plan above names every spelling below by
    `s.sk2`."""
    seen = _plan_keyspaces(monkeypatch, UNSOLD_MODEL, NESTED_ROWSET_QUERY)
    body = next(k for k in seen if "local._s_o2" in k.outputs)
    assert body.describe() == "{local._s_o2, local._s_sk2} | {local._s_sk2} ~['t.sk']"
    outer = next(k for k in seen if "s.d2" in k.outputs)
    assert outer.in_play_spans == frozenset({"s.sk2"})
    assert outer.witnessed == {
        "t.sk": "s.sk2",
        "local._s_sk2": "s.sk2",
        "local.item_sk": "s.sk2",
        "local._t_sk": "s.sk2",
    }


def test_binding_is_complete_once_the_where_empties_the_rows_it_lacks(monkeypatch):
    dead = _heal_keyspace(
        monkeypatch, _DERIVED, "select customer_id, status where status = 'delivered';"
    )
    assert dead.binding_is_complete("orders", CUSTOMER)


def test_a_healed_binding_leaves_the_plan_no_region(monkeypatch):
    planned = _planned_keyspace(
        monkeypatch, _DERIVED, "select customer_id, status where status = 'delivered';"
    )
    assert planned.extensions == ()
    assert planned.in_play_spans == frozenset()


def test_binding_stays_partial_while_the_rows_it_lacks_are_live(monkeypatch):
    live = _planned_keyspace(
        monkeypatch, _DERIVED, "select customer_id, status where name = 'cat';"
    )
    assert not live.binding_is_complete("orders", CUSTOMER)


def test_binding_out_of_play_is_not_called_complete(monkeypatch):
    keyspace = _planned_keyspace(
        monkeypatch, _DERIVED, "select order_id, status where status = 'delivered';"
    )
    assert CUSTOMER not in keyspace.in_play_spans
    assert not keyspace.binding_is_complete("orders", CUSTOMER)


def test_completion_is_emptied_by_a_column_only_the_partial_source_binds(monkeypatch):
    """No entity is absent on a line with no return, so no region dies; the
    rows `returns` lacks are still gone once `ret_order` must be non-null."""
    pinned = _heal_keyspace(
        monkeypatch,
        _PARTIAL_PROPERTY_SOURCE,
        "select order_id, item_id, ret_order where ret_order is not null;",
    )
    (base,) = pinned.regions
    (completion,) = base.completions
    assert completion.source == "returns"
    assert completion.emptied_by == frozenset({"local.ret_order"})
    assert pinned.binding_is_complete("returns", ORDER)


def test_completion_stays_live_under_a_column_both_sources_reach(monkeypatch):
    unpinned = _heal_keyspace(
        monkeypatch,
        _PARTIAL_PROPERTY_SOURCE,
        "select order_id, item_id, ret_order where qty > 1;",
    )
    assert not unpinned.binding_is_complete("returns", ORDER)


def test_pin_heal_reads_a_derived_null_rejection(monkeypatch):
    """`status` is derived and absent on an orderless customer: a WHERE that
    rejects NULL `status` empties the customer region, and orders' `~` on the
    customer key is complete for the statement."""
    keyspace = _heal_keyspace(
        monkeypatch,
        _DERIVED,
        "select customer_id, status where status = 'delivered' and name = 'cat';",
    )
    assert keyspace.binding_is_complete("orders", CUSTOMER)


def test_partial_sources_with_no_complete_one_never_heal():
    """With no complete source the key is not in play: nothing says which
    members either partial source lacks, so neither binding is complete."""
    pinned = _keyspace(
        _PEER_PARTIALS, "select order_id, web_id where web_id is not null;"
    )
    assert not pinned.binding_is_complete("web_orders", ORDER)
    assert not pinned.binding_is_complete("store_orders", ORDER)


def _domains(info) -> dict[str, frozenset[str]]:
    return {
        gid: attrs.extent_spans
        for gid, attrs in info.group_attrs.items()
        if attrs.extent_spans
    }


def test_basic_over_an_aggregate_is_keyed_on_what_it_reads():
    keyspace = _keyspace(
        _DERIVED, "select customer_id, coalesce(sum(amount), 0) as total;"
    )
    (extension,) = keyspace.extensions
    assert keyspace.defined_on("local.total", extension)


def test_extension_row_carries_what_its_span_reaches():
    keyspace = _keyspace(_DERIVED, "select customer_id, name, status;")
    (extension,) = keyspace.extensions
    assert keyspace.carried_on("local.name", extension)
    assert not keyspace.carried_on("local.status", extension)
    assert keyspace.region_of(extension.spans) is extension


def test_region_with_an_absent_derivation_gets_a_domain():
    info, _ = _plan(_DERIVED, "select customer_id, status;")
    assert set(_domains(info).values()) == {frozenset({CUSTOMER})}


def test_demanded_region_gets_a_domain_whatever_the_outputs():
    """The region's rows come from its domain even when nothing absent would
    take a value on a padded row: one ownership mechanism, not two."""
    info, _ = _plan(_DERIVED, "select customer_id, amount;")
    assert set(_domains(info).values()) == {frozenset({CUSTOMER})}


def test_unnamed_span_rides_the_domain_as_a_hidden_member():
    info, _ = _plan(_DERIVED, "select name, status;")
    ((gid, spans),) = _domains(info).items()
    assert spans == frozenset({CUSTOMER})
    assert CUSTOMER in info.group_attrs[gid].secondary_members
    assert CUSTOMER in info.group_attrs[gid].output_concepts


def test_each_extension_family_gets_its_own_domain():
    info, _ = _plan(
        FIELD_REPORT,
        "auto big <- case when price > 1 then 'big' else 'small' end;"
        " select order_id, item_id, user_id, product_id, big;",
    )
    assert len(_domains(info)) == len(info.keyspace.extensions) == 2


def test_aggregate_over_a_region_reads_its_domain():
    info, _ = _plan(_DERIVED, "select status, count(customer_id) as customers;")
    ((domain, _),) = _domains(info).items()
    readers = {
        info.group_attrs[gid].derivation.value
        for gid in info.group_graph.successors(domain)
        if gid != FINAL_NODE_ID
    }
    assert "aggregate" in readers


def _planned_info(monkeypatch, model: str, query: str):
    """The statement's plan through the full path, WHERE included."""
    from trilogy.core import query_processor

    seen = []
    original = query_processor.search_concepts_v4

    def capture(*args, **kwargs):
        seen.append(original(*args, **kwargs))
        return seen[-1]

    monkeypatch.setattr(query_processor, "search_concepts_v4", capture)
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(model)
    executor.generate_sql(query)
    return seen[0]


def test_where_over_a_carried_scalar_keeps_the_domain(monkeypatch):
    """`activity` is keyed on the span, so the region's rows hold it: the
    condition branch that computes it reads the domain, and the atom is
    hosted at FINAL over the united rows."""
    info = _planned_info(
        monkeypatch,
        _DERIVED + _ACTIVITY,
        "select customer_id, status where activity = 'dormant';",
    )
    ((domain, spans),) = _domains(info).items()
    assert spans == frozenset({CUSTOMER})
    readers = {
        gid
        for gid in info.group_graph.successors(domain)
        if "local.activity" in info.group_attrs[gid].primary_members
    }
    assert readers
    assert [str(a) for a in info.group_attrs[FINAL_NODE_ID].condition_atoms] == [
        "local.activity = dormant"
    ]
    assert not any(
        info.group_attrs[gid].condition_atoms
        for gid in info.group_graph.nodes
        if gid != FINAL_NODE_ID
    )


def test_where_over_an_absent_null_rejecting_value_empties_the_region(monkeypatch):
    """Healed before the plan: the region is gone, not merely empty."""
    info = _planned_info(
        monkeypatch, _DERIVED, "select customer_id, status where status = 'delivered';"
    )
    assert not _domains(info)
    assert info.keyspace.extensions == ()


# What the heal audit (`keyspace_audit.py`) established: the keyspace over the
# bindings as authored and the one over the rewritten bindings answer the same
# reader-visible questions, with heal a STATEMENT fact every plan inherits.


def test_emptied_completion_demands_nothing(monkeypatch):
    """Authored view: `returns` completes the base region but every row it
    holds is gone, so it is in play (a merge below the WHERE still sees its
    padding) and demanded by no output."""
    keyspace = _heal_keyspace(
        monkeypatch,
        _PARTIAL_PROPERTY_SOURCE,
        "select order_id, item_id, ret_order where ret_order is not null;",
    )
    (base,) = keyspace.regions
    assert base.completes == frozenset({ORDER, ITEM})
    assert base.live_completes == frozenset()
    assert keyspace.in_play_spans == frozenset({ORDER, ITEM})
    assert keyspace.output_demanded_spans == frozenset()


def test_entity_is_spelled_the_same_with_and_without_a_license(monkeypatch):
    """`customer_id as c2` earlier in the session makes `c2` the canonical
    spelling. Healing the last `~` must not change that: the plan keyspace
    (no license left) and heal's (as authored) key `late_name` alike."""
    healed, planned = _Capture(), _Capture()
    monkeypatch.setattr(partial_bridging, "build_keyspace", healed)
    monkeypatch.setattr(rowset_witness, "build_keyspace", planned)
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(_DERIVED + _ACTIVITY)
    executor.generate_sql("select customer_id as c2, status;")
    healed.seen.clear()
    planned.seen.clear()
    executor.generate_sql("select late_name;")
    assert planned.seen[0].extensions == ()
    spelled = healed.seen[0].keys_by_address["local.late_name"]
    assert spelled == frozenset({"local.c2"})
    assert planned.seen[0].keys_by_address["local.late_name"] == spelled


def test_sub_plan_without_the_where_inherits_the_statement_heal(monkeypatch):
    """The window feeder plans `order_seq` with no WHERE of its own. Were it
    to read the authored bindings it would pad the orderless customer and
    number the padding row: `order_seq = 1` for a customer with no order.
    Heal is decided once per statement and every plan under it is complete."""
    capture = _Capture()
    monkeypatch.setattr(rowset_witness, "build_keyspace", capture)
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(_DERIVED)
    executor.generate_sql("select customer_id, name where order_seq = 1;")
    feeders = [k for k in capture.seen if "local.order_seq" in k.keys_by_address]
    assert feeders
    assert all(k.demanded_spans == frozenset() for k in capture.seen)
