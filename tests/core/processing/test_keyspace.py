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
from trilogy import Dialects
from trilogy.core.processing import concept_strategies_v4
from trilogy.core.processing.v4_helper.keyspace import build_keyspace
from trilogy.core.processing.v4_helper.models import Keyspace

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
    monkeypatch.setattr(concept_strategies_v4, "build_keyspace", capture)
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
    keyspace = _planned_keyspace(
        monkeypatch, _DERIVED, "select customer_id, status where status = 'delivered';"
    )
    (extension,) = keyspace.extensions
    assert extension.emptied_by == frozenset({"local.status"})
    assert keyspace.demanded_spans == frozenset()


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
