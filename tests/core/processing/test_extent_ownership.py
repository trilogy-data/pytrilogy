"""Graph-time election of `~` extension-span owners (docs/extent_ownership.md).

These assert the DECISION, not a rendered shape: which group is licensed to
manufacture a span's extension rows, and that every other group is told not to.
The row-level consequences live in tests/engine/test_duckdb_partial_key_assembly
and tests/engine/test_duckdb_partial_fk_field_report.
"""

from tests.engine.test_duckdb_partial_fk_field_report import MODEL
from tests.helpers.models import TWO_FAMILIES
from tests.helpers.planning import plan
from trilogy.core.processing.v4_helper.constants import FINAL_NODE_ID


def _ownership(model: str, query: str):
    info, _ = plan(model, query)
    return info, info.group_attrs[FINAL_NODE_ID].extent_ownership


def test_every_span_is_owned_by_its_region_domain():
    """Each demanded region has a domain group of its own, and that domain is
    the span's owner; every other group is told not to manufacture either
    family, each domain not to manufacture the other's."""
    info, ownership = _ownership(
        MODEL,
        "select order_id, item_id, user_id, product_id, total_revenue,"
        " total_quantity, total_cost;",
    )
    assert ownership is not None
    assert ownership.spans == frozenset({"local.user_id", "local.product_id"})
    domains = {
        gid: attrs.extent_spans
        for gid, attrs in info.group_attrs.items()
        if attrs.extent_spans
    }
    assert len(domains) == 2
    for span, owner in ownership.owner_by_span.items():
        assert domains[owner] == frozenset({span})
        assert span in info.group_attrs[owner].primary_members
        assert ownership.suppressed_for(owner) == ownership.spans - {span}

    others = [
        gid for gid in info.group_attrs if gid not in domains and gid != FINAL_NODE_ID
    ]
    assert others
    for gid in others:
        assert ownership.suppressed_for(gid) == ownership.spans


def test_dimension_attribute_demands_its_key_span():
    """A dimension attribute in the output demands its key's extension rows,
    even though the key itself is never projected. Reading demand off the merge
    grain instead would sweep in join axes nobody asks extension rows of."""
    info, _ = plan(TWO_FAMILIES, "select state, brand;")
    assert info.keyspace.output_demanded_spans == frozenset(
        {"local.user_id", "local.product_id"}
    )


def test_span_nobody_projects_is_not_demanded():
    info, _ = plan(TWO_FAMILIES, "select order_id, total_qty;")
    assert info.keyspace.output_demanded_spans == frozenset()
    ownership = info.group_attrs[FINAL_NODE_ID].extent_ownership
    assert ownership is not None
    assert ownership.spans == frozenset()


def test_span_reached_only_through_joins_is_owned_by_its_domain():
    """`select state, brand` names neither key, so no bucket exposes one; the
    region domains carry the keys as hidden members and own the spans."""
    info, ownership = _ownership(TWO_FAMILIES, "select state, brand;")
    assert ownership is not None
    assert ownership.spans == frozenset({"local.user_id", "local.product_id"})
    for span, owner in ownership.owner_by_span.items():
        assert info.group_attrs[owner].extent_spans == frozenset({span})
        assert span in info.group_attrs[owner].carried_keys
