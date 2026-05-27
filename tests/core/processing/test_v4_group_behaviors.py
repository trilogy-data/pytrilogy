"""Unit tests for per-derivation group IO behaviors.

These run on synthetic nx graphs and GroupBucket fixtures — no
BuildEnvironment, no SQL. The point is to pin the per-derivation
contracts so we can reason about each rule in isolation.
"""

import networkx as nx
import pytest

from trilogy.core.enums import Derivation
from trilogy.core.processing.v4_helper.group_behaviors import (
    GROUP_BEHAVIORS,
    behavior_for,
    can_preserve_grain_subset,
    can_preserve_root,
    native_grain_basic_inherited,
    native_grain_declared,
    native_grain_root,
)
from trilogy.core.processing.v4_helper.models import GroupBucket


def _cg(nodes: dict[str, dict]) -> nx.DiGraph:
    """Build a tiny concept graph from {address: {'grain': set, 'derivation': ..., 'parents': [..]}}."""
    g: nx.DiGraph = nx.DiGraph()
    for addr, spec in nodes.items():
        g.add_node(
            addr,
            grain_components=frozenset(spec.get("grain", ())),
            derivation=spec.get("derivation", Derivation.ROOT.value),
            depth_label=spec.get("depth_label", "d*"),
        )
    for addr, spec in nodes.items():
        for parent in spec.get("parents", ()):
            g.add_edge(parent, addr, kind="lineage")
    return g


def _bucket(
    derivation: str,
    primaries: list[str],
    grain: set[str] | None = None,
) -> GroupBucket:
    return GroupBucket(
        depth_label="d*",
        derivation=derivation,
        grain_components=frozenset(grain or ()),
        primary_members=list(primaries),
    )


# ----- can_preserve_grain_subset --------------------------------------


class TestCanPreserveGrainSubset:
    def test_address_is_a_grain_key(self):
        cg = _cg({"week_seq": {"grain": {"date.id"}}})
        assert (
            can_preserve_grain_subset(cg, frozenset({"week_seq"}), "week_seq") is True
        )

    def test_grain_strict_subset(self):
        cg = _cg(
            {
                "text_id": {"grain": {"customer.id"}},
                "customer.id": {"grain": {"customer.id"}},
            }
        )
        assert (
            can_preserve_grain_subset(cg, frozenset({"customer.id"}), "text_id") is True
        )

    def test_grain_not_a_subset(self):
        cg = _cg({"item.id": {"grain": {"item.id"}}})
        assert (
            can_preserve_grain_subset(cg, frozenset({"week_seq"}), "item.id") is False
        )

    def test_empty_column_grain_always_passes(self):
        # Constants / scalars have empty grain — always rideable.
        cg = _cg({"the_year": {"grain": set()}})
        assert (
            can_preserve_grain_subset(cg, frozenset({"week_seq"}), "the_year") is True
        )

    def test_unknown_address_blocked(self):
        cg = _cg({})
        # Not in graph -> can't reason about it -> conservative reject.
        assert can_preserve_grain_subset(cg, frozenset(), "phantom") is False

    def test_q02_basic_carries_week_seq(self):
        """Regression: q02's BASIC at inherited grain {week_seq} must
        let `sales.date.week_seq` ride through. The compiler reports
        week_seq.grain = {date.id}, which is NOT a subset of {week_seq};
        the address-in-grain branch is what unblocks it."""
        cg = _cg({"sales.date.week_seq": {"grain": {"sales.date.id"}}})
        assert (
            can_preserve_grain_subset(
                cg, frozenset({"sales.date.week_seq"}), "sales.date.week_seq"
            )
            is True
        )

    def test_q04_basic_blocks_row_grain_leak(self):
        """Regression: q04's BASIC at {customer.id} must NOT carry
        through row-grain roots like date.year. Without this block,
        the BASIC's implicit GROUP-BY inflates to per-row."""
        cg = _cg({"date.year": {"grain": {"date.id"}}})
        assert (
            can_preserve_grain_subset(cg, frozenset({"customer.id"}), "date.year")
            is False
        )


# ----- can_preserve_root ----------------------------------------------


def test_can_preserve_root_always_false():
    """ROOT has no upstream to preserve from. The orchestrator skips
    asking, but the function should still be safe to call."""
    cg = _cg({"anything": {"grain": set()}})
    assert can_preserve_root(cg, frozenset(), "anything") is False


# ----- native_grain_root ----------------------------------------------


def test_native_grain_root_is_empty():
    cg = _cg({})
    bucket = _bucket(Derivation.ROOT.value, primaries=["x", "y"], grain={"x"})
    assert native_grain_root(bucket, cg) == frozenset()


# ----- native_grain_declared ------------------------------------------


def test_native_grain_declared_uses_bucket_grain():
    cg = _cg({})
    bucket = _bucket(
        Derivation.AGGREGATE.value,
        primaries=["sum_x"],
        grain={"week_seq"},
    )
    assert native_grain_declared(bucket, cg) == frozenset({"week_seq"})


def test_native_grain_declared_empty_when_no_grain():
    cg = _cg({})
    bucket = _bucket(Derivation.AGGREGATE.value, primaries=["count_x"])
    assert native_grain_declared(bucket, cg) == frozenset()


# ----- native_grain_basic_inherited -----------------------------------


class TestNativeGrainBasicInherited:
    def test_inherits_from_aggregate_parents(self):
        """q02 shape: BASIC primary's lineage parents are aggregates at
        {week_seq}. Inherited grain should be {week_seq}, not the
        compiler-reported source row grain on the BASIC itself."""
        cg = _cg(
            {
                "agg_sum": {"grain": {"week_seq"}},
                "round_result": {"grain": {"item.id", "order_id"}, "parents": ["agg_sum"]},
            }
        )
        bucket = _bucket(
            Derivation.BASIC.value,
            primaries=["round_result"],
            grain={"item.id", "order_id"},  # the misleading source grain
        )
        assert native_grain_basic_inherited(bucket, cg) == frozenset({"week_seq"})

    def test_falls_back_to_declared_when_no_lineage(self):
        cg = _cg({"constant_one": {"grain": set()}})
        bucket = _bucket(
            Derivation.BASIC.value,
            primaries=["constant_one"],
            grain={"customer.id"},
        )
        # constant_one has no lineage parents -> use declared
        assert native_grain_basic_inherited(bucket, cg) == frozenset({"customer.id"})

    def test_q04_basic_inherits_customer_grain(self):
        """q04 shape: BASIC primaries rename root concepts at {customer.id}.
        Inherited grain matches declared grain — both should land on
        {customer.id}."""
        cg = _cg(
            {
                "text_id": {"grain": {"customer.id"}},
                "customer_id_local": {"grain": {"customer.id"}, "parents": ["text_id"]},
            }
        )
        bucket = _bucket(
            Derivation.BASIC.value,
            primaries=["customer_id_local"],
            grain={"customer.id"},
        )
        assert native_grain_basic_inherited(bucket, cg) == frozenset({"customer.id"})

    def test_unions_grains_across_multiple_primaries(self):
        cg = _cg(
            {
                "agg_by_week": {"grain": {"week_seq"}},
                "agg_by_item": {"grain": {"item.id"}},
                "combo": {"grain": set(), "parents": ["agg_by_week", "agg_by_item"]},
            }
        )
        bucket = _bucket(Derivation.BASIC.value, primaries=["combo"])
        assert native_grain_basic_inherited(bucket, cg) == frozenset(
            {"week_seq", "item.id"}
        )


# ----- registry --------------------------------------------------------


@pytest.mark.parametrize(
    "derivation",
    [
        Derivation.ROOT.value,
        Derivation.BASIC.value,
        Derivation.AGGREGATE.value,
        Derivation.GROUP_TO.value,
        Derivation.WINDOW.value,
        Derivation.FILTER.value,
        Derivation.SUBSELECT.value,
    ],
)
def test_registry_covers_known_derivation(derivation: str):
    assert derivation in GROUP_BEHAVIORS
    beh = GROUP_BEHAVIORS[derivation]
    assert beh.derivation == derivation


def test_behavior_for_unknown_returns_default():
    """Unknown derivations get the conservative default — declared grain
    + subset preservation. The check is that it doesn't crash and that
    it produces a usable Behavior."""
    beh = behavior_for("__not_a_real_derivation__")
    assert beh is not None
    assert beh.native_grain is native_grain_declared
    assert beh.can_preserve is can_preserve_grain_subset


# ----- graph label / sub-graph partitioning ---------------------------


def test_node_id_default_label_is_bare_address():
    """Default-label keys stay as bare addresses so existing single-label
    consumers (everything pre-rowset) keep working without changes."""
    from trilogy.core.processing.v4_helper.concept_graph import node_id

    assert node_id("", "local.sales") == "local.sales"


def test_node_id_labeled_prefix():
    """Labeled keys carry the sub-graph name as a bracketed prefix so an
    inner copy of a concept can't collide with the outer copy."""
    from trilogy.core.processing.v4_helper.concept_graph import node_id

    assert node_id("q5_results", "local.channel_label") == (
        "[q5_results]local.channel_label"
    )


def test_partition_basics_does_not_merge_across_labels():
    """Two BASICs with identical grain but different labels must NOT
    end up in the same bucket — that's the regression that produced the
    q05 cycle (outer renames and rowset-internal derives at compatible
    grain collapsing through the rowset boundary)."""
    from trilogy.core.processing.v4_helper.group_rules import (
        partition_basics_by_subset_grain,
    )

    items = [
        (
            "local.sales",
            {
                "derivation": Derivation.BASIC.value,
                "depth_label": "d*",
                "grain_components": frozenset(),
                "label": "",
            },
        ),
        (
            "[q5_results]local.sales_metric",
            {
                "derivation": Derivation.BASIC.value,
                "depth_label": "d*",
                "grain_components": frozenset(),
                "label": "q5_results",
            },
        ),
    ]
    buckets = partition_basics_by_subset_grain(items)
    assert len(buckets) == 2
    labels = sorted(b.label for b in buckets)
    assert labels == ["", "q5_results"]


def test_partition_basics_does_merge_within_label():
    """The label split shouldn't break the within-label subset-merge
    behavior: two compatible-grain BASICs at the same label still collapse
    to a single bucket (the q04 case)."""
    from trilogy.core.processing.v4_helper.group_rules import (
        partition_basics_by_subset_grain,
    )

    items = [
        (
            "local.customer_id",
            {
                "derivation": Derivation.BASIC.value,
                "depth_label": "d*",
                "grain_components": frozenset({"customer.id"}),
                "label": "",
            },
        ),
        (
            "local.customer_first_name",
            {
                "derivation": Derivation.BASIC.value,
                "depth_label": "d*",
                "grain_components": frozenset({"customer.id"}),
                "label": "",
            },
        ),
    ]
    buckets = partition_basics_by_subset_grain(items)
    assert len(buckets) == 1
    assert {m for m in buckets[0].primary_members} == {
        "local.customer_id",
        "local.customer_first_name",
    }


def test_partition_roots_buckets_per_label():
    """Each sub-graph (outer + each rowset) gets its own ROOT bucket
    because their scans are independent."""
    from trilogy.core.processing.v4_helper.group_rules import partition_roots

    items = [
        (
            "sales.item.id",
            {
                "derivation": Derivation.ROOT.value,
                "depth_label": "root",
                "label": "",
            },
        ),
        (
            "[q5_results]sales.item.id",
            {
                "derivation": Derivation.ROOT.value,
                "depth_label": "root",
                "label": "q5_results",
            },
        ),
    ]
    buckets = partition_roots(items)
    assert len(buckets) == 2
    assert {b.label for b in buckets} == {"", "q5_results"}
