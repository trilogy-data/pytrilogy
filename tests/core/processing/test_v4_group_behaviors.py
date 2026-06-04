"""Unit tests for per-derivation group IO behaviors.

These run on synthetic nx graphs and GroupBucket fixtures — no
BuildEnvironment, no SQL. The point is to pin the per-derivation
contracts so we can reason about each rule in isolation.

Node state lives in a side dict (``dict[str, ConceptAttrs]``) keyed by
concept-graph node id, mirroring production; the graph itself carries only
topology + lineage edges. ``_cg`` builds both from a compact spec.
"""

import networkx as nx
import pytest

from trilogy.core.enums import Derivation
from trilogy.core.processing.v4_helper.group_behaviors import (
    GROUP_BEHAVIORS,
    _lineage_parent_addrs,
    behavior_for,
    can_preserve_grain_subset,
    can_preserve_grouping,
    can_preserve_root,
    native_grain_basic_inherited,
    native_grain_declared,
    native_grain_root,
)
from trilogy.core.processing.v4_helper.group_graph import (
    _virtual_filter_scoped_columns,
)
from trilogy.core.processing.v4_helper.models import (
    ConceptAttrs,
    GroupAttrs,
    GroupBucket,
)


def _attrs(address: str, spec: dict) -> ConceptAttrs:
    return ConceptAttrs(
        address=address,
        label=spec.get("label", ""),
        derivation=spec.get("derivation", Derivation.ROOT.value),
        purpose=spec.get("purpose", ""),
        granularity=spec.get("granularity", ""),
        depth_label=spec.get("depth_label", "d*"),
        grain_components=frozenset(spec.get("grain", ())),
        grouping_mode=spec.get("grouping_mode"),
        rowset_name=spec.get("rowset_name"),
        aggregate_input_grain=frozenset(spec.get("aggregate_input_grain", ())),
        existence_only=spec.get("existence_only", False),
    )


def _cg(nodes: dict[str, dict]) -> tuple[nx.DiGraph, dict[str, ConceptAttrs]]:
    """Build a tiny concept graph + typed attrs side dict from
    {address: {'grain': set, 'derivation': ..., 'parents': [..]}}. Node id ==
    address here (blank/bare labels), matching how production keys non-labeled
    nodes."""
    g: nx.DiGraph = nx.DiGraph()
    attrs: dict[str, ConceptAttrs] = {}
    for addr, spec in nodes.items():
        g.add_node(addr)
        attrs[addr] = _attrs(addr, spec)
    for addr, spec in nodes.items():
        for parent in spec.get("parents", ()):
            g.add_edge(parent, addr, kind="lineage")
    return g, attrs


def _item(address: str, **spec) -> tuple[str, ConceptAttrs]:
    """A `(node_id, ConceptAttrs)` pair as the grouping rules consume them."""
    return address, _attrs(address, spec)


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


def _noop_ensure(derivation: str) -> None:
    pass


# ----- can_preserve_grain_subset --------------------------------------


class TestCanPreserveGrainSubset:
    def test_address_is_a_grain_key(self):
        cg, ca = _cg({"week_seq": {"grain": {"date.id"}}})
        assert (
            can_preserve_grain_subset(cg, ca, frozenset({"week_seq"}), "week_seq")
            is True
        )

    def test_grain_strict_subset(self):
        cg, ca = _cg(
            {
                "text_id": {"grain": {"customer.id"}},
                "customer.id": {"grain": {"customer.id"}},
            }
        )
        assert (
            can_preserve_grain_subset(cg, ca, frozenset({"customer.id"}), "text_id")
            is True
        )

    def test_grain_not_a_subset(self):
        cg, ca = _cg({"item.id": {"grain": {"item.id"}}})
        assert (
            can_preserve_grain_subset(cg, ca, frozenset({"week_seq"}), "item.id")
            is False
        )

    def test_empty_column_grain_always_passes(self):
        # Constants / scalars have empty grain — always rideable.
        cg, ca = _cg({"the_year": {"grain": set()}})
        assert (
            can_preserve_grain_subset(cg, ca, frozenset({"week_seq"}), "the_year")
            is True
        )

    def test_unknown_address_blocked(self):
        cg, ca = _cg({})
        # Not in attrs -> can't reason about it -> conservative reject.
        assert can_preserve_grain_subset(cg, ca, frozenset(), "phantom") is False

    def test_q02_basic_carries_week_seq(self):
        """Regression: q02's BASIC at inherited grain {week_seq} must
        let `sales.date.week_seq` ride through. The compiler reports
        week_seq.grain = {date.id}, which is NOT a subset of {week_seq};
        the address-in-grain branch is what unblocks it."""
        cg, ca = _cg({"sales.date.week_seq": {"grain": {"sales.date.id"}}})
        assert (
            can_preserve_grain_subset(
                cg, ca, frozenset({"sales.date.week_seq"}), "sales.date.week_seq"
            )
            is True
        )

    def test_q04_basic_blocks_row_grain_leak(self):
        """Regression: q04's BASIC at {customer.id} must NOT carry
        through row-grain roots like date.year. Without this block,
        the BASIC's implicit GROUP-BY inflates to per-row."""
        cg, ca = _cg({"date.year": {"grain": {"date.id"}}})
        assert (
            can_preserve_grain_subset(cg, ca, frozenset({"customer.id"}), "date.year")
            is False
        )


# ----- _lineage_parent_addrs ------------------------------------------


class TestLineageParentAddrs:
    def test_unknown_address_is_empty(self):
        cg, ca = _cg({})
        assert _lineage_parent_addrs(cg, ca, "phantom") == set()

    def test_collects_only_lineage_parents(self):
        cg, ca = _cg(
            {
                "channel_label": {"grain": {"channel.id"}},
                "s_channel": {
                    "grain": {"channel.id"},
                    "parents": ["channel_label"],
                },
            }
        )
        assert _lineage_parent_addrs(cg, ca, "s_channel") == {"channel_label"}


# ----- can_preserve_grouping ------------------------------------------


class TestCanPreserveGrouping:
    def test_address_is_a_group_key(self):
        cg, ca = _cg({"channel.id": {"grain": {"channel.id"}}})
        assert (
            can_preserve_grouping(cg, ca, frozenset({"channel.id"}), "channel.id")
            is True
        )

    def test_unknown_address_blocked(self):
        cg, ca = _cg({})
        assert can_preserve_grouping(cg, ca, frozenset(), "phantom") is False

    def test_grain_subset_rides_through(self):
        cg, ca = _cg({"text_id": {"grain": {"customer.id"}}})
        assert (
            can_preserve_grouping(cg, ca, frozenset({"customer.id"}), "text_id") is True
        )

    def test_rename_of_grain_keys_rides_through(self):
        """q05: `s_channel` is a BASIC rename of the group key
        `channel_label`. Its own grain isn't a subset of the GROUP BY, but
        every lineage parent IS a group key, so it rides through."""
        cg, ca = _cg(
            {
                "channel_label": {"grain": {"channel.id"}},
                "s_channel": {
                    "grain": {"date.id"},  # misleading non-subset grain
                    "parents": ["channel_label"],
                    "derivation": Derivation.BASIC.value,
                },
            }
        )
        assert (
            can_preserve_grouping(cg, ca, frozenset({"channel_label"}), "s_channel")
            is True
        )

    def test_constant_empty_grain_rides_through(self):
        cg, ca = _cg(
            {"the_year": {"grain": set(), "derivation": Derivation.CONSTANT.value}}
        )
        assert (
            can_preserve_grouping(cg, ca, frozenset({"week_seq"}), "the_year") is True
        )

    def test_row_varying_empty_grain_blocked(self):
        """A row-varying empty-grain column (a CASE that isn't a key rename)
        would land in the SELECT with no GROUP BY entry — invalid SQL."""
        cg, ca = _cg({"flag": {"grain": set(), "derivation": Derivation.BASIC.value}})
        assert can_preserve_grouping(cg, ca, frozenset({"week_seq"}), "flag") is False

    def test_non_subset_grain_with_non_key_parents_blocked(self):
        cg, ca = _cg(
            {
                "row_thing": {"grain": {"item.id"}},
                "derived": {
                    "grain": {"item.id"},
                    "parents": ["row_thing"],
                    "derivation": Derivation.BASIC.value,
                },
            }
        )
        assert (
            can_preserve_grouping(cg, ca, frozenset({"week_seq"}), "derived") is False
        )


# ----- can_preserve_root ----------------------------------------------


def test_can_preserve_root_always_false():
    """ROOT has no upstream to preserve from. The orchestrator skips
    asking, but the function should still be safe to call."""
    cg, ca = _cg({"anything": {"grain": set()}})
    assert can_preserve_root(cg, ca, frozenset(), "anything") is False


# ----- native_grain_root ----------------------------------------------


def test_native_grain_root_is_empty():
    cg, ca = _cg({})
    bucket = _bucket(Derivation.ROOT.value, primaries=["x", "y"], grain={"x"})
    assert native_grain_root(bucket, cg, ca) == frozenset()


# ----- native_grain_declared ------------------------------------------


def test_native_grain_declared_uses_bucket_grain():
    cg, ca = _cg({})
    bucket = _bucket(
        Derivation.AGGREGATE.value,
        primaries=["sum_x"],
        grain={"week_seq"},
    )
    assert native_grain_declared(bucket, cg, ca) == frozenset({"week_seq"})


def test_native_grain_declared_empty_when_no_grain():
    cg, ca = _cg({})
    bucket = _bucket(Derivation.AGGREGATE.value, primaries=["count_x"])
    assert native_grain_declared(bucket, cg, ca) == frozenset()


# ----- native_grain_basic_inherited -----------------------------------


class TestNativeGrainBasicInherited:
    def test_inherits_from_aggregate_parents(self):
        """q02 shape: BASIC primary's lineage parents are aggregates at
        {week_seq}. Inherited grain should be {week_seq}, not the
        compiler-reported source row grain on the BASIC itself."""
        cg, ca = _cg(
            {
                "agg_sum": {"grain": {"week_seq"}},
                "round_result": {
                    "grain": {"item.id", "order_id"},
                    "parents": ["agg_sum"],
                },
            }
        )
        bucket = _bucket(
            Derivation.BASIC.value,
            primaries=["round_result"],
            grain={"item.id", "order_id"},  # the misleading source grain
        )
        assert native_grain_basic_inherited(bucket, cg, ca) == frozenset({"week_seq"})

    def test_falls_back_to_declared_when_no_lineage(self):
        cg, ca = _cg({"constant_one": {"grain": set()}})
        bucket = _bucket(
            Derivation.BASIC.value,
            primaries=["constant_one"],
            grain={"customer.id"},
        )
        # constant_one has no lineage parents -> use declared
        assert native_grain_basic_inherited(bucket, cg, ca) == frozenset(
            {"customer.id"}
        )

    def test_q04_basic_inherits_customer_grain(self):
        """q04 shape: BASIC primaries rename root concepts at {customer.id}.
        Inherited grain matches declared grain — both should land on
        {customer.id}."""
        cg, ca = _cg(
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
        assert native_grain_basic_inherited(bucket, cg, ca) == frozenset(
            {"customer.id"}
        )

    def test_unions_grains_across_multiple_primaries(self):
        cg, ca = _cg(
            {
                "agg_by_week": {"grain": {"week_seq"}},
                "agg_by_item": {"grain": {"item.id"}},
                "combo": {"grain": set(), "parents": ["agg_by_week", "agg_by_item"]},
            }
        )
        bucket = _bucket(Derivation.BASIC.value, primaries=["combo"])
        assert native_grain_basic_inherited(bucket, cg, ca) == frozenset(
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
        partition_basics_by_signature,
    )

    items = [
        _item("local.sales", derivation=Derivation.BASIC.value, label=""),
        _item(
            "[q5_results]local.sales_metric",
            derivation=Derivation.BASIC.value,
            label="q5_results",
        ),
    ]
    cg, ca = _cg(
        {
            "local.sales": {"derivation": Derivation.BASIC.value},
            "[q5_results]local.sales_metric": {
                "derivation": Derivation.BASIC.value,
                "label": "q5_results",
            },
        }
    )
    buckets = partition_basics_by_signature(items, cg, ca, {}, _noop_ensure)
    assert len(buckets) == 2
    labels = sorted(b.label for b in buckets)
    assert labels == ["", "q5_results"]


def test_partition_basics_does_merge_within_label():
    """The label split shouldn't break the within-label subset-merge
    behavior: two compatible-grain BASICs at the same label still collapse
    to a single bucket (the q04 case)."""
    from trilogy.core.processing.v4_helper.group_rules import (
        partition_basics_by_signature,
    )

    items = [
        _item(
            "local.customer_id",
            derivation=Derivation.BASIC.value,
            grain={"customer.id"},
        ),
        _item(
            "local.customer_first_name",
            derivation=Derivation.BASIC.value,
            grain={"customer.id"},
        ),
    ]
    cg, ca = _cg(
        {
            "local.customer_id": {"derivation": Derivation.BASIC.value},
            "local.customer_first_name": {"derivation": Derivation.BASIC.value},
        }
    )
    buckets = partition_basics_by_signature(items, cg, ca, {}, _noop_ensure)
    assert len(buckets) == 1
    assert {m for m in buckets[0].primary_members} == {
        "local.customer_id",
        "local.customer_first_name",
    }


def test_partition_aggregates_uses_input_grain():
    from trilogy.core.processing.v4_helper.group_rules import partition_aggregates

    items = [
        _item(
            "local.numcust",
            derivation=Derivation.AGGREGATE.value,
            grain={"local.cntrycode"},
            aggregate_input_grain={"local.cntrycode", "local.id"},
        ),
        _item(
            "local.totacctbal",
            derivation=Derivation.AGGREGATE.value,
            grain={"local.cntrycode"},
            aggregate_input_grain={"local.cntrycode", "local.id"},
        ),
        _item(
            "local.line_total",
            derivation=Derivation.AGGREGATE.value,
            grain={"local.cntrycode"},
            aggregate_input_grain={"local.cntrycode", "line.id"},
        ),
    ]
    cg, ca = _cg({node: {} for node, _ in items})
    buckets = partition_aggregates(items, cg, ca, {}, _noop_ensure)

    merged = [
        bucket
        for bucket in buckets
        if set(bucket.primary_members) == {"local.numcust", "local.totacctbal"}
    ]
    assert len(buckets) == 2
    assert len(merged) == 1


def test_partition_roots_buckets_per_label():
    """Each sub-graph (outer + each rowset) gets its own ROOT bucket
    because their scans are independent."""
    from trilogy.core.processing.v4_helper.group_rules import partition_roots

    items = [
        _item("sales.item.id", derivation=Derivation.ROOT.value, depth_label="root"),
        _item(
            "[q5_results]sales.item.id",
            derivation=Derivation.ROOT.value,
            depth_label="root",
            label="q5_results",
        ),
    ]
    cg, ca = _cg({"sales.item.id": {}, "[q5_results]sales.item.id": {}})
    buckets = partition_roots(items, cg, ca, {}, _noop_ensure)
    assert len(buckets) == 2
    assert {b.label for b in buckets} == {"", "q5_results"}


def test_virtual_filter_scoped_columns_collects_condition_args():
    # root_d1 feeds a FILTER group whose virt-filter output derives from the
    # `?` condition's columns (q21's `count(supplier ? receipt > commit)`).
    cg, ca = _cg(
        {
            "local.supplier_id": {},
            "local.receipt": {},
            "local.commit": {},
            "local._virt_filter": {
                "parents": ["local.supplier_id", "local.receipt", "local.commit"]
            },
        }
    )
    gg: nx.DiGraph = nx.DiGraph()
    gg.add_edge("root_d1", "filt", kind="lineage")
    attrs = {
        "root_d1": GroupAttrs(
            depth_label="root_d1",
            derivation=Derivation.ROOT.value,
            primary_members=("local.receipt", "local.commit", "local.supplier_id"),
        ),
        "filt": GroupAttrs(
            depth_label="d1",
            derivation=Derivation.FILTER.value,
            primary_members=("local._virt_filter",),
        ),
    }
    scoped = _virtual_filter_scoped_columns(gg, attrs, cg, ca, "root_d1")
    assert scoped == {"local.supplier_id", "local.receipt", "local.commit"}


def test_virtual_filter_scoped_columns_empty_without_filter_group():
    cg, ca = _cg({"local.x": {}, "local.cnt": {"parents": ["local.x"]}})
    gg: nx.DiGraph = nx.DiGraph()
    gg.add_edge("root_d1", "agg", kind="lineage")
    attrs = {
        "root_d1": GroupAttrs(
            depth_label="root_d1",
            derivation=Derivation.ROOT.value,
            primary_members=("local.x",),
        ),
        "agg": GroupAttrs(
            depth_label="d0",
            derivation=Derivation.AGGREGATE.value,
            primary_members=("local.cnt",),
        ),
    }
    assert _virtual_filter_scoped_columns(gg, attrs, cg, ca, "root_d1") == set()
