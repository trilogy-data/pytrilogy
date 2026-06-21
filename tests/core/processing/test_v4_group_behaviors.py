"""Unit tests for per-derivation group IO behaviors.

These run on synthetic nx graphs and GroupBucket fixtures — no
BuildEnvironment, no SQL. The point is to pin the per-derivation
contracts so we can reason about each rule in isolation.

Node state lives in a side dict (``dict[str, ConceptAttrs]``) keyed by
concept-graph node id, mirroring production; the graph itself carries only
topology + lineage edges. ``_cg`` builds both from a compact spec.
"""

import pytest

from trilogy.core import graph as nx
from trilogy.core.enums import Derivation, Granularity, Purpose
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import BuildConcept, BuildGrain
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment
from trilogy.core.processing.nodes import (
    FilterNode,
    History,
    MergeNode,
    SelectNode,
    StrategyNode,
)
from trilogy.core.processing.v4_helper.concept_graph import node_id
from trilogy.core.processing.v4_helper.constants import (
    FINAL_NODE_ID,
    DepthLabel,
    EdgeKind,
)
from trilogy.core.processing.v4_helper.edges import EdgeMap, add_edge
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
    _add_final_node,
    _refresh_final_contract,
    _refresh_input_contracts,
    _virtual_filter_scoped_columns,
)
from trilogy.core.processing.v4_helper.group_rules import (
    partition_aggregates,
    partition_basics_by_signature,
    partition_constants,
    partition_roots,
)
from trilogy.core.processing.v4_helper.models import (
    ConceptAttrs,
    FinalAssemblyContract,
    FinalContributorContract,
    GroupAttrs,
    GroupBucket,
    InputChannel,
)
from trilogy.core.processing.v4_helper.source_policy import STRICT_SOURCE_POLICY
from trilogy.core.processing.v4_helper.strategy_builder import (
    _filter_intrinsic_pushdown_safe,
    _final_contributor_contracts,
    _parent_nodes_for,
    _pre_merge_parents,
    _required_final_contract,
)


def _attrs(address: str, spec: dict) -> ConceptAttrs:
    return ConceptAttrs(
        address=address,
        label=spec.get("label", ""),
        derivation=spec.get("derivation", Derivation.ROOT),
        purpose=spec.get("purpose", Purpose.KEY),
        granularity=spec.get("granularity", Granularity.MULTI_ROW),
        depth_label=spec.get("depth_label", DepthLabel.STAR),
        grain_components=frozenset(spec.get("grain", ())),
        grouping_mode=spec.get("grouping_mode"),
        rowset_name=spec.get("rowset_name"),
        aggregate_input_grain=frozenset(spec.get("aggregate_input_grain", ())),
        existence_only=spec.get("existence_only", False),
    )


def _cg(
    nodes: dict[str, dict],
) -> tuple[nx.DiGraph, EdgeMap, dict[str, ConceptAttrs]]:
    """Build a tiny concept graph + typed edge map + typed attrs side dict from
    {address: {'grain': set, 'derivation': ..., 'parents': [..]}}. Node id ==
    address here (blank/bare labels), matching how production keys non-labeled
    nodes."""
    g: nx.DiGraph = nx.DiGraph()
    edges: EdgeMap = {}
    attrs: dict[str, ConceptAttrs] = {}
    for addr, spec in nodes.items():
        g.add_node(addr)
        attrs[addr] = _attrs(addr, spec)
    for addr, spec in nodes.items():
        for parent in spec.get("parents", ()):
            add_edge(g, edges, parent, addr, EdgeKind.LINEAGE)
    return g, edges, attrs


def _item(address: str, **spec) -> tuple[str, ConceptAttrs]:
    """A `(node_id, ConceptAttrs)` pair as the grouping rules consume them."""
    return address, _attrs(address, spec)


def _bucket(
    derivation: Derivation,
    primaries: list[str],
    grain: set[str] | None = None,
) -> GroupBucket:
    return GroupBucket(
        depth_label=DepthLabel.STAR,
        derivation=derivation,
        grain_components=frozenset(grain or ()),
        primary_members=list(primaries),
    )


def _noop_ensure(derivation: Derivation) -> None:
    pass


def test_final_node_declares_logical_output_grain_contract():
    customer_id = _build_concept("customer.id", Purpose.KEY)
    customer_name = _build_concept(
        "customer.name",
        Purpose.PROPERTY,
        datatype=DataType.STRING,
        grain={customer_id.address},
        keys={customer_id.address},
    )
    total_revenue = _build_concept(
        "total_revenue",
        Purpose.METRIC,
        derivation=Derivation.AGGREGATE,
        grain={customer_id.address},
        is_aggregate=True,
    )
    concept_graph = nx.DiGraph()
    concept_attrs = {
        concept.address: ConceptAttrs(
            address=concept.address,
            label="",
            derivation=concept.derivation,
            purpose=concept.purpose,
            granularity=concept.granularity,
            depth_label=DepthLabel.STAR,
            grain_components=frozenset(concept.grain.components),
        )
        for concept in (customer_id, customer_name, total_revenue)
    }
    concept_graph.add_nodes_from(concept_attrs)
    group_graph = nx.DiGraph()
    group_edges: EdgeMap = {}
    attrs: dict[str, GroupAttrs] = {}
    buckets = {
        "root": GroupBucket(
            depth_label=DepthLabel.ROOT,
            derivation=Derivation.ROOT,
            grain_components=frozenset(),
            primary_members=[customer_id.address, customer_name.address],
        ),
        "agg": GroupBucket(
            depth_label=DepthLabel.D0,
            derivation=Derivation.AGGREGATE,
            grain_components=frozenset({customer_id.address}),
            primary_members=[total_revenue.address],
        ),
    }
    for gid, bucket in buckets.items():
        group_graph.add_node(gid)
        attrs[gid] = GroupAttrs(
            depth_label=bucket.depth_label,
            derivation=bucket.derivation,
            grain_components=bucket.grain_components,
            primary_members=tuple(bucket.primary_members),
        )

    _add_final_node(
        group_graph,
        group_edges,
        attrs,
        concept_graph,
        concept_attrs,
        buckets,
        conditions=[],
        mandatory_list=[customer_id, customer_name, total_revenue],
    )

    contract = attrs[FINAL_NODE_ID].final_contract
    assert contract is not None
    assert contract.output_addresses == {
        customer_id.address,
        customer_name.address,
        total_revenue.address,
    }
    assert contract.required_grain == {customer_id.address}
    assert contract.deduplicate_to_grain is True


def test_final_contributor_contract_preserves_rowset_merge_grain_for_root():
    customer_id = _build_concept("customer.id", Purpose.KEY)
    customer_name = _build_concept(
        "customer.name",
        Purpose.PROPERTY,
        datatype=DataType.STRING,
        grain={customer_id.address},
        keys={customer_id.address},
    )
    bought_city = _build_concept(
        "bought_city",
        Purpose.PROPERTY,
        derivation=Derivation.ROWSET,
        datatype=DataType.STRING,
        grain={customer_id.address},
    )
    group_graph = nx.DiGraph()
    group_edges: EdgeMap = {}
    attrs = {
        "root": GroupAttrs(
            depth_label=DepthLabel.ROOT,
            derivation=Derivation.ROOT,
            output_concepts=(customer_id.address, customer_name.address),
        ),
        "rowset": GroupAttrs(
            depth_label=DepthLabel.D0,
            derivation=Derivation.ROWSET,
            grain_components=frozenset({customer_id.address}),
            output_concepts=(bought_city.address,),
        ),
        FINAL_NODE_ID: GroupAttrs(depth_label=DepthLabel.FINAL),
    }
    group_graph.add_nodes_from(attrs)
    add_edge(group_graph, group_edges, "root", FINAL_NODE_ID, EdgeKind.MERGE)
    add_edge(group_graph, group_edges, "rowset", FINAL_NODE_ID, EdgeKind.MERGE)

    _refresh_final_contract(
        group_graph,
        attrs,
        [customer_name, bought_city],
    )

    contract = attrs[FINAL_NODE_ID].final_contract
    assert contract is not None
    root_contract = next(
        item for item in contract.contributor_contracts if item.group_id == "root"
    )
    assert contract.merge_grain == {customer_id.address}
    assert root_contract.preserve_keys == {customer_id.address}
    assert root_contract.projection_grain == set()


def test_stage3_requires_declared_final_contract():
    attrs = {FINAL_NODE_ID: GroupAttrs(depth_label=DepthLabel.FINAL)}

    with pytest.raises(ValueError, match="FINAL contract missing"):
        _required_final_contract(attrs)


def test_stage3_requires_declared_final_contributor_contracts():
    contract = FinalAssemblyContract(
        contributor_contracts=(
            FinalContributorContract(group_id="root", output_addresses=frozenset()),
        )
    )

    with pytest.raises(ValueError, match="agg"):
        _final_contributor_contracts(contract, ["root", "agg"])


def test_input_contract_declares_projection_grain_with_barrier_sibling():
    store_id = _build_concept("store.id", Purpose.KEY)
    ride_date = _build_concept("ride_date", Purpose.KEY)
    station_name = _build_concept(
        "station.name",
        Purpose.PROPERTY,
        datatype=DataType.STRING,
        grain={store_id.address},
        keys={store_id.address},
    )
    group_graph = nx.DiGraph()
    group_edges: EdgeMap = {}
    attrs = {
        "agg_parent": GroupAttrs(
            depth_label=DepthLabel.D0,
            derivation=Derivation.AGGREGATE,
            grain_components=frozenset({ride_date.address}),
            output_concepts=("daily_rides", ride_date.address),
        ),
        "dim_parent": GroupAttrs(
            depth_label=DepthLabel.ROOT,
            derivation=Derivation.ROOT,
            output_concepts=(store_id.address, station_name.address),
        ),
        "consumer": GroupAttrs(
            depth_label=DepthLabel.D0,
            derivation=Derivation.AGGREGATE,
            grain_components=frozenset({store_id.address}),
            input_concepts=(
                "daily_rides",
                ride_date.address,
                store_id.address,
                station_name.address,
            ),
        ),
    }
    group_graph.add_nodes_from(attrs)
    add_edge(group_graph, group_edges, "agg_parent", "consumer", EdgeKind.LINEAGE)
    add_edge(group_graph, group_edges, "dim_parent", "consumer", EdgeKind.LINEAGE)

    _refresh_input_contracts(group_graph, group_edges, attrs)

    dim_contract = next(
        item
        for item in attrs["consumer"].input_contracts
        if item.parent_group_id == "dim_parent"
    )
    assert dim_contract.channel == InputChannel.ROW_STREAM
    assert dim_contract.may_project_dimension is True
    assert dim_contract.required_grain == {store_id.address, ride_date.address}
    assert dim_contract.preserve_keys == {store_id.address, ride_date.address}


def test_input_contract_keeps_existence_parent_side_channel():
    group_graph = nx.DiGraph()
    group_edges: EdgeMap = {}
    attrs = {
        "existence_src": GroupAttrs(
            depth_label=DepthLabel.D1,
            derivation=Derivation.ROOT,
            output_concepts=("local.zip",),
        ),
        "consumer": GroupAttrs(
            depth_label=DepthLabel.STAR,
            derivation=Derivation.BASIC,
            input_concepts=("local.customer_id",),
        ),
    }
    group_graph.add_nodes_from(attrs)
    add_edge(group_graph, group_edges, "existence_src", "consumer", EdgeKind.EXISTENCE)

    _refresh_input_contracts(group_graph, group_edges, attrs)

    contract = attrs["consumer"].input_contracts[0]
    assert contract.channel == InputChannel.EXISTENCE
    assert contract.required_grain == set()
    assert contract.preserve_keys == set()
    assert contract.may_project_dimension is False


def _build_concept(
    name: str,
    purpose: Purpose,
    *,
    datatype: DataType = DataType.INTEGER,
    derivation: Derivation = Derivation.ROOT,
    grain: set[str] | None = None,
    keys: set[str] | None = None,
    is_aggregate: bool = False,
) -> BuildConcept:
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=datatype,
        purpose=purpose,
        build_is_aggregate=is_aggregate,
        derivation=derivation,
        grain=BuildGrain(components=grain or set()),
        keys=keys,
        namespace="local",
    )


# ----- can_preserve_grain_subset --------------------------------------


class TestCanPreserveGrainSubset:
    def test_address_is_a_grain_key(self):
        cg, ce, ca = _cg({"week_seq": {"grain": {"date.id"}}})
        assert (
            can_preserve_grain_subset(cg, ce, ca, frozenset({"week_seq"}), "week_seq")
            is True
        )

    def test_grain_strict_subset(self):
        cg, ce, ca = _cg(
            {
                "text_id": {"grain": {"customer.id"}},
                "customer.id": {"grain": {"customer.id"}},
            }
        )
        assert (
            can_preserve_grain_subset(cg, ce, ca, frozenset({"customer.id"}), "text_id")
            is True
        )

    def test_grain_not_a_subset(self):
        cg, ce, ca = _cg({"item.id": {"grain": {"item.id"}}})
        assert (
            can_preserve_grain_subset(cg, ce, ca, frozenset({"week_seq"}), "item.id")
            is False
        )

    def test_empty_column_grain_always_passes(self):
        # Constants / scalars have empty grain — always rideable.
        cg, ce, ca = _cg({"the_year": {"grain": set()}})
        assert (
            can_preserve_grain_subset(cg, ce, ca, frozenset({"week_seq"}), "the_year")
            is True
        )

    def test_unknown_address_blocked(self):
        cg, ce, ca = _cg({})
        # Not in attrs -> can't reason about it -> conservative reject.
        assert can_preserve_grain_subset(cg, ce, ca, frozenset(), "phantom") is False

    def test_q02_basic_carries_week_seq(self):
        """Regression: q02's BASIC at inherited grain {week_seq} must
        let `sales.date.week_seq` ride through. The compiler reports
        week_seq.grain = {date.id}, which is NOT a subset of {week_seq};
        the address-in-grain branch is what unblocks it."""
        cg, ce, ca = _cg({"sales.date.week_seq": {"grain": {"sales.date.id"}}})
        assert (
            can_preserve_grain_subset(
                cg, ce, ca, frozenset({"sales.date.week_seq"}), "sales.date.week_seq"
            )
            is True
        )

    def test_q04_basic_blocks_row_grain_leak(self):
        """Regression: q04's BASIC at {customer.id} must NOT carry
        through row-grain roots like date.year. Without this block,
        the BASIC's implicit GROUP-BY inflates to per-row."""
        cg, ce, ca = _cg({"date.year": {"grain": {"date.id"}}})
        assert (
            can_preserve_grain_subset(
                cg, ce, ca, frozenset({"customer.id"}), "date.year"
            )
            is False
        )


# ----- _lineage_parent_addrs ------------------------------------------


class TestLineageParentAddrs:
    def test_unknown_address_is_empty(self):
        cg, ce, ca = _cg({})
        assert _lineage_parent_addrs(cg, ce, ca, "phantom") == set()

    def test_collects_only_lineage_parents(self):
        cg, ce, ca = _cg(
            {
                "channel_label": {"grain": {"channel.id"}},
                "s_channel": {
                    "grain": {"channel.id"},
                    "parents": ["channel_label"],
                },
            }
        )
        assert _lineage_parent_addrs(cg, ce, ca, "s_channel") == {"channel_label"}


# ----- can_preserve_grouping ------------------------------------------


class TestCanPreserveGrouping:
    def test_address_is_a_group_key(self):
        cg, ce, ca = _cg({"channel.id": {"grain": {"channel.id"}}})
        assert (
            can_preserve_grouping(cg, ce, ca, frozenset({"channel.id"}), "channel.id")
            is True
        )

    def test_unknown_address_blocked(self):
        cg, ce, ca = _cg({})
        assert can_preserve_grouping(cg, ce, ca, frozenset(), "phantom") is False

    def test_grain_subset_rides_through(self):
        cg, ce, ca = _cg({"text_id": {"grain": {"customer.id"}}})
        assert (
            can_preserve_grouping(cg, ce, ca, frozenset({"customer.id"}), "text_id")
            is True
        )

    def test_rename_of_grain_keys_rides_through(self):
        """q05: `s_channel` is a BASIC rename of the group key
        `channel_label`. Its own grain isn't a subset of the GROUP BY, but
        every lineage parent IS a group key, so it rides through."""
        cg, ce, ca = _cg(
            {
                "channel_label": {"grain": {"channel.id"}},
                "s_channel": {
                    "grain": {"date.id"},  # misleading non-subset grain
                    "parents": ["channel_label"],
                    "derivation": Derivation.BASIC,
                },
            }
        )
        assert (
            can_preserve_grouping(cg, ce, ca, frozenset({"channel_label"}), "s_channel")
            is True
        )

    def test_constant_empty_grain_rides_through(self):
        cg, ce, ca = _cg(
            {"the_year": {"grain": set(), "derivation": Derivation.CONSTANT}}
        )
        assert (
            can_preserve_grouping(cg, ce, ca, frozenset({"week_seq"}), "the_year")
            is True
        )

    def test_row_varying_empty_grain_blocked(self):
        """A row-varying empty-grain column (a CASE that isn't a key rename)
        would land in the SELECT with no GROUP BY entry — invalid SQL."""
        cg, ce, ca = _cg({"flag": {"grain": set(), "derivation": Derivation.BASIC}})
        assert (
            can_preserve_grouping(cg, ce, ca, frozenset({"week_seq"}), "flag") is False
        )

    def test_non_subset_grain_with_non_key_parents_blocked(self):
        cg, ce, ca = _cg(
            {
                "row_thing": {"grain": {"item.id"}},
                "derived": {
                    "grain": {"item.id"},
                    "parents": ["row_thing"],
                    "derivation": Derivation.BASIC,
                },
            }
        )
        assert (
            can_preserve_grouping(cg, ce, ca, frozenset({"week_seq"}), "derived")
            is False
        )


# ----- can_preserve_root ----------------------------------------------


def test_can_preserve_root_always_false():
    """ROOT has no upstream to preserve from. The orchestrator skips
    asking, but the function should still be safe to call."""
    cg, ce, ca = _cg({"anything": {"grain": set()}})
    assert can_preserve_root(cg, ce, ca, frozenset(), "anything") is False


# ----- native_grain_root ----------------------------------------------


def test_native_grain_root_is_empty():
    cg, ce, ca = _cg({})
    bucket = _bucket(Derivation.ROOT, primaries=["x", "y"], grain={"x"})
    assert native_grain_root(bucket, cg, ce, ca) == frozenset()


# ----- native_grain_declared ------------------------------------------


def test_native_grain_declared_uses_bucket_grain():
    cg, ce, ca = _cg({})
    bucket = _bucket(
        Derivation.AGGREGATE,
        primaries=["sum_x"],
        grain={"week_seq"},
    )
    assert native_grain_declared(bucket, cg, ce, ca) == frozenset({"week_seq"})


def test_native_grain_declared_empty_when_no_grain():
    cg, ce, ca = _cg({})
    bucket = _bucket(Derivation.AGGREGATE, primaries=["count_x"])
    assert native_grain_declared(bucket, cg, ce, ca) == frozenset()


# ----- native_grain_basic_inherited -----------------------------------


class TestNativeGrainBasicInherited:
    def test_inherits_from_aggregate_parents(self):
        """q02 shape: BASIC primary's lineage parents are aggregates at
        {week_seq}. Inherited grain should be {week_seq}, not the
        compiler-reported source row grain on the BASIC itself."""
        cg, ce, ca = _cg(
            {
                "agg_sum": {"grain": {"week_seq"}},
                "round_result": {
                    "grain": {"item.id", "order_id"},
                    "parents": ["agg_sum"],
                },
            }
        )
        bucket = _bucket(
            Derivation.BASIC,
            primaries=["round_result"],
            grain={"item.id", "order_id"},  # the misleading source grain
        )
        assert native_grain_basic_inherited(bucket, cg, ce, ca) == frozenset(
            {"week_seq"}
        )

    def test_falls_back_to_declared_when_no_lineage(self):
        cg, ce, ca = _cg({"constant_one": {"grain": set()}})
        bucket = _bucket(
            Derivation.BASIC,
            primaries=["constant_one"],
            grain={"customer.id"},
        )
        # constant_one has no lineage parents -> use declared
        assert native_grain_basic_inherited(bucket, cg, ce, ca) == frozenset(
            {"customer.id"}
        )

    def test_q04_basic_inherits_customer_grain(self):
        """q04 shape: BASIC primaries rename root concepts at {customer.id}.
        Inherited grain matches declared grain — both should land on
        {customer.id}."""
        cg, ce, ca = _cg(
            {
                "text_id": {"grain": {"customer.id"}},
                "customer_id_local": {"grain": {"customer.id"}, "parents": ["text_id"]},
            }
        )
        bucket = _bucket(
            Derivation.BASIC,
            primaries=["customer_id_local"],
            grain={"customer.id"},
        )
        assert native_grain_basic_inherited(bucket, cg, ce, ca) == frozenset(
            {"customer.id"}
        )

    def test_unions_grains_across_multiple_primaries(self):
        cg, ce, ca = _cg(
            {
                "agg_by_week": {"grain": {"week_seq"}},
                "agg_by_item": {"grain": {"item.id"}},
                "combo": {"grain": set(), "parents": ["agg_by_week", "agg_by_item"]},
            }
        )
        bucket = _bucket(Derivation.BASIC, primaries=["combo"])
        assert native_grain_basic_inherited(bucket, cg, ce, ca) == frozenset(
            {"week_seq", "item.id"}
        )


# ----- registry --------------------------------------------------------


@pytest.mark.parametrize(
    "derivation",
    [
        Derivation.ROOT,
        Derivation.BASIC,
        Derivation.AGGREGATE,
        Derivation.GROUP_TO,
        Derivation.WINDOW,
        Derivation.FILTER,
        Derivation.SUBSELECT,
    ],
)
def test_registry_covers_known_derivation(derivation: Derivation):
    assert derivation in GROUP_BEHAVIORS
    beh = GROUP_BEHAVIORS[derivation]
    assert beh.derivation == derivation


def test_behavior_for_unknown_returns_default():
    """Unenumerated derivations get the conservative default — declared grain
    + subset preservation. The check is that it doesn't crash and that
    it produces a usable Behavior."""
    beh = behavior_for(Derivation.MULTISELECT)
    assert beh is not None
    assert beh.native_grain is native_grain_declared
    assert beh.can_preserve is can_preserve_grain_subset


# ----- graph label / sub-graph partitioning ---------------------------


def test_node_id_default_label_is_bare_address():
    """Default-label keys stay as bare addresses so existing single-label
    consumers (everything pre-rowset) keep working without changes."""

    assert node_id("", "local.sales") == "local.sales"


def test_node_id_labeled_prefix():
    """Labeled keys carry the sub-graph name as a bracketed prefix so an
    inner copy of a concept can't collide with the outer copy."""

    assert node_id("q5_results", "local.channel_label") == (
        "[q5_results]local.channel_label"
    )


def test_partition_basics_does_not_merge_across_labels():
    """Two BASICs with identical grain but different labels must NOT
    end up in the same bucket — that's the regression that produced the
    q05 cycle (outer renames and rowset-internal derives at compatible
    grain collapsing through the rowset boundary)."""

    items = [
        _item("local.sales", derivation=Derivation.BASIC, label=""),
        _item(
            "[q5_results]local.sales_metric",
            derivation=Derivation.BASIC,
            label="q5_results",
        ),
    ]
    cg, ce, ca = _cg(
        {
            "local.sales": {"derivation": Derivation.BASIC},
            "[q5_results]local.sales_metric": {
                "derivation": Derivation.BASIC,
                "label": "q5_results",
            },
        }
    )
    buckets = partition_basics_by_signature(items, cg, ce, ca, {}, _noop_ensure)
    assert len(buckets) == 2
    labels = sorted(b.label for b in buckets)
    assert labels == ["", "q5_results"]


def test_partition_basics_does_merge_within_label():
    """The label split shouldn't break the within-label subset-merge
    behavior: two compatible-grain BASICs at the same label still collapse
    to a single bucket (the q04 case)."""

    items = [
        _item(
            "local.customer_id",
            derivation=Derivation.BASIC,
            grain={"customer.id"},
        ),
        _item(
            "local.customer_first_name",
            derivation=Derivation.BASIC,
            grain={"customer.id"},
        ),
    ]
    cg, ce, ca = _cg(
        {
            "local.customer_id": {"derivation": Derivation.BASIC},
            "local.customer_first_name": {"derivation": Derivation.BASIC},
        }
    )
    buckets = partition_basics_by_signature(items, cg, ce, ca, {}, _noop_ensure)
    assert len(buckets) == 1
    assert {m for m in buckets[0].primary_members} == {
        "local.customer_id",
        "local.customer_first_name",
    }


def test_partition_basics_merges_nested_nonempty_signatures():

    items = [
        _item("local.key_alias", derivation=Derivation.BASIC),
        _item(
            "local.metric",
            derivation=Derivation.BASIC,
            grain={"source.key", "lookup.key"},
        ),
    ]
    cg, ce, ca = _cg(
        {
            "source.key": {"derivation": Derivation.ROWSET},
            "source.value": {"derivation": Derivation.ROWSET},
            "lookup.value": {"derivation": Derivation.ROWSET},
            "local.key_alias": {
                "derivation": Derivation.BASIC,
                "parents": ["source.key"],
            },
            "local.metric": {
                "derivation": Derivation.BASIC,
                "grain": {"source.key", "lookup.key"},
                "parents": ["source.value", "lookup.value"],
            },
        }
    )
    primary_group = {
        "source.key": "grp:source",
        "source.value": "grp:source",
        "lookup.value": "grp:lookup",
    }

    buckets = partition_basics_by_signature(
        items, cg, ce, ca, primary_group, _noop_ensure
    )

    assert len(buckets) == 1
    assert set(buckets[0].primary_members) == {"local.key_alias", "local.metric"}


def test_partition_basics_empty_signature_does_not_merge_into_sourced_basic():

    items = [
        _item("local.constant_label", derivation=Derivation.BASIC),
        _item("local.metric", derivation=Derivation.BASIC),
    ]
    cg, ce, ca = _cg(
        {
            "source.value": {"derivation": Derivation.ROWSET},
            "local.constant_label": {"derivation": Derivation.BASIC},
            "local.metric": {
                "derivation": Derivation.BASIC,
                "parents": ["source.value"],
            },
        }
    )

    buckets = partition_basics_by_signature(
        items,
        cg,
        ce,
        ca,
        {"source.value": "grp:source"},
        _noop_ensure,
    )

    assert len(buckets) == 2


def test_partition_basics_root_signature_subset_does_not_merge():

    items = [
        _item("local.root_rename", derivation=Derivation.BASIC),
        _item("local.aggregate_derive", derivation=Derivation.BASIC),
    ]
    cg, ce, ca = _cg(
        {
            "root.week_seq": {"derivation": Derivation.ROOT},
            "local.year_flag": {"derivation": Derivation.AGGREGATE},
            "local.root_rename": {
                "derivation": Derivation.BASIC,
                "parents": ["root.week_seq"],
            },
            "local.aggregate_derive": {
                "derivation": Derivation.BASIC,
                "parents": ["root.week_seq", "local.year_flag"],
            },
        }
    )
    primary_group = {
        "root.week_seq": "grp:root:root:empty",
        "local.year_flag": "grp:aggregate:d0:root.week_seq",
    }

    buckets = partition_basics_by_signature(
        items, cg, ce, ca, primary_group, _noop_ensure
    )

    assert len(buckets) == 2


def test_partition_aggregates_uses_input_grain():

    items = [
        _item(
            "local.numcust",
            derivation=Derivation.AGGREGATE,
            grain={"local.cntrycode"},
            aggregate_input_grain={"local.cntrycode", "local.id"},
        ),
        _item(
            "local.totacctbal",
            derivation=Derivation.AGGREGATE,
            grain={"local.cntrycode"},
            aggregate_input_grain={"local.cntrycode", "local.id"},
        ),
        _item(
            "local.line_total",
            derivation=Derivation.AGGREGATE,
            grain={"local.cntrycode"},
            aggregate_input_grain={"local.cntrycode", "line.id"},
        ),
    ]
    cg, ce, ca = _cg({node: {} for node, _ in items})
    buckets = partition_aggregates(items, cg, ce, ca, {}, _noop_ensure)

    merged = [
        bucket
        for bucket in buckets
        if set(bucket.primary_members) == {"local.numcust", "local.totacctbal"}
    ]
    assert len(buckets) == 2
    assert len(merged) == 1


def test_partition_rollup_aggregates_share_bucket():

    items = [
        _item(
            "local.total_sum",
            derivation=Derivation.AGGREGATE,
            grain={"local.category", "local.class"},
            grouping_mode="rollup",
            aggregate_input_grain={
                "local.category",
                "local.class",
                "local.item_id",
                "local.ticket_number",
            },
        ),
        _item(
            "local.g_class",
            derivation=Derivation.AGGREGATE,
            grain={"local.category", "local.class"},
            grouping_mode="rollup",
            aggregate_input_grain={"local.category", "local.class", "local.item_id"},
        ),
    ]
    cg, ce, ca = _cg({node: {} for node, _ in items})
    buckets = partition_aggregates(items, cg, ce, ca, {}, _noop_ensure)

    assert len(buckets) == 1
    assert set(buckets[0].primary_members) == {"local.total_sum", "local.g_class"}
    assert buckets[0].aggregate_input_grain == frozenset(
        {"local.category", "local.class", "local.item_id", "local.ticket_number"}
    )


def test_partition_rollup_aggregates_split_by_source_signature():

    items = [
        _item(
            "local.store_total",
            derivation=Derivation.AGGREGATE,
            grain={"local.category", "local.class"},
            grouping_mode="rollup",
            aggregate_input_grain={"local.category", "local.class", "store.line_id"},
        ),
        _item(
            "local.web_total",
            derivation=Derivation.AGGREGATE,
            grain={"local.category", "local.class"},
            grouping_mode="rollup",
            aggregate_input_grain={"local.category", "local.class", "web.line_id"},
        ),
    ]
    cg, ce, ca = _cg(
        {
            "store.line_id": {"derivation": Derivation.ROOT},
            "web.line_id": {"derivation": Derivation.ROOT},
            "local.store_total": {
                "derivation": Derivation.AGGREGATE,
                "parents": ["store.line_id"],
            },
            "local.web_total": {
                "derivation": Derivation.AGGREGATE,
                "parents": ["web.line_id"],
            },
        }
    )
    root_items = [
        ("store.line_id", ca["store.line_id"]),
        ("web.line_id", ca["web.line_id"]),
    ]
    primary_group: dict[str, str] = {}

    def ensure_assigned(derivation: Derivation) -> None:
        if derivation != Derivation.ROOT or primary_group:
            return
        for bucket in partition_roots(
            root_items, cg, ce, ca, primary_group, _noop_ensure
        ):
            gid = f"root:{'|'.join(bucket.primary_members)}"
            for nid in bucket.primary_node_ids:
                primary_group[nid] = gid

    buckets = partition_aggregates(items, cg, ce, ca, primary_group, ensure_assigned)

    assert len(buckets) == 2
    assert {tuple(bucket.primary_members) for bucket in buckets} == {
        ("local.store_total",),
        ("local.web_total",),
    }


def test_pre_merge_carries_sibling_join_keys_without_metrics():

    env = BuildEnvironment()
    part_name = _build_concept(
        "part.name",
        Purpose.PROPERTY,
        datatype=DataType.STRING,
        grain={"local.part.id"},
        keys={"local.part.id"},
    )
    order_id = _build_concept("order.id", Purpose.KEY)
    charge_price = _build_concept("charge_price", Purpose.METRIC)
    charge_percent = _build_concept(
        "charge_percent",
        Purpose.METRIC,
        derivation=Derivation.BASIC,
    )
    row_source = StrategyNode(
        input_concepts=[],
        output_concepts=[part_name, order_id, charge_price],
        environment=env,
    )
    row_projection = SelectNode(
        input_concepts=[charge_price],
        output_concepts=[charge_price],
        parents=[row_source],
        environment=env,
    )
    ratio_source = StrategyNode(
        input_concepts=[],
        output_concepts=[part_name, charge_percent],
        environment=env,
    )
    ratio_projection = SelectNode(
        input_concepts=[part_name, charge_percent],
        output_concepts=[part_name, charge_percent],
        parents=[ratio_source],
        environment=env,
    )

    merged = _pre_merge_parents([row_projection, ratio_projection], env)

    assert len(merged) == 1
    assert isinstance(merged[0], MergeNode)
    row_outputs = {concept.address for concept in row_projection.output_concepts}
    assert "local.part.name" in row_outputs
    assert "local.charge_percent" not in row_outputs


def test_conditioned_filter_does_not_cover_unfiltered_parent_outputs():

    env = BuildEnvironment()
    supplier_id = _build_concept("supplier.id", Purpose.KEY)
    order_id = _build_concept("order.id", Purpose.KEY)
    filtered_supplier = _build_concept("late_supplier_id", Purpose.KEY)
    root_node = StrategyNode(
        input_concepts=[],
        output_concepts=[supplier_id, order_id],
        environment=env,
    )
    filter_node = FilterNode(
        input_concepts=[supplier_id, order_id],
        output_concepts=[filtered_supplier, supplier_id, order_id],
        parents=[root_node],
        environment=env,
    )
    filter_node.conditions = object()
    group_graph = nx.DiGraph()
    group_edges: EdgeMap = {}
    add_edge(group_graph, group_edges, "root", "filter", EdgeKind.LINEAGE)
    add_edge(group_graph, group_edges, "root", "agg", EdgeKind.LINEAGE)
    add_edge(group_graph, group_edges, "filter", "agg", EdgeKind.LINEAGE)
    attrs = {
        "root": GroupAttrs(
            depth_label=DepthLabel.ROOT,
            derivation=Derivation.ROOT,
            primary_members=(supplier_id.address, order_id.address),
        ),
        "filter": GroupAttrs(
            depth_label=DepthLabel.D1,
            derivation=Derivation.FILTER,
            primary_members=(filtered_supplier.address,),
        ),
        "agg": GroupAttrs(
            depth_label=DepthLabel.D1,
            derivation=Derivation.AGGREGATE,
            primary_members=("local.supplier_counts",),
        ),
    }

    parents = _parent_nodes_for(
        group_graph,
        group_edges,
        attrs,
        {"root": root_node, "filter": filter_node},
        "agg",
        env,
        ReferenceGraph(),
        History(base_environment=Environment()),
        STRICT_SOURCE_POLICY,
        needed={supplier_id.address, order_id.address, filtered_supplier.address},
    )

    assert {parent.group_id for parent in parents} == {"root", "filter"}
    assert {type(parent.node) for parent in parents} == {StrategyNode, FilterNode}


def test_filter_intrinsic_pushdown_blocks_shared_unfiltered_ancestor():

    graph = nx.DiGraph()
    graph.add_edge("root", "filter")
    graph.add_edge("root", "aggregate")
    graph.add_edge("filter", "aggregate")
    graph.add_edge("root", FINAL_NODE_ID)
    graph.add_edge("filter", FINAL_NODE_ID)

    assert _filter_intrinsic_pushdown_safe(graph, "filter") is False


def test_filter_intrinsic_pushdown_ignores_final_sink():

    graph = nx.DiGraph()
    graph.add_edge("root", "filter")
    graph.add_edge("root", FINAL_NODE_ID)
    graph.add_edge("filter", FINAL_NODE_ID)

    assert _filter_intrinsic_pushdown_safe(graph, "filter") is True


def test_partition_roots_buckets_per_label():
    """Each sub-graph (outer + each rowset) gets its own ROOT bucket
    because their scans are independent."""

    items = [
        _item("sales.item.id", derivation=Derivation.ROOT, depth_label=DepthLabel.ROOT),
        _item(
            "[q5_results]sales.item.id",
            derivation=Derivation.ROOT,
            depth_label=DepthLabel.ROOT,
            label="q5_results",
        ),
    ]
    cg, ce, ca = _cg({"sales.item.id": {}, "[q5_results]sales.item.id": {}})
    buckets = partition_roots(items, cg, ce, ca, {}, _noop_ensure)
    assert len(buckets) == 2
    assert {b.label for b in buckets} == {"", "q5_results"}


def test_virtual_filter_scoped_columns_collects_condition_args():
    # root_d1 feeds a FILTER group whose virt-filter output derives from the
    # `?` condition's columns (q21's `count(supplier ? receipt > commit)`).
    cg, ce, ca = _cg(
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
    gg.add_edge("root_d1", "filt")
    attrs = {
        "root_d1": GroupAttrs(
            depth_label=DepthLabel.ROOT_D1,
            derivation=Derivation.ROOT,
            primary_members=("local.receipt", "local.commit", "local.supplier_id"),
        ),
        "filt": GroupAttrs(
            depth_label=DepthLabel.D1,
            derivation=Derivation.FILTER,
            primary_members=("local._virt_filter",),
        ),
    }
    scoped = _virtual_filter_scoped_columns(gg, attrs, cg, ce, ca, "root_d1")
    assert scoped == {"local.supplier_id", "local.receipt", "local.commit"}


def test_virtual_filter_scoped_columns_empty_without_filter_group():
    cg, ce, ca = _cg({"local.x": {}, "local.cnt": {"parents": ["local.x"]}})
    gg: nx.DiGraph = nx.DiGraph()
    gg.add_edge("root_d1", "agg")
    attrs = {
        "root_d1": GroupAttrs(
            depth_label=DepthLabel.ROOT_D1,
            derivation=Derivation.ROOT,
            primary_members=("local.x",),
        ),
        "agg": GroupAttrs(
            depth_label=DepthLabel.D0,
            derivation=Derivation.AGGREGATE,
            primary_members=("local.cnt",),
        ),
    }
    assert _virtual_filter_scoped_columns(gg, attrs, cg, ce, ca, "root_d1") == set()


def test_partition_constants_merges_select_and_condition_phases():
    """A constant referenced in both the SELECT (d*) and a constant-only WHERE
    (d1) is one population: `partition_constants` must merge the two phase nodes
    into a single STAR bucket so the WHERE rides on the SELECT output instead of
    stranding on a discarded upstream condition-feeder (the date_diff render
    regression)."""
    select_item = (
        "local.today",
        _attrs("local.today", {"derivation": Derivation.CONSTANT}),
    )
    cond_item = (
        "local.today@condition",
        _attrs(
            "local.today",
            {
                "derivation": Derivation.CONSTANT,
                "label": "@condition",
                "depth_label": DepthLabel.D1,
            },
        ),
    )
    buckets = partition_constants(
        [select_item, cond_item], nx.DiGraph(), {}, {}, {}, lambda d: None
    )
    assert len(buckets) == 1
    bucket = buckets[0]
    assert bucket.depth_label == DepthLabel.STAR
    assert list(bucket.primary_members) == ["local.today", "local.today"]


def test_partition_constants_splits_distinct_grain():
    """Distinct grains still split: a constant is grain-agnostic but the key
    must keep different-grain populations apart (mirrors the default rule)."""
    a = (
        "local.a",
        _attrs("local.a", {"derivation": Derivation.CONSTANT, "grain": {"x"}}),
    )
    b = (
        "local.b",
        _attrs("local.b", {"derivation": Derivation.CONSTANT, "grain": {"y"}}),
    )
    buckets = partition_constants([a, b], nx.DiGraph(), {}, {}, {}, lambda d: None)
    assert len(buckets) == 2
