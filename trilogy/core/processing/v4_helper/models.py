from dataclasses import dataclass, field
from enum import Enum

from trilogy.core import graph as nx
from trilogy.core.enums import (
    AggregateGroupingMode,
    Derivation,
    Granularity,
    Purpose,
)
from trilogy.core.models.build import BoolExpr
from trilogy.core.processing.nodes import StrategyNode

from .constants import DepthLabel
from .edges import EdgeMap, copy_edges


def nulls_grouping_keys(mode: AggregateGroupingMode | None) -> bool:
    """Whether a group written with this GROUP BY mode NULLs its own grouping
    keys on some of the rows it emits.

    ROLLUP/CUBE/GROUPING SETS add subtotal and grand-total rows whose rolled-up
    key columns are NULL. Every consumer above such a group must treat those
    keys as unusable: a WHERE, a join axis, or a re-aggregation keyed on them
    silently drops the subtotals. This is the single question the planner
    should ask — not "what does the group id string look like"."""
    return mode is not None and mode != AggregateGroupingMode.STANDARD


@dataclass
class FinalContributorContract:
    """Logical contract for one group feeding the FINAL sink."""

    group_id: str
    output_addresses: frozenset[str] = frozenset()
    preserve_keys: frozenset[str] = frozenset()
    projection_grain: frozenset[str] = frozenset()


class InputChannel(Enum):
    ROW_STREAM = "row_stream"
    EXISTENCE = "existence"


@dataclass
class GroupInputContract:
    """Logical contract for one parent group feeding one consumer group."""

    parent_group_id: str
    consumer_group_id: str
    required_outputs: frozenset[str] = frozenset()
    required_grain: frozenset[str] = frozenset()
    preserve_keys: frozenset[str] = frozenset()
    channel: InputChannel = InputChannel.ROW_STREAM
    may_project_dimension: bool = False


@dataclass
class FinalAssemblyContract:
    """Logical contract for assembling the FINAL sink.

    Stage 2 owns these semantic requirements: which user-visible concepts the
    final query must expose, and the grain those outputs should be unique at.
    Stage 3 may still skip a physical GroupNode when the chosen source already
    satisfies the contract.
    """

    output_addresses: frozenset[str] = frozenset()
    required_grain: frozenset[str] = frozenset()
    merge_grain: frozenset[str] = frozenset()
    contributor_contracts: tuple[FinalContributorContract, ...] = ()
    deduplicate_to_grain: bool = True


@dataclass
class GroupAttrs:
    """Strongly-typed per-group state. Lives in a side dict
    (``dict[str, GroupAttrs]``) keyed by group id rather than on the
    nx.DiGraph node attributes — the graph stays as topology + edge metadata
    only, and downstream consumers get attribute access (and mypy coverage)
    instead of stringly-typed dict lookups.

    ``derivation`` is ``None`` only for the FINAL sink (which has no
    derivation); every real group carries its bucket's derivation."""

    depth_label: DepthLabel
    derivation: Derivation | None = None
    grain_components: frozenset[str] = frozenset()
    label: str = ""
    members: tuple[str, ...] = ()
    primary_members: tuple[str, ...] = ()
    secondary_members: tuple[str, ...] = ()
    member_depths: dict[str, DepthLabel] = field(default_factory=dict)
    # For an aggregate group, the row grain its inputs must be normalized to
    # before aggregation. This is the grouping grain plus the natural grain of
    # the aggregate arguments.
    aggregate_input_grain: frozenset[str] = frozenset()
    # Atoms (BoolExpr) applied AT this group. A clause like
    # `state='TN' AND year=2000` is decomposed and each atom finds its own
    # highest-allowed group independently — so a single clause may live at
    # multiple groups, or one group may collect atoms from several clauses.
    condition_atoms: list[BoolExpr] = field(default_factory=list)
    # String renderings of the atoms above, just for visualization.
    conditions: list[str] = field(default_factory=list)
    # How this group's GROUP BY is written. Non-STANDARD modes NULL-inject
    # their grouping keys on the subtotal rows they add, which is what
    # `nulls_grouping_keys` exists to ask about.
    grouping_mode: AggregateGroupingMode = AggregateGroupingMode.STANDARD
    # Populated by `_compute_concept_sets`. Empty tuples until then.
    output_concepts: tuple[str, ...] = ()
    hidden_concepts: tuple[str, ...] = ()
    input_concepts: tuple[str, ...] = ()
    # Populated only for FINAL: the logical output/grain contract Stage 3
    # physically satisfies or prunes.
    final_contract: FinalAssemblyContract | None = None
    # Populated for non-FINAL groups after `_compute_concept_sets`.
    input_contracts: tuple[GroupInputContract, ...] = ()

    @property
    def nulls_grouping_keys(self) -> bool:
        return nulls_grouping_keys(self.grouping_mode)


@dataclass
class ConceptAttrs:
    """Strongly-typed per-concept-node state. Like `GroupAttrs`, lives in a
    side dict (``dict[str, ConceptAttrs]``) keyed by concept-graph node id
    rather than on the nx.DiGraph node attributes — the graph stays as
    topology + edge metadata (``kind``) only, and the stage-2 grouping pipeline
    reads node state with attribute access (and mypy coverage) instead of
    stringly-typed dict lookups.

    ``address`` is the bare concept address; the node id may differ from it
    for any non-blank phase/label (the ``[label]address`` form)."""

    address: str
    label: str
    derivation: Derivation
    purpose: Purpose
    granularity: Granularity
    depth_label: DepthLabel
    grain_components: frozenset[str] = frozenset()
    # None for a non-aggregate concept; otherwise the aggregate's GROUP BY
    # mode, which `partition_aggregates` splits buckets on (one CTE cannot
    # carry both a flat GROUP BY and a GROUP BY ROLLUP).
    grouping_mode: AggregateGroupingMode | None = None
    rowset_name: str | None = None
    aggregate_input_grain: frozenset[str] = frozenset()
    keys: frozenset[str] = frozenset()
    # Addresses this concept answers for under another identity (scoped-join
    # canonical collapse, `merge into`): lets grouping relate a property root
    # to its key root when the key was collapsed onto a different address.
    pseudonyms: frozenset[str] = frozenset()
    # True for a pure rename (``alias(...)`` lineage) — a pseudonym of its
    # source. The renderer resolves it transparently to the source column, so
    # it must not be folded into a rollup group like a genuine transform dim.
    is_rename: bool = False
    # Tagged post-build for a concept that appears ONLY as an existence arg
    # (semijoin RHS) and never as a row arg — `partition_roots` places such a
    # node in its own scan bucket (side-channel subselect source).
    existence_only: bool = False


@dataclass
class BuildInfo:
    """Result bundle for discovery: the raw concept graph, the grouped graph,
    per-group attributes, and the materialized StrategyNode produced by
    walking the group graph."""

    concept_graph: nx.DiGraph = field(default_factory=nx.DiGraph)
    merged_group_graph: nx.DiGraph = field(default_factory=nx.DiGraph)
    group_graph: nx.DiGraph = field(default_factory=nx.DiGraph)
    group_attrs: dict[str, GroupAttrs] = field(default_factory=dict)
    concept_attrs: dict[str, ConceptAttrs] = field(default_factory=dict)
    # Typed edge-metadata side maps, one per graph above (the graphs themselves
    # carry only topology).
    concept_edges: EdgeMap = field(default_factory=dict)
    merged_group_edges: EdgeMap = field(default_factory=dict)
    group_edges: EdgeMap = field(default_factory=dict)
    strategy_node: StrategyNode | None = None

    def copy(self) -> "BuildInfo":
        return BuildInfo(
            concept_graph=self.concept_graph.copy(),
            merged_group_graph=self.merged_group_graph.copy(),
            group_graph=self.group_graph.copy(),
            group_attrs={k: _copy_attrs(v) for k, v in self.group_attrs.items()},
            concept_attrs={
                k: _copy_concept_attrs(v) for k, v in self.concept_attrs.items()
            },
            concept_edges=copy_edges(self.concept_edges),
            merged_group_edges=copy_edges(self.merged_group_edges),
            group_edges=copy_edges(self.group_edges),
            strategy_node=self.strategy_node.copy() if self.strategy_node else None,
        )


def _copy_attrs(a: GroupAttrs) -> GroupAttrs:
    return GroupAttrs(
        depth_label=a.depth_label,
        derivation=a.derivation,
        grain_components=a.grain_components,
        label=a.label,
        members=a.members,
        primary_members=a.primary_members,
        secondary_members=a.secondary_members,
        member_depths=dict(a.member_depths),
        condition_atoms=list(a.condition_atoms),
        conditions=list(a.conditions),
        output_concepts=a.output_concepts,
        hidden_concepts=a.hidden_concepts,
        input_concepts=a.input_concepts,
        aggregate_input_grain=a.aggregate_input_grain,
        final_contract=a.final_contract,
        input_contracts=a.input_contracts,
    )


def _copy_concept_attrs(a: ConceptAttrs) -> ConceptAttrs:
    return ConceptAttrs(
        address=a.address,
        label=a.label,
        derivation=a.derivation,
        purpose=a.purpose,
        granularity=a.granularity,
        depth_label=a.depth_label,
        grain_components=a.grain_components,
        grouping_mode=a.grouping_mode,
        rowset_name=a.rowset_name,
        aggregate_input_grain=a.aggregate_input_grain,
        keys=a.keys,
        pseudonyms=a.pseudonyms,
        is_rename=a.is_rename,
        existence_only=a.existence_only,
    )


@dataclass
class GroupBucket:
    """In-flight working state for one group while we're assembling
    `group_graph`. Once all groups are populated, fields are unpacked onto the
    final nx node as attributes.

    ``label`` is the sub-graph this bucket belongs to. Empty string is
    the outer query; non-empty (e.g. ``"q5_results"``) is a rowset's
    inner sub-graph. The grouping pipeline partitions per-label so
    inner and outer BASICs at compatible grain don't merge into one
    bucket and form a group-level cycle through the rowset boundary."""

    depth_label: DepthLabel
    derivation: Derivation
    grain_components: frozenset[str]
    # primary/secondary members are concept ADDRESSES — what downstream
    # strategy assembly cares about. primary_node_ids holds the matching
    # concept-graph node ids (which differ from addresses for any non-blank
    # phase/label), keyed parallel to primary_members.
    primary_members: list[str] = field(default_factory=list)
    primary_node_ids: list[str] = field(default_factory=list)
    secondary_members: list[str] = field(default_factory=list)
    member_depths: dict[str, DepthLabel] = field(default_factory=dict)
    label: str = ""
    # Optional disambiguator for rules that produce multiple buckets sharing
    # the (label, derivation, depth, grain) tuple — e.g. BASIC's signature
    # split, which can land two co-grain buckets with disjoint upstream
    # sources. Empty string for rules that don't need it.
    discriminator: str = ""
    # Grain to normalize this aggregate's inputs to before aggregating.
    aggregate_input_grain: frozenset[str] = frozenset()
    # SEMANTICS of this group's GROUP BY, as opposed to `discriminator`, which
    # only exists to keep distinct buckets at distinct group ids. Ask
    # `nulls_grouping_keys`, never the id string.
    grouping_mode: AggregateGroupingMode = AggregateGroupingMode.STANDARD

    @property
    def nulls_grouping_keys(self) -> bool:
        return nulls_grouping_keys(self.grouping_mode)
