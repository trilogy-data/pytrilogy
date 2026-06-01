from dataclasses import dataclass, field

import networkx as nx

from trilogy.core.models.build import BoolExpr
from trilogy.core.processing.nodes import StrategyNode


@dataclass
class GroupAttrs:
    """Strongly-typed per-group state. Lives in a side dict
    (``dict[str, GroupAttrs]``) keyed by group id rather than on the
    nx.DiGraph node attributes — the graph stays as topology + edge metadata
    only, and downstream consumers get attribute access (and mypy coverage)
    instead of stringly-typed dict lookups."""

    depth_label: str
    derivation: str
    grain_components: frozenset[str] = frozenset()
    label: str = ""
    members: tuple[str, ...] = ()
    primary_members: tuple[str, ...] = ()
    secondary_members: tuple[str, ...] = ()
    member_depths: dict[str, str] = field(default_factory=dict)
    # For an aggregate group whose count(s) must count distinct entities, the
    # grain its input is reduced to before aggregating (e.g. {order_number} for
    # `count(order_number)`). Empty when no reduction is needed. Drives an
    # intermediate dedup GroupNode in the strategy builder.
    dedup_grain: frozenset[str] = frozenset()
    # Atoms (BoolExpr) applied AT this group. A clause like
    # `state='TN' AND year=2000` is decomposed and each atom finds its own
    # highest-allowed group independently — so a single clause may live at
    # multiple groups, or one group may collect atoms from several clauses.
    condition_atoms: list[BoolExpr] = field(default_factory=list)
    # String renderings of the atoms above, just for visualization.
    conditions: list[str] = field(default_factory=list)
    # Populated by `_compute_concept_sets`. Empty tuples until then.
    output_concepts: tuple[str, ...] = ()
    hidden_concepts: tuple[str, ...] = ()
    input_concepts: tuple[str, ...] = ()


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
    derivation: str
    purpose: str
    granularity: str
    depth_label: str
    grain_components: frozenset[str] = frozenset()
    grouping_mode: str | None = None
    rowset_name: str | None = None
    agg_dedup_grain: frozenset[str] = frozenset()
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
    group_graph: nx.DiGraph = field(default_factory=nx.DiGraph)
    group_attrs: dict[str, GroupAttrs] = field(default_factory=dict)
    concept_attrs: dict[str, ConceptAttrs] = field(default_factory=dict)
    strategy_node: StrategyNode | None = None

    def copy(self) -> "BuildInfo":
        return BuildInfo(
            concept_graph=self.concept_graph.copy(),
            group_graph=self.group_graph.copy(),
            group_attrs={k: _copy_attrs(v) for k, v in self.group_attrs.items()},
            concept_attrs={
                k: _copy_concept_attrs(v) for k, v in self.concept_attrs.items()
            },
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
        dedup_grain=a.dedup_grain,
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
        agg_dedup_grain=a.agg_dedup_grain,
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

    depth_label: str
    derivation: str
    grain_components: frozenset[str]
    # primary/secondary members are concept ADDRESSES — what downstream
    # strategy assembly cares about. primary_node_ids holds the matching
    # concept-graph node ids (which differ from addresses for any non-blank
    # phase/label), keyed parallel to primary_members.
    primary_members: list[str] = field(default_factory=list)
    primary_node_ids: list[str] = field(default_factory=list)
    secondary_members: list[str] = field(default_factory=list)
    member_depths: dict[str, str] = field(default_factory=dict)
    label: str = ""
    # Optional disambiguator for rules that produce multiple buckets sharing
    # the (label, derivation, depth, grain) tuple — e.g. BASIC's signature
    # split, which can land two co-grain buckets with disjoint upstream
    # sources. Empty string for rules that don't need it.
    discriminator: str = ""
    # Grain to reduce this aggregate's input to before aggregating (a count of
    # distinct entities); empty when no reduction is needed. See GroupAttrs.
    dedup_grain: frozenset[str] = frozenset()
