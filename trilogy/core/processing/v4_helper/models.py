from dataclasses import dataclass, field
from functools import cached_property
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
from .edges import EdgeMap


def nulls_grouping_keys(mode: AggregateGroupingMode | None) -> bool:
    """Whether a group written with this GROUP BY mode NULLs its own grouping
    keys on some of the rows it emits.

    ROLLUP/CUBE/GROUPING SETS add subtotal and grand-total rows whose rolled-up
    key columns are NULL. Every consumer above such a group must treat those
    keys as unusable: a WHERE, a join axis, or a re-aggregation keyed on them
    silently drops the subtotals. This is the single question the planner
    should ask, never "what does the group id string look like"."""
    return mode is not None and mode.nulls_grouping_keys


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
    required_grain: frozenset[str] = frozenset()
    preserve_keys: frozenset[str] = frozenset()
    channel: InputChannel = InputChannel.ROW_STREAM
    may_project_dimension: bool = False


@dataclass(frozen=True)
class ExtentOwnership:
    """Which group manufactures the extension rows of each ``~``-licensed span.

    Elected on the group graph before any node is built (see
    ``extent_ownership.elect_extent_owners``) and carried on the FINAL sink's
    attrs, because extent routing is a statement-level decision: exactly one
    group per span carries that dimension's unmatched members, its ancestors
    may pad on the way there, and every other group joins on solid keys.
    """

    spans: frozenset[str] = frozenset()
    owner_by_span: dict[str, str] = field(default_factory=dict)
    # gid -> spans that group may extend (it owns them, or an owner is downstream)
    permitted: dict[str, frozenset[str]] = field(default_factory=dict)
    # address -> the region domain group carrying it: on an extension row only
    # that group has the member's value
    carried: dict[str, str] = field(default_factory=dict)

    def permitted_for(self, gid: str) -> frozenset[str]:
        return self.permitted.get(gid, frozenset())

    def suppressed_for(self, gid: str) -> frozenset[str]:
        return self.spans - self.permitted_for(gid)

    def suppressed_carried_for(self, gid: str) -> dict[str, frozenset[str]]:
        """address -> the spans `gid` may not extend whose domain carries it."""
        suppressed = self.suppressed_for(gid)
        out: dict[str, frozenset[str]] = {}
        for address, domain in self.carried.items():
            spans = frozenset(
                span
                for span, owner in self.owner_by_span.items()
                if owner == domain and span in suppressed
            )
            if spans and domain != gid:
                out[address] = spans
        return out

    def owner_of(self, address: str) -> str | None:
        return self.owner_by_span.get(address) or self.carried.get(address)


@dataclass(frozen=True)
class Completion:
    """A source holding only SOME rows of a region, beside sources holding the
    rest (``returns`` beside ``lines``; ``web_orders`` beside ``store_orders``).
    ``spans`` are the ``~`` keys that say so. ``emptied_by`` are the concepts
    the WHERE null-rejects that only this source supplies: no entity is absent
    on the rows it lacks, but none of them survives the statement."""

    source: str
    spans: frozenset[str]
    emptied_by: frozenset[str] = frozenset()


@dataclass(frozen=True)
class Region:
    """One kind of row a plan can return: the entity keys PRESENT on it.

    ``spans`` are the ``~`` bindings that keep this kind of row from being
    absorbed into a larger one (a customer no order references); empty for the
    base region. ``completes`` are the ``~`` keys of a source the plan needs
    that holds only some of this region's rows (``returns`` beside ``lines``):
    no entity is absent, but the rest still have to come from somewhere.
    ``emptied_by`` are the concepts the plan's WHERE null-rejects that are
    ABSENT here, so no row of this kind survives the statement."""

    present: frozenset[str]
    spans: frozenset[str] = frozenset()
    completes: frozenset[str] = frozenset()
    sources: frozenset[str] = frozenset()
    emptied_by: frozenset[str] = frozenset()
    # every source whose rows ARE this kind of row
    witnesses: frozenset[str] = frozenset()
    completions: tuple[Completion, ...] = ()

    @property
    def is_extension(self) -> bool:
        return bool(self.spans)

    @property
    def has_own_rows(self) -> bool:
        """Some source's rows ARE this kind of row and no larger source holds
        them all: the unmatched members of a dimension. A region kept only by a
        completion (a partial aggregate table beside its fact) has none: every
        row of it is a row of the larger source, where nothing is absent."""
        return bool(self.sources)

    @property
    def is_empty(self) -> bool:
        return bool(self.emptied_by)

    def describe(self) -> str:
        body = "{" + ", ".join(sorted(self.present)) + "}"
        if self.spans:
            body += f" ~{sorted(self.spans)}"
        if self.completes:
            body += f" completes {sorted(self.completes)}"
        if self.emptied_by:
            body += f" EMPTY by {sorted(self.emptied_by)}"
        return body


@dataclass(frozen=True)
class Keyspace:
    """A plan's row universe (``keyspace.build_keyspace``): disjoint regions
    over the requested entity keys. A concept is DEFINED on a region when every
    one of its keys is present there; elsewhere it is absent, which renders as
    NULL but is not a NULL value."""

    entities: frozenset[str] = frozenset()
    regions: tuple[Region, ...] = ()
    # requested concept address -> the entity keys it is a function of
    keys_by_address: dict[str, frozenset[str]] = field(default_factory=dict)
    outputs: tuple[str, ...] = ()
    # span -> the entities a keyed lookup from that span alone arrives at
    span_reach: dict[str, frozenset[str]] = field(default_factory=dict)

    @property
    def extensions(self) -> tuple[Region, ...]:
        return tuple(r for r in self.regions if r.is_extension)

    @property
    def live_regions(self) -> tuple[Region, ...]:
        return tuple(r for r in self.regions if not r.is_empty)

    @property
    def demanded_spans(self) -> frozenset[str]:
        return frozenset().union(*(r.spans for r in self.live_regions))

    @cached_property
    def in_play_spans(self) -> frozenset[str]:
        """Every span a join of this plan can pad for. An emptied region still
        counts: its rows are gone once the WHERE has run, and a merge below
        that point still sees their padding."""
        return frozenset().union(*(r.spans | r.completes for r in self.regions))

    @cached_property
    def output_demanded_spans(self) -> frozenset[str]:
        """What the extent election asks: the spans whose unmatched members
        carry an OUTPUT, one that is a function of what the span alone reaches."""
        in_play: frozenset[str] = frozenset().union(
            *(r.spans | r.completes for r in self.live_regions)
        )
        return frozenset(
            span
            for span in in_play
            if any(
                self.keys_by_address.get(o)
                and self.keys_by_address[o] <= self.span_reach.get(span, frozenset())
                for o in self.outputs
            )
        )

    def binding_is_complete(self, source: str, span: str) -> bool:
        """Does ``source``'s ``~`` on ``span`` cost this plan nothing? The
        binding says the source lacks some of the key's members. Those live on
        the regions other sources witness, and on the rows of its own region it
        holds no match for; when the WHERE empties every one of them, the
        source is complete for this plan. False when the span is not in play."""
        if span not in self.in_play_spans:
            return False
        for region in self.regions:
            if region.is_empty:
                continue
            if span in region.spans and source not in region.witnesses:
                return False
            if any(
                c.source == source and span in c.spans and not c.emptied_by
                for c in region.completions
            ):
                return False
        return True

    def defined_on(self, address: str, region: Region) -> bool:
        return self.keys_by_address.get(address, frozenset()) <= region.present

    def absent_regions(self, address: str) -> tuple[Region, ...]:
        return tuple(r for r in self.live_regions if not self.defined_on(address, r))

    def region_of(self, spans: frozenset[str]) -> Region | None:
        return next((r for r in self.regions if r.spans == spans), None)

    def carried_on(self, address: str, region: Region) -> bool:
        """Does an extension row of ``region`` hold a value for ``address``: it
        is keyed on what a lookup from the region's spans reaches. An entity
        merely cross-joined onto the region is present, but not carried."""
        keys = self.keys_by_address.get(address, frozenset())
        reach: frozenset[str] = frozenset().union(
            *(self.span_reach.get(span, frozenset()) for span in region.spans)
        )
        return bool(keys) and keys <= reach

    def describe(self) -> str:
        return " | ".join(r.describe() for r in self.regions)


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
    nx.DiGraph node attributes: the graph stays as topology + edge metadata
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
    # Members merged onto this group from a coarser-input-grain sibling bucket
    # (count-of-a-key over a finer row stream): render COUNT(DISTINCT ...)
    # instead of dedup-then-COUNT.
    aggregate_distinct_addrs: frozenset[str] = frozenset()
    # Atoms (BoolExpr) applied AT this group. A clause like
    # `state='TN' AND year=2000` is decomposed and each atom finds its own
    # highest-allowed group independently, so a single clause may live at
    # multiple groups, or one group may collect atoms from several clauses.
    condition_atoms: list[BoolExpr] = field(default_factory=list)
    # Conjunctive siblings of an atom hosted here, delivered so an aggregate
    # RECOMPUTE filters its input by the full clause. Kept apart from
    # `condition_atoms`: the builder folds them in only when the group
    # genuinely recomputes over rows (a twin-reused value is read through,
    # and the extra WHERE would resurrect the redundant fact-rescan parent).
    conjunction_atoms: list[BoolExpr] = field(default_factory=list)
    # How this group's GROUP BY is written. Non-STANDARD modes NULL-inject
    # their grouping keys on the subtotal rows they add, which is what
    # `nulls_grouping_keys` exists to ask about.
    grouping_mode: AggregateGroupingMode = AggregateGroupingMode.STANDARD
    # Populated by `_compute_concept_sets`. Empty tuples until then.
    output_concepts: tuple[str, ...] = ()
    input_concepts: tuple[str, ...] = ()
    # Populated only for FINAL: the logical output/grain contract Stage 3
    # physically satisfies or prunes, and the statement's extent routing.
    final_contract: FinalAssemblyContract | None = None
    extent_ownership: ExtentOwnership | None = None
    # Set on a ROOT group that exists only to carry one extension region's own
    # rows (`group_graph._add_region_domain_buckets`): the region's spans.
    extent_spans: frozenset[str] = frozenset()
    # Set on a single-entity dimension ROOT group: the entity's key(s).
    dim_keys: frozenset[str] = frozenset()
    # Populated for non-FINAL groups after `_compute_concept_sets`.
    input_contracts: tuple[GroupInputContract, ...] = ()

    @property
    def nulls_grouping_keys(self) -> bool:
        return nulls_grouping_keys(self.grouping_mode)


@dataclass
class ConceptAttrs:
    """Strongly-typed per-concept-node state. Like `GroupAttrs`, lives in a
    side dict (``dict[str, ConceptAttrs]``) keyed by concept-graph node id
    rather than on the nx.DiGraph node attributes: the graph stays as
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
    # True for a COUNT whose argument's value IS the key carrying the
    # aggregate's residual input grain (count of a key, or of a FILTER over a
    # key). Such a count may share a finer-grain sibling input stream by
    # rendering COUNT(DISTINCT ...) instead of dedup-then-COUNT.
    aggregate_distinct_rewritable: bool = False
    keys: frozenset[str] = frozenset()
    # For a ROOT whose declared keys the query never names: the KEY roots that
    # jointly determine it through the environment's FD closure (a dimension
    # behind a foreign key the fact binds). `keys` is one FD step; this is the
    # chain.
    determining_key_roots: frozenset[str] = frozenset()
    # Addresses this concept answers for under another identity (scoped-join
    # canonical collapse, `merge into`): lets grouping relate a property root
    # to its key root when the key was collapsed onto a different address.
    pseudonyms: frozenset[str] = frozenset()
    # For ROOT-leaf concepts only: identifiers of the datasources that bind
    # this concept as an output column. Two roots sharing a binding sit on one
    # physical row stream even when the demanded lineage graph never relates
    # them (a fact FK column aliased beside a fact property has one BASIC per
    # root and no shared consumer).
    datasource_bindings: frozenset[str] = frozenset()
    # True for a pure rename (``alias(...)`` lineage), a pseudonym of its
    # source. The renderer resolves it transparently to the source column, so
    # it must not be folded into a rollup group like a genuine transform dim.
    is_rename: bool = False
    # Tagged post-build for a concept that appears ONLY as an existence arg
    # (semijoin RHS) and never as a row arg; `partition_roots` places such a
    # node in its own scan bucket (side-channel subselect source).
    existence_only: bool = False

    @property
    def keys_are_conditional_fd(self) -> bool:
        """True when `keys` do NOT functionally determine this concept's value.

        An empty-grain FILTER virtual renders as `CASE WHEN pred THEN content
        END`, which varies with predicate inputs the keys never capture, and a
        parent-sourced copy gets no MAX collapse (`filter_collapses_to_grain`
        needs an empty source_map). Nothing may be PROVEN through such a
        concept's keys; it can still ride a grouping via the lineage-parents
        rule once its content and predicate inputs are all determined."""
        return not self.grain_components and self.derivation == Derivation.FILTER


@dataclass
class BuildInfo:
    """Result bundle for discovery: the raw concept graph, the grouped graph,
    per-group attributes, and the materialized StrategyNode produced by
    walking the group graph."""

    concept_graph: nx.DiGraph = field(default_factory=nx.DiGraph)
    group_graph: nx.DiGraph = field(default_factory=nx.DiGraph)
    group_attrs: dict[str, GroupAttrs] = field(default_factory=dict)
    concept_attrs: dict[str, ConceptAttrs] = field(default_factory=dict)
    # Typed edge-metadata side maps, one per graph above (the graphs themselves
    # carry only topology).
    concept_edges: EdgeMap = field(default_factory=dict)
    group_edges: EdgeMap = field(default_factory=dict)
    strategy_node: StrategyNode | None = None
    keyspace: Keyspace = field(default_factory=Keyspace)

    def copy(self) -> "BuildInfo":
        """Only the strategy node is mutated downstream; the graphs and
        attribute maps are read-only after build and shared."""
        return BuildInfo(
            concept_graph=self.concept_graph,
            group_graph=self.group_graph,
            group_attrs=self.group_attrs,
            concept_attrs=self.concept_attrs,
            concept_edges=self.concept_edges,
            group_edges=self.group_edges,
            strategy_node=self.strategy_node.copy() if self.strategy_node else None,
            keyspace=self.keyspace,
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
    # primary/secondary members are concept ADDRESSES, which is what downstream
    # strategy assembly cares about. primary_node_ids holds the matching
    # concept-graph node ids (which differ from addresses for any non-blank
    # phase/label), keyed parallel to primary_members.
    primary_members: list[str] = field(default_factory=list)
    primary_node_ids: list[str] = field(default_factory=list)
    secondary_members: list[str] = field(default_factory=list)
    member_depths: dict[str, DepthLabel] = field(default_factory=dict)
    label: str = ""
    # Optional disambiguator for rules that produce multiple buckets sharing
    # the (label, derivation, depth, grain) tuple, e.g. BASIC's signature
    # split, which can land two co-grain buckets with disjoint upstream
    # sources. Empty string for rules that don't need it.
    discriminator: str = ""
    # Grain to normalize this aggregate's inputs to before aggregating.
    aggregate_input_grain: frozenset[str] = frozenset()
    # Member addresses to render COUNT(DISTINCT ...), merged in from a
    # coarser-input-grain sibling whose dedup folds into the aggregate.
    aggregate_distinct_addrs: set[str] = field(default_factory=set)
    # SEMANTICS of this group's GROUP BY, as opposed to `discriminator`, which
    # only exists to keep distinct buckets at distinct group ids. Ask
    # `nulls_grouping_keys`, never the id string.
    grouping_mode: AggregateGroupingMode = AggregateGroupingMode.STANDARD
    extent_spans: frozenset[str] = frozenset()
    dim_keys: frozenset[str] = frozenset()

    @property
    def nulls_grouping_keys(self) -> bool:
        return nulls_grouping_keys(self.grouping_mode)

    @property
    def group_id(self) -> str:
        grain_key = "|".join(sorted(self.grain_components)) or "∅"
        label_prefix = f"[{self.label}]" if self.label else ""
        suffix = f":{self.discriminator}" if self.discriminator else ""
        return (
            f"grp:{label_prefix}{self.derivation.value}:{self.depth_label.value}:"
            f"{grain_key}{suffix}"
        )
