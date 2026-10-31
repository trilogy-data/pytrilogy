"""Stage 3/4 materialization for v4 discovery.

Stage 3 walks the group graph in topological order, hands each group's
already-built parents to its v4 generator, and stashes the resulting node.
ROOT groups delegate concrete datasource selection to `source_planning`.

Stage 4 assembles the FINAL sink by merging the minimum materialized
contributors that cover the mandatory outputs, then applies final-only
filters and output-grain deduping.

Parents are explicit, derived from the group graph's lineage edges;
generator dispatch lives in `v4_node_generators.dispatch.build_node`."""

import logging
from collections import Counter, defaultdict
from collections.abc import Callable, Collection, Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import replace as dc_replace
from datetime import date, datetime
from functools import partial
from typing import cast

from trilogy.constants import MagicConstants, logger
from trilogy.core import graph as nx
from trilogy.core.constants import ALL_ROWS_ADDRESS, rowset_alias_prefix
from trilogy.core.enums import (
    ComparisonOperator,
    Derivation,
    FunctionType,
    JoinType,
    Ordering,
    Purpose,
    WindowType,
)
from trilogy.core.exceptions import UnbuiltGroupException, UnresolvableQueryException
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import (
    BoolExpr,
    BuildAggregateWrapper,
    BuildCaseWhen,
    BuildComparison,
    BuildConcept,
    BuildConceptArgs,
    BuildDatasource,
    BuildExpr,
    BuildFilterItem,
    BuildFunction,
    BuildGrain,
    BuildNumberingWindowItem,
    BuildOrderItem,
    BuildRowsetItem,
    BuildWhereClause,
    LooseBuildConceptList,
    generate_concept_name,
    nonstandard_grouping_lineage,
)
from trilogy.core.models.build_environment import BuildEnvironment, SpanScope
from trilogy.core.models.core import arg_to_datatype
from trilogy.core.models.execute import Join
from trilogy.core.models.keyspace import Keyspace
from trilogy.core.processing import plan_trace
from trilogy.core.processing.aggregate_rollup import (
    _is_rollable_aggregate,
    finer_than_grain,
    pending_rollups,
)
from trilogy.core.processing.condition_utility import (
    combine_condition_atoms,
    condition_implies,
    decompose_condition,
)
from trilogy.core.processing.discovery_utility import raise_if_disconnected_for
from trilogy.core.processing.grain_utility import non_null_proofs
from trilogy.core.processing.node_generators.presence_probe import (
    axis_scalar_reads,
    is_presence_probe,
)
from trilogy.core.processing.nodes import (
    FilterNode,
    GroupNode,
    MergeNode,
    RowsetNode,
    SelectNode,
    StrategyNode,
    UnionNode,
    WindowNode,
)
from trilogy.core.processing.nodes.base_node import (
    NodeJoin,
    get_all_parent_partial,
    readable_outputs,
    region_reads,
)
from trilogy.utility import unique

from .concept_graph import (
    _relation_mates,
    _statement_scoped_relation_members,
    renamed_coalescing_relation,
    through_renames,
)
from .condition_injection import (
    ConditionSources,
    condition_row_args,
    inject_condition_at_node,
)
from .condition_placement import accumulated_atoms_above, feeder_conditions_implied
from .constants import (
    AGGREGATING_DERIVATIONS,
    FINAL_NODE_ID,
    GROUPING_DERIVATIONS,
    ROW_PRESERVING_AGGREGATE_INPUT_DERIVATIONS,
    ROW_SHAPE_BARRIER_DERIVATIONS,
    DepthLabel,
    EdgeKind,
)
from .edges import (
    EdgeAttrs,
    EdgeMap,
    dependency_subgraph,
    edge_kind,
    lineage_predecessors,
    remove_edge,
)
from .extent_ownership import takes_a_value_on_padding
from .final_cover import elect_cover, output_covers, record_twin_reuse
from .functional_dependency import (
    build_fd_closure,
    build_fd_determines,
    build_fd_determines_all,
)
from .group_graph import read_row_parents, row_parents, trace_group_graph
from .history import V4History
from .join_plan import JoinPlan, elect_bridges, plan_final_join
from .models import (
    ExtentOwnership,
    FinalAssemblyContract,
    GroupAttrs,
    InputChannel,
)
from .network_coalescing import axis_arms_delivered
from .projection import (
    concept_satisfiable,
    filter_row_predicate,
    iter_row_lineage,
    lineage_crosses_row_shape_barrier,
    literal_producible,
    parent_output_addresses,
    renderable_addresses,
    row_lineage_arguments,
    satisfiable_from,
    satisfiable_outputs,
    statement_filter_population,
    widen_projection,
)
from .region_reads import inline_arguments_taking_a_value, nameable
from .renders import declared_partial, parents_render, renders
from .source_planning import SourceRequest, plan_source, scan_regroups


@dataclass
class ParentBuild:
    group_id: str
    node: StrategyNode


@dataclass(frozen=True)
class RootRequest:
    """The question a ROOT group's node answers: what the build asked for, the
    WHERE it asked under and the scope. A ROOT re-sources from datasources, so
    its parents are no part of it. Two equal requests have one answer, so a
    group, a consumer slice or the FINAL asking it again reads the built node
    instead of planning it again."""

    outputs: frozenset[str]
    conditions: BuildWhereClause | None
    scope: SpanScope
    # the ancestors' atoms a ROOT re-applies to the rows it re-sources
    preexisting: BuildWhereClause | None = None
    # the rows feed only aggregates (`SourceRequest.arm_local`)
    arm_local: bool = False

    def answered_by(self, node: StrategyNode, asked: "RootRequest") -> bool:
        """Whether `node`, built for `asked`, already answers this request:
        it binds every output fully, and no proper subset of its scans would
        (planning afresh would prune a join the wider question dragged in)."""
        partial = {c.address for c in node.partial_concepts}
        if not (
            self.conditions == asked.conditions
            and self.preexisting == asked.preexisting
            and self.scope == asked.scope
            and self.outputs <= {c.address for c in node.output_concepts} - partial
        ):
            return False
        return not _strict_leaf_subset_binds(node, set(self.outputs))


@contextmanager
def under_span_scope(environment: BuildEnvironment, scope: SpanScope) -> Iterator[None]:
    """Plan under `scope`, restoring the environment's own afterwards."""
    outer = environment.span_scope
    environment.span_scope = scope
    try:
        yield
    finally:
        environment.span_scope = outer


def _concept_at(environment: BuildEnvironment, address: str) -> BuildConcept | None:
    """Resolve a (possibly pseudonym) group-member address to its concept.

    A derivable pseudonym address (e.g. the struct field `unnest_array.a`)
    resolves through `environment.concepts` to its *canonical* concept (`local.a`,
    lineage None), not the attr-access origin that actually computes it. Resolve
    such synonyms through `alias_origin_lookup` so the strategy builder builds
    the field's projection instead of a dead-end key.
    Only the exact-address match from `concepts` is trusted; otherwise the
    origin (whose `.address` equals the requested pseudonym) wins."""
    concept = environment.concepts.get(address)
    if concept is not None and concept.address == address:
        return concept
    origin = environment.alias_origin_lookup.get(address)
    if origin is not None:
        return origin
    return concept


def _wrap_atoms(atoms: list[BoolExpr]) -> BuildWhereClause | None:
    """AND-combine a list of condition atoms into a single BuildWhereClause."""
    if not atoms:
        return None
    combined = combine_condition_atoms(atoms)
    if combined is None:
        return None
    return BuildWhereClause(conditional=combined)


def _root_atoms_satisfiable_from(
    atoms: list[BoolExpr],
    concepts: list[BuildConcept],
) -> list[BoolExpr]:
    available = {concept.address for concept in concepts}
    return [
        atom
        for atom in atoms
        if all(concept_satisfiable(arg, available) for arg in atom.row_arguments)
    ]


def _own_members(attrs: dict[str, GroupAttrs], gid: str) -> set[str]:
    """What the group computes and the keys it holds. Not what it emits: a
    ROOT's re-source at FINAL asks which merge keys are its own, and a hidden
    pass-through is not one."""
    return set(attrs[gid].members)


def _atoms_at(attrs: dict[str, GroupAttrs], gid: str) -> list[BoolExpr]:
    """Atoms injected AT `gid` only. These become the WHERE for this node."""
    return list(attrs[gid].condition_atoms)


def _dedupe_arg_groups(
    groups: list[tuple[BuildConcept, ...]],
) -> list[tuple[BuildConcept, ...]]:
    out: list[tuple[BuildConcept, ...]] = []
    seen: set[tuple[str, ...]] = set()
    for group in groups:
        key = tuple(concept.address for concept in group)
        if key and key not in seen:
            seen.add(key)
            out.append(group)
    return out


def _flatten_arg_groups(
    groups: list[tuple[BuildConcept, ...]],
) -> list[BuildConcept]:
    out: list[BuildConcept] = []
    seen: set[str] = set()
    for group in groups:
        for concept in group:
            if concept.address not in seen:
                seen.add(concept.address)
                out.append(concept)
    return out


def _lineage_existence_arg_groups(
    concepts: Sequence[BuildConcept | None],
) -> list[tuple[BuildConcept, ...]]:
    """Existence arg groups reachable through the lineage of `concepts`.

    Each comparison's RHS stays one tuple: a composite membership renders
    against a single subselect source, so the tuple, not the address, is the
    unit of sourcing. Flattening would let a pair be fed from two independent
    dimension groups (a cross product, not co-occurrence).

    A FILTER with a semijoin where is often inlined into the BASIC concept
    that wraps it rather than built as its own node, so the existence arg can
    live a few lineage hops down."""
    out: list[tuple[BuildConcept, ...]] = []
    visited: set[str] = set()
    stack = list(concepts)
    while stack:
        concept = stack.pop()
        if concept is None or concept.address in visited:
            continue
        visited.add(concept.address)
        if isinstance(concept.lineage, BuildFilterItem):
            out.extend(concept.lineage.where.existence_arguments or ())
        # A BASIC concept whose lineage is (or wraps) a membership comparison
        # (`x in <set>`, e.g. a projected `x in set as flag`) carries the set
        # as a direct existence arg that needs a feeder too.
        elif isinstance(concept.lineage, BuildConceptArgs):
            out.extend(concept.lineage.existence_arguments or ())
        if concept.lineage is not None:
            stack.extend(concept.lineage.concept_arguments)
    return _dedupe_arg_groups(out)


def _deep_copy_node(node: StrategyNode) -> StrategyNode:
    """`copy()` shallow-shares `parents`; this recursively copies the whole
    subtree so the result shares no node object with the original tree."""
    clone = node.copy()
    clone.parents = [_deep_copy_node(p) for p in node.parents]
    return clone


class _CleanFeederCache:
    """Builds a standalone source for an existence (`IN <subselect>`) arg group,
    independent of the already-built strategy tree: the one nested planner for
    a set the group graph holds no built provider for.

    The group graph materializes a set's lineage as groups and orders them
    before the host, so the provider is normally built. It is not when the
    only group producing the set is a lineage descendant of its own consumer
    (a self-referential membership whose filter group reads the
    membership-conditioned ROOT: wiring it would form a cycle), or when the
    host is a FINAL-time re-source. The set Y in `X in Y` is by definition the
    unfiltered set, so it is re-sourced from its own lineage (no outer
    conditions) once and shared. Cached per arg group; returns independent
    copies so each consumer owns its parent pointer.

    A multi-component group is searched as one unit, so the feeder is the
    tuple's co-occurrence island (as `search_parent(existence_args)` builds on
    the plain-`where` path) rather than a per-component source."""

    def __init__(
        self,
        environment: BuildEnvironment,
        g: ReferenceGraph,
        history: V4History,
    ) -> None:
        self._environment = environment
        self._g = g
        self._history = history
        self._cache: dict[tuple[str, ...], StrategyNode | None] = {}

    def get(self, group: tuple[BuildConcept, ...]) -> StrategyNode | None:
        key = tuple(sorted({concept.address for concept in group}))
        if key not in self._cache:
            self._cache[key] = self._build(group)
        node = self._cache[key]
        return node.copy() if node is not None else None

    def _build(self, group: tuple[BuildConcept, ...]) -> StrategyNode | None:
        # Imported lazily: `concept_strategies_v4` imports this module's package.
        from trilogy.core.processing.concept_strategies_v4 import search_concepts

        v4_history = self._history
        addresses = {concept.address for concept in group}
        if len(group) == 1 and not isinstance(group[0].lineage, BuildFilterItem):
            # A single-column set can be widened to its keys: the extra columns
            # only shape the feeder's grain. A tuple must not be widened; an
            # extra column would change which rows the subselect projects. Nor
            # a filter: shown alone its predicate narrows the rows, where
            # beside a key it is a CASE whose NULL would be a member.
            addresses |= set(group[0].keys or set())
        search = [
            self._environment.concepts[address]
            for address in sorted(addresses)
            if address in self._environment.concepts
        ]
        if not search:
            return None
        info = search_concepts(
            mandatory_list=search,
            history=V4History(
                base_environment=v4_history.base_environment,
                build_caches=v4_history.build_caches,
            ),
            environment=self._environment,
            depth=1,
            g=self._g,
            conditions=[],
        )
        node = info.strategy_node
        if node is not None and len(group) > 1:
            # Side-channel-only: slice to the subselect's columns so a shared
            # extra output can't promote the feeder to a row-join candidate
            # (mirrors `resolve_existence_sources`).
            sliced = [o for o in node.output_concepts if o.address in addresses]
            if sliced and len(sliced) < len(node.output_concepts):
                node.set_output_concepts(sliced)
        return node


def _covering_built_node(
    addresses: set[str],
    built: dict[str, StrategyNode],
    skip: StrategyNode | None,
    preferred: Sequence[str],
) -> StrategyNode | None:
    """The first built group able to supply EVERY address of an arg group,
    the host's existence-edge predecessors first. A composite membership
    renders as one subselect, so a node covering only part of the tuple is
    not a candidate."""
    ordered = [gid for gid in preferred if gid in built]
    ordered += [gid for gid in built if gid not in preferred]
    for gid in ordered:
        source_node = built[gid]
        if skip is not None and source_node is skip:
            continue
        if addresses <= {o.address for o in source_node.output_concepts}:
            return source_node
    return None


def _feeder_at_set_grain(
    feeder: StrategyNode, group: tuple[BuildConcept, ...]
) -> StrategyNode:
    """Project a provider down to a semijoin RHS: the subselect's columns
    only, one row per set value. The slice keeps a shared extra output from
    promoting the feeder to a row-join candidate; the group dedups a provider
    built at a finer grain than the set. `GroupNode` elides itself when the
    provider is already at the set's grain."""
    addresses = {concept.address for concept in group}
    members = [o for o in feeder.output_concepts if o.address in addresses]
    return GroupNode(
        output_concepts=members,
        input_concepts=members,
        environment=feeder.environment,
        parents=[feeder],
        partial_concepts=[c for c in feeder.partial_concepts if c.address in addresses],
        nullable_concepts=[
            c for c in feeder.nullable_concepts if c.address in addresses
        ],
    )


def _existence_parents_for(
    arg_groups: list[tuple[BuildConcept, ...]],
    built: dict[str, StrategyNode],
    skip: StrategyNode | None = None,
    feeder_cache: "_CleanFeederCache | None" = None,
    preferred: Sequence[str] = (),
) -> list[StrategyNode]:
    existence_parents: list[StrategyNode] = []
    for group in arg_groups:
        addresses = {concept.address for concept in group}
        source_node = _covering_built_node(addresses, built, skip, preferred)
        if source_node is None:
            # No built provider: a tuple whose components only exist on
            # separate built groups (each dimension enriched independently;
            # wiring them would test a dimension cross product rather than
            # pairs present on the fact), or a set the graph never built for
            # this host. The standalone feeder is the source.
            if feeder_cache is not None:
                feeder = feeder_cache.get(group)
                if feeder is not None:
                    existence_parents.append(feeder)
            continue
        # `copy()` shallow-shares parents, so a candidate whose subtree
        # contains `skip` would wire `skip -> candidate -> ... -> skip`, a
        # row-stream cycle that recurses forever in `resolve()`. The set in
        # `X in <candidate>` is the unfiltered set, so re-source it standalone
        # (no outer conditions) and share that acyclic feeder. Fall back to a
        # deep copy of the conditioned subtree only when no standalone feeder
        # can be built: still acyclic, just verbose.
        is_cyclic = skip is not None and any(
            n is skip for n in _strategy_nodes(source_node)
        )
        if is_cyclic and feeder_cache is not None:
            feeder = feeder_cache.get(group)
            if feeder is not None:
                existence_parents.append(feeder)
                continue
        # one slice per membership: each projects only its own set's columns
        feeder = _deep_copy_node(source_node) if is_cyclic else source_node.copy()
        existence_parents.append(_feeder_at_set_grain(feeder, group))
    return existence_parents


def _condition_existence_arg_groups(
    condition: BoolExpr | None,
) -> list[tuple[BuildConcept, ...]]:
    if condition is None:
        return []
    return _dedupe_arg_groups(list(condition.existence_arguments))


def _node_existence_arg_groups(node: StrategyNode) -> list[tuple[BuildConcept, ...]]:
    # A membership the node computes itself (`x in <set> as flag` projected
    # alongside an aggregate) needs the subselect feeder wired HERE: the group
    # sweep attaches it to `built[gid]`, but a consumer took its copy of that
    # node before the attach ran, so only the assembled tree can see which node
    # actually renders the comparison.
    return _dedupe_arg_groups(
        _condition_existence_arg_groups(node.conditions)
        + _lineage_existence_arg_groups(list(node.output_concepts))
    )


def _iter_strategy_nodes(root: StrategyNode) -> Iterator[StrategyNode]:
    """`root` and every node above it, each visited once. Lazy, so an `any()`
    over it stops at the first match instead of walking the whole tree."""
    seen: set[int] = set()
    stack = [root]
    while stack:
        node = stack.pop()
        if id(node) in seen:
            continue
        seen.add(id(node))
        yield node
        stack.extend(node.parents)


def _strategy_nodes(root: StrategyNode) -> list[StrategyNode]:
    return list(_iter_strategy_nodes(root))


def _feeds_only_aggregates(
    group_graph: nx.DiGraph, attrs: dict[str, GroupAttrs], gid: str
) -> bool:
    successors = list(group_graph.successors(gid))
    return bool(successors) and all(
        attrs[succ].derivation == Derivation.AGGREGATE for succ in successors
    )


def _arm_local(readers: "_Plan", gid: str, arms_delivered: bool) -> bool:
    """Whether a ROOT's scan may read a coalescing axis off one arm: its rows
    feed only aggregates in the plan before any fold."""
    return arms_delivered or _feeds_only_aggregates(
        readers.group_graph, readers.attrs, gid
    )


def _leaf_datasources(node: StrategyNode) -> dict[str, BuildDatasource]:
    """The concrete datasources scanned in this subtree: its physical join
    footprint. Used to decide whether a per-consumer ROOT re-slice genuinely
    prunes a join or merely re-derives the same conditioned scan."""
    return {
        n.datasource.identifier: n.datasource
        for n in _strategy_nodes(node)
        if isinstance(n, SelectNode) and n.datasource is not None
    }


def _leaf_datasource_ids(node: StrategyNode) -> set[str]:
    return set(_leaf_datasources(node))


def _strict_leaf_subset_binds(node: StrategyNode, addresses: set[str]) -> bool:
    """Whether some proper subset of the subtree's scans could bind every
    address by column: a re-slice can only prune a join when one exists."""
    binders: dict[str, set[str]] = {address: set() for address in addresses}
    leaves = _leaf_datasources(node)
    for identifier, datasource in leaves.items():
        for column in datasource.columns:
            for address in column.concept.equivalent_addresses & addresses:
                binders[address].add(identifier)
    if any(not bound for bound in binders.values()):
        return False
    essential = {next(iter(bound)) for bound in binders.values() if len(bound) == 1}
    return essential < set(leaves)


def _attach_existence_to_node(
    node: StrategyNode,
    arg_groups: list[tuple[BuildConcept, ...]],
    built: dict[str, StrategyNode],
    feeder_cache: "_CleanFeederCache | None" = None,
    preferred: Sequence[str] = (),
) -> None:
    """Wire the SubselectComparison right sides as `existence_concepts` plus
    extra parents, rendered as a subselect lookup, not joined into the row
    stream.

    An arg group a parent already supplies is left alone: a nested plan's tree
    arrives here again under the OUTER plan's `built`, which holds no provider
    for its set. A group listed with no parent behind it is wired again: an
    earlier pass found no provider, and a later one may."""
    if not arg_groups:
        return
    existing_parent_outputs = {
        output.address for parent in node.parents for output in parent.output_concepts
    }
    existing_concepts = {concept.address for concept in node.existence_concepts}
    # a row parent supplying the set's address (`dx ? x in dx` reads dx on
    # the row) does not wire it: only a set listed here or on that parent
    listed = existing_concepts | {
        concept.address
        for parent in node.parents
        for concept in parent.existence_concepts
    }
    wired = existing_parent_outputs & listed
    arg_groups = [
        group for group in arg_groups if not {c.address for c in group} <= wired
    ]
    if not arg_groups:
        return
    node.existence_concepts = list(node.existence_concepts) + [
        concept
        for concept in _flatten_arg_groups(arg_groups)
        if concept.address not in existing_concepts
    ]
    node.parents = list(node.parents) + _existence_parents_for(
        arg_groups, built, skip=node, feeder_cache=feeder_cache, preferred=preferred
    )
    node.rebuild_cache()


def _wire_existence(
    node: StrategyNode,
    built: dict[str, StrategyNode],
    feeder_cache: "_CleanFeederCache | None",
    preferred: Sequence[str] = (),
) -> None:
    """Wire the subselect feeder of every membership hosted in `node`'s
    subtree. Generators host an `IN <set>` atom but never plan the set: the
    group graph holds its lineage and builds it before the host, so this runs
    as soon as a node exists and before any consumer copies it. Idempotent: a
    node already wired (a shared parent, a nested plan's own tree) is left
    alone."""
    for current in _strategy_nodes(node):
        _attach_existence_to_node(
            current,
            _node_existence_arg_groups(current),
            built,
            feeder_cache,
            preferred,
        )


def node_nulls_grouping_keys(node: StrategyNode) -> bool:
    """Whether this node emits ROLLUP/CUBE/GROUPING SETS rows: subtotal rows
    whose rolled-up key columns are NULL.

    The node-level companion to `nulls_grouping_keys`, and a two-sided
    contract: such a node passes through the FINAL without a dedup
    (`_group_to_grain_if_required`; a dedup would re-aggregate the subtotals
    away), so nothing may join back to it on a non-unique key, because no
    later dedup will absorb the duplicates. Both halves must ask this one
    question or they drift apart."""
    return any(
        nonstandard_grouping_lineage(o) is not None for o in node.output_concepts
    )


def _nonstandard_grouping_key_addresses(
    environment: BuildEnvironment, attrs: dict[str, GroupAttrs], gid: str
) -> set[str]:
    """Grouping-key addresses of this group's non-standard (ROLLUP/CUBE/
    GROUPING SETS) aggregate members, across the `by` spec and every grouping
    set."""
    out: set[str] = set()
    for addr in attrs[gid].primary_members:
        concept = _concept_at(environment, addr)
        lineage = nonstandard_grouping_lineage(concept) if concept else None
        if lineage is not None:
            out |= {b.address for b in lineage.by}
            for grouping_set in lineage.grouping_sets:
                out |= {b.address for b in grouping_set}
    return out


def _provider_feeds_other_grouping(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    provider: str,
    consumer: str,
) -> bool:
    return any(
        successor not in (consumer, FINAL_NODE_ID)
        and attrs[successor].derivation in GROUPING_DERIVATIONS
        for successor in group_graph.successors(provider)
    )


def _select_addresses(a: GroupAttrs) -> tuple[str, ...]:
    """A group the demand pass left without outputs projects every member."""
    return a.output_concepts or a.members


@dataclass
class _PlannedReads:
    """The fold read off stage-2 facts: what each group renders, and the
    derivations above it."""

    group_graph: nx.DiGraph
    group_edges: EdgeMap
    attrs: dict[str, GroupAttrs]
    rendered: dict[str, frozenset[str]]
    decided: set[str]
    environment: BuildEnvironment
    g: ReferenceGraph
    history: V4History
    # a ROOT's scan verdict: the search behind it is asked once per plan
    scans: dict[str, bool]
    # the plan before any fold, which a ROOT's build reads its readers off
    readers: "_Plan"
    arms_delivered: bool

    def has(self, gid: str) -> bool:
        return gid in self.rendered

    def outputs(self, gids: list[str]) -> set[str]:
        return set().union(*(self.rendered[gid] for gid in gids))

    def barrier(self, gid: str) -> bool:
        return any(
            self.attrs[g].derivation in _BARRIER_DERIVATIONS
            or (self.attrs[g].derivation == Derivation.ROOT and self._scan_groups(g))
            for g in {gid, *nx.ancestors(self.group_graph, gid)}
            if g in self.attrs
        )

    def _scan_groups(self, gid: str) -> bool:
        """Whether a ROOT's scan dedups its rows (`scan_regroups`)."""
        if gid not in self.scans:
            arm_local = _arm_local(self.readers, gid, self.arms_delivered)
            self.scans[gid] = self._scan(self.attrs[gid], arm_local)
        return self.scans[gid]

    def _scan(self, group: GroupAttrs, arm_local: bool) -> bool:
        return scan_regroups(
            SourceRequest(
                outputs=[
                    c
                    for a in _select_addresses(group)
                    if (c := _concept_at(self.environment, a)) is not None
                ],
                environment=self.environment,
                graph=self.g,
                history=self.history,
                conditions=_wrap_atoms(group.condition_atoms),
                arm_local=arm_local,
            )
        )

    def regroups(
        self,
        outputs: list[BuildConcept],
        gids: list[str],
        environment: BuildEnvironment,
    ) -> bool:
        from trilogy.core.processing.v4_node_generators.basic import (  # cycle
            grain_claim_unheld,
        )

        if len(gids) != 1:
            return False
        held = self.rendered[gids[0]]
        concepts = [c for a in sorted(held) if (c := _concept_at(environment, a))]
        grain = set(
            BuildGrain.from_concepts(concepts, environment=environment).components
        )
        by_address = {c.address: c for c in concepts}
        widened = list(outputs) + [
            by_address[k]
            for o in outputs
            if o.lineage is not None and o.grain is not None
            for k in sorted(o.grain.components)
            if k in by_address and k not in {c.address for c in outputs}
        ]
        return grain_claim_unheld(
            unique(widened, "address"), grain, set(held), environment
        )

    def twin_emits(
        self, attrs: dict[str, GroupAttrs], addresses: set[str], skip: str | None
    ) -> bool:
        return any(
            other != skip
            and other in self.rendered
            and attrs[other].depth_label == DepthLabel.D1
            and self._computes(other, addresses & self.rendered[other])
            for other in self.decided
        )

    def _computes(self, gid: str, addresses: set[str]) -> bool:
        """Whether `gid`'s node computes any of `addresses`: a row stream
        computes a member only off what its row parents render."""
        if self.attrs[gid].derivation in (Derivation.ROOT, *_BARRIER_DERIVATIONS):
            return bool(addresses)
        available = self.outputs(
            read_row_parents(self.group_graph, self.group_edges, self.attrs, gid)
        )
        return any(
            (concept := _concept_at(self.environment, a)) is None
            or concept_satisfiable(concept, available)
            for a in addresses
        )


_BARRIER_DERIVATIONS = {*GROUPING_DERIVATIONS, Derivation.ROWSET}


def _projects_parent_rows(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    gid: str,
    parents: list[str],
    reads: "_PlannedReads",
    environment: BuildEnvironment,
    mandatory_list: list[BuildConcept],
) -> bool:
    """Whether `gid`'s node would project its parents' rows as they stand: no
    WHERE, no regroup, no subselect. Read off the graph and what `reads` says
    of the parents, the way its generator would, so the answer needs no node
    of `gid`'s own."""
    a = attrs[gid]
    if a.condition_atoms or any(
        edge_kind(group_edges, pgid, gid) == EdgeKind.EXISTENCE
        for pgid in group_graph.predecessors(gid)
    ):
        return False
    available = reads.outputs(parents)
    outputs = satisfiable_from(
        [
            c
            for addr in _select_addresses(a)
            if (c := _concept_at(environment, addr)) is not None
        ],
        available,
    )
    if _lineage_existence_arg_groups(outputs):
        return False
    if a.derivation == Derivation.FILTER:
        # a semijoin RHS is built as the set it feeds
        if any(
            edge_kind(group_edges, gid, succ) == EdgeKind.EXISTENCE
            for succ in group_graph.successors(gid)
        ):
            return False
        narrows = _filter_intrinsic_pushdown_safe(
            group_graph, attrs, gid, outputs, mandatory_list, environment
        )
        return filter_row_predicate(outputs, available, narrows) is None
    return not reads.regroups(outputs, parents, environment)


def _aggregate_inlines(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    reads: "_PlannedReads",
    consumer: str,
    gid: str,
    environment: BuildEnvironment,
    mandatory_list: list[BuildConcept],
) -> bool:
    """Whether aggregate `consumer` computes its row-preserving input `gid`
    inline, reading `gid`'s parents in its place. Decided from the graph and
    those parents' nodes and never from a node of `gid`'s own, so a group
    every reader inlines is not built (`_inlined_by_every_reader`)."""
    a = attrs[gid]
    members = set(a.primary_members)
    if (
        a.derivation not in ROW_PRESERVING_AGGREGATE_INPUT_DERIVATIONS
        or a.derivation == Derivation.ROOT
        # Preserve a provider shared by grouping consumers. Re-rooting only
        # this aggregate forces the other grouping branch to retain the
        # projection, then rejoin it. Row-preserving consumers can still
        # inline independently and fold later.
        or _provider_feeds_other_grouping(group_graph, attrs, gid, consumer)
        or _group_filter_has_existence(attrs, environment, gid)
    ):
        return False
    if not members <= _aggregate_row_preserving_input_addresses(
        [
            concept
            for addr in attrs[consumer].primary_members
            if (concept := _concept_at(environment, addr)) is not None
        ]
    ):
        return False
    # A derived key of the consumer's ROLLUP/CUBE/GROUPING SETS spec must stay
    # materialized by its own projection: folding it re-renders the key inline
    # from its source columns, and grouping(<key>) then names an expression
    # the GROUP BY clause doesn't.
    if members & _nonstandard_grouping_key_addresses(environment, attrs, consumer):
        return False
    # Don't fold a row-preserving group whose output is also produced by
    # another already-built node (a condition-phase twin materialized as its
    # own CTE). Folding re-roots the aggregate on the grandparent, but the
    # resolver then binds the folded column to that sibling CTE, which isn't
    # in the aggregate's FROM: a dangling reference.
    if reads.twin_emits(attrs, members, skip=gid):
        return False
    parents = [
        pgid for pgid in row_parents(group_graph, group_edges, gid) if reads.has(pgid)
    ]
    if not parents or any(reads.barrier(pgid) for pgid in parents):
        return False
    return _projects_parent_rows(
        group_graph,
        group_edges,
        attrs,
        gid,
        parents,
        reads,
        environment,
        mandatory_list,
    ) and _group_renderable_from(attrs, environment, gid, reads.outputs(parents))


def _aggregate_input_groups(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    reads: "_PlannedReads",
    gid: str,
    environment: BuildEnvironment,
    mandatory_list: list[BuildConcept],
) -> list[str]:
    """The groups aggregate `gid` reads: in place of each input it computes
    inline, that input's own parents; to a fixed point, since those may be
    inlined too."""
    candidates = [
        pgid
        for pgid in read_row_parents(group_graph, group_edges, attrs, gid)
        if reads.has(pgid)
    ]
    while True:
        expanded: list[str] = []
        for pgid in candidates:
            if _aggregate_inlines(
                group_graph,
                group_edges,
                attrs,
                reads,
                gid,
                pgid,
                environment,
                mandatory_list,
            ):
                expanded.extend(
                    fgid
                    for fgid in row_parents(group_graph, group_edges, pgid)
                    if reads.has(fgid)
                )
            else:
                expanded.append(pgid)
        deduped = list(dict.fromkeys(expanded))
        if deduped == candidates:
            return candidates
        candidates = deduped


def _raise_if_inlined_input_is_unreadable(
    reads: _PlannedReads, gid: str, inputs: list[str]
) -> None:
    """The groups folded into aggregate `gid` are never built, so nothing else
    can supply their members: the groups it reads must render each, and no
    condition-phase twin built before it may compute one."""
    available = reads.outputs([pgid for pgid in inputs if reads.has(pgid)])
    unreadable = [
        address
        for address in reads.attrs[gid].inlined_members
        if reads.twin_emits(reads.attrs, {address}, skip=None)
        or (concept := _concept_at(reads.environment, address)) is None
        or not concept_satisfiable(concept, available)
    ]
    if unreadable:
        raise ValueError(
            f"[v4] {gid} was planned to compute {unreadable} inline, but its "
            "parents cannot render them. This is a planner bug."
        )


def _condition_twins(attrs: dict[str, GroupAttrs], gid: str) -> list[str]:
    """The condition-phase groups the graph shows carrying `gid`'s members."""
    members = set(attrs[gid].primary_members)
    return [
        other
        for other, o in attrs.items()
        if other not in (gid, FINAL_NODE_ID)
        and o.depth_label == DepthLabel.D1
        and members & {*_select_addresses(o), *o.primary_members}
    ]


def _readers(group_graph: nx.DiGraph, gid: str) -> list[str]:
    return [s for s in group_graph.successors(gid) if s != FINAL_NODE_ID]


def _reader_inlines(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    reads: "_PlannedReads",
    reader: str,
    gid: str,
    environment: BuildEnvironment,
    mandatory_list: list[BuildConcept],
) -> bool:
    """Whether `reader` computes `gid` inline: an aggregate that does, or an
    input its own aggregates inline, and `gid` with it."""
    if edge_kind(group_edges, gid, reader) == EdgeKind.EXISTENCE:
        return False
    above = (
        [reader]
        if attrs[reader].derivation == Derivation.AGGREGATE
        else _readers(group_graph, reader)
    )
    if not above or not all(
        attrs[aggregate].derivation == Derivation.AGGREGATE
        and _aggregate_inlines(
            group_graph,
            group_edges,
            attrs,
            reads,
            aggregate,
            gid,
            environment,
            mandatory_list,
        )
        for aggregate in above
    ):
        return False
    if above == [reader]:
        return True
    # the reader's fold is decided off its parents' nodes, `gid`'s in its place
    if any(
        pgid != gid and not reads.has(pgid)
        for pgid in row_parents(group_graph, group_edges, reader)
    ):
        return False
    return _inlined_by_every_reader(
        *_folded(group_graph, group_edges, attrs, gid),
        reads,
        reader,
        environment,
        mandatory_list,
    )


def _inlined_by_every_reader(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    reads: "_PlannedReads",
    gid: str,
    environment: BuildEnvironment,
    mandatory_list: list[BuildConcept],
) -> bool:
    """Whether no node of `gid`'s own would ever be read: every group reading
    it computes it inline (`_reader_inlines`), and the FINAL takes what it
    exposes from those readers or from its parents."""
    a = attrs[gid]
    readers = _readers(group_graph, gid)
    if not readers:
        return False
    # the cheap set tests first: the reader checks below recurse, and copy the
    # group graph to judge a fold
    ownership = attrs[FINAL_NODE_ID].extent_ownership or ExtentOwnership()
    if gid in {*ownership.owner_by_span.values(), *ownership.carried.values()}:
        return False
    final_reads = {c.address for c in mandatory_list} | {
        arg.address
        for atom in attrs[FINAL_NODE_ID].condition_atoms
        for arg in atom.row_arguments
    }
    # its rows are its parents' rows, so what it passes through they carry
    carried: set[str] = set().union(
        *(attrs[r].output_concepts for r in readers),
        *(attrs[p].output_concepts for p in row_parents(group_graph, group_edges, gid)),
    )
    if not set(a.output_concepts) & final_reads <= carried:
        return False
    if not all(
        _reader_inlines(
            group_graph,
            group_edges,
            attrs,
            reads,
            reader,
            gid,
            environment,
            mandatory_list,
        )
        for reader in readers
    ):
        return False
    # a condition-phase twin built between here and a reader would veto the
    # reader's fold, unless it is folded too: a reader folded with `gid`, or a
    # twin its own readers inline
    folded = {r for r in readers if attrs[r].derivation != Derivation.AGGREGATE}
    twins = [t for t in _condition_twins(attrs, gid) if t not in folded]
    if twins and a.depth_label == DepthLabel.D1:
        return False
    return all(
        _inlined_by_every_reader(
            group_graph, group_edges, attrs, reads, twin, environment, mandatory_list
        )
        for twin in twins
    )


def _read_parents_in_place(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    gid: str,
) -> None:
    """Each reader reads `gid`'s parents where it read `gid`, and computes
    `gid`'s members itself, with whatever was folded into `gid`."""
    parents = row_parents(group_graph, group_edges, gid)
    for reader in _readers(group_graph, gid):
        # in-edges are re-added in order, so the parents take `gid`'s place
        feeds = {
            pgid: group_edges[(pgid, reader)]
            for pgid in group_graph.predecessors(reader)
        }
        for pgid in feeds:
            remove_edge(group_graph, group_edges, pgid, reader)
        for pgid in feeds:
            for source in parents if pgid == gid else [pgid]:
                if not group_graph.has_edge(source, reader):
                    group_graph.add_edge(source, reader)
                    group_edges[(source, reader)] = _rewired_feed(
                        feeds, parents, gid, source
                    )
        a = attrs[reader]
        a.inlined_members = (
            *a.inlined_members,
            *attrs[gid].inlined_members,
            *attrs[gid].primary_members,
        )
        a.input_contracts = tuple(
            c for c in a.input_contracts if c.parent_group_id != gid
        )
    for edge in [e for e in group_edges if gid in e]:
        del group_edges[edge]
    group_graph.remove_node(gid)
    del attrs[gid]


def _rewired_feed(
    feeds: dict[str, EdgeAttrs], parents: list[str], gid: str, source: str
) -> EdgeAttrs:
    """The reader's edge from `source` once `gid` is folded into it: a row
    parent of `gid` feeds the reader rows now, whatever side channel it fed
    the reader before (the reader computes `gid`'s members off its rows)."""
    own = feeds.get(source)
    if own is None or (source in parents and own.kind == EdgeKind.EXISTENCE):
        return dc_replace(feeds[gid])
    return own


def _folded(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    gid: str,
) -> tuple[nx.DiGraph, EdgeMap, dict[str, GroupAttrs]]:
    """The group graph as it would stand with `gid` folded; the plan's own is
    left as it is. A fold rewrites only its readers' attrs."""
    graph, edges = group_graph.copy(), dict(group_edges)
    folded = dict(attrs)
    for reader in _readers(group_graph, gid):
        folded[reader] = dc_replace(attrs[reader])
    _read_parents_in_place(graph, edges, folded, gid)
    return graph, edges, folded


def _fold_into_readers(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    gid: str,
) -> None:
    """Take `gid` out of the plan: its readers read its parents, and nothing
    else names it."""
    _read_parents_in_place(group_graph, group_edges, attrs, gid)
    final = attrs[FINAL_NODE_ID]
    if final.extent_ownership is not None:
        final.extent_ownership.permitted.pop(gid, None)
    logger.info(f"[v4] {gid} is computed inline by its readers; not built")
    trace_group_graph(f"inlined {gid}", group_graph, group_edges, attrs)


def _valued_on_another_fact(
    members: Sequence[str], environment: BuildEnvironment
) -> bool:
    """A row derivation that takes a value where its own keys are absent
    (`case when amount > 6 ... else 'small'`), under a plan whose rows only a
    join of facts holds: no single source witnesses them, so the shared scan
    pads each fact's keys on the other's rows. It is computed on a slice of its
    own fact, before that padding."""
    keyspace = environment.span_scope.keyspace
    if not keyspace.rows_pair_facts:
        return False
    return any(
        takes_a_value_on_padding(
            m,
            keyspace.row_absent(keyspace.keys_of(m)),
            keyspace,
            environment,
        )
        for m in members
    )


def _reads_only_merged_attributes(
    members: Sequence[str], environment: BuildEnvironment
) -> bool:
    """A row derivation of merged attributes alone (`s_year + 1` under `union
    join w_year = s_year`) is a value of the attribute: computed on a slice of
    it, never on the rows that pair the arms."""
    concepts = [environment.concepts.get(m) for m in members]
    return bool(concepts) and all(
        c is not None and axis_scalar_reads(c, environment) for c in concepts
    )


def _one_entity_beside_paired_facts(
    members: Sequence[str], environment: BuildEnvironment
) -> bool:
    """A row derivation of one entity's attributes (`year + 1`, keyed on the
    date) under a plan whose rows pair two facts: computed on that entity's
    slice, never on the row stream joining the facts through it."""
    keyspace = environment.span_scope.keyspace
    if not keyspace.rows_pair_facts:
        return False
    keys = {keyspace.keys_of(m) for m in members}
    return len(keys) == 1 and len(next(iter(keys))) == 1


def _parent_nodes_for(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    gid: str,
    environment: BuildEnvironment,
    graph: ReferenceGraph,
    history: V4History,
    *,
    needed: set[str],
    root_requests: dict[str, RootRequest],
    mandatory_list: list[BuildConcept],
    complete_partials: bool = True,
    staged_conditions: list[BuildWhereClause] | None = None,
    feeder_cache: "_CleanFeederCache | None" = None,
    arms_delivered: bool = False,
    aggregate_inputs: Sequence[str] = (),
) -> list[ParentBuild]:
    """Look up the already-built StrategyNodes for `gid`'s lineage
    predecessors. Topological order guarantees they exist (or that the
    generator was skipped, in which case we just skip that parent).

    Drop an ancestor predecessor X when some descendant predecessor Y
    already provides everything X would contribute to `needed` (the set
    of concepts this group actually consumes). Pass-through nodes like
    filter re-expose their parent's columns, so root often ends up
    redundant once filter is in the candidate set. Including both creates
    multi-parent ambiguity for non-merge generators: no JOIN gets
    emitted and the SQL renderer references the dropped parent by name,
    yielding a binder error."""
    # Imported here: `v4_node_generators` imports this package for its
    # condition helpers, so a module-level import is a cycle.
    from trilogy.core.processing.v4_node_generators import build_node

    candidates: list[tuple[str, StrategyNode]] = []
    # Existence-kind edges feed a subselect, not the row stream;
    # `_wire_existence` wires them as side-channel parents post-build.
    for pgid in read_row_parents(group_graph, group_edges, attrs, gid):
        node = built.get(pgid)
        if node is not None:
            candidates.append((pgid, node))

    if attrs[gid].derivation == Derivation.AGGREGATE:
        candidates = [(pgid, built[pgid]) for pgid in aggregate_inputs if pgid in built]
    elif attrs[gid].inlined_members:
        raise ValueError(
            f"[v4] {gid} was planned to be computed inline along with "
            f"{list(attrs[gid].inlined_members)}, but is built. This is a "
            "planner bug."
        )

    def provides(pgid: str, node: StrategyNode) -> set[str]:
        if isinstance(node, FilterNode) and node.conditions is not None:
            return set(attrs[pgid].primary_members) & needed
        return {o.address for o in node.output_concepts} & needed

    consumer = attrs[gid]
    slices_roots = (
        consumer.derivation in GROUPING_DERIVATIONS
        or _valued_on_another_fact(consumer.primary_members, environment)
        or _reads_only_merged_attributes(consumer.primary_members, environment)
        or _one_entity_beside_paired_facts(consumer.primary_members, environment)
    )

    def parent_for_consumer(pgid: str, node: StrategyNode) -> StrategyNode:
        if attrs[pgid].derivation != Derivation.ROOT or not slices_roots:
            return node.copy()
        # a region domain is read as built: a re-sourced slice is a fresh scan
        # holding no region, so the aggregate would compute its row-stream
        # arguments over the padded merge (`count(status)` counted the
        # customer with no order once the slice stopped hitting a stale cache)
        if region_reads(node):
            return node.copy()
        parent_outputs = {concept.address for concept in node.output_concepts}
        slice_addresses = needed & parent_outputs
        # A scoped-relation member this scan carries for its MATE (the rowset
        # handle the merge joins on) is rendered from the mate's own column, so
        # the slice keeps that column even though the consumer never named it.
        for address in sorted(slice_addresses):
            slice_addresses |= _relation_mates(address, environment) & parent_outputs
        if not slice_addresses or slice_addresses == parent_outputs:
            return node.copy()
        # Adopt a narrower rebuild only when it strictly prunes the source set
        # (drops a join the slice no longer spans). Otherwise re-deriving the
        # same conditioned join just to carry fewer columns is pure CTE
        # duplication: share the already-built ROOT and let column projection
        # narrow it.
        # The exception is a shared ROOT carrying the WRONG SIDE of a relation
        # this consumer reads from its mate: under `union join a = rs.b`, a
        # consumer of `rs.b` needs the rowset's column, and the anchor's `a` is
        # the same axis, so the merge pairs on it too and the join gains a key
        # that matches nothing. Projection can't shed it (the two members are
        # pseudonyms, so the shared CTE exposes the handle's alias whatever the
        # outputs say); only a scan without the column will do.
        relation_members = _statement_scoped_relation_members(environment)
        carries_wrong_side = any(
            address in relation_members
            and bool(_relation_mates(address, environment) & needed)
            for address in parent_outputs - slice_addresses
        )
        slice_demand = slice_addresses | {
            arg.address
            for atom in attrs[pgid].condition_atoms
            for arg in atom.row_arguments
        }
        if not (carries_wrong_side or _strict_leaf_subset_binds(node, slice_demand)):
            return node.copy()
        # A conditioned scan outputs its WHERE's arguments beyond what it was
        # asked for, so a slice of just the asked-for columns is the parent's
        # own request: it prunes nothing (`_strict_leaf_subset_binds` counts
        # binders, not join paths).
        conditions = _wrap_atoms(attrs[pgid].condition_atoms)
        asked = root_requests.get(pgid)
        preexisting = asked.preexisting if asked else None
        # a slice is the parent's rows narrowed, so it is sourced under the
        # parent's own scope: under the consumer's, a span a region domain owns
        # is completed again, and the slice grows a join instead of losing one
        # an aggregate's input slice may read one coalescing arm's rows
        arm_local = arms_delivered or consumer.derivation == Derivation.AGGREGATE
        request = RootRequest(
            frozenset(slice_addresses),
            conditions,
            asked.scope if asked else environment.span_scope,
            preexisting=preexisting,
            arm_local=arm_local,
        )
        if asked == request:
            return node.copy()
        outputs = [
            c
            for address in sorted(slice_addresses)
            if (c := _concept_at(environment, address)) is not None
        ]
        with under_span_scope(environment, request.scope):
            sliced = build_node(
                derivation=Derivation.ROOT,
                outputs=outputs,
                parents=[],
                environment=environment,
                conditions=conditions,
                preexisting_conditions=preexisting,
                complete_partials=complete_partials,
                history=history,
                g=graph,
                staged_conditions=staged_conditions,
                arm_local=arm_local,
            )
            if sliced is None:
                sliced = plan_source(
                    SourceRequest(
                        outputs=outputs,
                        environment=environment,
                        graph=graph,
                        history=history,
                        conditions=conditions,
                        complete_partials=complete_partials,
                        arm_local=arm_local,
                    )
                )
        if sliced is None:
            return node.copy()
        # weighed with its membership feeders wired, as `node` was: unwired,
        # the slice drops the feeders' scans from its leaf set and always
        # looks narrower, so every grouping consumer re-scans its own copy
        _wire_existence(sliced, built, feeder_cache)
        if not (
            carries_wrong_side
            or _leaf_datasource_ids(sliced) < _leaf_datasource_ids(node)
        ):
            return node.copy()
        return sliced

    parents: list[ParentBuild] = []
    for pgid, node in candidates:
        my_provides = provides(pgid, node)
        covered_by_descendant = False
        for other_pgid, other_node in candidates:
            if other_pgid == pgid:
                continue
            # A row-feeder parent whose entire contribution is already carried by
            # a sibling GROUPING contributor (an aggregate/window at this grouping
            # consumer's grain) is a redundant fact re-scan: the consumer reuses
            # the sibling's already-grouped rows, so the feeder only re-supplies
            # grouping keys the sibling holds. A feeder that ALSO feeds raw
            # recompute inputs has columns the aggregated sibling lacks, so its
            # `provides` is not a subset and it survives. Drop only when the
            # sibling applies every row-reducing condition the feeder's subtree
            # does; otherwise the feeder narrows rows the sibling does not and
            # `provides` (columns only) wouldn't catch it. Otherwise require the
            # covering sibling be a lineage descendant.
            covers_as_grouping_sibling = (
                attrs[gid].derivation in GROUPING_DERIVATIONS
                and attrs[pgid].derivation not in GROUPING_DERIVATIONS
                and not node.existence_concepts
                and not node.force_group
                and attrs[other_pgid].derivation in GROUPING_DERIVATIONS
                and feeder_conditions_implied(group_graph, attrs, pgid, other_pgid)
            )
            if not (
                covers_as_grouping_sibling
                or pgid in nx.ancestors(group_graph, other_pgid)
            ):
                continue
            # a region's rows are covered by nothing that does not hold them
            if not region_reads(node) <= region_reads(other_node):
                continue
            if my_provides <= provides(other_pgid, other_node):
                covered_by_descendant = True
                break
        if not covered_by_descendant:
            parents.append(ParentBuild(pgid, parent_for_consumer(pgid, node)))
    return parents


def _raise_on_discarded_parent_atoms(
    gid: str, parent_group_ids: set[str], attrs: dict[str, GroupAttrs]
) -> None:
    """A ROOT re-plans its rows from the datasources and reads no parent's
    node, so a WHERE atom placed on a parent and not on the ROOT itself would
    silently vanish. A parent's intrinsic filters (a recursive terminal, the
    set behind a derived membership) are re-sourced with its concept."""
    own = attrs[gid].condition_atoms
    for pgid in sorted(parent_group_ids):
        missing = [a for a in attrs[pgid].condition_atoms if a not in own]
        if missing:
            raise ValueError(
                f"[v4] ROOT {gid} would discard the WHERE atom(s) {missing} "
                f"placed on its parent {pgid}. This is a planner bug: the "
                "condition was placed on a group the ROOT does not read."
            )


def _fold_constant_parents(
    parents: list[StrategyNode], needed: set[str]
) -> list[StrategyNode]:
    """Fold a constant-only parent into a non-constant sibling instead of
    cross-joining it ON 1=1. A constant is a literal rendered inline, valid in
    any projection, so its needed constants are appended to a sibling's outputs
    and the constant scan is dropped. Constants not in `needed` (the `by
    all_rows` grand-total marker) are just dropped."""
    if len(parents) <= 1:
        return parents
    targets = [p for p in parents if not _is_constant_only(p)]
    if not targets:
        return parents
    target = targets[0]
    dropped: set[int] = set()
    for p in parents:
        if not _is_constant_only(p):
            continue
        keep = [c for c in p.output_concepts if c.address in needed]
        if keep:
            widen_projection(target, keep)
        dropped.add(id(p))
    return [p for p in parents if id(p) not in dropped]


def _is_constant_only(node: StrategyNode) -> bool:
    return bool(node.output_concepts) and all(
        c.derivation == Derivation.CONSTANT for c in node.output_concepts
    )


def _same_relation(left: StrategyNode, right: StrategyNode) -> bool:
    """Both nodes resolve to the same QueryDatasource, i.e. they render as one
    CTE. Resolution is cached on the node and happens anyway downstream."""
    return left.resolve().identifier == right.resolve().identifier


def _derives_from(node: StrategyNode, other: StrategyNode) -> bool:
    """True when `other` is the relation `node` was built on top of."""
    stack = list(node.parents)
    seen: set[int] = set()
    while stack:
        current = stack.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if current is other or _same_relation(current, other):
            return True
        stack.extend(current.parents)
    return False


def _drop_ancestor_parents(parents: list[StrategyNode]) -> list[StrategyNode]:
    """Drop a parent that IS the relation another parent derives from.

    The planner can hand a consumer both a derived node and the very relation
    that node was built on, e.g. a virt-filter projection plus the row scan it
    filters. Merging them joins a node back to its own source, and once the
    consumer binds the shared columns to the source side, that source becomes
    load-bearing: a full extra pass over the row-grain relation to re-read
    columns the derived node already carries.

    Sound because the descendant's rows are all built FROM the ancestor, so
    matching them back on their shared columns is a 1:1 self-lookup that can
    neither filter nor fan out. The `<=` guard makes that exact: the
    descendant must already expose every column the ancestor would contribute,
    so dropping it removes a join and nothing else. Never a region's rows: a
    descendant that paired the ancestor on solid keys holds fewer rows."""
    if len(parents) <= 1:
        return parents
    dropped: set[int] = set()
    for ancestor in parents:
        if id(ancestor) in dropped:
            continue
        ancestor_outputs = {c.address for c in ancestor.output_concepts}
        for descendant in parents:
            if descendant is ancestor or id(descendant) in dropped:
                continue
            if not ancestor_outputs <= {c.address for c in descendant.output_concepts}:
                continue
            if not region_reads(ancestor) <= region_reads(descendant):
                continue
            if not _derives_from(descendant, ancestor):
                continue
            dropped.add(id(ancestor))
            break
    return [p for p in parents if id(p) not in dropped]


def _is_row_preserving_filter(node: StrategyNode) -> bool:
    """A virt-filter projection: CASE-WHEN columns over its parents with no
    row-reducing WHERE or semijoin, so it emits exactly its parents' rows."""
    return (
        isinstance(node, FilterNode)
        and node.conditions is None
        and not node.existence_concepts
    )


def _synthesizes_handle(
    concept: BuildConcept, node: StrategyNode, available: set[str]
) -> bool:
    """Rendering rowset handle `concept` on `node` would compute it from the
    handle's content over `node`'s rows. A handle is its body's rows, so
    anywhere but that rowset's own boundary this pairs the body with `node`'s
    rows on no declared join."""
    lineage = concept.lineage
    if not isinstance(lineage, BuildRowsetItem):
        return False
    if concept.address in available or available.intersection(concept.pseudonyms):
        return False
    return not (
        isinstance(node, RowsetNode)
        and any(
            isinstance(o.lineage, BuildRowsetItem)
            and o.lineage.rowset.name == lineage.rowset.name
            for o in node.output_concepts
        )
    )


def _merges_coalesced_sides(node: StrategyNode) -> bool:
    """A merge pairing the sides of a `union`/`full` relation holds every side's
    rows: no single sibling's rows render it (`union join ticket = rs.t` keeps
    the tickets the rowset never saw)."""
    if not isinstance(node, MergeNode) or len(node.parents) < 2:
        return False
    coalescing = node.environment.domain_graph.coalescing_relation_members()
    return any(o.address in coalescing for o in node.output_concepts)


def _relation_atoms(node: StrategyNode) -> list[BoolExpr]:
    """The atoms a scan applies as its datasource's own declared `where`: the
    relation's definition, which every scan of that datasource reads under,
    not a filter the scan imposes on a consumer's rows."""
    if (
        not isinstance(node, SelectNode)
        or node.datasource is None
        or node.datasource.where is None
    ):
        return []
    return list(decompose_condition(node.datasource.where.conditional))


def _rows_passed(
    node: StrategyNode,
    condition: BoolExpr | None,
    relation: list[BoolExpr] | None = None,
) -> bool:
    """Every atom of `condition` is one `node` applies or already holds, or an
    atom of `relation` (a scan's own declared `where`, see `_relation_atoms`)."""
    if condition is None:
        return True
    applied = [
        atom
        for clause in (node.conditions, node.preexisting_conditions)
        if clause is not None
        for atom in decompose_condition(clause)
    ] + (relation or [])
    return all(atom in applied for atom in decompose_condition(condition))


def _renders_no_less_completely(b: StrategyNode, a: StrategyNode) -> bool:
    """`b` may stand in for `a` only where every column `a` holds complete stays
    complete read off `b`'s parents. Read off a member declared a subset of it
    (`subset join buyers.id = id` renders `id` from `buyers.id`) a column is
    only as complete as the subset, and a sibling holding it partial cannot
    stand in for a complete binding."""
    would_be_partial = {
        c.address
        for c in get_all_parent_partial(a.output_concepts, b.parents, b.environment)
    }
    return not any(
        o.address in would_be_partial and o.address not in a.partial_lcl
        for o in a.output_concepts
    )


def _crosses_unsourced_aggregate(
    concept: BuildConcept, available: set[str], seen: set[str] | None = None
) -> bool:
    """Deriving `concept` off `available` would recompute an aggregate's
    inner expression per row (the bare CASE, silently dropping the `avg()`)."""
    if concept.address in available:
        return False
    seen = seen or set()
    if concept.address in seen:
        return False
    seen.add(concept.address)
    if concept.derivation == Derivation.AGGREGATE:
        return True
    return any(
        _crosses_unsourced_aggregate(arg, available, seen)
        for arg in row_lineage_arguments(concept)
    )


def _fold_passthrough_parents(parents: list[StrategyNode]) -> list[StrategyNode]:
    """Absorb a parent into a row-preserving sibling that can render it.

    When a plain projection B (a non-grouping SelectNode) can render every one
    of another parent A's outputs from B's own source (each output is either
    directly available off B's parents or derivable from columns that are), B
    takes A's columns and A is dropped, instead of cross-joining two views of
    the same scan on `1=1`.

    A real row-shape barrier A is never dissolved: its rows are an aggregate,
    window, or row-reducing semijoin, not a row-wise re-derivable projection. A
    finer-grain sibling can spuriously look able to "render" a global
    aggregate's output by recomputing the aggregate's inner expression (the
    bare CASE, silently dropping the `avg()`). Only a row-preserving
    contributor is foldable: a SelectNode or plain (non-grouping) MergeNode
    such as a multi-table root scan, whose WHERE B's rows already passed, or a
    virt-filter FilterNode (a CASE-WHEN projection with no row-reducing
    WHERE/semijoin).

    Widen B's OUTPUT with A's outputs and B's INPUT with A's inputs (the source
    columns A consumed). `resolve_concept_map` then sources a passthrough from
    the parent (it's an input) and derives the rest inline from those inputs."""

    dropped: set[int] = set()
    coalesced_merges = {id(a) for a in parents if _merges_coalesced_sides(a)}
    for b in parents:
        if id(b) in dropped or not isinstance(b, SelectNode) or b.force_group:
            continue
        available = parent_output_addresses(b)
        coalescing = b.environment.domain_graph.coalescing_relation_members()
        b_regions = region_reads(b)
        for a in parents:
            if a is b or id(a) in dropped or not a.output_concepts:
                continue
            if region_reads(a) != b_regions:
                continue
            # Never dissolve a row-shape barrier into a row sibling. Foldable:
            # a SelectNode or non-grouping MergeNode whose WHERE, if any, `b`'s
            # rows already passed, or a row-preserving FilterNode (a
            # CASE-WHEN virt-filter with no row-reducing WHERE or semijoin).
            if (
                a.force_group
                or not _rows_passed(b, a.conditions, _relation_atoms(a))
                or not (
                    isinstance(a, (SelectNode, MergeNode))
                    or _is_row_preserving_filter(a)
                )
            ):
                continue
            if any(
                o.derivation == Derivation.ROWSET for o in b.output_concepts
            ) and any(o.derivation != Derivation.ROWSET for o in a.output_concepts):
                continue
            if any(_synthesizes_handle(o, b, available) for o in a.output_concepts):
                continue
            if id(a) in coalesced_merges:
                continue
            # A `union join` mate answers for the member only on its own
            # side's rows: the other side's keys are what `a` contributes.
            if any(
                o.address in coalescing and o.address not in available
                for o in a.output_concepts
            ):
                continue
            if any(
                _crosses_unsourced_aggregate(o, available) for o in a.output_concepts
            ):
                continue
            if not all(concept_satisfiable(o, available) for o in a.output_concepts):
                continue
            if not _renders_no_less_completely(b, a):
                continue
            widen_projection(
                b,
                a.output_concepts,
                input_candidates=[*a.input_concepts, *a.output_concepts],
                available_addresses=available,
            )
            dropped.add(id(a))
    return [p for p in parents if id(p) not in dropped]


def _elide_single_parent_passthrough(node: StrategyNode) -> StrategyNode:
    """Collapse one level of pure projection into its parent. Called both as a
    group's node is published (consumers copy from `built`, so the shape they
    see is the shape they plan against) and bottom-up over the assembled tree
    by `_elide_passthrough_tree` for the passthroughs post-publication mutation
    creates. Neither call site subsumes the other."""
    if not isinstance(node, SelectNode):
        return node
    if (
        node.datasource is not None
        or len(node.parents) != 1
        or node.conditions is not None
        or node.preexisting_conditions is not None
        or node.ordering is not None
        or node.existence_concepts
        or node.force_group
    ):
        return node
    parent = node.parents[0]
    # A UNION output is rendered by member-substitution from sibling columns of
    # the parent scan; collapsing the projection into the scan drops those
    # member columns (set_output_concepts keeps only the union outputs) and the
    # union concept then renders as a bare, undefined column.
    if any(c.derivation == Derivation.UNION for c in node.output_concepts):
        return node
    visible = {concept.address for concept in parent.usable_outputs}
    if not node.output_concepts:
        return node
    if any(concept.address not in visible for concept in node.output_concepts):
        return node
    if any(concept.address not in visible for concept in node.input_concepts):
        return node
    collapsed = parent.copy()
    collapsed.set_output_concepts(list(node.output_concepts), rebuild=False)
    collapsed.hidden_concepts = set(node.hidden_concepts)
    collapsed.partial_concepts = collapsed.derive_partials(list(node.partial_concepts))
    collapsed.nullable_concepts = list(node.nullable_concepts)
    collapsed.rollup_concepts = list(node.rollup_concepts)
    # the region contract rides the projection (a rowset boundary that is a
    # region's domain), not only what it projects from
    collapsed.region_spans = parent.region_spans | node.region_spans
    # so does the group tag: the collapsed node is published as the group's
    if collapsed.origin_group is None:
        collapsed.origin_group = node.origin_group
    collapsed.resolution_cache = None
    return collapsed


def _elide_passthrough_tree(
    node: StrategyNode, seen: dict[int, StrategyNode] | None = None
) -> StrategyNode:
    seen = seen or {}
    node_id = id(node)
    if node_id in seen:
        return seen[node_id]
    original = node.parents
    node.parents = [_elide_passthrough_tree(parent, seen) for parent in original]
    # a merge's authored joins name its parents; they follow the collapse
    if isinstance(node, MergeNode) and node.node_joins:
        swap = {id(old): new for old, new in zip(original, node.parents)}
        node.node_joins = [
            join.bind(lambda n: swap.get(id(n), n)) for join in node.node_joins
        ]
    # a resolution stays valid while every parent is the same, still-resolved node
    if any(
        new is not old or new.resolution_cache is None
        for old, new in zip(original, node.parents)
    ):
        node.resolution_cache = None
    collapsed = _elide_single_parent_passthrough(node)
    seen[node_id] = collapsed
    return collapsed


def _row_lineage_closure(concept: BuildConcept) -> list[BuildConcept]:
    return list(iter_row_lineage(concept))


def _aggregate_row_preserving_inputs(concept: BuildConcept) -> list[BuildConcept]:
    if not isinstance(concept.lineage, BuildAggregateWrapper):
        return []
    return [
        arg
        for arg in concept.lineage.function.arguments
        if isinstance(arg, BuildConcept)
        and arg.derivation in ROW_PRESERVING_AGGREGATE_INPUT_DERIVATIONS
        and not lineage_crosses_row_shape_barrier(arg)
    ]


def _aggregate_row_preserving_input_addresses(outputs: list[BuildConcept]) -> set[str]:
    addresses: set[str] = set()
    for concept in outputs:
        for input_concept in _aggregate_row_preserving_inputs(concept):
            addresses.update(arg.address for arg in _row_lineage_closure(input_concept))
    return addresses


def _group_filter_has_existence(
    attrs: dict[str, GroupAttrs],
    environment: BuildEnvironment,
    gid: str,
) -> bool:
    for addr in attrs[gid].primary_members:
        concept = _concept_at(environment, addr)
        if not concept or not isinstance(concept.lineage, BuildFilterItem):
            continue
        if any(concept.lineage.where.existence_arguments):
            return True
    return False


def _group_renderable_from(
    attrs: dict[str, GroupAttrs],
    environment: BuildEnvironment,
    gid: str,
    available: set[str],
) -> bool:
    for addr in attrs[gid].primary_members:
        concept = _concept_at(environment, addr)
        if (
            concept is None
            or lineage_crosses_row_shape_barrier(concept)
            or not concept_satisfiable(concept, available)
        ):
            return False
    return True


def _row_content(arg: BuildConcept) -> BuildConcept:
    lineage = arg.lineage
    if isinstance(lineage, BuildFilterItem) and isinstance(
        lineage.content, BuildConcept
    ):
        return lineage.content
    return arg


def _row_content_derivation(arg: BuildConcept) -> Derivation:
    return _row_content(arg).derivation


def _aggregate_inputs_are_row_preserving(
    outputs: list[BuildConcept],
    primary_addrs: set[str],
    parents: list[StrategyNode],
) -> bool:
    row_preserving_inputs: list[BuildConcept] = []
    for concept in outputs:
        if concept.address not in primary_addrs:
            continue
        if not isinstance(concept.lineage, BuildAggregateWrapper):
            continue
        for arg in concept.lineage.function.arguments:
            # Only a top-level row-preserving BuildConcept arg lets the aggregate
            # skip input-grain normalization. Collecting the leaves of an inline
            # function arg (`sum(a - coalesce(b, 0))`) would skip normalization
            # even when the parent rows aren't yet at the aggregate's input
            # grain: miscounts, and a renamed ROLLUP key desyncs from its GROUP BY.
            if not isinstance(arg, BuildConcept):
                return False
            if (
                arg.derivation not in ROW_PRESERVING_AGGREGATE_INPUT_DERIVATIONS
                or lineage_crosses_row_shape_barrier(arg)
            ):
                return False
            row_preserving_inputs.append(arg)
    if not row_preserving_inputs:
        return False
    # All-ROOT inputs are raw scan columns: the parent emits rows at the
    # datasource's natural (line) grain, which is usually FINER than the
    # aggregate's input grain (e.g. `count(order_number)` reads item|order line
    # rows). Skipping normalization would aggregate the un-deduped rows and
    # over-count, so force it: the inputs are regrouped to the aggregate's
    # input grain first.
    # A FILTER over a ROOT content is the same case: the filtered copy renders
    # as a per-row CASE, so the rows still arrive at the scan's line grain.
    # Classify the arg by its CONTENT's derivation. A KEY-purpose content is
    # one step further: aggregating a key ranges over the key's distinct
    # domain, but a DERIVED key (BASIC over row columns) is computed per scan
    # row, so its rows also arrive at line grain.
    if all(
        _row_content_derivation(arg) == Derivation.ROOT
        or _row_content(arg).purpose == Purpose.KEY
        for arg in row_preserving_inputs
    ):
        return False
    available = {
        output.address for parent in parents for output in parent.output_concepts
    }
    return all(concept_satisfiable(arg, available) for arg in row_preserving_inputs)


def _takes_a_value_beside(
    concept: BuildConcept, region_spans: frozenset[str], environment: BuildEnvironment
) -> bool:
    """`concept` takes a value on a padded row of a region joined on
    `region_spans`: null-opaque at any depth of its lineage (`1 +
    coalesce(amount, 0)`) over something absent there."""
    keyspace = environment.span_scope.keyspace
    return any(
        takes_a_value_on_padding(concept.address, region, keyspace, environment)
        for region in keyspace.live_regions_within(region_spans)
    )


def _named_argument(argument: BuildFunction) -> BuildConcept:
    """A concept standing in for an inline aggregate argument, so the solid
    rows can carry its value through the join that pads them."""
    name = generate_concept_name(argument)
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=argument.output_datatype,
        purpose=Purpose.PROPERTY,
        build_is_aggregate=False,
        derivation=Derivation.BASIC,
        lineage=argument,
        grain=BuildGrain(
            components={
                component
                for read in argument.concept_arguments
                if read.grain is not None
                for component in read.grain.components
            }
        ),
    )


def _name_inline_arguments(
    outputs: list[BuildConcept],
    primary_addrs: set[str],
    region_spans: frozenset[str],
    environment: BuildEnvironment,
) -> tuple[list[BuildConcept], frozenset[str]]:
    """Stand a concept in for each inline aggregate argument that takes a
    value on a padded row of a region joined on `region_spans` (`sum(coalesce(
    amount, 0))` under a ROLLUP the region's rows enter below), and return the
    rewritten outputs with the addresses named. `_project_basic_aggregate_inputs`
    then computes them on the solid rows, as it does a named argument."""
    keyspace = environment.span_scope.keyspace
    regions = keyspace.live_regions_within(region_spans)
    named: dict[int, BuildConcept] = {}
    rewritten: list[BuildConcept] = []
    for concept in outputs:
        if concept.address not in primary_addrs or not isinstance(
            concept.lineage, BuildAggregateWrapper
        ):
            rewritten.append(concept)
            continue
        taking = [
            argument
            for region in regions
            for argument in inline_arguments_taking_a_value(
                concept, region, keyspace, environment
            )
            if nameable(argument)
        ]
        if taking:
            for argument in taking:
                named.setdefault(id(argument), _named_argument(argument))
            function = dc_replace(
                concept.lineage.function,
                arguments=[
                    named.get(id(argument), argument)
                    for argument in concept.lineage.function.arguments
                ],
            )
            concept = dc_replace(
                concept, lineage=dc_replace(concept.lineage, function=function)
            )
        rewritten.append(concept)
    return rewritten, frozenset(c.address for c in named.values())


def _project_basic_aggregate_inputs(
    outputs: list[BuildConcept],
    primary_addrs: set[str],
    parents: list[StrategyNode],
    planned: set[str],
    environment: BuildEnvironment,
    region_spans: frozenset[str],
    named: frozenset[str] = frozenset(),
) -> list[StrategyNode]:
    """Project scalar aggregate inputs without exposing the merge's join inputs.
    Whether the parent can compute them is read off what the aggregate's
    planned inputs render (`planned`), not off the built parent.

    `region_spans`: the parent is the solid row stream beside a region domain
    (so a plain scan is projected too, and the spans it joins the domain on
    are kept). Its BASIC arguments are computed on the solid rows and the
    domain's rows pad them NULL, instead of being re-derived over the padded
    rows (`count(status)` must not count the customer with no order)."""
    if len(parents) != 1 or not (
        isinstance(parents[0], MergeNode)
        or (
            region_spans
            and isinstance(parents[0], (SelectNode, UnionNode))
            and not parents[0].force_group
        )
    ):
        return parents
    scalar_inputs: list[BuildConcept] = []
    for concept in outputs:
        if concept.address not in primary_addrs:
            continue
        if nonstandard_grouping_lineage(concept) is not None and not region_spans:
            return parents
        scalar_inputs.extend(
            aggregate_input
            for aggregate_input in _aggregate_row_preserving_inputs(concept)
            if aggregate_input.derivation == Derivation.BASIC
            # beside a region domain only an argument that takes a value on
            # a padded row (CASE, COALESCE) has to be computed before the
            # padding; arithmetic is NULL there either way and inlines
            and not (
                region_spans
                and aggregate_input.address not in named
                and not _takes_a_value_beside(
                    aggregate_input, region_spans, environment
                )
            )
        )
    if not scalar_inputs:
        return parents

    parent = parents[0].copy()
    available = {output.address for output in parent.output_concepts}
    if isinstance(parent, SelectNode):
        available |= renderable_addresses(parent)
    elif isinstance(parent, UnionNode):
        # each arm computes the input from its own scan (`widen_projection`)
        available |= set.intersection(
            *(renderable_addresses(arm) for arm in parent.parents)
        )
    if not all(concept_satisfiable(concept, planned) for concept in scalar_inputs):
        return parents
    # one the parent already computes is not an input to hand it
    parent_outputs = {o.address for o in parent.output_concepts}
    to_widen = [
        concept for concept in scalar_inputs if concept.address not in parent_outputs
    ]
    # a BASIC between a named argument and the parent's columns (`amount_or_zero`
    # under `amount_or_zero + 1`) is no node of the plan either: it renders in
    # the same projection
    between = unique(
        [
            lineage
            for concept in to_widen
            if concept.address in named
            for lineage in _row_lineage_closure(concept)
            if lineage.derivation == Derivation.BASIC
            and lineage.address not in available
            and lineage.address != concept.address
        ],
        "address",
    )
    to_widen = between + to_widen
    if to_widen:
        # what the parent computes itself is read off its own projection,
        # never handed to it as an input
        computed = parent_outputs - {c.address for c in parent.input_concepts}
        widen_projection(
            parent,
            to_widen,
            input_candidates=(
                lineage
                for concept in to_widen
                for lineage in _row_lineage_closure(concept)
            ),
            available_addresses=available - computed,
        )

    # Keep every direct argument this group reads, not just the BASIC ones
    # widened above: narrowing to the widened subset drops a sibling aggregate's
    # own argument (`count_distinct(warehouse)` beside `bool_or(is_returned)`)
    # and leaves it unsourced. Only ONE lineage level, so the raw column behind
    # a projected BASIC (`_ret` under `is_returned`) still stays hidden. Grain
    # components come from the widened inputs alone: an aggregate's `by` args
    # are also direct arguments, and their key grain is not part of the row
    # stream this group aggregates over. A FILTER argument renders inline as
    # a CASE over its content and WHERE row inputs, so those count as direct.
    # So does a BASIC argument left to render inline (`sum(amount * 2)`
    # beside a projected `count(status)`): its row inputs are what it reads.
    keep = {concept.address for concept in outputs} | set(region_spans)
    keep.update(concept.address for concept in between)
    projected_addrs = {concept.address for concept in scalar_inputs}
    for concept in outputs:
        if concept.address not in primary_addrs or concept.lineage is None:
            continue
        for arg in concept.lineage.concept_arguments:
            keep.add(arg.address)
            if (
                arg.derivation == Derivation.BASIC
                and arg.address not in projected_addrs
            ):
                keep.update(c.address for c in _row_lineage_closure(arg))
            if isinstance(arg.lineage, BuildFilterItem):
                keep.update(a.address for a in arg.lineage.where.row_arguments)
                keep.update(a.address for a in arg.lineage.content_concept_arguments)
    for concept in scalar_inputs:
        if concept.grain is not None:
            keep.update(concept.grain.components)
    projected = [
        concept for concept in parent.output_concepts if concept.address in keep
    ]
    if not projected:
        return parents
    parent.set_output_concepts(projected)
    return [parent]


def _equality_pinned_addresses(condition: BoolExpr | None) -> set[str]:
    """Addresses a conjunctive condition pins to a single literal
    (`channel = 'STORE'`). A pinned column is single-valued and non-null on
    every row that passes, so it cannot contribute distinct rows to a grain
    that omits it."""
    if condition is None:
        return set()
    pinned: set[str] = set()
    for atom in decompose_condition(condition):
        if (
            not isinstance(atom, BuildComparison)
            or atom.operator != ComparisonOperator.EQ
        ):
            continue
        for concept_side, literal_side in (
            (atom.left, atom.right),
            (atom.right, atom.left),
        ):
            if isinstance(concept_side, BuildConcept) and isinstance(
                literal_side, (str, int, float, bool, date, datetime)
            ):
                pinned.add(concept_side.address)
    return pinned


def _subtree_pinned_addresses(
    node: StrategyNode, seen: set[int] | None = None
) -> set[str]:
    """Literal-pinned addresses that hold on every row `node` emits.

    A pin keeps holding downstream through row-preserving or row-reducing
    shapes (select/filter/group/window) and through joins for children on a
    preserved side (each output row embeds one of their rows). It does NOT
    survive a union (other arms are unpinned) or null-extension (an outer
    join's non-preserved side), so those pins are dropped. At a join, a
    child's pin counts only when every sibling exposing the same address
    also pins it non-extended; otherwise the merged column may bind to the
    unpinned (or NULL-padded) source."""
    seen = seen or set()
    if id(node) in seen:
        return set()
    seen.add(id(node))
    pinned = _equality_pinned_addresses(node.conditions)
    if not isinstance(node, (SelectNode, GroupNode, FilterNode, WindowNode, MergeNode)):
        return pinned
    if isinstance(node, MergeNode):
        extended: set[str] = set()
        # A RIGHT_OUTER/FULL with an implicit (None) left side extends the
        # whole accumulated left, so treat every child but the named right
        # side as extended (a superset of the true accumulation: safe).
        implicit_left_rights: list[str] = []
        for join in node.resolve().joins:
            if not isinstance(join, Join):
                continue
            if join.join_type in (JoinType.LEFT_OUTER, JoinType.FULL):
                extended.add(join.right.identifier)
            if join.join_type in (JoinType.RIGHT_OUTER, JoinType.FULL):
                if join.left is None:
                    implicit_left_rights.append(join.right.identifier)
                else:
                    extended.add(join.left.identifier)
        child_pins: list[set[str]] = []
        child_outputs: list[set[str]] = []
        for parent in node.parents:
            identifier = parent.resolve().identifier
            null_extended = identifier in extended or any(
                identifier != right for right in implicit_left_rights
            )
            child_pins.append(
                set() if null_extended else _subtree_pinned_addresses(parent, seen)
            )
            child_outputs.append({c.address for c in parent.output_concepts})
        for i, pins in enumerate(child_pins):
            for addr in pins:
                if all(
                    addr not in outputs or addr in child_pins[j]
                    for j, outputs in enumerate(child_outputs)
                    if j != i
                ):
                    pinned.add(addr)
        return pinned
    for parent in node.parents:
        pinned |= _subtree_pinned_addresses(parent, seen)
    return pinned


def _window_rows_at_grain(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
) -> list[StrategyNode]:
    """A window ranks the distinct rows at the grain of what it reads. A
    parent finer than that (fact rows brought in only to compute a coarser
    aggregate the window orders by) repeats each row once per fact row, so it
    is grouped first. The group keeps every parent column at or above that
    grain, which a later pass may carry through the window."""
    needed = {c.address: c for c in outputs}
    for concept in outputs:
        if concept.lineage is not None:
            needed |= {a.address: a for a in concept.lineage.concept_arguments}
    grain = {
        component
        for concept in needed.values()
        if not concept.grain.abstract
        for component in concept.grain.components
    }
    kept = unique(
        [
            concept
            for parent in parents
            for concept in parent.output_concepts
            if concept.address in needed
            or concept.grain.abstract
            or concept.grain.components <= grain
        ],
        "address",
    )
    addresses = {concept.address for concept in kept}
    if all(
        build_fd_determines_all(
            environment, addresses, parent.resolve().grain.components
        )
        for parent in parents
    ):
        return parents
    return [
        GroupNode(
            output_concepts=kept,
            input_concepts=kept,
            environment=environment,
            parents=parents,
        )
    ]


def _parents_already_at_input_grain(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    input_grain: frozenset[str],
    environment: BuildEnvironment,
) -> bool:
    """True when the parents already emit one row per aggregate-input-grain key,
    so the input-grain normalization GROUP is a no-op.

    When the SELECT sources its rows from the dimension keyed by the input
    grain, the rows are already unique at that grain, so deduping them is pure
    SQL bloat. Proven by resolving each parent's physical row grain and
    checking every component is functionally determined by the input-grain
    keys: fixing the input grain fixes the row. A finer parent key that the
    input grain does NOT determine (a per-item key under a per-order count)
    keeps the normalization, so a fact-line scan is still regrouped before
    aggregation.

    Never fires for non-standard grouping (ROLLUP/CUBE/GROUPING_SETS): those need
    the explicit normalization GROUP so a renamed grouping key stays in sync with
    its GROUP BY clause."""
    if not input_grain:
        return False
    if any(nonstandard_grouping_lineage(c) is not None for c in outputs):
        return False
    keys = set(input_grain)

    def at_input_grain(parent: StrategyNode, seen: set[int] | None = None) -> bool:
        seen = seen or set()
        if id(parent) in seen:
            return False
        seen.add(id(parent))
        resolved_grain = parent.resolve().grain.components
        unresolved = [
            component
            for component in resolved_grain
            if component not in keys
            and not build_fd_determines(
                environment, keys, component, include_empty_grain=False
            )
        ]
        # A leftover component the stream pins to a single literal
        # (`channel = 'STORE'` under an all-channel union) can't split any
        # input-grain key group, so the rows are still unique at the input
        # grain without it.
        if not unresolved or set(unresolved) <= _subtree_pinned_addresses(parent):
            return True
        if (
            isinstance(parent, SelectNode)
            and parent.force_group is False
            and len(parent.parents) == 1
        ):
            return at_input_grain(parent.parents[0], seen)
        if isinstance(parent, MergeNode) and parent.force_group is not True:
            grained = [
                candidate
                for candidate in parent.parents
                if candidate.resolve().grain.components
            ]
            if len(grained) == 1:
                return at_input_grain(grained[0], seen)
        return False

    for parent in parents:
        if not at_input_grain(parent):
            return False
    return True


# A declared join key is normally one hop from the scan that binds it; the cap
# stops a pathological chain from walking the whole plan per key.
_JOIN_KEY_CHAIN_LIMIT = 4


# Node kinds whose projection can be widened with a column derived from their
# own row inputs without changing which rows they emit: plain projections,
# non-grouping merges, virt-filter/WHERE projections (a FilterNode restricts
# rows, never regroups them) and unions (widened arm-by-arm, all or nothing).
_WIDENABLE_PROJECTIONS = (SelectNode, MergeNode, FilterNode, UnionNode)


def _widen_scan_chain(
    node: StrategyNode, concept: BuildConcept, depth: int = 0
) -> bool:
    """Project `concept` off `node`, widening the passthrough projection chain
    beneath it when `node`'s own inputs cannot satisfy it. Returns whether the
    concept ends up on `node`.

    A merge parent can sit one or more projections above the scan that binds the
    key (`count(grain(k) ? ...)` inserts a hash projection under the dedup group,
    itself over the fact merge). Only the scan sees the key's column, so without
    descending to it the declared key stops being carryable and the merge
    cross-joins ON 1=1."""
    if concept.address in {o.address for o in node.output_concepts}:
        return True
    if not isinstance(node, _WIDENABLE_PROJECTIONS):
        return False
    available = renderable_addresses(node)
    if not concept_satisfiable(concept, available):
        if depth >= _JOIN_KEY_CHAIN_LIMIT:
            return False
        # A single-stream projection (SelectNode, FilterNode) would change what
        # the widened column means if it descended a fan-in; a MergeNode joins
        # its parents, so any arm that binds the key can supply it. A union is
        # widened only from what every arm already renders: descending its arms
        # one by one could leave some carrying a column the stack lacks. A
        # grain-collapsing parent is never widened: that would move its
        # grouping key.
        if isinstance(node, UnionNode):
            return False
        if isinstance(node, (SelectNode, FilterNode)) and len(node.parents) != 1:
            return False
        if not any(
            isinstance(below, _WIDENABLE_PROJECTIONS)
            and not below.force_group
            and _widen_scan_chain(below, concept, depth + 1)
            for below in node.parents
        ):
            return False
        available = renderable_addresses(node)
    if _synthesizes_handle(concept, node, available):
        return False
    widen_projection(
        node,
        [concept],
        input_candidates=_row_lineage_closure(concept),
        available_addresses=available,
    )
    return True


def _widen_passthrough_group(
    group: StrategyNode, join_key_concepts: list[BuildConcept]
) -> None:
    group_outputs = {o.address for o in group.output_concepts}
    # `inner` must rebuild per key (it is `group`'s parent, so a stale QDS would
    # be what `group` resolves against); `group` itself only needs one at the end.
    group_dirty = False
    for concept in join_key_concepts:
        if concept.address in group_outputs:
            continue
        for inner in group.parents:
            # `force_group` on the inner scan is fine here: the wrapping
            # GroupNode being widened performs the dedup either way.
            if not _widen_scan_chain(inner, concept):
                continue
            group_dirty |= widen_projection(
                group, [concept], input_candidates=[concept], rebuild=False
            )
            group_outputs.add(concept.address)
            break
    if group_dirty:
        group.rebuild_cache()


def _mangled_rowset_content_addresses(environment: BuildEnvironment) -> set[str]:
    """Addresses of the hidden per-rowset alias concepts a rowset body's renamed
    outputs materialize (`select aid as k` stores `local._rs_k`; see
    ``SemanticState.mangle_rowset_alias``). These are internals of the rowset;
    an un-renamed output's content is the base concept itself and is excluded.
    Their auto-pseudonym back to the authored source is rename plumbing, not a
    join axis."""
    out: set[str] = set()
    for concept in [
        *environment.concepts.values(),
        *environment.alias_origin_lookup.values(),
    ]:
        lineage = concept.lineage
        if isinstance(lineage, BuildRowsetItem) and lineage.content.name.startswith(
            rowset_alias_prefix(lineage.rowset.name)
        ):
            out.add(lineage.content.address)
    return out


def _carried_handle_is_partial(
    environment: BuildEnvironment, concept: BuildConcept, available: set[str]
) -> bool:
    """A rowset handle carried onto a non-rowset scan is a PARTIAL binding
    when the scan's own member of the handle's scoped relation spans only
    part of the handle's domain: the member is a declared SUBSET side, or the
    relation is coalescing (`union`) so any single arm under-covers the axis.
    The anchor of a subset relation still carries the handle complete. Without
    the marking the scan's spelling of the handle reads as a full binding, the
    boundary dedups away as extraneous, and the readback adopts the scan's own
    key domain."""
    if concept.derivation != Derivation.ROWSET:
        return False
    subset_sides = environment.domain_graph.subset_sources()
    coalescing = environment.domain_graph.coalescing_relation_members()
    for canonical, members in environment.scoped_join_key_groups.items():
        if concept.address != canonical and concept.address not in members:
            continue
        bound = ((members | {canonical}) - {concept.address}) & available
        if not bound:
            continue
        if bound & subset_sides or members & coalescing:
            return True
    return False


def _carry_needs_own_node(concept: BuildConcept) -> bool:
    """True when synthesizing ``concept`` from a parent's row stream would need
    a row-shaping node of its own (aggregate, window, recursion, ...), so it
    cannot be widened onto a scan.

    A ROWSET handle terminates the walk: its value arrives materialized at the
    rowset boundary rather than being recomputed here, so a derived key over one
    (`agg.period + 53`) stays carryable onto a scan its relation binds.
    """
    stack = [concept]
    seen: set[str] = set()
    while stack:
        current = stack.pop()
        if current.address in seen:
            continue
        seen.add(current.address)
        if current.derivation == Derivation.ROWSET:
            continue
        if current.derivation in ROW_SHAPE_BARRIER_DERIVATIONS:
            return True
        if current.lineage:
            stack.extend(current.lineage.concept_arguments)
    return False


def _join_key_concepts(
    parents: list[StrategyNode], environment: BuildEnvironment, addresses: set[str]
) -> list[BuildConcept]:
    sibling_outputs: dict[str, BuildConcept] = {}
    for parent in parents:
        for concept in parent.output_concepts:
            sibling_outputs.setdefault(concept.address, concept)
    out: list[BuildConcept] = []
    for address in sorted(addresses):
        join_key = sibling_outputs.get(address) or _concept_at(environment, address)
        if join_key is not None:
            out.append(join_key)
    return out


def _unprojected_expression_mates(
    parents: list[StrategyNode], environment: BuildEnvironment
) -> set[str]:
    """Scoped-join members that are expressions over a rowset handle, reachable
    by two or more parents but projected by none. The boundary binding the
    handle exposes such a member; a projection wrapper over that boundary can
    render it but does not project it, so the merge has no pairing until it is
    widened onto whichever parent renders it."""
    projected = {o.address for parent in parents for o in parent.output_concepts}
    reach = [
        {o.address for o in parent.output_concepts} | renderable_addresses(parent)
        for parent in parents
    ]
    mates: set[str] = set()
    for group in environment.scoped_join_relations():
        candidates: dict[str, BuildConcept] = {}
        for address in group - projected:
            mate = _concept_at(environment, address)
            if (
                mate is not None
                and mate.derivation == Derivation.BASIC
                and mate.lineage is not None
                and any(
                    arg.derivation == Derivation.ROWSET
                    for arg in mate.lineage.concept_arguments
                )
            ):
                candidates[address] = mate
        if not candidates:
            continue
        reached = sum(
            1
            for available in reach
            if group & available
            or any(concept_satisfiable(m, available) for m in candidates.values())
        )
        if reached >= 2:
            mates |= candidates.keys()
    return mates


def _widen_merge_join_keys(
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    join_key_addresses: frozenset[str],
) -> None:
    if len(parents) <= 1 or not join_key_addresses:
        return
    _carry_join_keys(
        parents,
        _join_key_concepts(parents, environment, set(join_key_addresses)),
        environment,
    )
    mates = _unprojected_expression_mates(parents, environment)
    if mates:
        _carry_join_keys(
            parents, _join_key_concepts(parents, environment, mates), environment
        )


def _carry_planned_keys(
    parents: list[StrategyNode],
    origin: dict[int, str],
    plan: JoinPlan,
    environment: BuildEnvironment,
) -> None:
    """Widen each FINAL parent with the keys its own side of its planned
    pairings holds, then with the expression relation members two parents
    reach and none projects."""
    if len(parents) <= 1:
        return
    for parent in parents:
        gid = _origin_group(parent, origin)
        if gid is None or not (keys := plan.keys_for(gid)):
            continue
        _carry_join_keys(
            [parent], _join_key_concepts(parents, environment, set(keys)), environment
        )
    mates = _unprojected_expression_mates(parents, environment)
    if mates:
        _carry_join_keys(
            parents, _join_key_concepts(parents, environment, mates), environment
        )


def _carry_join_keys(
    parents: list[StrategyNode],
    join_key_concepts: list[BuildConcept],
    environment: BuildEnvironment,
) -> None:
    for parent in parents:
        # A pure dedup GroupNode (every output rides through from its parents;
        # nothing aggregated locally, force_group dedups included) can safely
        # carry a declared join key: the key joins its parents' row stream, and
        # the dedup grain widens with it. Widen the inner scan first, then the
        # group's own projection.
        if isinstance(parent, GroupNode):
            inner_available = parent_output_addresses(parent)
            if parent.parents and all(
                o.address in inner_available for o in parent.output_concepts
            ):
                _widen_passthrough_group(parent, join_key_concepts)
            continue
        if parent.force_group or not isinstance(parent, _WIDENABLE_PROJECTIONS):
            continue
        # A leaf datasource scan can still emit any column its datasource binds,
        # so a partial merge key (a fact's `?d1` column that canonicalizes to the
        # declared join key) is carried as the join key instead of the merge
        # cross-joining the sibling that owns the key's complete domain (a
        # date-spine LEFT_OUTER merge: facts.d1->s1 vs the spine's complete s1 ->
        # `FULL JOIN ... on 1=1` cartesian).
        available = renderable_addresses(parent)
        if not available:
            continue
        parent_outputs = {concept.address for concept in parent.output_concepts}
        parent_rowsets = {
            concept.lineage.rowset.name
            for concept in parent.output_concepts
            if isinstance(concept.lineage, BuildRowsetItem)
        }
        existence = {concept.address for concept in parent.existence_concepts}
        carried: list[BuildConcept] = []
        input_candidates: list[BuildConcept] = []
        for concept in join_key_concepts:
            if concept.address in parent_outputs:
                continue
            # An existence concept is consumed via a subselect, not joined as a
            # row column, so it can't be carried as a widenable output.
            if concept.address in existence:
                continue
            # A rowset handle names a column of ITS OWN boundary. Row lineage
            # alone makes it look satisfiable from any scan over the same base
            # (`agg.period` and `fut.period` both descend from `s.period`), but
            # synthesizing it on ANOTHER rowset's boundary emits a different value
            # under that address: `subset join fut.period + 53 = agg.period` then
            # joins `agg.period = agg.period` off the fut side and pairs every
            # period with itself. A non-rowset parent (a plain scan the relation
            # substitutes the handle onto) is unaffected; `_synthesizes_handle`
            # below keeps it from computing a handle it does not already carry.
            if (
                parent_rowsets
                and isinstance(concept.lineage, BuildRowsetItem)
                and concept.lineage.rowset.name not in parent_rowsets
            ):
                continue
            if not concept_satisfiable(concept, available) or _synthesizes_handle(
                concept, parent, available
            ):
                continue
            # Carrying a key the parent does not already emit means SYNTHESIZING
            # it from that parent's row inputs. A grain-collapsing derivation
            # cannot render that way (it needs its own aggregate/window node),
            # so widening a plain scan onto one emits a column nothing can
            # source (`avg(amount) by part` asked of the bare `rows` scan).
            if concept.address not in available and _carry_needs_own_node(concept):
                continue
            carried.append(concept)
            input_candidates.extend(_row_lineage_closure(concept))
        if carried:
            # Outputs and partials both feed the same resolve; batch them into
            # one rebuild at the end.
            dirty = widen_projection(
                parent,
                carried,
                input_candidates=input_candidates,
                available_addresses=available,
                rebuild=False,
            )
            if any(o.derivation == Derivation.ROWSET for o in parent.output_concepts):
                existing_partials = {c.address for c in parent.partial_concepts}
                parent.partial_concepts.extend(
                    [
                        c
                        for c in carried
                        if c.derivation != Derivation.ROWSET
                        and c.address not in existing_partials
                    ]
                )
                dirty = True
            if not parent_rowsets:
                existing_partials = {c.address for c in parent.partial_concepts}
                partial_handles = [
                    c
                    for c in carried
                    if c.address not in existing_partials
                    and _carried_handle_is_partial(environment, c, available)
                ]
                if partial_handles:
                    parent.partial_concepts.extend(partial_handles)
                    dirty = True
            if dirty:
                parent.rebuild_cache()


def _descends_from_any(node: StrategyNode, targets: set[int]) -> bool:
    return any(id(n) in targets for n in _iter_strategy_nodes(node))


def _subtree_restrictions(node: StrategyNode) -> tuple[list[BoolExpr], bool]:
    """Row-reducing filters applied anywhere under `node`: the conditions, and
    whether it also restricts in a way nothing can be compared against (a
    semijoin subselect or a row limit)."""
    conditions: list[BoolExpr] = []
    opaque = False
    for current in _iter_strategy_nodes(node):
        if current.conditions is not None:
            conditions.append(current.conditions)
        if current.existence_concepts or current.limit is not None:
            opaque = True
    return conditions, opaque


def _restrictions_survive(node: StrategyNode, siblings: list[StrategyNode]) -> bool:
    """Every row restriction `node` applies, a sibling applies too, so
    dropping it from the merge removes no rows."""
    conditions, opaque = _subtree_restrictions(node)
    if opaque:
        return False
    if not conditions:
        return True
    sibling_conditions: list[BoolExpr] = []
    for sibling in siblings:
        sibling_conditions.extend(_subtree_restrictions(sibling)[0])
    return all(
        any(condition_implies(other, condition) for other in sibling_conditions)
        for condition in conditions
    )


def _resolved_grain(node: StrategyNode) -> frozenset[str]:
    return frozenset(node.resolve().grain.components)


def _merge_component_count(visible: list[set[str]], live: list[int]) -> int:
    """How many join-connected groups the live contributors form; a shared
    output address is the axis the merge would join them on."""
    component = {idx: idx for idx in live}
    for position, left in enumerate(live):
        for right in live[position + 1 :]:
            if not visible[left] & visible[right]:
                continue
            merged = {component[left], component[right]}
            target = min(merged)
            component = {
                idx: target if group in merged else group
                for idx, group in component.items()
            }
    return len(set(component.values()))


def _fold_covered_contributors(
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    needed: set[str],
    cover_nodes: set[int],
    extent_owners: set[int],
) -> list[StrategyNode]:
    """Drop a merge contributor a sibling can now render in full.

    Election reads `output_concepts`, a projection boundary; join-key
    materialization then widens a sibling PAST that boundary, down to the scan
    that binds the key. A contributor elected as the only exposer of a key can
    therefore end up rendering no column at all: the merge joins it to read
    nothing. This is the first moment that is knowable, and it is still the
    planner, so the join is never built rather than built and deleted.

    Rendering nothing is only the first condition; the rest are what make the
    drop row-identical rather than merely tidy:

    - The contributor was elected to cover a mandatory concept. An axis-only
      contributor (`_add_partial_completion_contributors`) is deliberately
      column-invisible
      (the axis IS its contribution), and so is an elected extent owner, whose
      contribution is the span's extension rows. Neither is ever folded.
    - Dropping it leaves the survivors no less connected: it may be the only
      parent bridging two siblings that share no axis with each other.
    - A surviving sibling renders every needed address it carries, no less
      completely: a sibling holding the address PARTIAL cannot stand in for a
      complete binding.
    - What it shares with the survivors is exactly its own grain, so the join
      neither fanned them out nor constrained them.
    - It restricts no rows the survivors don't already restrict.
    """
    if len(parents) <= 1:
        return parents
    relation_members = environment.all_scoped_join_group_members()
    visible = [_visible_addresses(p) for p in parents]
    partials = [{c.address for c in p.partial_concepts} for p in parents]
    dropped: set[int] = set()
    for idx, parent in enumerate(parents):
        if visible[idx] & relation_members:
            continue
        if not _descends_from_any(parent, cover_nodes):
            continue
        if _descends_from_any(parent, extent_owners):
            continue
        live = [j for j in range(len(parents)) if j not in dropped]
        others = [j for j in live if j != idx]
        # its rows are a region's: only a survivor holding them can stand in
        if (parent_spans := region_reads(parent)) and not any(
            parent_spans <= region_reads(parents[j]) for j in others
        ):
            continue
        contribution = visible[idx] & needed
        # Nothing at all in `needed` means this is an axis contributor whose
        # value is the join itself, not a column; only a cover contributor
        # whose columns moved elsewhere is foldable here.
        if not others or not contribution:
            continue
        # Its columns living elsewhere doesn't make it inert: it may be the
        # only contributor sharing an axis with two siblings that share none
        # with each other, and dropping the bridge cross-joins them ON 1=1.
        if _merge_component_count(visible, others) > _merge_component_count(
            visible, live
        ):
            continue
        if not all(
            any(
                address in visible[j]
                and (address in partials[idx] or address not in partials[j])
                for j in others
            )
            for address in contribution
        ):
            continue
        # Exactly its own key is shared with the survivors: fewer and the join
        # fans out, more and the join CONSTRAINS: the contributor is pairing
        # columns (item->order) that the survivors would otherwise pair freely,
        # so dropping it is not a no-op even though every column still renders.
        shared = visible[idx] & set().union(*(visible[j] for j in others))
        if _resolved_grain(parent) != shared:
            continue
        if not _restrictions_survive(parent, [parents[j] for j in others]):
            continue
        logger.info(
            f"[v4] folding invisible FINAL contributor {type(parent).__name__} "
            f"outputs={sorted(visible[idx])}; siblings render all of "
            f"{sorted(contribution)}"
        )
        dropped.add(idx)
    return [p for idx, p in enumerate(parents) if idx not in dropped]


def _visible_addresses(node: StrategyNode) -> set[str]:
    return {
        o.address for o in node.output_concepts if o.address not in node.hidden_concepts
    }


def _padding_path(stream: StrategyNode) -> list[StrategyNode]:
    """`stream`, then each single-parent projection or grouping under it, down
    to the merge where its region padding happens; empty when there is none."""
    path = [stream]
    while not isinstance(path[-1], MergeNode):
        node = path[-1]
        if not isinstance(node, (SelectNode, GroupNode)) or len(node.parents) != 1:
            return []
        path.append(node.parents[0])
    return path


def _row_preserving(path: list[StrategyNode]) -> bool:
    """The merge's rows are `stream`'s: it is the merge, or projects it."""
    return len(path) == 1 or (
        len(path) == 2 and isinstance(path[0], SelectNode) and not path[0].conditions
    )


def _carries_through(
    path: list[StrategyNode],
    carried: list[BuildConcept],
    environment: BuildEnvironment,
) -> bool:
    """`carried` can pass up `path` without changing its rows. A grouping
    carries only what its keys determine, and only when it is keyed on each
    region's own span: a group uniting a region's padding with value NULLs
    (`count(customer_id) by bucket`, cat in the NULL bucket) would split on
    a column that is NULL on one and not the other."""
    for node in path:
        if not isinstance(node, GroupNode):
            continue
        keys = {
            c.address
            for c in node.output_concepts
            if c.derivation != Derivation.AGGREGATE
        }
        if not region_reads(node) <= keys or not build_fd_determines_all(
            environment, keys, [c.address for c in carried]
        ):
            return False
    return True


def _pads_a_value_null_group(
    stream: StrategyNode, contributor: StrategyNode, keys: set[str], keyspace: Keyspace
) -> bool:
    """`stream` NULLs one of `keys` on a region's rows it holds, and
    `contributor`, holding no region, has a value-NULL group on it."""
    regions = keyspace.live_regions_within(region_reads(stream))
    value_nulls = {c.address for c in contributor.nullable_concepts}
    return any(
        key in value_nulls
        and any(not keyspace.defined_on(key, region) for region in regions)
        for key in keys
    )


def _pair_inside_padding_streams(
    parents: list[StrategyNode], environment: BuildEnvironment
) -> list[StrategyNode]:
    """A grouping contributor holding no region pairs with a stream that
    padded one inside its own merge, there and not at FINAL.

    The stream's NULL on a `?` key it padded is mixed: padding on a region's
    row (cat, no order, so no bucket), a value elsewhere (order 100's NULL
    bucket). FINAL can only keep them apart with a column NULL exactly on the
    padded rows, and the stream need not carry one. Inside the merge the
    padded side's own keys are columns, so the earlier-padding guard
    (`join_resolution._padding_guards`) applies."""
    keyspace = environment.span_scope.keyspace
    out = list(parents)
    for stream in parents:
        path = _padding_path(stream)
        if not path or not region_reads(stream):
            continue
        merge = path[-1]
        for other in list(out):
            if (
                other is stream
                or any(n is stream for n in _iter_strategy_nodes(other))
                or any(n is other for n in _iter_strategy_nodes(stream))
            ):
                continue
            shared = _visible_addresses(other) & _visible_addresses(stream)
            if not shared or not _pads_a_value_null_group(
                stream, other, shared, keyspace
            ):
                continue
            carried = [
                o
                for o in other.output_concepts
                if o.address in _visible_addresses(other) - _visible_addresses(stream)
            ]
            # the merge reads `other`'s outputs already; the nodes above it
            # dropped them
            in_merge = (
                bool(carried)
                and {c.address for c in carried} <= merge.output_lcl.addresses
            )
            if (
                not in_merge
                and not (
                    isinstance(other, GroupNode)
                    and not region_reads(other)
                    and _row_preserving(path)
                )
            ) or not _carries_through(path, carried, environment):
                continue
            logger.info(
                f"[v4] {sorted(c.address for c in carried)} pair inside the merge "
                f"that pads {sorted(shared)}, not at FINAL"
            )
            if not in_merge:
                merge.add_parents([other])
                merge.input_concepts = unique(merge.input_concepts + carried, "address")
                merge.add_output_concepts(carried, rebuild=False)
            for node in reversed(path[:-1]):
                node.input_concepts = unique(node.input_concepts + carried, "address")
                node.add_output_concepts(carried, rebuild=False)
            for node in reversed(path):
                node.rebuild_cache()
            out.remove(other)
    return out


def _raise_if_rowset_islanded(
    parents: list[StrategyNode],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    graph: ReferenceGraph,
) -> None:
    """A FINAL contributor sharing no join axis with any sibling (no common
    output address, pseudonym link, or scoped-relation mate) is about to
    cross-join ON 1=1. With a rowset boundary involved that is the
    Disconnected case (a rowset is opaque; only a declared join relates it back
    to its base), not a legitimate scalar cross join: confirm against the
    shared connectivity check with rowset islanding ON and surface the typed
    subgraph error. Grainless parents (constants, global aggregates) cross-join
    legitimately and stay out of the component analysis."""
    row_bearing = [p for p in parents if p.grain and p.grain.components]
    if len(row_bearing) < 2:
        return
    if not any(
        isinstance(o.lineage, BuildRowsetItem)
        for p in row_bearing
        for o in p.output_concepts
    ):
        return
    mangled_contents = _mangled_rowset_content_addresses(environment)
    keys: list[set[str]] = []
    for parent in row_bearing:
        addrs: set[str] = set()
        for o in parent.output_concepts:
            addrs.add(o.address)
            # A mangled body-local's pseudonym back to its authored source
            # (`_rs_k` ~ `a.aid`) is rename plumbing, not a join axis; it is
            # the phantom bridge islanding exists to sever.
            if o.address not in mangled_contents:
                addrs.update(o.pseudonyms)
        for relation in environment.scoped_join_relations():
            if addrs & relation:
                addrs |= relation
        keys.append(addrs)
    component = list(range(len(row_bearing)))
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            if keys[i] & keys[j]:
                merged = {component[i], component[j]}
                target = min(merged)
                component = [target if c in merged else c for c in component]
    if len(set(component)) > 1:
        raise_if_disconnected_for(mandatory_list, None, environment, graph)

        # The connectivity check can pass while the components still share no
        # axis: authored relations can route the components through a THIRD
        # rowset no contributor materializes (`subset join weeks.ws =
        # cur.src_ws` + `subset join nxt.nxt_ws = weeks.nxt`, a bridge). The
        # declarations relate the models, but realizing them as a join axis
        # needs the bridge rowset's (ws, nxt) pairs as a merge contributor,
        # which synthesis does not build; the merge would silently cross-join
        # ON 1=1. Detect the bridge and raise the typed error instead.
        def _rowset_of(addr: str) -> str | None:
            concept = environment.concepts.get(addr)
            if concept is not None and isinstance(concept.lineage, BuildRowsetItem):
                return concept.lineage.rowset.name
            return None

        exposed_rowsets = {
            name
            for p in row_bearing
            for o in p.output_concepts
            if (name := _rowset_of(o.address)) is not None
        }
        rowsets_by_component: dict[int, set[str]] = defaultdict(set)
        for idx, comp in enumerate(component):
            for addr in keys[idx]:
                name = _rowset_of(addr)
                if name is not None and name not in exposed_rowsets:
                    rowsets_by_component[comp].add(name)
        bridge_counts: Counter[str] = Counter()
        for names in rowsets_by_component.values():
            bridge_counts.update(names)
        bridges = sorted(n for n, count in bridge_counts.items() if count > 1)
        if bridges:
            raise UnresolvableQueryException(
                f"Query outputs are related only through rowset(s) "
                f"{bridges} joined on both sides (a bridge), which no "
                "output-producing source materializes; planning this join is "
                "not supported. Select a column from the bridge rowset, or "
                "join the outputs' sources directly."
            )


def _final_span_scope(scope: SpanScope) -> SpanScope:
    return dc_replace(scope, extent_free=scope.owned, extent_free_carried={})


def _group_span_scope(
    scope: SpanScope, ownership: ExtentOwnership, gid: str
) -> SpanScope:
    return dc_replace(
        scope,
        extent_free=ownership.suppressed_for(gid) | scope.owned,
        extent_free_carried=ownership.suppressed_carried_for(gid),
    )


def _filter_intrinsic_pushdown_safe(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    gid: str,
    outputs: list[BuildConcept],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
) -> bool:
    """May this filter group's predicate narrow its ROWS? Only when the plan
    shows nothing but filter values over that one predicate, this group
    produces a shown one, and no consumer reads an unfiltered ancestor of it
    for something this group does not carry. Otherwise it stays a per-row
    CASE."""
    if (
        statement_filter_population(
            mandatory_list, environment.statement_hidden_addresses
        )
        is None
    ):
        return False
    mandatory = {c.address for c in mandatory_list}
    if not any(o.address in mandatory for o in outputs):
        return False
    ancestors = nx.ancestors(group_graph, gid)
    if not ancestors:
        return True
    emitted = {o.address for o in outputs}
    for succ in _readers(group_graph, gid):
        unfiltered = ancestors & set(group_graph.predecessors(succ))
        if not unfiltered:
            continue
        supplied = frozenset().union(*(_own_members(attrs, a) for a in unfiltered))
        if not _consumer_reads(attrs[succ], environment) & supplied <= emitted:
            return False
    return True


def _consumer_reads(consumer: GroupAttrs, environment: BuildEnvironment) -> set[str]:
    """The addresses a group reads off its parents: what it derives reads its
    arguments; its grain and what rides through it are read as themselves."""
    read = set(consumer.grain_components) | (
        set(_select_addresses(consumer)) - set(consumer.primary_members)
    )
    for member in consumer.primary_members:
        concept = _concept_at(environment, member)
        if concept is not None and concept.lineage is not None:
            read |= {a.address for a in concept.lineage.concept_arguments}
    return read


def _reads_only_values_defined_everywhere(atom: BoolExpr, keyspace: Keyspace) -> bool:
    """Every value the atom reads is defined on every region of the plan: a
    value pinned to the select's grain takes its fallback on a padded row,
    so `status is null` is false there however the row is joined, and the
    scan hosting the atom holds exactly its population."""
    arguments = list(atom.row_arguments)
    return bool(arguments) and all(
        arg.address in keyspace.keys_by_address
        and keyspace.defined_on(arg.address, region)
        for arg in arguments
        for region in keyspace.live_regions
    )


def _pre_merge_parents(
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    join_key_addresses: frozenset[str] = frozenset(),
    needed: set[str] | None = None,
    group_graph: nx.DiGraph | None = None,
    built: dict[str, StrategyNode] | None = None,
    force_join_type: JoinType | None = None,
    preexisting_conditions: BoolExpr | None = None,
    attrs: dict[str, GroupAttrs] | None = None,
    origin: dict[int, str] | None = None,
    candidates: Sequence[str] = (),
) -> list[StrategyNode]:
    """Collapse a multi-parent set into a single MergeNode that auto-joins
    on shared output concepts. Non-merging generators (GroupNode for
    aggregate/group_to, WindowNode, FilterNode) emit `joins=[]` and let the
    renderer pick one parent as base, so multi-parent without a merge
    yields `Referenced table "X" not found` binder errors when the SELECT
    references the dropped parent. Wrapping here keeps the generators
    simple and the join logic in one place.

    `force_join_type` overrides join inference; a caller merging in a FILTER
    scan (a WHERE-only root a constraint edge feeds this consumer) passes
    INNER, since a filter may only remove rows and a preserving join would
    re-admit the rows it rejects. `preexisting_conditions` are the request
    atoms the parents' groups already applied: the merge's typing must not
    null-extend the branch carrying one (as the FINAL merge already knows),
    or a declared-subset boundary FULL-joined to a filtered scan of its
    superset resurrects the rows the WHERE rejected."""
    if len(parents) <= 1:
        return parents
    parents = _fold_constant_parents(parents, needed or set())
    if len(parents) <= 1:
        return parents
    parents = _fold_passthrough_parents(parents)
    if len(parents) <= 1:
        return parents
    parents = _drop_ancestor_parents(parents)
    if len(parents) <= 1:
        return parents
    plan = _merge_plan(
        parents,
        attrs,
        origin,
        group_graph,
        candidates,
        needed,
        join_key_addresses,
        environment,
    )
    _widen_merge_join_keys(parents, environment, join_key_addresses)
    # After widening, not before: a parent that has just been handed the join
    # key it was missing needs no bridge.
    if plan is not None and built is not None:
        parents = _join_elected_bridges(
            parents,
            [(built[gid], plan.keys_for(gid)) for gid in plan.extras],
            environment,
        )
    all_outputs = readable_outputs(parents)
    merged = MergeNode(
        input_concepts=all_outputs,
        output_concepts=all_outputs,
        environment=environment,
        parents=parents,
        force_join_type=force_join_type,
        preexisting_conditions=preexisting_conditions,
        # a region domain hosts its spans' extension rows: preserve it, and
        # the feeder only where it holds a value-NULL key
        host_stitch=any(p.region_spans for p in parents),
        planned_keys=(
            plan.over(side_groups(parents, origin)).permitted_keys
            if plan is not None and origin is not None
            else None
        ),
    )
    return [merged]


def _merge_plan(
    parents: list[StrategyNode],
    attrs: dict[str, GroupAttrs] | None,
    origin: dict[int, str] | None,
    group_graph: nx.DiGraph | None,
    candidates: Sequence[str],
    needed: set[str] | None,
    join_key_addresses: frozenset[str],
    environment: BuildEnvironment,
) -> JoinPlan | None:
    """A consumer's join plan over the groups its row parents came from, with
    the bridges it elects from `candidates` as extras. None when a side is no
    group's (a merge assembled below the consumer): resolve discovers the
    keys."""
    if attrs is None or origin is None or group_graph is None:
        return None
    sides = side_groups(parents, origin)
    if len(sides) != len(parents):
        return None
    wanted = needed or set()
    cover = {
        gid: [
            c
            for address in attrs[gid].output_concepts
            if address in wanted and (c := _concept_at(environment, address))
        ]
        for gid in sides
    }
    return elect_bridges(
        group_graph,
        attrs,
        cover,
        candidates,
        [c for concepts in cover.values() for c in concepts],
        environment,
        _required_final_contract(attrs).authored_axes,
        split=frozenset(gid for gid in sides if sides.count(gid) > 1),
        contracted={gid: join_key_addresses for gid in sides},
    )


def _union_arm_parents(
    parent_builds: list[ParentBuild],
    attrs: dict[str, GroupAttrs],
    environment: BuildEnvironment,
    needed: set[str],
    group_graph: nx.DiGraph,
    built: dict[str, StrategyNode],
    candidates: Sequence[str],
) -> list[StrategyNode]:
    """One parent per union arm. Arms stack, so parents merge only WITHIN an
    arm scope (a key scan beside a constant feeding the same arm), never
    across arms: two arms share no key and their join would be a cross
    product. Arm scopes are the parent groups' labels (see `union_arms`)."""
    by_arm: dict[str, list[StrategyNode]] = {}
    for parent in parent_builds:
        by_arm.setdefault(attrs[parent.group_id].label, []).append(parent.node)
    arms: list[StrategyNode] = []
    for arm_parents in by_arm.values():
        arms.extend(
            _pre_merge_parents(
                arm_parents,
                environment,
                needed=needed,
                group_graph=group_graph,
                built=built,
                candidates=candidates,
            )
        )
    return arms


def _contains_shape_barrier(node: StrategyNode) -> bool:
    return any(
        isinstance(n, (GroupNode, WindowNode)) or n.force_group
        for n in _iter_strategy_nodes(node)
    )


def _input_contract_projection_grain(
    group_attrs: GroupAttrs, parent_group_ids: set[str] | None = None
) -> frozenset[str]:
    grain: set[str] = set()
    for contract in group_attrs.input_contracts:
        if (
            parent_group_ids is not None
            and contract.parent_group_id not in parent_group_ids
        ):
            continue
        if contract.channel != InputChannel.ROW_STREAM:
            continue
        if not contract.may_project_dimension:
            continue
        grain |= set(contract.required_grain)
        grain |= set(contract.preserve_keys)
    return frozenset(grain)


def _input_contract_join_keys(
    group_attrs: GroupAttrs, parent_group_ids: set[str] | None = None
) -> frozenset[str]:
    keys: set[str] = set()
    for contract in group_attrs.input_contracts:
        if (
            parent_group_ids is not None
            and contract.parent_group_id not in parent_group_ids
        ):
            continue
        if contract.channel != InputChannel.ROW_STREAM:
            continue
        keys |= set(contract.preserve_keys)
    return frozenset(keys)


def _apply_input_contracts(
    parent_builds: list[ParentBuild],
    group_attrs: GroupAttrs,
    needed: set[str],
    environment: BuildEnvironment,
) -> list[StrategyNode]:
    parents = [parent.node for parent in parent_builds]
    parent_group_ids = {parent.group_id for parent in parent_builds}
    projection_grain_components = _input_contract_projection_grain(
        group_attrs, parent_group_ids
    )
    return _satisfy_parent_projection_contract(
        parents,
        needed,
        projection_grain_components,
        environment,
    )


def _fd_at_grain(concept: BuildConcept, grain_components: frozenset[str]) -> bool:
    if concept.address in grain_components:
        return True
    concept_grain = (
        frozenset(concept.grain.components) if concept.grain else frozenset()
    )
    if concept_grain and concept_grain <= grain_components:
        return True
    concept_keys = frozenset(concept.keys or set())
    return bool(concept_keys) and concept_keys <= grain_components


def _satisfy_parent_projection_contract(
    parents: list[StrategyNode],
    needed: set[str],
    projection_grain_components: frozenset[str],
    environment: BuildEnvironment,
) -> list[StrategyNode]:
    """Physically satisfy a declared parent projection-grain contract.

    Stage 2 chooses the projection grain. This adapter only decides whether a
    parent can be safely wrapped to satisfy that grain before it merges with an
    already-shaped sibling; it must not infer a target grain from sibling
    outputs or group attrs.
    """
    if len(parents) <= 1 or not projection_grain_components:
        return parents

    # Pre-grouping a dimension parent to the group grain only makes sense to
    # protect an ALREADY-grouped sibling (an aggregate at the group grain) from
    # being fanned out by a row-grain detail merge. When every parent is a
    # row-grain scan feeding INTO this aggregate, there is no such sibling;
    # projecting one to the group grain just strips its finer row-grain join
    # key and the merge degrades to a 1=1 cross product.
    if not any(_contains_shape_barrier(parent) for parent in parents):
        return parents

    # Two different questions, two different sets. "What is available TO this
    # parent" is its own parents' outputs (`parent_output_addresses`): that is
    # what `parent_needed` asks, and a leaf scan's empty answer keeps leaf
    # scans out of this projection entirely. "What does a SIBLING supply to
    # the merge" is that sibling's own projection: a WINDOW sibling READS
    # `event_time` and emits only `lag(event_time)`, so crediting it with its
    # inputs would strip `event_time` off the dimension parent and leave the
    # consumer computing `event_time - prior_event_time` with no source.
    outputs_by_parent = [parent_output_addresses(parent) for parent in parents]
    own_outputs_by_parent = [
        {output.address for output in parent.usable_outputs} for parent in parents
    ]
    projected: list[StrategyNode] = []
    for idx, parent in enumerate(parents):
        if _contains_shape_barrier(parent):
            projected.append(parent)
            continue
        parent_needed = outputs_by_parent[idx] & needed
        other_outputs = set().union(
            *(outputs for j, outputs in enumerate(own_outputs_by_parent) if j != idx)
        )
        fd_candidates = {
            addr
            for addr in parent_needed
            if (c := _concept_at(environment, addr)) is not None
            and _fd_at_grain(c, projection_grain_components)
        }
        fd_needed = {
            addr
            for addr in fd_candidates
            if not (addr in projection_grain_components and addr in other_outputs)
        }
        non_fd_needed = parent_needed - fd_candidates
        concepts = [
            c
            for addr in sorted(fd_needed)
            if (c := _concept_at(environment, addr)) is not None
        ]
        if not concepts or non_fd_needed:
            projected.append(parent)
            continue
        # `parent_needed` is measured off the parent's INPUTS, so a value the
        # parent computes itself (a BASIC sibling's `revenue`, a date
        # projection) is invisible to it, and the projection below would strip
        # it, leaving the consuming aggregate with no source for that output.
        # Carry those through. Restricted to FD-at-grain so the projection's
        # row count is unchanged, and to what no sibling parent already
        # supplies, so this never re-shapes a plain dimension re-join. One
        # that is NOT FD at the grain (`quantity as q` beside `count(...) by
        # item_desc`) cannot be projected to it at all: the parent stays as
        # built, like an input in `non_fd_needed`.
        own_needed = {
            output.address
            for output in parent.usable_outputs
            if output.address in needed
            and output.address not in parent_needed
            and output.address not in other_outputs
        }
        carry = {
            addr
            for addr in own_needed
            if (c := _concept_at(environment, addr)) is not None
            and _fd_at_grain(c, projection_grain_components)
        }
        if own_needed - carry:
            projected.append(parent)
            continue
        concepts.extend(
            c
            for addr in sorted(carry)
            if (c := _concept_at(environment, addr)) is not None
        )
        # When the dimension's projected grain (fd_needed) shares NO key with the
        # barrier sibling, the post-projection merge has nothing to join on and
        # cross-joins ON 1=1: the bridge between the two is a projection-grain
        # key the sibling outputs but fd_needed dropped (an aggregate's
        # input-grain key, `ride_date` linking `start_station.id` to the inner
        # `daily_rides` aggregate). Keep that bridge by grouping to the combined
        # grain, a per-(dim,key) dedup CTE. Guarded on disjointness so the
        # normal case (a dimension that already shares its keys with the
        # aggregate) is left to `_wrap_for_grain` untouched. The bridge may only
        # be DERIVABLE here (the dimension carries `ride_start_time`, from which
        # `ride_date` projects), so test satisfiability.
        join_keys = {
            addr
            for addr in projection_grain_components
            if addr not in fd_needed
            and addr in other_outputs
            and (c := _concept_at(environment, addr)) is not None
            and concept_satisfiable(c, outputs_by_parent[idx])
        }
        if join_keys and fd_needed.isdisjoint(other_outputs):
            # The GroupNode reads from `parent`, so its grain may only include
            # concepts the parent actually outputs. `fd_needed` is derived from
            # what is available to the parent (its own parents' outputs) and can
            # contain an FD attribute the parent drops, e.g. a row key that is
            # FD-determined by the row grain but lives only as a WHERE filter
            # applied at the scan, never as a column here. Grouping on it would
            # fail input validation; keep only real outputs.
            parent_outputs = {o.address for o in parent.usable_outputs}
            grain_concepts = [
                c
                for addr in sorted((fd_needed | join_keys | carry) & parent_outputs)
                if (c := _concept_at(environment, addr)) is not None
            ]
            projected.append(
                GroupNode(
                    output_concepts=grain_concepts,
                    input_concepts=grain_concepts,
                    environment=environment,
                    parents=[parent],
                )
            )
            continue
        projected.extend(
            _wrap_for_grain(parent, concepts, environment, projection_grain_components)
        )
    return projected


def _topological_order(group_graph: nx.DiGraph, group_edges: EdgeMap) -> list[str]:
    """Topological order across all dependency edge kinds (lineage /
    constraint / existence). Each kind expresses a different dataflow
    relationship downstream, but all of them require the source group to
    be built before the consumer: a constraint sibling has to be
    JOIN-ready, an existence source has to be subselect-ready.

    A cycle means no build order exists, which is always a planner bug.
    Raising is deliberate: an empty order would build nothing, fall through
    to a partial plan, and return rows, turning an unorderable dependency
    into a silently dropped filter."""
    dep_graph = dependency_subgraph(group_graph, group_edges)
    try:
        return list(nx.topological_sort(dep_graph))
    except nx.NetworkXUnfeasible as exc:
        cycle = nx.find_cycle(dep_graph)
        raise UnresolvableQueryException(
            "Query planning produced a circular dependency between group nodes, "
            f"so there is no valid build order: {cycle}"
        ) from exc


def _bridge_pseudonyms(node: StrategyNode, provided: list[BuildConcept]) -> None:
    """Add the merge-canonical concepts as hidden bridge outputs on a sole FINAL
    contributor so the CTE layer can map every requested alias of a merged key
    onto the column it computes.

    A single contributor computes a merged key under one alias (`unnest_array.a`,
    an `unnest`/struct field carrying the attr-access lineage) but the user may
    write a *different* alias of the same key (`local.a`, the merge origin of the
    written `wrapper.a`). Sibling aliases don't list each other as pseudonyms
    (each only knows the canonical origin), so the CTE-layer pseudonym match
    can't bridge the written `wrapper.a` to the output `unnest_array.a` and the
    column drops from the SELECT. `per_group` resolved each output to the
    canonical concept, which carries the full equivalence class; expose it as a
    *hidden* output (it has no lineage of its own, so it renders via its
    canonical sibling): the user-facing alias resolves through it, while hiding
    keeps it out of the grain/GROUP BY/projection. A no-op when an output already
    names the canonical (the user wrote the produced alias directly), so the
    direct-match cases (`unnest_array.a` selected as itself) are untouched."""
    out_addrs = {o.address for o in node.output_concepts}
    bridges: list[BuildConcept] = []
    for m in provided:
        if m.address in out_addrs:
            continue
        if any(output_covers(o, m) for o in node.output_concepts):
            bridges.append(m)
    if not bridges:
        return
    node.set_output_concepts(
        list(node.output_concepts) + bridges,
        rebuild=False,
        change_visibility=False,
    )
    node.hidden_concepts = set(node.hidden_concepts or set()) | {
        m.address for m in bridges
    }
    node.rebuild_cache()


def _holds_statement_rows(
    reader: GroupAttrs, required: frozenset[str], environment: BuildEnvironment
) -> bool:
    """A reader of a region carries the statement's rows: a row stream does,
    an aggregate only when its grain determines the statement's."""
    if reader.derivation not in GROUPING_DERIVATIONS:
        return True
    return build_fd_determines_all(
        environment, reader.grain_components, required, include_empty_grain=False
    )


def _add_region_domain_contributors(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    groups: Sequence[str],
    per_group: dict[str, list[BuildConcept]],
    projections: dict[str, list[BuildConcept]],
    environment: BuildEnvironment,
) -> None:
    """A region domain contributes ROWS (its members, NULL on everything
    absent there), so a domain whose every column a sibling also renders is
    added as a contributor of no concepts, unless a contributor already reads
    it and holds the statement's rows. An aggregate over the region holds them
    only at its own grain: aggregates by `status` and by `name` each read the
    region, but only the domain's rows pair a name with a status. A rename of
    something the domain carries (`item_desc as d`) is projected on the
    domain: any other host holds it for matched members only. The domain's
    scan is never asked for it: a rename in the request widens the source."""
    required = _required_final_contract(attrs).required_grain
    for gid in sorted(groups):
        if not attrs[gid].extent_spans:
            continue
        readers = nx.descendants(group_graph, gid)
        if gid not in per_group and any(
            other in readers
            and _holds_statement_rows(attrs[other], required, environment)
            for other in per_group
        ):
            continue
        per_group.setdefault(gid, [])
        renames = [
            concept
            for other, concepts in per_group.items()
            if other != gid
            for concept in concepts
            if _renders_rename_of_member(concept, attrs[gid])
        ]
        moved = {c.address for c in renames}
        for other in [o for o in per_group if o != gid]:
            per_group[other] = [c for c in per_group[other] if c.address not in moved]
            if other in projections:
                projections[other] = [
                    c for c in projections[other] if c.address not in moved
                ]
        per_group[gid].extend(renames)
        projections.setdefault(gid, []).extend(renames)


def _project_final_contributors(
    built: dict[str, StrategyNode],
    projections: dict[str, list[BuildConcept]],
    environment: BuildEnvironment,
) -> None:
    """Wrap each contributor in one projection of the columns the FINAL
    election hosted on it beyond what its build renders."""
    for gid, concepts in projections.items():
        if not concepts:
            continue
        base = built[gid]
        projected = SelectNode(
            output_concepts=list(base.output_concepts),
            input_concepts=list(base.output_concepts),
            environment=environment,
            parents=[base],
            partial_concepts=list(base.partial_concepts),
        )
        widen_projection(projected, concepts)
        built[gid] = projected


def _renders_rename_of_member(concept: BuildConcept, domain: GroupAttrs) -> bool:
    """`concept` renames a member of `domain` its rows can render: it outputs
    the rename itself or every argument of it."""
    if not (
        isinstance(concept.lineage, BuildFunction)
        and concept.lineage.operator == FunctionType.ALIAS
    ):
        return False
    args = {a.address for a in concept.lineage.concept_arguments}
    held = {*domain.output_concepts, *domain.members}
    return args <= set(domain.primary_members) and (
        concept.address in held or args <= held
    )


def _add_partial_completion_contributors(
    groups: Sequence[str],
    per_group: dict[str, list[BuildConcept]],
    attrs: dict[str, GroupAttrs],
    rendered: dict[str, frozenset[str]],
    environment: BuildEnvironment,
) -> None:
    """A mandatory scoped-join member completes through its group-mate's
    boundary: add as an axis-only contributor a group rendering a COMPLETE
    rowset-handle mate the chosen contributors don't carry (the request itself
    happened in `_unsourced_relation_mates`). Two triggers:

    - the covering contributor leaves the member PARTIAL: it is a declared
      subset side, and only the mate's boundary carries the axis
      (rowset-anchor `subset join cust = members.mid`); the FINAL merge then
      pairs the sides and `_clear_groupmate_completed_partials` un-marks the
      member.
    - the member belongs to a COALESCING (`union`/`full`) relation: a
      demanded member projects the unified axis, so every side's boundary must
      join the merge even though it covers no mandatory output (bare-member
      projection).

    Decided off what each group renders (`renders.renders`). Edits
    `per_group` in place."""
    if not environment.scoped_join_key_groups:
        return
    chosen = set(per_group)

    def _complete_outputs(gid: str) -> set[str]:
        return set(rendered[gid]) - declared_partial(attrs[gid], environment)

    def _outputs(gid: str) -> set[str]:
        return set(rendered[gid])

    covered: set[str] = set()
    covered_partial: set[str] = set()
    for gid, concepts in per_group.items():
        partial = declared_partial(attrs[gid], environment)
        # a rename projects its member: `a.k as k` covers `a.k`
        for concept in through_renames(concepts):
            covered.add(concept.address)
            if concept.address in partial:
                covered_partial.add(concept.address)

    coalescing = environment.domain_graph.coalescing_relation_members()
    for relation in environment.scoped_join_relations():
        # Coalescing sides are BOTH partial by declaration (neither domain
        # contains the other; the coalesce of the sides is what's complete),
        # so the mate boundary qualifies by exposing the member at all. A
        # subset anchor must expose it COMPLETE: a partial carrier can't clear
        # the demanded member's own partial.
        if relation & covered & coalescing:
            expose = _outputs
            triggers = relation & covered
        elif relation & covered_partial:
            expose = _complete_outputs
            triggers = relation & covered_partial
        else:
            continue
        mates = {
            addr
            for addr in relation - triggers
            if (c := environment.concepts.get(addr)) is not None and c.address == addr
        }
        exposed: set[str] = set()
        for gid in chosen:
            exposed |= expose(gid)
        for addr in sorted(mates - exposed):
            provider = next(
                (
                    gid
                    for gid in sorted(groups)
                    if gid not in chosen and addr in expose(gid)
                ),
                None,
            )
            if provider is not None:
                per_group.setdefault(provider, [])
                chosen.add(provider)


def _fold_descendant_contributors(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    per_group: dict[str, list[BuildConcept]],
    passthroughs: dict[str, list[BuildConcept]],
    rendered: dict[str, frozenset[str]],
    environment: BuildEnvironment,
) -> None:
    """Reroute FINAL to read a contributor's columns *through* a basic
    descendant instead of merging the two.

    A basic group B preserves the row set of the contributor S it was grafted
    onto (by `_route_basics_through_richer_siblings`), so B can pass S's
    columns straight through. Move S's coverage onto B as a passthrough and
    drop S as a separate contributor; otherwise the FINAL merge re-joins B to
    S on whatever column they happen to share, which for a rename of a
    grouping key is the value itself and fans out. Works for any basic, not
    just renames: B already resolved against S, we only widen its projection.
    Edits `per_group` in place; B's passthrough is owed in `passthroughs`.

    Passthrough = add S's concepts to B's input AND output: `resolve_concept_
    map` sources an output from a parent only when it's also an input
    (`inherited`); an output that isn't an input is re-derived in B's own CTE
    (which would recompute S's aggregates from their source columns). Whether
    B's parents carry S's columns is read off what they render
    (`renders.parents_render`)."""
    for b_gid in list(per_group.keys()):
        if b_gid not in per_group or attrs[b_gid].derivation not in (
            Derivation.BASIC,
            Derivation.WINDOW,
        ):
            continue
        reads = parents_render(
            group_graph, group_edges, attrs, b_gid, rendered, environment
        )
        for s_gid in nx.ancestors(group_graph, b_gid):
            if s_gid not in per_group:
                continue
            s_concepts = per_group[s_gid]
            if not all(c.address in reads for c in s_concepts):
                continue
            per_group[b_gid].extend(s_concepts)
            passthroughs.setdefault(b_gid, []).extend(s_concepts)
            del per_group[s_gid]


def _pass_through_final_contributors(
    built: dict[str, StrategyNode],
    passthroughs: dict[str, list[BuildConcept]],
) -> None:
    """Widen a copy of each basic the FINAL reads an ancestor contributor's
    columns through, taking as inputs the ones its parents render."""
    for gid, concepts in passthroughs.items():
        node = built[gid].copy()
        if widen_projection(
            node,
            concepts,
            input_candidates=concepts,
            available_addresses=parent_output_addresses(node),
        ):
            built[gid] = node


def _hosts_rename(
    concept: BuildConcept, available: set[str], environment: BuildEnvironment
) -> bool:
    """A node with `available` outputs can render `concept`. A rename of a
    `union join` member is the coalesced axis: only a node carrying every
    member of the relation holds all of its rows."""
    relation = {c.address for c in renamed_coalescing_relation(concept, environment)}
    return (
        relation <= available
        and concept_satisfiable(concept, available)
        and not _crosses_unsourced_aggregate(concept, available)
    )


def _promote_final_aliases_to_grouping_contributors(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    per_group: dict[str, list[BuildConcept]],
    projections: dict[str, list[BuildConcept]],
    mandatory_list: list[BuildConcept],
    rendered: dict[str, frozenset[str]],
    environment: BuildEnvironment,
) -> None:
    """Host a FINAL rename on a grouping contributor that renders what it
    renames, decided off what each contributor renders (`renders.renders`)
    and the concepts the cover gave it. A hosted rename is owed in
    `projections`."""

    def has_grouping_output(gid: str) -> bool:
        return _renders_grouping_rows(rendered[gid], environment)

    def available(gid: str) -> set[str]:
        return set(rendered[gid]) | {c.address for c in per_group[gid]}

    for concept in mandatory_list:
        current_gid = next(
            (
                gid
                for gid, concepts in per_group.items()
                if any(c.address == concept.address for c in concepts)
            ),
            None,
        )
        grouping_candidates = [
            gid
            for gid in per_group
            if gid != current_gid
            and attrs[gid].derivation in GROUPING_DERIVATIONS
            and has_grouping_output(gid)
        ]
        for gid in grouping_candidates:
            if not _hosts_rename(concept, available(gid), environment):
                continue
            projections.setdefault(gid, []).append(concept)
            for old_gid in list(per_group):
                per_group[old_gid] = [
                    c for c in per_group[old_gid] if c.address != concept.address
                ]
                if not per_group[old_gid]:
                    del per_group[old_gid]
            per_group[gid].append(concept)
            break

    # A contributor whose EVERY mandatory output is a pure rename satisfiable
    # off one grouping sibling rides that sibling wholesale, rendering the
    # alias inside the aggregate's own GROUP BY select. Otherwise the FINAL
    # joins the rename host back to the aggregate null-safe on the whole dim
    # tuple just to read the aliases. Group-level, not per-concept: promoting
    # only some outputs leaves the contributor (and its join) alive, so
    # nothing is won.
    for src_gid in list(per_group):
        if attrs[src_gid].derivation in GROUPING_DERIVATIONS:
            continue
        members = per_group.get(src_gid)
        if not members:
            continue
        if any(
            edge_kind(group_edges, pgid, src_gid) == EdgeKind.EXISTENCE
            for pgid in group_graph.predecessors(src_gid)
        ):
            continue
        if not all(
            isinstance(c.lineage, BuildFunction)
            and c.lineage.operator == FunctionType.ALIAS
            for c in members
        ):
            continue
        for gid in list(per_group):
            if gid == src_gid:
                continue
            # `attrs[gid].derivation` is the GROUP's classification, and a
            # ROLLUP that also carries its own renamed dimensions classifies
            # BASIC, so the derivation test alone misses it and the FINAL
            # joins the rename host back onto the rollup. That join is not
            # merely redundant: the FINAL will not dedup a grouping-set node
            # (see `node_nulls_grouping_keys`), and a join on a dimension
            # column that is non-unique in its own table (an SCD business key)
            # multiplies every rollup row. Ask what the node EMITS, which is
            # the same question the dedup asks.
            # The group the renames read holds their rows by construction:
            # built apart, the rename stream joins its host back on whatever
            # row key both were sourced at.
            if (
                attrs[gid].derivation not in GROUPING_DERIVATIONS
                and not has_grouping_output(gid)
                and gid not in nx.ancestors(group_graph, src_gid)
            ):
                continue
            if not all(_hosts_rename(c, available(gid), environment) for c in members):
                continue
            if not feeder_conditions_implied(group_graph, attrs, src_gid, gid):
                continue
            projections.setdefault(gid, []).extend(members)
            member_addrs = {c.address for c in members}
            for old_gid in list(per_group):
                if old_gid == gid:
                    continue
                per_group[old_gid] = [
                    c for c in per_group[old_gid] if c.address not in member_addrs
                ]
                if not per_group[old_gid]:
                    del per_group[old_gid]
            per_group[gid].extend(members)
            break


def _renders_grouping_rows(
    rendered: frozenset[str], environment: BuildEnvironment
) -> bool:
    """Whether a group renders ROLLUP/CUBE/GROUPING SETS rows: the stage-2
    twin of `node_nulls_grouping_keys`."""
    return any(
        (c := environment.concepts.get(address)) is not None
        and nonstandard_grouping_lineage(c) is not None
        for address in rendered
    )


def _projection_root_concepts(
    concepts: list[BuildConcept],
    environment: BuildEnvironment,
) -> list[BuildConcept]:
    addresses: set[str] = set()
    for concept in concepts:
        addresses.add(concept.address)
        grain_components = (
            frozenset(concept.grain.components) if concept.grain else frozenset()
        )
        addresses.update(grain_components)
        # A property's keys identify the row it belongs to (address.city ->
        # address.id), so the dim scan needs them. But a self-grained identifier
        # (grain == {itself}) may carry keys that are a COARSER parent grain
        # (a fact's customer id keyed by the fact's line grain); expanding
        # those drags the fact into an otherwise pure dimension scan. Skip keys
        # for such identifiers.
        if grain_components != {concept.address}:
            addresses.update(concept.keys or set())
    # an aggregate is read at its own grain: a key finer than it (`site` under
    # a sum by `zone`) would read a summary's rows un-rolled
    rolled = [c for c in concepts if c.is_aggregate]
    named = {c.address for c in concepts}
    return [
        c
        for address in sorted(addresses)
        if (c := _concept_at(environment, address)) is not None
        and (
            address in named
            or not any(
                finer_than_grain(c, r.grain, environment.concepts) for r in rolled
            )
        )
    ]


def _fresh_final_root_projection(
    projected: list[BuildConcept],
    environment: BuildEnvironment,
    graph: ReferenceGraph,
    history: V4History,
    conditions: BuildWhereClause | None,
    arm_local: bool,
) -> StrategyNode | None:
    node = plan_source(
        SourceRequest(
            outputs=projected,
            environment=environment,
            graph=graph,
            history=history,
            conditions=conditions,
            arm_local=arm_local,
        )
    )
    if node is None or conditions is None:
        return node
    # plan_source validates a conditioned request as COMPLETE when the plan
    # merely CARRIES the condition's row args (see `_conditions_met`'s
    # found-addresses clause), on the assumption that a later step applies the
    # WHERE. This re-slice has no such after-step, so an unapplied condition
    # silently vanishes (a NULL-enrichment row rides the FINAL merge back in).
    # Wrap unless the plan provably applies it.
    for existing in (node.conditions, node.preexisting_conditions):
        if existing is not None and condition_implies(existing, conditions.conditional):
            return node
    return SelectNode(
        output_concepts=node.output_concepts,
        input_concepts=node.output_concepts,
        environment=environment,
        parents=[node],
        conditions=conditions.conditional,
    )


def _add_needed_concept(needed: set[str], concept: BuildConcept) -> None:
    needed.add(concept.address)
    if concept.grain is not None:
        needed.update(concept.grain.components)


def _add_aggregate_needed_concepts(needed: set[str], concept: BuildConcept) -> None:
    if not isinstance(concept.lineage, BuildAggregateWrapper):
        if concept.lineage is None:
            return
        for arg in concept.lineage.concept_arguments:
            _add_needed_concept(needed, arg)
        return
    for arg in concept.lineage.function.concept_arguments:
        _add_needed_concept(needed, arg)
    for group_key in concept.lineage.by:
        needed.add(group_key.address)
    for input_concept in _aggregate_row_preserving_inputs(concept):
        for row_input in _row_lineage_closure(input_concept):
            if row_input.address != input_concept.address:
                _add_needed_concept(needed, row_input)


def _parent_supplied_args(
    concept: BuildConcept, primary_addrs: set[str]
) -> list[BuildConcept]:
    """Lineage args of `concept` that a PARENT has to supply.

    An arg that is itself a primary member is computed at this group (a `case`
    over `grouping(...)` reads the grouping virtual the same node emits), so the
    parent owns its inputs, not the arg itself. Walk through those; stop at
    everything else."""
    stack = list(concept.lineage.concept_arguments) if concept.lineage else []
    seen: set[str] = set()
    supplied: list[BuildConcept] = []
    while stack:
        arg = stack.pop()
        if arg.address in seen:
            continue
        seen.add(arg.address)
        if arg.address in primary_addrs and arg.lineage is not None:
            stack.extend(arg.lineage.concept_arguments)
            continue
        supplied.append(arg)
    return supplied


def _with_null_members(
    node: StrategyNode,
    spans: frozenset[str],
    environment: BuildEnvironment,
    datasources: Sequence[BuildDatasource],
) -> StrategyNode:
    """The domain's rows beside the NULL member of each `?` span: the rows of
    every source binding the span `?` where it is NULL, one each, FULL-joined
    on the span (they match no dimension row). The member rides the domain
    as a value NULL, so its join back pairs null-safely with the fact's rows
    keyed on it (`get_modifiers`); `_distinct_projection` above folds the
    members into one row."""
    joins: list[NodeJoin] = []
    parents: list[StrategyNode] = [node]
    for span in sorted(spans):
        key = environment.concepts[span]
        for datasource in datasources:
            if not any(
                key.equivalent_addresses & nc.equivalent_addresses
                for nc in datasource.nullable_concepts
            ):
                continue
            member = SelectNode(
                input_concepts=[key],
                output_concepts=[key],
                environment=environment,
                datasource=datasource,
                nullable_concepts=[key],
                force_group=True,
                conditions=BuildComparison(
                    left=key, right=MagicConstants.NULL, operator=ComparisonOperator.IS
                ),
            )
            joins.append(
                NodeJoin(
                    right=member, join_type=JoinType.FULL, left=node, concepts=[key]
                )
            )
            parents.append(member)
    if len(parents) == 1:
        return node
    return MergeNode(
        input_concepts=list(node.output_concepts),
        output_concepts=list(node.output_concepts),
        environment=environment,
        parents=parents,
        node_joins=joins,
        partial_concepts=list(node.partial_concepts),
        nullable_concepts=[environment.concepts[span] for span in sorted(spans)],
    )


def _distinct_projection(
    node: StrategyNode, concepts: list[BuildConcept], environment: BuildEnvironment
) -> StrategyNode:
    """`node` deduplicated to `concepts`, whole; itself when already there."""
    usable = {o.address for o in node.usable_outputs}
    outputs = unique([c for c in concepts if c.address in usable], "address")
    grain = set(node.grain.components) if node.grain else set()
    if not outputs or (grain and grain <= {c.address for c in outputs}):
        return node
    return GroupNode(
        output_concepts=outputs,
        input_concepts=outputs,
        environment=environment,
        parents=[node],
        partial_concepts=list(node.partial_concepts),
        preexisting_conditions=node.preexisting_conditions,
        force_group=True,
    )


def _wrap_for_grain(
    parent_node: StrategyNode,
    needed_concepts: list[BuildConcept],
    environment: BuildEnvironment,
    merge_grain_components: frozenset[str] = frozenset(),
    dedup_orthogonal: bool = False,
) -> list[StrategyNode]:
    """When a parent feeds a merge edge, its grain may be wider than the
    natural grain of the concepts the merge actually wants; joining the
    parent's wider-grain rows into a per-key aggregate blows up cardinality.

    For each natural-grain bucket among `needed_concepts`, emit a GroupNode
    that aggregates `parent_node` to that grain and exposes only those
    concepts plus the grain keys. Buckets whose grain already matches the
    parent's grain pass through unchanged. Applies to intermediate aggregates
    as well as roots (a `sum(...) by (a, b)` whose downstream merge only needs
    grain `{a}` for one column)."""
    if not needed_concepts:
        return [parent_node]

    # A needed concept that is NOT functionally determined by the merge grain is
    # a finer/orthogonal row key (e.g. `product_id` next to a `group(store_id) by
    # wh_id` whose merge grain is {store_id, wh_id}). Bucketing it to its own
    # natural grain shatters the scan into per-key GroupNodes that share no join
    # key, so the FINAL merge cross-joins them ON 1=1 (a forced-join disambiguator
    # selecting a group property alongside an unrelated key). Keep the parent
    # whole at its row grain; the FINAL dedup groups it down to the output grain.
    # UNLESS the caller says that dedup never runs (`dedup_orthogonal`: a
    # ROLLUP/CUBE/GROUPING SETS sibling makes the FINAL skip it, since a dedup
    # would re-aggregate the subtotal rows); then collapse to distinct rows of
    # the needed projection + join keys HERE, one GroupNode so the join keys
    # stay beside the orthogonal dims (a leaf dim outside the grouping key list
    # must join back per distinct pair, not per source row).
    if merge_grain_components and any(
        not _fd_at_grain(concept, merge_grain_components) for concept in needed_concepts
    ):
        if not dedup_orthogonal:
            return [parent_node]
        parent_usable = {o.address for o in parent_node.usable_outputs}
        distinct_by_addr: dict[str, BuildConcept] = {}
        for needed in needed_concepts:
            distinct_by_addr.setdefault(needed.address, needed)
        for addr in sorted(merge_grain_components & parent_usable):
            key_concept = _concept_at(environment, addr)
            if key_concept is not None:
                distinct_by_addr.setdefault(addr, key_concept)
        distinct_outputs = list(distinct_by_addr.values())
        if any(o.address not in parent_usable for o in distinct_outputs):
            return [parent_node]
        return [
            GroupNode(
                output_concepts=distinct_outputs,
                input_concepts=distinct_outputs,
                environment=environment,
                parents=[parent_node],
                partial_concepts=list(parent_node.partial_concepts),
                preexisting_conditions=parent_node.preexisting_conditions,
                force_group=True,
            )
        ]

    parent_grain_components = (
        frozenset(parent_node.grain.components) if parent_node.grain else frozenset()
    )

    # Each concept's natural grain is the key it functionally depends on
    # (e.g. text_id is a property of customer.id, so its grain is
    # {customer.id}). `BuildGrain.from_concepts([c])` is the wrong helper
    # here: that asks "what grain do these concepts collectively require",
    # which can include the concept itself as a self-key.
    parent_outputs = {o.address for o in parent_node.usable_outputs}
    by_grain: dict[frozenset[str], list[BuildConcept]] = defaultdict(list)
    for concept in needed_concepts:
        grain_components = (
            frozenset(concept.grain.components) if concept.grain else frozenset()
        )
        # A dimension reached *through* the merge grain key (its `keys` are the
        # merge grain) is functionally determined by it and already lives in
        # this parent at that grain, e.g. `order.customer.id` (keys={order.id})
        # selected next to `sum(...) by order.id`. Deduping it to its own
        # key-grain ({order.customer.id}) drops `order.id`, so the FINAL merge
        # loses its join key and degrades to `ON 1=1` (fan-out). Project it at
        # the merge grain instead, keeping the join key. Only the part of the
        # merge grain this parent can SUPPLY participates: a parent that
        # carries `customer_sk` but not the sibling aggregate's `state` still
        # projects its FD dims at {customer_sk}; demanding the full axis would
        # shatter the scan into self-grain buckets with no join key.
        #
        # With no merge grain to supply (a pure projection: no aggregate, no
        # rowset, so the join plan pins nothing) fall back to
        # the parent's own grain, then to the concept's own keys. A dimension
        # this parent's rows determine must ride that identity rather than
        # dedup to its own key-grain, or two properties of one key bound by a
        # single key-grain source become two keyless GroupNodes and the merge
        # pairs every key with every value.
        axis = (merge_grain_components or parent_grain_components) & parent_outputs
        if not axis:
            axis = frozenset(concept.keys or ()) & parent_outputs
        if (
            axis
            and grain_components.isdisjoint(axis)
            # The concept-map FD closure is the authority on "is this
            # determined by that": it walks grain, keys and equivalence
            # classes transitively (nation.id -> customer.id), which neither a
            # raw `keys` subset test nor `BuildGrain.from_concepts` does on its
            # own; `from_concepts` folds the property hierarchy only, so an
            # enum declared `key city` that a source binds at grain(tree_id)
            # never folds into it.
            and build_fd_determines(
                environment, axis, concept.address, include_empty_grain=False
            )
        ):
            grain_components = axis
        by_grain[grain_components].append(concept)

    wraps: list[StrategyNode] = []
    for grain_comps, concepts in by_grain.items():
        if grain_comps == parent_grain_components or not grain_comps:
            wraps.append(parent_node)
            continue
        if not grain_comps <= parent_outputs:
            wraps.append(parent_node)
            continue
        grain_concepts = [
            c for a in grain_comps if (c := _concept_at(environment, a)) is not None
        ]
        # Dedup by address, keep concept order stable.
        outputs_by_addr: dict[str, BuildConcept] = {}
        for c in concepts + grain_concepts:
            outputs_by_addr.setdefault(c.address, c)
        outputs = list(outputs_by_addr.values())
        if any(output.address not in parent_outputs for output in outputs):
            wraps.append(parent_node)
            continue
        wraps.append(
            GroupNode(
                output_concepts=outputs,
                input_concepts=outputs,
                environment=environment,
                parents=[parent_node],
            )
        )
    return wraps


def _pairing_addresses(node: StrategyNode, environment: BuildEnvironment) -> set[str]:
    """Addresses a merge can pair this parent on, as join resolution sees them:
    its outputs plus every alias of the same axis. Two sides of an authored
    relation (`subset join kb = ka`) name one axis under two addresses, so a raw
    address comparison reads them as sharing nothing."""
    addresses: set[str] = set()
    for concept in node.output_concepts:
        addresses.add(concept.address)
        for pseudonym in concept.pseudonyms:
            addresses.add(pseudonym)
            origin = environment.alias_origin_lookup.get(pseudonym)
            if origin is not None:
                addresses.add(origin.address)
    for relation in environment.scoped_join_relations():
        if addresses & relation:
            addresses |= relation
    return addresses


def _pairing_islands(outputs: list[set[str]]) -> list[set[int]]:
    """Parent indices grouped by what pairs them: two parents sharing a
    pairing column, directly or through a third, are one island."""
    islands: list[tuple[set[int], set[str]]] = []
    for index, own in enumerate(outputs):
        members, columns = {index}, set(own)
        for island in [i for i in islands if i[1] & own]:
            islands.remove(island)
            members |= island[0]
            columns |= island[1]
        islands.append((members, columns))
    return [members for members, _ in islands]


def _join_elected_bridges(
    parents: list[StrategyNode],
    bridges: Sequence[tuple[StrategyNode, frozenset[str]]],
    environment: BuildEnvironment,
) -> list[StrategyNode]:
    """Join the bridges the join plan elected (`join_plan.elect_bridges`),
    each with the keys the plan pairs it on, so the merge below joins on an
    axis instead of `ON 1=1`.

    `select id, min(x) by id, min(y) by cell` leaves two aggregates, one keyed
    on `id` and one on `cell`: the row stream holding both keys bridges them.

    The plan elects only bridges whose every pairing is keyed. A bridge that
    no longer joins two islands as built (a member widened to its key pairs
    directly) is not joined."""
    if len(parents) < 2:
        return parents
    parents = list(parents)
    outputs = [_pairing_addresses(parent, environment) for parent in parents]
    for bridge, keys in bridges:
        islands = _pairing_islands(outputs)
        if len(islands) < 2:
            break
        if any(bridge is parent for parent in parents):
            continue
        if keys:
            _carry_join_keys(
                [bridge],
                _join_key_concepts([bridge], environment, set(keys)),
                environment,
            )
        exposed = _pairing_addresses(bridge, environment)
        touched = [
            island
            for island in islands
            if exposed & set().union(*(outputs[i] for i in island))
        ]
        if len(touched) < 2:
            continue
        parents.append(bridge)
        outputs.append(exposed)
    return parents


def _bridge_candidates(
    group_graph: nx.DiGraph,
    order: Sequence[str],
    unbuilt: Collection[str],
    consumer: str = FINAL_NODE_ID,
) -> list[str]:
    """The groups the build order decides before `consumer` (every group, for
    the FINAL: one feeding no output may sort after it) that are still in the
    plan and built a node, most derived first. A group folded into its readers
    left the plan at its turn."""
    decided = order if consumer == FINAL_NODE_ID else order[: order.index(consumer)]
    groups = {
        gid
        for gid in decided
        if gid in group_graph and gid != FINAL_NODE_ID and gid not in unbuilt
    }
    return sorted(
        groups,
        key=lambda gid: (
            sum(1 for a in nx.ancestors(group_graph, gid) if a in groups),
            gid,
        ),
        reverse=True,
    )


def _gate_grain_keys(concepts: list[BuildConcept]) -> frozenset[str]:
    return frozenset(
        key
        for concept in concepts
        if concept.derivation == Derivation.AGGREGATE and concept.grain
        for key in concept.grain.components
    )


def _filter_arg_parents(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    missing_addrs: set[str],
    spans: frozenset[str] = frozenset(),
) -> tuple[list[StrategyNode], list[BuildConcept]]:
    """Built groups (most-downstream) producing each `missing_addr`: a
    FINAL-deferred filter's row-arg that isn't a user output (a global
    aggregate compared against a per-key aggregate). Returned as cross-join
    parents plus the concepts they supply, so the FINAL filter node can pull
    them in as hidden inputs.

    Beside a region's rows (`spans`), a producer carrying every span comes
    first: a row value (`status`) pairs on the span, where an aggregate by it
    (`sum(amount) by status`) holds only its groups and cross-joins."""
    nodes: list[StrategyNode] = []
    concepts: list[BuildConcept] = []
    seen: set[str] = set()
    for addr in missing_addrs:
        gid = _filter_arg_producer(group_graph, attrs, built, addr, spans)
        if gid is None:
            continue
        supplier = built[gid]
        concept = next(o for o in supplier.output_concepts if o.address == addr)
        # The group is being pulled in PRECISELY to supply this column, so a
        # projection that hides it defeats the purpose: the FINAL merge reads
        # parents' `usable_outputs`, so a hidden-only supplier trips the node
        # input invariant instead of rendering (a correlated inline subquery
        # whose rowset hides its own correlation key).
        if addr in supplier.hidden_concepts:
            supplier.unhide_output_concepts([concept])
        concepts.append(concept)
        if gid not in seen:
            seen.add(gid)
            nodes.append(built[gid])
    return nodes, concepts


def _filter_arg_producer(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    candidates: Collection[str],
    addr: str,
    spans: frozenset[str],
) -> str | None:
    """The most derived of `candidates` outputting `addr`; beside a region's
    rows, one carrying every span first."""
    return max(
        (gid for gid in candidates if addr in attrs[gid].output_concepts),
        key=lambda gid: (
            spans <= set(attrs[gid].output_concepts),
            sum(1 for a in nx.ancestors(group_graph, gid) if a in candidates),
        ),
        default=None,
    )


def _final_extra_sides(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    candidates: Sequence[str],
    sides: Sequence[str],
    filter_only_addrs: set[str],
    environment: BuildEnvironment,
) -> list[str]:
    """The groups the FINAL merge joins beside `sides` to evaluate its WHERE:
    each filter-only argument's producer, and the row stream computing a gate
    key none of them renders (`count(id) by genus`, genus derived)."""
    available = set().union(*(attrs[gid].output_concepts for gid in sides))
    spans = frozenset().union(
        *(_group_region_reads(group_graph, attrs, gid) for gid in sides)
    )
    extras: list[str] = []
    args: list[BuildConcept] = []
    for addr in sorted(
        _region_paired_args(filter_only_addrs, available, spans, environment)
    ):
        gid = _filter_arg_producer(group_graph, attrs, candidates, addr, spans)
        if gid is None:
            continue
        extras.append(gid)
        if (concept := _concept_at(environment, addr)) is not None:
            args.append(concept)
    for address in sorted(_gate_grain_keys(args) - available):
        if host := _gate_key_row_stream(address, attrs, candidates):
            extras.append(host)
    return list(dict.fromkeys(extras))


def _group_region_reads(
    group_graph: nx.DiGraph, attrs: dict[str, GroupAttrs], gid: str
) -> frozenset[str]:
    """The region domains `gid` reads, as `region_reads` finds them on its
    built node: a rowset boundary's body reads are the body's."""
    out: set[str] = set()
    stack, seen = [gid], set()
    while stack:
        g = stack.pop()
        if g in seen or g not in attrs:
            continue
        seen.add(g)
        out |= attrs[g].extent_spans
        if attrs[g].derivation == Derivation.ROWSET and g != gid:
            continue
        stack.extend(group_graph.predecessors(g))
    return frozenset(out)


def _region_paired_args(
    filter_only: set[str],
    available: set[str],
    spans: frozenset[str],
    environment: BuildEnvironment,
) -> set[str]:
    """The FINAL atom's hidden inputs no contributor supplies. Beside a
    region's rows an aggregate pairs on its grain, read off the row stream
    (`coalesce(sum(amount) by status, 0)` needs each row's `status`)."""
    missing = filter_only - available
    if not spans:
        return missing
    grain = _gate_grain_keys(
        [c for a in missing if (c := _concept_at(environment, a)) is not None]
    )
    return missing | (grain - available)


def _required_final_contract(attrs: dict[str, GroupAttrs]) -> FinalAssemblyContract:
    contract = attrs[FINAL_NODE_ID].final_contract
    if contract is None:
        raise ValueError("FINAL contract missing; Stage 2 must declare final_contract")
    return contract


def _group_to_grain_if_required(
    node: StrategyNode,
    mandatory_list: list[BuildConcept],
    final_contract: FinalAssemblyContract,
    environment: BuildEnvironment,
) -> StrategyNode:
    """Dedup a non-grouping FINAL contributor to the requested output grain.

    A row-preserving contributor (a ROOT scan or plain projection) whose source
    grain is finer than the selected concepts' grain must be grouped down to
    that grain; otherwise duplicate rows at the coarser grain survive into the
    output and inflate any downstream aggregate that reads them. This is the
    dedup the FINAL assembly otherwise skips for the single-contributor case.

    The group-required decision is made against the user-requested concepts
    only, but the GroupNode keeps any hidden grain keys the node exposed for
    sibling joins: grouping by them alongside the requested columns preserves
    those keys (and the join handle) without changing the dedup grain.
    Aggregates/windows already sit at their own grain and are left untouched;
    a MergeNode resolves its own grouping via `force_group`."""
    from trilogy.core.processing.discovery_utility import check_if_group_required

    if not final_contract.deduplicate_to_grain:
        return node
    contract_outputs = [
        concept
        for concept in mandatory_list
        if concept.address in final_contract.output_addresses
    ]
    if isinstance(node, (GroupNode, WindowNode)):
        return node
    if node.force_group and not _holds_summary_rows(node, contract_outputs):
        return node
    # A non-standard-grouping (ROLLUP/CUBE/GROUPING SETS) contributor's rows are
    # already final-shape: subtotal/total rows are distinct outputs, so a dedup
    # to the requested grain re-aggregates them away (and a grouping()-derived
    # dim can't be re-grouped outside its grouping set). Flat passthrough.
    # Skipping the dedup here is what obliges the FINAL not to join such a
    # node on a non-unique key: same predicate, both sides.
    if node_nulls_grouping_keys(node):
        return node
    if (
        check_if_group_required(
            downstream_concepts=contract_outputs,
            parents=[node.resolve()],
            environment=environment,
        ).required
        is not True
    ):
        return node
    mandatory_addrs = set(final_contract.output_addresses)
    targets = [o for o in node.output_concepts if o.address in mandatory_addrs]
    # Pseudonym fallback: a struct field surfaces under its origin address
    # (`s.a`, carrying the attr-access lineage) while the requested output is the
    # canonical key (`local.a`) that no output names directly. Keep the node's
    # covering output so the projection renders; the CTE layer maps the two by
    # pseudonym for the user-facing alias.
    if not targets:
        targets = [
            o
            for o in node.output_concepts
            if any(output_covers(o, m) for m in contract_outputs)
        ]
    if isinstance(node, MergeNode):
        # Narrow to the requested grain *before* force-grouping: a MergeNode
        # exposes its join/filter columns as outputs, so force_group alone
        # would GROUP BY the full merge grain and dedup nothing.
        node.force_group = True
        node.set_output_concepts(targets, rebuild=False, change_visibility=False)
        node.rebuild_cache()
        return node
    # A rollable aggregate exposed by a row-preserving scan being grouped to a
    # coarser grain is a precomputed value at the finer grain (a
    # materialized-root rollup from a finer summary table), so re-aggregate it
    # with SUM rather than dedup it. Exact-grain materialized aggregates never
    # reach here: their scan already matches the target grain, so no group is
    # required.
    rollup = [o for o in targets if o.is_aggregate and _is_rollable_aggregate(o)]
    return GroupNode(
        output_concepts=targets,
        input_concepts=targets,
        environment=environment,
        parents=[node],
        partial_concepts=list(node.partial_concepts),
        preexisting_conditions=node.preexisting_conditions,
        hidden_concepts=set(node.hidden_concepts) if node.hidden_concepts else None,
        rollup_concepts=rollup or None,
    )


def _holds_summary_rows(node: StrategyNode, outputs: list[BuildConcept]) -> bool:
    """A grouping that kept a summary's keys (a join handle a filter needed)
    still holds a summary value per summary row; the dedup to the requested
    grain is the re-sum that finishes it."""
    resolved = node.resolve()
    demanded = {c.address for c in outputs}
    return any(
        c.address in demanded
        for c in pending_rollups(resolved.rollup_concepts, resolved.grain)
    )


def _hide_final_only_grain_keys(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    gid: str,
    node: StrategyNode,
    mandatory_addresses: set[str],
) -> None:
    output_addrs = {o.address for o in node.output_concepts}
    grain_addrs = set(attrs[gid].grain_components)
    for anc in nx.ancestors(group_graph, gid):
        if anc != FINAL_NODE_ID and attrs[anc].derivation in GROUPING_DERIVATIONS:
            grain_addrs |= set(attrs[anc].grain_components)
    hide = (grain_addrs & output_addrs) - mandatory_addresses
    if not hide:
        return
    existing = set(node.hidden_concepts or set())
    node.hidden_concepts = existing | hide
    node.rebuild_cache()


def _subtree_applies_conditions(node: StrategyNode, where: BuildWhereClause) -> bool:
    """Whether some node in this subtree already applies (implies) `where`.

    A FINAL-deferred atom can be double-placed at a lower host (the aggregate
    group's pre-filter wrapper). Re-applying it on a sole contributor is at
    best redundant; for a ROLLUP contributor it is destructive: the feeder
    re-join pairs on grouping keys the ROLLUP NULLs at subtotal rows, so the
    subtotal/total rows drop."""
    row_args = {c.address for c in condition_row_args(where)}
    stack: list[StrategyNode] = [node]
    while stack:
        current = stack.pop()
        for applied in (current.conditions, current.preexisting_conditions):
            if applied is not None and condition_implies(applied, where.conditional):
                return True
        for parent in current.parents:
            # A standard GroupNode applies its WHERE to the rows it aggregates.
            # The groups it emits are gated only when the atom is decided per
            # group, i.e. every row arg is one of its outputs; otherwise a row
            # stream merged back onto those groups re-admits rows the atom
            # rejects, so nothing applied at or below it counts for `node`.
            if (
                isinstance(parent, GroupNode)
                and not node_nulls_grouping_keys(parent)
                and not row_args <= {o.address for o in parent.output_concepts}
            ):
                continue
            stack.append(parent)
    return False


def _push_row_condition_before_group(
    node: StrategyNode,
    where: BuildWhereClause,
    environment: BuildEnvironment,
) -> bool:
    current = node
    while isinstance(current, SelectNode) and len(current.parents) == 1:
        current = current.parents[0]
    if not isinstance(current, GroupNode) or not current.parents:
        return False
    parent_output_by_addr = {
        output.address: output
        for parent in current.parents
        for output in parent.output_concepts
    }
    if any(
        arg.address not in parent_output_by_addr for arg in condition_row_args(where)
    ):
        return False
    parent_outputs = list(parent_output_by_addr.values())
    current.parents = [
        SelectNode(
            input_concepts=parent_outputs,
            output_concepts=parent_outputs,
            environment=environment,
            parents=list(current.parents),
            conditions=where.conditional,
        )
    ]
    current.rebuild_cache()
    return True


def _clear_groupmate_completed_partials(
    node: StrategyNode, environment: BuildEnvironment
) -> None:
    """Un-mark a scoped-join key the merge itself completes.

    A subset-side member (`subset join a.store = b.store`, a ⊆ b) is partial on
    its own contributor (it spans only the subset domain), but this merge pairs
    it with its complete group-mate via the authored equality, so the merged
    relation spans the anchor's domain and the key renders as the coalesced
    group axis. Leaving it partial trips the final no-complete-source guard
    for a value that is in fact complete here.

    A relation whose anchor members are ABSENT from the plan entirely (the
    query never references the anchor side, so it was never sourced) is pure
    domain metadata: the subset side's own domain IS the output domain, and
    its partial clears too (collapsing to the subset side alone)."""
    if not node.partial_concepts or not environment.scoped_join_key_groups:
        return
    complete_outputs: set[str] = set()
    all_outputs: set[str] = set()
    for parent in node.parents:
        parent_partial = {c.address for c in parent.partial_concepts}
        all_outputs |= {c.address for c in parent.output_concepts}
        complete_outputs |= {
            c.address for c in parent.output_concepts if c.address not in parent_partial
        }
    rowset_addresses = {
        c.address
        for parent in node.parents
        for c in parent.output_concepts
        if c.derivation == Derivation.ROWSET
    }
    keep: list[BuildConcept] = []
    for concept in node.partial_concepts:
        mates: set[str] = set()
        for canonical, members in environment.scoped_join_key_groups.items():
            if concept.address in members or concept.address == canonical:
                mates |= (members | {canonical}) - {concept.address}
        if not mates:
            keep.append(concept)
            continue
        if concept.derivation == Derivation.ROWSET:
            # A boundary-marked subset member clears through a complete mate,
            # or when no mate is in the plan at all.
            if mates & complete_outputs or not mates & all_outputs:
                continue
            keep.append(concept)
            continue
        # A RAW datasource-bound member (`subset join cust = members.mid`) is
        # partial on its own contributor but complete through the relation when
        # its mate is a ROWSET handle carrying the anchor's whole domain: the
        # key renders as the coalesced axis, exactly as for a rowset-handle
        # member. Without a complete ROWSET mate it must keep tripping the
        # guard; that is the deliberate author-facing error for a member whose
        # only binding is its own scoped declaration.
        if mates & complete_outputs & rowset_addresses:
            continue
        keep.append(concept)
    if len(keep) != len(node.partial_concepts):
        node.partial_concepts = keep
        node.partial_lcl = LooseBuildConceptList(concepts=keep)


def _resourced_root_contributor(
    gid: str,
    node: StrategyNode,
    group_concepts: list[BuildConcept],
    projection_grain: frozenset[str],
    attrs: dict[str, GroupAttrs],
    environment: BuildEnvironment,
    ownership: ExtentOwnership,
    root_request: RootRequest,
    graph: ReferenceGraph,
    history: V4History,
    arms_delivered: bool,
    grouping_sibling: bool,
) -> list[StrategyNode]:
    """A ROOT contributor to FINAL, re-sourced when the merge needs more of
    it than its build carries (preserved join keys, filter-only args), and
    wrapped to the grain the merge joins it at."""
    # A filter-only WHERE arg the SELECT never projects (a dim attribute
    # FD by this dim bucket's key) is not in `group_concepts`, so
    # `_root_atoms_satisfiable_from` would drop its atom and the fresh
    # re-source would lose the WHERE. When such an arg was peeled INTO
    # this bucket (a primary member), add it to the projection so
    # plan_source sources the dim table and applies the filter. It
    # isn't mandatory, so the FINAL merge selects only the outputs and
    # never leaks it. Restricted to bucket members so a
    # global-aggregate/cross-arm filter arg (handled as a hidden
    # cross-join input via `_filter_arg_parents`) is untouched.
    bucket_members = _own_members(attrs, gid)
    seen_group_addrs = {c.address for c in group_concepts}
    # A materialized aggregate's scan applies a WHERE finer than its grain
    # itself (`ConceptAttrs.scan_filtered`); projecting that filter column
    # would read the summary's rows at the filter's grain, un-rolled.
    rolled = [c for c in group_concepts if c.is_aggregate]
    filter_only_concepts = [
        c
        for address in sorted(
            arg.address
            for atom in _atoms_at(attrs, gid)
            for arg in atom.row_arguments
            if arg.address in bucket_members
        )
        if address not in seen_group_addrs
        and (c := _concept_at(environment, address)) is not None
        and not any(finer_than_grain(c, r.grain, environment.concepts) for r in rolled)
    ]
    group_concepts.extend(filter_only_concepts)
    root_atoms = _atoms_at(attrs, gid)
    satisfiable = _root_atoms_satisfiable_from(root_atoms, group_concepts)
    # The fresh re-source keeps the root group's own WHERE, and is
    # skipped when an atom is one the scan cannot state (its value
    # comes from a constraint parent): `node` alone applies it.
    # A solid root re-sources under its group's own scope, as it was
    # built; a region domain is the FINAL's own rows and keeps the
    # FINAL's scope.
    scope = environment.span_scope
    if not attrs[gid].extent_spans:
        scope = _group_span_scope(scope, ownership, gid)
    with under_span_scope(environment, scope):
        # a region domain's rows are identified by its spans: a
        # rename it carries (`group_id as g`) adds no grain of its own
        projected = (
            unique(group_concepts, "address")
            if attrs[gid].extent_spans
            else _projection_root_concepts(group_concepts, environment)
        )
        request = RootRequest(
            frozenset(c.address for c in projected),
            _wrap_atoms(satisfiable),
            environment.span_scope,
            preexisting=root_request.preexisting,
        )
        # Only a request the built node does not answer is planned:
        # the preserved keys and filter-only args widened it beyond
        # what the scan carries, or the scope moved.
        fresh = (
            _fresh_final_root_projection(
                projected,
                environment,
                graph,
                history,
                request.conditions,
                arms_delivered,
            )
            if projected
            and len(satisfiable) == len(root_atoms)
            and not request.answered_by(node, root_request)
            else None
        )
    if fresh is not None:
        node = fresh
    # The filter-only args above exist so the scan can SOURCE and APPLY
    # the WHERE; they are not columns the merge consumes. Bucketing them
    # by natural grain shatters off a GroupNode at the filter's own grain
    # (`date_dim.date` -> {date_sk}) that projects nothing anyone reads,
    # and it shares no key with the real projection, so the merge
    # cross-joins it ON 1=1. The condition is already applied inside
    # `node`, so dropping them here loses nothing.
    merge_concepts = [c for c in group_concepts if c not in filter_only_concepts]
    if attrs[gid].extent_spans:
        # A region's rows are identified by ALL its spans together:
        # bucketing the keys by natural grain would stitch each back
        # on its own and NULL the rest on the extension rows.
        wrapped = [_distinct_projection(node, merge_concepts, environment)]
        wrapped[0].region_spans = attrs[gid].extent_spans
    else:
        wrapped = _wrap_for_grain(
            node,
            merge_concepts,
            environment,
            projection_grain,
            dedup_orthogonal=grouping_sibling,
        )
    return wrapped


def _assemble_sole_contributor(
    gid: str,
    per_group: dict[str, list[BuildConcept]],
    final_conditions: BuildWhereClause | None,
    final_probe_args: list[BuildConcept],
    filter_only_addrs: set[str],
    apply_final_conditions: Callable[[StrategyNode], StrategyNode],
    mandatory_list: list[BuildConcept],
    final_contract: FinalAssemblyContract,
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    environment: BuildEnvironment,
) -> StrategyNode:
    """FINAL when one group covers every output: that group's node, deduped
    to the output grain, under the WHERE deferred onto FINAL."""
    mandatory_addresses = {c.address for c in mandatory_list}
    sole_node = built[gid]
    # A sole contributor can CONTAIN the completion merge (the ratio BASIC
    # over `subset join a.wk = b.wk` pairs both boundaries internally), so
    # the subset-side key it carries is complete here even though the
    # multi-contributor clearing at the FINAL merge never runs.
    _clear_groupmate_completed_partials(sole_node, environment)
    # A FINAL-deferred presence-probe filter joins its feeder back on the
    # probe's key group. The normal path hides non-mandatory grain keys and
    # dedups to the output grain FIRST, which strips the join key and
    # degrades the feeder join to 1=1; apply the condition over the raw
    # contributor (keys intact), then dedup the filtered rows. Same path
    # for a feeder that participates in a scoped relation with this
    # contributor (`where return_demos.r_ticket is not null` over a
    # `union join return_demos.demo_id = c_demo` selecting only c_name):
    # its join back rides the relation axis, which only the raw
    # contributor can still widen to.
    final_already_applied = final_conditions is not None and (
        _subtree_applies_conditions(sole_node, final_conditions)
    )
    # A row condition available on the contributor's input is
    # population-scope. Apply it there before aggregation rather than
    # materializing the same input as a filter-only sibling. This also
    # preserves ROLLUP subtotal rows whose grouping keys become NULL.
    if (
        final_conditions is not None
        and not final_already_applied
        and not final_conditions.existence_arguments
        and _push_row_condition_before_group(sole_node, final_conditions, environment)
    ):
        final_already_applied = True
    # Same again for a feeder joining a region holder on its span (`select
    # name where status is null`: the condition scan carries `customer_id`,
    # the domain hides it), or on a column the holder carries beside its
    # outputs (`count(order_id) by city`), which the dedup to `name` would
    # strip.
    relation_paired_feeders = False
    region_paired_feeders = False
    if final_conditions is not None and not final_already_applied:
        sole_avail = {o.address for o in sole_node.output_concepts}
        feeder_nodes, _ = _filter_arg_parents(
            group_graph, attrs, built, filter_only_addrs - sole_avail
        )
        feeder_outs = [{o.address for o in f.output_concepts} for f in feeder_nodes]
        scoped_addrs = environment.all_scoped_join_group_members()
        relation_paired_feeders = any(outs & scoped_addrs for outs in feeder_outs)
        spans = region_reads(sole_node)
        region_paired_feeders = bool(spans) and any(
            outs & (spans | sole_avail) - mandatory_addresses for outs in feeder_outs
        )
    if final_probe_args or relation_paired_feeders or region_paired_feeders:
        conditioned = apply_final_conditions(sole_node)
        # The feeder join reads the probe at ITS OWN row grain (the fact
        # side of the relation), fanning the contributor out; the merge's
        # claimed grain predates that join, so grain-satisfaction checks
        # wave the dedup through. Collapse explicitly to the requested
        # outputs after the filter.
        if conditioned is not sole_node and final_contract.deduplicate_to_grain:
            targets = [
                o
                for o in conditioned.output_concepts
                if o.address in mandatory_addresses
            ] or list(conditioned.output_concepts)
            final_node: StrategyNode = GroupNode(
                output_concepts=targets,
                input_concepts=targets,
                environment=environment,
                parents=[conditioned],
                partial_concepts=list(conditioned.partial_concepts),
                preexisting_conditions=conditioned.preexisting_conditions,
                force_group=True,
            )
        else:
            final_node = _group_to_grain_if_required(
                conditioned, mandatory_list, final_contract, environment
            )
        _bridge_pseudonyms(final_node, per_group[gid])
        return final_node
    # The contributing group's outputs include grain keys it exposed
    # for sibling JOINs (see `_compute_concept_sets`). At the user-
    # facing FINAL projection those keys aren't part of mandatory and
    # would otherwise leak into the SELECT. Mask them with
    # hidden_concepts; only valid at the FINAL layer, since hiding
    # them at an intermediate group blocks downstream consumers from
    # using them as JOIN keys (MergeNode validates non-hidden parent
    # outputs only).
    # A basic riding a window-over-aggregate (dimensions over a
    # ROLLUP-then-rank) passes the aggregate's grain keys through as
    # row-identity / partition columns. Those aren't this basic's declared
    # grain, so add every grouping ancestor's grain to the hide candidates;
    # otherwise the carried keys leak into the FINAL projection alongside
    # their mandatory rename.
    _hide_final_only_grain_keys(
        group_graph,
        attrs,
        gid,
        sole_node,
        mandatory_addresses,
    )
    final_node = _group_to_grain_if_required(
        sole_node,
        mandatory_list,
        final_contract,
        environment,
    )
    # The multi-contributor path projects `per_group` directly; the
    # single-contributor path returns the node's raw output, which can name a
    # merged key under a sibling alias the user didn't write. Bridge last, so
    # the hidden bridge concepts can't perturb the grain decision above.
    _bridge_pseudonyms(final_node, per_group[gid])
    conditioned = (
        final_node if final_already_applied else apply_final_conditions(final_node)
    )
    if conditioned is final_node:
        return conditioned
    # Applying a FINAL-deferred condition can wrap the contributor in a node
    # that reads it at a finer grain than the output: a membership
    # (`cust_id in <set>`) filters a contributor that carries an extra grain
    # key (`channel`) only so the IN-set subselect can read it, so the
    # filtered rows still duplicate at the output grain. Re-dedup the
    # conditioned result (no-op when it already sits at the output grain).
    return _group_to_grain_if_required(
        conditioned, mandatory_list, final_contract, environment
    )


def _apply_final_conditions(
    node: StrategyNode,
    final_conditions: BuildWhereClause | None,
    filter_only_addrs: set[str],
    mandatory_list: list[BuildConcept],
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    environment: BuildEnvironment,
    feeder_cache: "_CleanFeederCache | None",
) -> StrategyNode:
    """`node` under the WHERE deferred onto FINAL, its filter-only args'
    producers joined in as hidden inputs."""
    mandatory_addresses = {c.address for c in mandatory_list}
    if final_conditions is None:
        return node
    # Project only the user-requested columns. The merge below may expose
    # extra align inputs (per-arm keys folded into the align key) that
    # aren't mandatory and don't render at this layer.
    keep = [o for o in node.output_concepts if o.address in mandatory_addresses]
    avail = {o.address for o in node.output_concepts}
    spans = region_reads(node)
    arg_nodes, arg_concepts = _filter_arg_parents(
        group_graph,
        attrs,
        built,
        _region_paired_args(filter_only_addrs, avail, spans, environment),
        spans,
    )
    row_arg_addrs = {c.address for c in condition_row_args(final_conditions)}
    row_concepts = [
        concept for concept in node.output_concepts if concept.address in row_arg_addrs
    ]
    # A membership (`x in <set>`) deferred onto FINAL is hosted on a merge
    # built here, so its subselect feeder is wired here too.
    ex_groups = _condition_existence_arg_groups(final_conditions.conditional)
    ex_concepts = _flatten_arg_groups(ex_groups)
    ex_parents = (
        _existence_parents_for(ex_groups, built, skip=node, feeder_cache=feeder_cache)
        if ex_groups
        else []
    )
    # A feeder that participates in a scoped relation must join back on
    # the relation axis, not cross-join: widen the contributor (and the
    # feeder) with the authored members each side can render: a leaf
    # scan picks up the mate it binds, the boundary its member handle.
    # Feeders with no relation stay hidden cross-join inputs.
    if arg_nodes:
        # A gate keyed by a column the contributor never projects
        # (`sum(amount) by status > 35 or ...` over `select oid`) pairs on
        # that key; carried hidden, or the merge has nothing to join on.
        gate_keys = _gate_grain_keys(arg_concepts) - avail
        if gate_keys:
            _widen_merge_join_keys([node, *arg_nodes], environment, gate_keys)
        # A row-level atom over the facts (`undelivered`) beside a node
        # holding a region joins it on the span: cross-joined, any fact
        # row's value would pass for every member of the region.
        if node_spans := region_reads(node):
            _widen_merge_join_keys([node, *arg_nodes], environment, node_spans)
    if arg_nodes and environment.scoped_join_key_groups:
        relation_keys: set[str] = set()
        for feeder in arg_nodes:
            feeder_outs = {o.address for o in feeder.output_concepts}
            for relation in environment.scoped_join_relations():
                if feeder_outs & relation:
                    relation_keys |= relation
        if relation_keys:
            _widen_merge_join_keys(
                [node, *arg_nodes], environment, frozenset(relation_keys)
            )
    sources = ConditionSources(
        row_concepts=row_concepts + arg_concepts,
        row_parents=arg_nodes,
        existence_concepts=ex_concepts,
        existence_parents=ex_parents,
    )
    return inject_condition_at_node(
        node,
        final_conditions,
        keep,
        environment,
        sources,
        hidden_concepts=(
            {c.address for c in arg_concepts} - mandatory_addresses
            if arg_nodes
            else None
        ),
        input_concepts=[
            c for c in node.output_concepts if c.address not in node.hidden_concepts
        ]
        + arg_concepts,
        condition_on_merge=bool(arg_nodes),
        combine_existing=False,
    )


def _gate_key_row_stream(
    address: str, attrs: dict[str, GroupAttrs], candidates: Collection[str]
) -> str | None:
    """The row stream computing a gate's derived key: a BASIC group, never the
    gate's own feeder (which carries the key too). The select phase's first."""
    hosts = sorted(
        (gid.startswith("grp:[@"), gid)
        for gid in candidates
        if attrs[gid].derivation == Derivation.BASIC
        and address in attrs[gid].output_concepts
    )
    return hosts[0][1] if hosts else None


@dataclass
class _FinalCover:
    """The FINAL election, decided before the build loop: the concepts each
    contributor provides, and the columns owed on a contributor beyond its
    build: hosted over it (`projections`) or passed through it from an
    ancestor contributor (`passthroughs`)."""

    per_group: dict[str, list[BuildConcept]]
    projections: dict[str, list[BuildConcept]]
    passthroughs: dict[str, list[BuildConcept]]


def _elect_final_cover(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    groups: Sequence[str],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
) -> _FinalCover:
    """Elect the FINAL's contributors from `groups`, off what each renders."""
    cover = _FinalCover(
        elect_cover(
            group_graph,
            group_edges,
            attrs,
            groups,
            mandatory_list,
            environment,
            attrs[FINAL_NODE_ID].extent_ownership or ExtentOwnership(),
        ),
        {},
        {},
    )
    if not cover.per_group:
        return cover
    rendered = renders(group_graph, group_edges, attrs, environment)
    _add_partial_completion_contributors(
        groups, cover.per_group, attrs, rendered, environment
    )
    _fold_descendant_contributors(
        group_graph,
        group_edges,
        attrs,
        cover.per_group,
        cover.passthroughs,
        rendered,
        environment,
    )
    _promote_final_aliases_to_grouping_contributors(
        group_graph,
        group_edges,
        attrs,
        cover.per_group,
        cover.projections,
        mandatory_list,
        rendered,
        environment,
    )
    # last: the promotion above drops a contributor left with no concepts,
    # which is all a row-only domain ever has
    _add_region_domain_contributors(
        group_graph, attrs, groups, cover.per_group, cover.projections, environment
    )
    return cover


def _assemble_final_node(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    graph: ReferenceGraph,
    history: V4History,
    root_requests: dict[str, RootRequest],
    cover: "_FinalCover",
    candidates: Sequence[str] = (),
    feeder_cache: "_CleanFeederCache | None" = None,
    arms_delivered: bool = False,
) -> StrategyNode | None:
    """Build the FINAL output node: merge the groups the stage-2 `cover`
    elected to cover `mandatory_list`. When a single group already covers
    every mandatory concept, return it as-is. Otherwise wrap the contributing
    groups in a MergeNode whose auto-join logic links them on shared output
    concepts.

    ROOT contributions are projected down to the needed concepts' natural
    grain via `_wrap_for_grain` so the merge join doesn't blow up cardinality
    (a customer property at customer grain instead of one row per fact
    line)."""
    if not built:
        return None
    # A cross-arm post-merge filter (e.g. `cnt_00 <= cnt_99`) that no pre-final
    # group could host was deferred onto FINAL by `_inject_conditions`; apply it
    # as a WHERE over the assembled merge, where both columns coexist.
    final_conditions = _wrap_atoms(attrs[FINAL_NODE_ID].condition_atoms)
    final_contract = _required_final_contract(attrs)
    mandatory_addresses = {c.address for c in mandatory_list}
    # Row-args a FINAL-deferred filter needs that aren't user outputs (a global
    # aggregate compared against a per-key one). Their producing groups get
    # cross-joined in as hidden inputs below, else the WHERE dangles.
    filter_only_addrs: set[str] = set()
    for atom in attrs[FINAL_NODE_ID].condition_atoms:
        filter_only_addrs |= {a.address for a in atom.row_arguments}
    filter_only_addrs -= mandatory_addresses

    apply_final_conditions = partial(
        _apply_final_conditions,
        final_conditions=final_conditions,
        filter_only_addrs=filter_only_addrs,
        mandatory_list=mandatory_list,
        group_graph=group_graph,
        attrs=attrs,
        built=built,
        environment=environment,
        feeder_cache=feeder_cache,
    )

    ownership = attrs[FINAL_NODE_ID].extent_ownership or ExtentOwnership()
    per_group = cover.per_group
    if not per_group:
        return apply_final_conditions(
            _group_to_grain_if_required(
                next(iter(built.values())),
                mandatory_list,
                final_contract,
                environment,
            )
        )
    _pass_through_final_contributors(built, cover.passthroughs)
    _project_final_contributors(built, cover.projections, environment)
    contributing = list(per_group.keys())
    final_probe_args = (
        [
            arg
            for arg in condition_row_args(final_conditions)
            if is_presence_probe(arg.address)
        ]
        if final_conditions is not None
        else []
    )
    if len(contributing) == 1:
        return _assemble_sole_contributor(
            contributing[0],
            per_group,
            final_conditions,
            final_probe_args,
            filter_only_addrs,
            apply_final_conditions,
            mandatory_list,
            final_contract,
            group_graph,
            attrs,
            built,
            environment,
        )

    # The join plan states, from stage-2 facts over the cover, what identifies
    # each contributor's rows and which keys each ROOT must carry to pair with
    # its siblings. Its merge grain is the grain the grouping and barrier-pinned
    # contributors fix: a dimension scan projects its FD attributes at it
    # (keeping the join key) instead of deduping to its own key grain and
    # cross-joining ON 1=1.
    bridges = elect_bridges(
        group_graph,
        attrs,
        per_group,
        candidates,
        mandatory_list,
        environment,
        final_contract.authored_axes,
    ).extras
    plan = plan_final_join(
        group_graph,
        attrs,
        per_group,
        mandatory_list,
        environment,
        final_contract.authored_axes,
        extras=bridges
        + _final_extra_sides(
            group_graph,
            attrs,
            list(built),
            [*per_group, *bridges],
            filter_only_addrs,
            environment,
        ),
    )
    final_merge_grain = plan.merge_grain
    origin = {id(node): gid for gid, node in built.items()}

    # A grouping-sets sibling suppresses the FINAL dedup (`_group_to_grain_if_
    # required` passes a subtotal-bearing merge through), so a row-grain leaf
    # contributor must dedup itself before the merge instead.
    grouping_sibling = any(node_nulls_grouping_keys(built[g]) for g in contributing)

    parents: list[StrategyNode] = []
    for gid in contributing:
        node = built[gid]
        if attrs[gid].derivation != Derivation.ROOT:
            parents.append(node)
            continue
        # Only root scans are re-sourced and wrapped: their grain is the
        # row-level source-table grain (often much wider than the merge wants),
        # and a SELECT DISTINCT-style projection is always safe. Wrapping an
        # intermediate aggregate is not: it would re-aggregate partial sums.
        carry = plan.carry_demands[gid]
        group_concepts = list(per_group[gid])
        seen_group_concepts = {concept.address for concept in group_concepts}
        group_concepts.extend(
            c
            for address in sorted(carry)
            if (c := _concept_at(environment, address)) is not None
            and address not in seen_group_concepts
        )
        # A carried key must survive the wrap: grouping to a grain without the
        # key it was re-sourced to carry dedups it straight back out.
        wraps = _resourced_root_contributor(
            gid,
            node,
            group_concepts,
            final_merge_grain | carry,
            attrs,
            environment,
            ownership,
            root_requests[gid],
            graph,
            history,
            arms_delivered,
            grouping_sibling,
        )
        origin.update((id(wrap), gid) for wrap in wraps)
        parents.extend(wraps)

    parents = [
        (
            _at_declared_identity(
                parent, parents, attrs[side], plan.identity[side], environment
            )
            if (side := origin.get(id(parent))) is not None
            else parent
        )
        for parent in parents
    ]

    # Sibling contributors that descend from a common richer parent (one
    # renames the aggregate's grouping key while others derive from that same
    # aggregate) expose no shared output key (their declared grain is the
    # source-row grain), so the merge would cross-join them ON 1=1. Fold any
    # contributor whose outputs a row-preserving sibling already renders off
    # its own parents, collapsing the columns into one projection instead of
    # joining (same passthrough logic the per-group `_pre_merge_parents`
    # uses).
    final_needed = set(mandatory_addresses) | set(final_merge_grain)
    parents = _fold_constant_parents(parents, final_needed)
    parents = _satisfy_parent_projection_contract(
        parents,
        final_needed,
        final_merge_grain,
        environment,
    )
    parents = _fold_passthrough_parents(parents)
    _carry_planned_keys(parents, origin, plan, environment)
    parents = _fold_covered_contributors(
        parents,
        environment,
        final_needed | filter_only_addrs,
        {id(built[gid]) for gid, concepts in per_group.items() if concepts},
        {id(built[gid]) for gid in ownership.owner_by_span.values() if gid in built},
    )
    parents = _join_elected_bridges(
        parents, [(built[gid], plan.keys_for(gid)) for gid in bridges], environment
    )
    parents = _pair_inside_padding_streams(parents, environment)
    _raise_if_rowset_islanded(parents, mandatory_list, environment, graph)

    available: set[str] = set()
    for p in parents:
        for o in p.output_concepts:
            available.add(o.address)
    # A mandatory concept a parent computes only under a pseudonym alias (`merge
    # derived_measure into measure`: the parent outputs the derivation, the user
    # wrote the merge target) is covered but not address-available. It must ride
    # as an OUTPUT (resolve_concept_map's targets loop maps it to the parent via
    # the pseudonym) but never as an INPUT; an inherited input is skipped by
    # that loop and the column would dangle. The sole-contributor path solves
    # the same gap with _bridge_pseudonyms; this is the merge-path equivalent.
    pseudonym_only = {
        c.address
        for c in mandatory_list
        if c.address not in available
        and any(
            output_covers(o, c)
            for p in parents
            for o in p.output_concepts
            if o.address not in p.hidden_concepts
        )
    }
    outputs = [
        c
        for c in mandatory_list
        if c.address in available or c.address in pseudonym_only
    ]
    # Pull in any filter-only condition arg (e.g. the global aggregate) not
    # already supplied by a contributor, as a hidden cross-join input.
    spans = frozenset().union(*(region_reads(p) for p in parents))
    arg_nodes, arg_concepts = _filter_arg_parents(
        group_graph,
        attrs,
        built,
        _region_paired_args(filter_only_addrs, available, spans, environment),
        spans,
    )
    # A filter-only arg a contributor ALREADY supplies (`rs.sa is not null`
    # beside a boundary outputting rs.sa) must ride the merge as a hidden
    # input; otherwise the merge's WHERE references a column it never
    # carried and join resolution re-joins the producer as a second,
    # PRE-filtered sibling (a preserving relation then re-admits the
    # filtered rows).
    supplied_filter_args = [
        o
        for p in parents
        for o in p.output_concepts
        if o.address in (filter_only_addrs & available)
        and o.address not in p.hidden_concepts
    ]
    # A mandatory COALESCING key-group member whose mate rides an axis-only
    # completion contributor (see `_add_partial_completion_contributors`): the
    # merge must EMIT the mate (hidden below) or parent dedup drops the mate's
    # side as redundant and the projected member silently collapses back to
    # its own side's domain (bare-member projection).
    axis_mates: list[BuildConcept] = []
    if environment.scoped_join_key_groups:
        coalescing_addrs = environment.domain_graph.coalescing_relation_members()
        for relation in environment.scoped_join_relations():
            if not relation & mandatory_addresses & coalescing_addrs:
                continue
            axis_mates.extend(
                c
                for addr in sorted((relation - mandatory_addresses) & available)
                if (c := _concept_at(environment, addr)) is not None
            )
    # A region's rows join back on its spans, from the domain or from whatever
    # read it. One the statement never names still has to be an output of this
    # merge: the host side is the one carrying the licensed keys the merge EMITS.
    region_keys = [
        c
        for span in sorted(spans)
        if span in available
        and span not in mandatory_addresses
        and (c := _concept_at(environment, span)) is not None
    ]
    outputs = unique(outputs + axis_mates + region_keys, "address")
    # A gate keyed by a column no contributor projects (`count(id) by genus`,
    # genus derived from a scanned column) pairs on that key, as in
    # `_apply_final_conditions`: carried hidden, or the merge joins on nothing.
    if arg_nodes and (gate_keys := _gate_grain_keys(arg_concepts) - available):
        _widen_merge_join_keys([*parents, *arg_nodes], environment, gate_keys)
        # one no contributor can render (a key pinned to columns its rows
        # lack) is joined in from the row stream computing it
        carried = {o.address for p in parents for o in p.output_concepts}
        for address in sorted(gate_keys - carried):
            host = _gate_key_row_stream(address, attrs, built)
            if host and built[host] not in parents:
                parents = parents + [built[host]]
    parents = parents + arg_nodes
    sides = side_groups(parents, origin)
    if unplanned := set(sides) - set(plan.contributors):
        raise ValueError(
            f"[v4] FINAL merge joins {sorted(unplanned)}, which its join plan "
            "never elected. This is a planner bug."
        )
    merge_plan = plan.over(sides)
    logger.info(f"[v4] FINAL join plan: {merge_plan}")
    merge_inputs = unique(
        [c for c in outputs if c.address not in pseudonym_only]
        + arg_concepts
        + supplied_filter_args,
        "address",
    )
    hidden = {
        c.address
        for c in (*arg_concepts, *supplied_filter_args, *axis_mates, *region_keys)
    } - mandatory_addresses
    # A non-grouping dimension contributor only supplies FD attributes; if it
    # sits at a finer (row-level) grain it must not widen the merge grain, or it
    # fans the aggregate out (customer dims joined through a returns fact land
    # at returns grain). Pinning the grain lets the merge's
    # force_group collapse back to the aggregate grain. Left None when there is
    # no grouping contributor, so plain row merges keep their current behavior.
    merge_grain = (
        BuildGrain.from_concepts(final_merge_grain, environment=environment)
        if final_merge_grain
        else None
    )
    # The request atoms already applied inside contributing groups: the final
    # merge must know them so its join typing never null-extends the branch
    # that carries one; a row-preserving join there resurrects rows the
    # request WHERE rejected.
    applied_atoms = _wrap_atoms(
        [atom for group in attrs.values() for atom in group.condition_atoms]
    )
    merged = MergeNode(
        input_concepts=merge_inputs,
        output_concepts=outputs,
        environment=environment,
        parents=parents,
        grain=merge_grain,
        conditions=final_conditions.conditional if final_conditions else None,
        preexisting_conditions=applied_atoms.conditional if applied_atoms else None,
        hidden_concepts=hidden or None,
        # A bare axis-member projection's output IS the joined relation row by
        # row (contract stage 2 set deduplicate_to_grain=False); the merge must
        # not collapse the authored fan-out back to distinct key pairs.
        whole_grain=not final_contract.deduplicate_to_grain,
        host_stitch=True,
        planned_keys=merge_plan.permitted_keys,
    )
    _clear_groupmate_completed_partials(merged, environment)
    # A hidden axis mate rides sides kept at their own finer row grain, and the
    # merge's claimed grain predates that fan, so grain-satisfaction checks
    # wave the dedup through, same trap as the probe-feeder branch above.
    # Collapse explicitly to the coalesced axis. A hidden region join key is
    # the same story: the in-place narrowing would take it off the merge that
    # hosts by it.
    if (axis_mates or region_keys) and final_contract.deduplicate_to_grain:
        targets = [
            o for o in merged.output_concepts if o.address in mandatory_addresses
        ] or list(merged.output_concepts)
        return GroupNode(
            output_concepts=targets,
            input_concepts=targets,
            environment=environment,
            parents=[merged],
            partial_concepts=list(merged.partial_concepts),
            preexisting_conditions=merged.preexisting_conditions,
            force_group=True,
        )
    # Dedup the assembled merge to the requested output grain (as the
    # single-contributor path above does). A contributor left whole at a
    # finer row grain (a root scan kept at {store, wh, product} to preserve a
    # join key) otherwise leaks duplicate rows when its internal keys drop
    # out of the output grain. No-op when the merge already sits at the
    # mandatory grain (the common aggregate+dim case force_groups to
    # merge_grain == output grain).
    return _group_to_grain_if_required(
        merged,
        mandatory_list,
        final_contract,
        environment,
    )


def _at_declared_identity(
    node: StrategyNode,
    siblings: list[StrategyNode],
    group: GroupAttrs,
    identity: frozenset[str],
    environment: BuildEnvironment,
) -> StrategyNode:
    """A row stream built off rows finer than the identity its group declares
    (a filter of an item attribute sourced off the sale lines), deduplicated
    to that identity when an independent sibling exposes the finer row key
    too: the two would pair on the stream's row key, which no plan states. Its
    outputs are functions of the identity, so nothing is lost."""
    if (
        group.derivation in GROUPING_DERIVATIONS
        or group.derivation == Derivation.ROWSET
        or group.extent_spans
        or node_nulls_grouping_keys(node)
    ):
        return node
    keys = identity - {ALL_ROWS_ADDRESS}
    if not keys or not keys <= {o.address for o in node.usable_outputs}:
        return node
    closure = build_fd_closure(environment, keys, include_empty_grain=False)
    finer = _resolved_grain(node) - closure
    # a sibling the stream is built on, or built on it, holds the same rows
    exposed = set().union(
        *(
            _visible_addresses(other)
            for other in siblings
            if other is not node
            and not _derives_from(node, other)
            and not _derives_from(other, node)
        )
    )
    if not finer & exposed or not set(group.output_concepts) <= closure:
        return node
    kept = [o for o in node.usable_outputs if o.address in closure]
    return GroupNode(
        output_concepts=kept,
        input_concepts=kept,
        environment=environment,
        parents=[node],
        partial_concepts=[c for c in node.partial_concepts if c.address in closure],
        preexisting_conditions=node.preexisting_conditions,
        force_group=True,
    )


def _origin_group(node: StrategyNode, origin: dict[int, str]) -> str | None:
    """The group `node` came from, looking through the single-parent wraps
    assembly puts over a contributor."""
    while id(node) not in origin and len(node.parents) == 1:
        node = node.parents[0]
    return origin.get(id(node))


def side_groups(parents: list[StrategyNode], origin: dict[int, str]) -> list[str]:
    """The group each of `parents` came from; a contributor split into several
    wraps appears once per wrap."""
    return [
        gid for parent in parents if (gid := _origin_group(parent, origin)) is not None
    ]


def _first_row_marker(partition: list[BuildConcept]) -> BuildConcept:
    """`row_number() over (partition by <partition>)`: 1 on one row per tuple."""
    lineage = BuildNumberingWindowItem(
        type=WindowType.ROW_NUMBER,
        arguments=[partition[0]],
        order_by=[BuildOrderItem(expr=partition[0], order=Ordering.ASCENDING)],
        over=partition,
    )
    name = generate_concept_name(lineage)
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=lineage.output_datatype,
        purpose=Purpose.PROPERTY,
        build_is_aggregate=False,
        derivation=Derivation.WINDOW,
        lineage=lineage,
        grain=BuildGrain(components={c.address for c in partition}),
    )


def _read_first_rows(
    outputs: list[BuildConcept],
    first_row_grains: dict[str, frozenset[str]],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
) -> tuple[list[BuildConcept], list[StrategyNode]]:
    """Point each member of `first_row_grains` at the first row per tuple of
    its grain (`sum(case when <marker> = 1 then amount end)`), the marker a
    window over the shared stream. One pass reads two facts joined below it,
    so each fact's rows repeat per row of the other; this is the dedup a pass
    of its own would have had (`group_rules._first_row_grains`)."""
    available: dict[str, BuildConcept] = {}
    for parent in parents:
        for output in parent.output_concepts:
            for address in (output.address, *output.pseudonyms):
                available.setdefault(address, output)
    markers: dict[frozenset[str], BuildConcept] = {}
    rewritten: list[BuildConcept] = []
    for concept in outputs:
        grain = first_row_grains.get(concept.address)
        if grain is None or not isinstance(concept.lineage, BuildAggregateWrapper):
            rewritten.append(concept)
            continue
        missing = grain - set(available)
        if missing:
            raise UnresolvableQueryException(
                f"{concept.address} reads a stream repeating its rows, and the "
                f"stream does not carry {sorted(missing)} to read one row of each"
            )
        if grain not in markers:
            markers[grain] = _first_row_marker(
                [available[address] for address in sorted(grain)]
            )
        function = concept.lineage.function
        value, *rest = function.arguments
        first_only = BuildFunction(
            operator=FunctionType.CASE,
            arguments=[
                BuildCaseWhen(
                    comparison=BuildComparison(
                        left=markers[grain],
                        right=1,
                        operator=ComparisonOperator.EQ,
                    ),
                    expr=cast(BuildExpr, value),
                )
            ],
            output_data_type=arg_to_datatype(value),
            output_purpose=Purpose.PROPERTY,
            arg_count=-1,
        )
        lineage = dc_replace(
            concept.lineage,
            function=dc_replace(function, arguments=[first_only, *rest]),
        )
        rewritten.append(dc_replace(concept, lineage=lineage))
    if not markers:
        return outputs, parents
    stream = readable_outputs(parents)
    window = WindowNode(
        input_concepts=stream,
        output_concepts=[*stream, *markers.values()],
        environment=environment,
        parents=parents,
        nullable_concepts=unique(
            [c for p in parents for c in p.resolve().nullable_concepts], "address"
        ),
    )
    return rewritten, [window]


@dataclass(frozen=True)
class _FoldPlan:
    """The inline fold, decided before anything is built: the groups every
    reader computes inline, and the groups each aggregate reads in place of
    the inputs it alone computes inline."""

    inlined: set[str]
    aggregate_inputs: dict[str, list[str]]
    # what each group renders over the folded plan
    rendered: dict[str, frozenset[str]]

    def render(self, gids: Iterable[str]) -> set[str]:
        return set().union(*(self.rendered[gid] for gid in gids))


@dataclass(frozen=True)
class _Plan:
    """A group graph with its edges and attrs."""

    group_graph: nx.DiGraph
    group_edges: EdgeMap
    attrs: dict[str, GroupAttrs]


@dataclass
class _GroupBuild:
    """What every group of one `build_strategy_node` call reads and fills."""

    group_graph: nx.DiGraph
    group_edges: EdgeMap
    attrs: dict[str, GroupAttrs]
    mandatory_list: list[BuildConcept]
    environment: BuildEnvironment
    g: ReferenceGraph
    history: V4History
    complete_partials: bool
    staged_conditions: list[BuildWhereClause] | None
    depth: int
    arms_delivered: bool
    built: dict[str, StrategyNode]
    root_requests: dict[str, RootRequest]
    feeder_cache: _CleanFeederCache
    unbuilt: dict[str, list[BuildConcept]]
    order: list[str]
    # the plan before any fold: a group's build reads its readers there
    readers: "_Plan"
    folds: "_FoldPlan"


def _merge_origin_intermediates(
    outputs: list[BuildConcept],
    primary_addrs: set[str],
    environment: BuildEnvironment,
) -> list[BuildConcept]:
    """Merge origins this group computes only for its own outputs' lineage.

    A lineage arg spelled by a merge-demoted key (`upper(source_label)` after
    `merge label into source_label`) has no lineage of its own; the graph puts
    its origin in this group, but the origin renders only as a column, so it
    has to be projected for the consumer to read it."""
    emitted = {c.address for c in outputs}
    found: list[BuildConcept] = []
    stack = [c for c in outputs if c.address in primary_addrs and c.lineage]
    while stack:
        concept = stack.pop()
        assert concept.lineage is not None
        for arg in concept.lineage.concept_arguments:
            if arg.lineage is not None:
                stack.append(arg)
                continue
            for origin in environment.merge_origins(arg):
                if origin.address in primary_addrs and origin.address not in emitted:
                    emitted.add(origin.address)
                    found.append(origin)
                    stack.append(origin)
    return found


def _build_group(gid: str, ctx: _GroupBuild) -> StrategyNode | None:
    """Build one group of the plan with explicit parent nodes, under the
    group's span scope. None when it builds nothing (`ctx.unbuilt` says what
    it owed)."""
    from trilogy.core.processing.v4_node_generators import build_node  # cycle

    group_graph, group_edges, attrs = ctx.group_graph, ctx.group_edges, ctx.attrs
    environment, g, history = ctx.environment, ctx.g, ctx.history
    mandatory_list, built, feeder_cache = (
        ctx.mandatory_list,
        ctx.built,
        ctx.feeder_cache,
    )
    complete_partials, staged_conditions = ctx.complete_partials, ctx.staged_conditions
    depth, arms_delivered, root_requests = (
        ctx.depth,
        ctx.arms_delivered,
        ctx.root_requests,
    )
    a = attrs[gid]
    # Only the FINAL sink carries a None derivation, and it is skipped above.
    assert a.derivation is not None
    derivation = a.derivation
    select_addrs = _select_addresses(a)
    if derivation == Derivation.ROWSET:
        # A boundary group's outputs can carry ANOTHER rowset's handles
        # (a deferred WHERE's args exposed through a scoped relation).
        # `resolve_rowset` plans the rowset of the first handle it sees, so
        # order this group's OWN handles (its primary members) first so a
        # foreign condition-arg handle can't hijack the boundary. Permanent:
        # a body is a statement planned in its own scope, so the boundary
        # can never take a foreign handle from a parent.
        primary = set(a.primary_members)
        select_addrs = (
            *(addr for addr in select_addrs if addr in primary),
            *(addr for addr in select_addrs if addr not in primary),
        )
    outputs = [
        c for addr in select_addrs if (c := _concept_at(environment, addr)) is not None
    ]
    if not outputs:
        ctx.unbuilt[gid] = outputs
        return None
    primary_addrs = set(a.primary_members)
    if derivation not in AGGREGATING_DERIVATIONS:
        outputs += _merge_origin_intermediates(outputs, primary_addrs, environment)
    valued = {
        c.address
        for c in outputs
        if c.address in primary_addrs and c.lineage is not None
    }
    atoms = _atoms_at(attrs, gid)
    # Conjunction-coverage siblings only bind when this group recomputes
    # its aggregate over rows; a fully twin-reused value is read through,
    # so its input population is not this group's to filter, and keeping
    # the atom would resurrect the redundant fact-rescan parent through
    # its `needed` args.
    if a.conjunction_atoms and valued and valued <= a.twin_reused:
        atoms = [atom for atom in atoms if atom not in a.conjunction_atoms]
    injected = _wrap_atoms(atoms)
    preexisting = _wrap_atoms(accumulated_atoms_above(group_graph, attrs, gid))
    # The "needed" set drives ancestor-dedup: a parent is kept only if
    # it contributes something to it that no descendant parent also
    # provides. Includes the output addresses themselves, the lineage
    # args of *primary* outputs (the columns this group actually
    # computes), and the inputs of any conditions applied at this
    # group. Passthroughs' lineage is intentionally NOT walked: a
    # passthrough output like `sum_sales` rides through this group
    # from an aggregate parent; if we walked its lineage we'd add
    # `sales_price` to `needed`, which ROOT provides but the aggregate
    # doesn't, and ROOT would escape dedup. The aggregate already
    # owns that lineage upstream.
    needed: set[str] = set()
    for c in outputs:
        needed.add(c.address)
        if c.address in primary_addrs and c.lineage is not None:
            if derivation in AGGREGATING_DERIVATIONS:
                # A post-condition aggregate that REUSES a same-grain twin
                # (its pre-condition d1 sibling already materialized this
                # value) reads the value through rather than recomputing,
                # so its raw recompute inputs (the input-grain keys, measure
                # columns) don't belong in `needed`. Pulling them keeps a
                # redundant fact-rescan ROOT parent alive that only
                # re-supplies grouping keys the twin already carries. Treat
                # it like a passthrough: skip its lineage.
                if c.address not in a.twin_reused:
                    _add_aggregate_needed_concepts(needed, c)
            else:
                # Recurse through args that are THEMSELVES primary members
                # of this group (intermediates computed here, e.g. a window
                # output exposed through a wrapping BASIC): their inputs
                # must come from parents, so they belong in `needed`; else
                # a parent supplying only those deep inputs reads as
                # redundant and dedup can drop every candidate (the
                # lead-over-derived-partition-key shape). Non-primary args
                # stay unwalked (the passthrough rule above).
                stack = list(c.lineage.concept_arguments)
                walked: set[str] = set()
                while stack:
                    arg = stack.pop()
                    if arg.address in walked:
                        continue
                    walked.add(arg.address)
                    needed.add(arg.address)
                    if arg.address in primary_addrs and arg.lineage is not None:
                        stack.extend(arg.lineage.concept_arguments)
    if injected is not None:
        for arg in condition_row_args(injected):
            _add_needed_concept(needed, arg)
    # Honor the group-planning contract's declared join keys: a bridge key
    # (e.g. `order` linking a window-derived dimension to the fact scan) is
    # not an aggregate input, so it isn't in `needed` and `_parent_nodes_for`
    # would slice it off the root scan, leaving the merge with no shared
    # key (ON 1=1). Pull in only the EXTRA bridge keys (those not already in
    # the group's grain/outputs) so a grouping key (e.g. a `by rollup`
    # dimension) is never re-added to `needed` and forced into the SELECT
    # outside its GROUP BY.
    # a row derivation sliced off paired facts joins back on its own grain
    keeps_grain = derivation not in GROUPING_DERIVATIONS and (
        _one_entity_beside_paired_facts(a.primary_members, environment)
    )
    needed |= (
        set(_input_contract_join_keys(a))
        - (set() if keeps_grain else set(a.grain_components))
        - set(a.output_concepts)
    )
    # a dimension peel is read with its key, the axis its rows are paired on
    for pgid in lineage_predecessors(group_graph, group_edges, gid):
        needed |= attrs[pgid].anchor_keys
    parent_builds = _parent_nodes_for(
        group_graph,
        group_edges,
        attrs,
        built,
        gid,
        environment,
        g,
        history,
        needed=needed,
        root_requests=root_requests,
        mandatory_list=mandatory_list,
        complete_partials=complete_partials,
        staged_conditions=staged_conditions,
        feeder_cache=feeder_cache,
        arms_delivered=arms_delivered,
        aggregate_inputs=ctx.folds.aggregate_inputs.get(gid, ()),
    )
    parent_group_ids = {parent.group_id for parent in parent_builds}
    join_key_addresses = _input_contract_join_keys(a, parent_group_ids)
    candidates = _bridge_candidates(group_graph, ctx.order, ctx.unbuilt, gid)
    if derivation == Derivation.UNION:
        parents = _union_arm_parents(
            parent_builds, attrs, environment, needed, group_graph, built, candidates
        )
    else:
        # A WHERE-only root scan a constraint edge feeds into this consumer
        # is a filter on the input rows: it may only remove them, so the merge is INNER.
        filter_scan = any(
            attrs[parent.group_id].derivation == Derivation.ROOT
            and edge_kind(group_edges, parent.group_id, gid) == EdgeKind.CONSTRAINT
            for parent in parent_builds
        )
        parents = _apply_input_contracts(parent_builds, a, needed, environment)
        origin = {id(node): pgid for pgid, node in built.items()} | {
            id(build.node): build.group_id for build in parent_builds
        }
        # A ROOT scan hosting a null-rejecting request atom emits exactly
        # the atom's population, so the merge may claim it. A grouping
        # parent applies the atom to its INPUT rows and its output claims
        # nothing (the FINAL still gates); an `is null` atom is satisfied
        # by the padding a preserving join adds, unless what it reads is
        # defined on every row (a pinned value), where it is as exact.
        keyspace = environment.span_scope.keyspace
        applied_atoms = _wrap_atoms(
            [
                atom
                for parent in parent_builds
                if attrs[parent.group_id].derivation == Derivation.ROOT
                for atom in attrs[parent.group_id].condition_atoms
                if non_null_proofs(atom)
                or _reads_only_values_defined_everywhere(atom, keyspace)
            ]
        )
        applied = applied_atoms.conditional if applied_atoms else None
        # a parent holds a region's rows when it reads its domain, the
        # domain itself or a derivation over what it carries (`upper(name)`)
        reads = [region_reads(p) for p in parents]
        domains = [p for p, read in zip(parents, reads) if read]
        if derivation == Derivation.AGGREGATE and domains:
            # an aggregate evaluated OVER a region: its row-stream
            # arguments are computed on the solid rows first, then the
            # region's rows pad them. The solid merge pairs on solid
            # keys: this group may extend the spans (it reads the
            # domain), the merge below the domain may not.
            domain_spans: frozenset[str] = frozenset().union(*reads)
            # a condition feeder keyed by the span (`count(return_id) by
            # item_sk = 0`) is a value per member of the region: it joins
            # the united rows, not the solid stream, or a member the solid
            # stream lacks (an item with returns and no sale) is padded
            # past it and reads its count as 0. So does one value for the
            # whole statement (`avg(bal) by *`), NULL on a padded row when
            # it rides the solid stream
            feeders = [
                p
                for p, build in zip(parents, parent_builds)
                if p not in domains
                and edge_kind(group_edges, build.group_id, gid) == EdgeKind.CONSTRAINT
                and attrs[build.group_id].grain_components
                and attrs[build.group_id].grain_components
                <= domain_spans | {ALL_ROWS_ADDRESS}
            ]
            group_scope = environment.span_scope
            with under_span_scope(
                environment,
                dc_replace(
                    group_scope, extent_free=group_scope.extent_free | domain_spans
                ),
            ):
                solid = _pre_merge_parents(
                    [p for p in parents if p not in domains and p not in feeders],
                    environment,
                    join_key_addresses=join_key_addresses,
                    needed=needed,
                    group_graph=group_graph,
                    built=built,
                    preexisting_conditions=applied,
                    attrs=attrs,
                    origin=origin,
                    candidates=candidates,
                )
            outputs, named_arguments = _name_inline_arguments(
                outputs, primary_addrs, domain_spans, environment
            )
            parents = (
                _project_basic_aggregate_inputs(
                    outputs,
                    primary_addrs,
                    solid,
                    ctx.folds.render(
                        build.group_id
                        for p, build in zip(parents, parent_builds)
                        if p not in domains and p not in feeders
                    ),
                    environment,
                    region_spans=domain_spans,
                    named=named_arguments,
                )
                + domains
                + feeders
            )
        parents = _pre_merge_parents(
            parents,
            environment,
            join_key_addresses=join_key_addresses,
            needed=needed,
            group_graph=group_graph,
            built=built,
            force_join_type=JoinType.INNER if filter_scan else None,
            preexisting_conditions=applied,
            attrs=attrs,
            origin=origin,
            candidates=candidates,
        )
    # ROOT scans source columns from datasources directly, not from their
    # group-graph predecessors. A `constraint`-edge predecessor (e.g. a
    # d1 aggregate feeding a HAVING-style filter on this root) is real
    # row-flow at SQL time (INNER JOIN to apply the filter) but doesn't
    # supply the root's primary scan columns. Pruning by parent outputs
    # there would strip every requested column and the root would never
    # build.
    if derivation not in (Derivation.ROOT, Derivation.UNNEST):
        wanted = outputs
        outputs = satisfiable_outputs(outputs, parents)
        if not outputs:
            ctx.unbuilt[gid] = wanted
            return None
    # For aggregating derivations, peel `injected` off into a pre-filter
    # wrapper so the GroupNode itself sees no `conditions`. GroupNode's
    # non-scalar-condition path reacts to a condition that references an
    # aggregate concept (`cp > 1.2 * avg`) by appending the condition's
    # row args to the group's outputs, which then leak into the GROUP BY
    # and shrink every row to a unique (state, cp, avg) bucket. Wrapping
    # in a SelectNode does the WHERE first; the GroupNode then aggregates
    # the filtered rows with a clean GROUP BY at the intended grain.
    condition_for_generator = injected
    # WINDOW gets the same peel: WindowNode has no `conditions` slot (its
    # generator folds them into `preexisting_conditions`, silently dropping
    # the filter), and WHERE-before-window is exactly the required
    # semantics: the window computes over the filtered rows.
    if (
        injected is not None
        and derivation in (*AGGREGATING_DERIVATIONS, Derivation.WINDOW)
        and parents
    ):
        parent_output_by_addr = {
            output.address: output
            for parent in parents
            for output in parent.output_concepts
        }
        parent_outputs = list(parent_output_by_addr.values())
        wrapper = SelectNode(
            input_concepts=parent_outputs,
            output_concepts=parent_outputs,
            environment=environment,
            parents=parents,
            conditions=injected.conditional,
        )
        parents = [wrapper]
        condition_for_generator = None
    if derivation == Derivation.AGGREGATE and parents:
        parents = _project_basic_aggregate_inputs(
            outputs,
            primary_addrs,
            parents,
            ctx.folds.render(ctx.folds.aggregate_inputs.get(gid, ())),
            environment,
            region_spans=frozenset().union(*(region_reads(p) for p in parents)),
        )
    # Normalize aggregate inputs to the row grain implied by their
    # arguments before the aggregate runs. This is generic across aggregate
    # functions: the normalization preserves both the input-grain keys and
    # the argument columns the aggregate will read.
    if (
        derivation == Derivation.AGGREGATE
        and a.aggregate_input_grain
        and a.aggregate_input_grain != a.grain_components
        and parents
        and not _aggregate_inputs_are_row_preserving(outputs, primary_addrs, parents)
        and not _parents_already_at_input_grain(
            outputs, parents, a.aggregate_input_grain, environment
        )
    ):
        normalize_addrs = set(a.aggregate_input_grain)
        aggregate_arg_addrs: set[str] = set()
        for c in outputs:
            normalize_addrs.add(c.address)
            if c.address not in primary_addrs or c.lineage is None:
                continue
            for arg in _parent_supplied_args(c, primary_addrs):
                normalize_addrs.add(arg.address)
                aggregate_arg_addrs.add(arg.address)
        normalize_parent_output_by_addr: dict[str, BuildConcept] = {}
        for parent in parents:
            for output in parent.output_concepts:
                normalize_parent_output_by_addr.setdefault(output.address, output)
        # A parent may carry a needed address only under a pseudonym twin
        # (`count(actor)` over a boundary that emits `stages.event_id`, the
        # global-merge alias); dropping it here would dedup the input
        # stream at the wrong grain before the aggregate reads it.
        normalize_concepts: list[BuildConcept] = []
        matched_addrs: set[str] = set()
        for addr in sorted(normalize_addrs):
            concept_match = normalize_parent_output_by_addr.get(addr)
            if concept_match is None:
                concept_match = next(
                    (
                        output
                        for output in normalize_parent_output_by_addr.values()
                        if addr in output.pseudonyms
                    ),
                    None,
                )
            if concept_match is None and addr in aggregate_arg_addrs:
                # An aggregate argument the parent never materialized (a
                # filter virtual the plan meant to re-derive at the
                # aggregate itself) must be computed BELOW the dedup:
                # re-deriving it above the normalization GROUP both
                # evaluates the filter on already-deduped rows and strands
                # the virtual's row inputs outside the projection. Widen
                # the parent that can render it.
                candidate = _concept_at(environment, addr)
                if candidate is not None:
                    for parent in parents:
                        available = renderable_addresses(parent)
                        if not concept_satisfiable(candidate, available):
                            continue
                        widen_projection(
                            parent,
                            [candidate],
                            input_candidates=_row_lineage_closure(candidate),
                            available_addresses=available,
                        )
                        concept_match = candidate
                        break
            if concept_match is not None and concept_match.address not in matched_addrs:
                matched_addrs.add(concept_match.address)
                normalize_concepts.append(concept_match)
        if normalize_concepts:
            parents = [
                GroupNode(
                    output_concepts=normalize_concepts,
                    input_concepts=normalize_concepts,
                    environment=environment,
                    parents=parents,
                )
            ]
    if derivation == Derivation.WINDOW and parents:
        parents = _window_rows_at_grain(outputs, parents, environment)
    if derivation == Derivation.AGGREGATE and a.aggregate_first_row_grains:
        outputs, parents = _read_first_rows(
            outputs, a.aggregate_first_row_grains, parents, environment
        )
    # The same root in two phases (the d1 twin feeding the condition-phase
    # aggregates) asks the same question when the WHERE placed on each is
    # the same; the second reads the first's answer.
    unfolded = ctx.readers
    arm_local = _arm_local(unfolded, gid, arms_delivered)
    request = (
        RootRequest(
            frozenset(c.address for c in outputs),
            condition_for_generator,
            environment.span_scope,
            preexisting,
            arm_local,
        )
        if derivation == Derivation.ROOT
        else None
    )
    # the region contract is the group's, not the request's: a twin's
    # copy would carry the twin's region marker
    twin = next(
        (
            other
            for other, asked in root_requests.items()
            if asked == request and attrs[other].extent_spans == a.extent_spans
        ),
        None,
    )
    node: StrategyNode | None
    if twin is not None:
        node = built[twin].copy()
        node.origin_group = gid
        logger.info(f"[v4] built {gid} reads {twin}: the same ROOT request")
    else:
        if derivation == Derivation.ROOT:
            _raise_on_discarded_parent_atoms(gid, parent_group_ids, attrs)
        node = build_node(
            derivation=derivation,
            outputs=outputs,
            parents=parents,
            environment=environment,
            conditions=condition_for_generator,
            preexisting_conditions=preexisting,
            intrinsic_filter_pushdown=_filter_intrinsic_pushdown_safe(
                unfolded.group_graph,
                unfolded.attrs,
                gid,
                outputs,
                mandatory_list,
                environment,
            ),
            existence_source=any(
                edge_kind(unfolded.group_edges, gid, succ) == EdgeKind.EXISTENCE
                for succ in unfolded.group_graph.successors(gid)
            ),
            collapse_to_grain=all(
                unfolded.attrs[succ].derivation != Derivation.AGGREGATE
                for succ in unfolded.group_graph.successors(gid)
            ),
            complete_partials=complete_partials,
            history=history,
            g=g,
            staged_conditions=staged_conditions,
            depth=depth,
            arm_local=arm_local,
        )
        if node is not None and derivation == Derivation.AGGREGATE:
            node.distinct_counts = frozenset(a.aggregate_distinct_addrs)
    # a generator may hand back a parent's node; that one keeps its group
    if node is not None and node.origin_group is None:
        node.origin_group = gid
    if node is not None and request is not None:
        root_requests[gid] = request
    if logger.isEnabledFor(logging.INFO):
        logger.info(
            f"[v4] built {gid} derivation={derivation} "
            f"outputs={[o.address for o in outputs]} "
            f"parents={[type(p).__name__ for p in parents]} "
            f"-> {type(node).__name__ if node else None}"
        )
    if plan_trace.active():
        plan_trace.record(
            f"built {gid}",
            plan_trace.NodeBuiltStep(
                group=gid,
                derivation=derivation.value,
                attrs=plan_trace.jsonable(a),
                outputs=plan_trace.addresses(outputs),
                needed=sorted(needed),
                atoms=[plan_trace.expression(atom) for atom in atoms],
                preexisting=plan_trace.expression(preexisting),
                parent_groups=sorted(parent_group_ids),
                join_keys=sorted(join_key_addresses),
                span_scope=plan_trace.span_scope(environment.span_scope),
                node=plan_trace.strategy_node(node),
            ),
        )
    if node is None:
        ctx.unbuilt[gid] = outputs
        return None
    if a.extent_spans:
        if derivation == Derivation.ROWSET:
            # a boundary's rows are the body's (a sold item once per sale
            # line, its grain claim notwithstanding); the domain's are the
            # region's own members, once each
            members = [o for o in node.output_concepts if o.address in select_addrs]
            node = GroupNode(
                output_concepts=members,
                input_concepts=members,
                environment=environment,
                parents=[node],
                partial_concepts=list(node.partial_concepts),
                force_group=True,
            )
        if a.null_member_spans:
            node = _with_null_members(
                node, a.null_member_spans, environment, g.scope.datasources
            )
        # the region contract: this node's rows are the region's own
        node.region_spans = a.extent_spans
    # Elide here, not only in the tree pass: consumers take their own copy
    # of this node, so a passthrough left standing becomes the SHARED
    # parent two single-column consumers each regroup over, and the merge
    # above them has no key to pair on (union-TVF arm outputs split into a
    # cross join).
    node = _elide_single_parent_passthrough(node)
    # The subtree walk lands the wiring on whichever node emits the WHERE
    # referencing the IN-RHS concept (the peeled SelectNode wrapper for an
    # aggregating derivation, not the GroupNode above it).
    _wire_existence(
        node,
        built,
        feeder_cache,
        preferred=[
            pgid
            for pgid in group_graph.predecessors(gid)
            if edge_kind(group_edges, pgid, gid) == EdgeKind.EXISTENCE
        ],
    )
    return node


def _inlined_groups(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    order: list[str],
    environment: BuildEnvironment,
    mandatory_list: list[BuildConcept],
    g: ReferenceGraph,
    history: V4History,
    readers: "_Plan",
    arms_delivered: bool,
) -> "_FoldPlan":
    """The inline fold, decided before anything is built. Each group is
    judged in build order off what the groups before it render, over a copy of
    the plan the folds before it are applied to; each aggregate's inputs over
    the plan every fold is applied to."""
    graph, edges = group_graph.copy(), dict(group_edges)
    planned = {gid: dc_replace(a) for gid, a in attrs.items()}
    rendered = renders(graph, edges, planned, environment)
    scans: dict[str, bool] = {}
    inlined: set[str] = set()
    for idx, gid in enumerate(order):
        if gid == FINAL_NODE_ID:
            continue
        reads = _PlannedReads(
            graph,
            edges,
            planned,
            rendered,
            {other for other in order[:idx] if other in planned},
            environment,
            g,
            history,
            scans,
            readers,
            arms_delivered,
        )
        if _inlined_by_every_reader(
            graph, edges, planned, reads, gid, environment, mandatory_list
        ):
            inlined.add(gid)
            _read_parents_in_place(graph, edges, planned, gid)
            rendered = renders(graph, edges, planned, environment)
    aggregate_inputs: dict[str, list[str]] = {}
    for idx, gid in enumerate(order):
        if gid not in planned or planned[gid].derivation != Derivation.AGGREGATE:
            continue
        reads = _PlannedReads(
            graph,
            edges,
            planned,
            rendered,
            {other for other in order[:idx] if other in planned},
            environment,
            g,
            history,
            scans,
            readers,
            arms_delivered,
        )
        aggregate_inputs[gid] = _aggregate_input_groups(
            graph, edges, planned, reads, gid, environment, mandatory_list
        )
        _raise_if_inlined_input_is_unreadable(reads, gid, aggregate_inputs[gid])
    return _FoldPlan(inlined, aggregate_inputs, rendered)


def build_strategy_node(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    g: ReferenceGraph,
    history: V4History,
    complete_partials: bool = True,
    staged_conditions: list[BuildWhereClause] | None = None,
    depth: int = 0,
) -> StrategyNode | None:
    """Walk groups in topological order, dispatching each to its v4 generator
    with explicit parent nodes. Returns the most-downstream built node, or
    None if nothing built. `depth` is the nesting of this plan inside rowset
    bodies, for trace indentation."""
    # whether a request may read a coalescing axis off one arm: decided once,
    # from the statement's shape
    arms_delivered = axis_arms_delivered(
        mandatory_list, environment, g.scope.datasources
    )

    order = _topological_order(group_graph, group_edges)
    record_twin_reuse(order, attrs)
    readers = _Plan(
        group_graph.copy(),
        dict(group_edges),
        {gid: dc_replace(a) for gid, a in attrs.items()},
    )
    folds = _inlined_groups(
        group_graph,
        group_edges,
        attrs,
        order,
        environment,
        mandatory_list,
        g,
        history,
        readers,
        arms_delivered,
    )
    built: dict[str, StrategyNode] = {}
    unbuilt: dict[str, list[BuildConcept]] = {}
    ctx = _GroupBuild(
        group_graph,
        group_edges,
        attrs,
        mandatory_list,
        environment,
        g,
        history,
        complete_partials,
        staged_conditions,
        depth,
        arms_delivered,
        built,
        {},
        _CleanFeederCache(environment, g, history),
        unbuilt,
        order,
        readers,
        folds,
    )
    ownership = attrs[FINAL_NODE_ID].extent_ownership or ExtentOwnership()
    plan_scope = environment.span_scope

    for gid in order:
        if gid in folds.inlined:
            _fold_into_readers(group_graph, group_edges, attrs, gid)
    # a group none of whose selected columns resolves builds nothing
    contributors = [
        gid
        for gid in order
        if gid != FINAL_NODE_ID
        and gid not in folds.inlined
        and any(
            _concept_at(environment, a) is not None
            for a in _select_addresses(attrs[gid])
        )
    ]
    with under_span_scope(environment, _final_span_scope(plan_scope)):
        cover = _elect_final_cover(
            group_graph, group_edges, attrs, contributors, mandatory_list, environment
        )
    for gid in order:
        if gid == FINAL_NODE_ID or gid in folds.inlined:
            continue
        plan_trace.set_context(gid)
        a = attrs[gid]
        # Scope the group's extent routing over its whole build, including the
        # consumer-side re-sources `_parent_nodes_for` plans below. A region
        # domain is the FINAL's own rows and is built under the FINAL's scope,
        # as its every reader sees it there (`_assemble_final_node`).
        with under_span_scope(
            environment,
            (
                _final_span_scope(plan_scope)
                if a.extent_spans
                else _group_span_scope(plan_scope, ownership, gid)
            ),
        ):
            node = _build_group(gid, ctx)
        if node is not None:
            built[gid] = node

    _raise_if_unbuilt_group_owed(group_graph, attrs, built, unbuilt)
    # The FINAL assembly is where the owner and the extent-free branches meet;
    # it must see every span again to host the owner's rows, except the ones
    # the plan above holds.
    with under_span_scope(environment, _final_span_scope(plan_scope)):
        if not built:
            return None
        plan_trace.set_context("FINAL")
        final = _assemble_final_node(
            group_graph,
            group_edges,
            attrs,
            built,
            mandatory_list,
            environment,
            g,
            history,
            ctx.root_requests,
            cover,
            candidates=_bridge_candidates(group_graph, order, unbuilt),
            feeder_cache=ctx.feeder_cache,
            arms_delivered=arms_delivered,
        )
        if plan_trace.active():
            plan_trace.record(
                "FINAL assembled",
                plan_trace.FinalStep(
                    contract=plan_trace.jsonable(attrs[FINAL_NODE_ID].final_contract),
                    extent_ownership=plan_trace.jsonable(ownership),
                    built={gid: repr(node) for gid, node in built.items()},
                    node=plan_trace.strategy_node(final),
                ),
            )
        plan_trace.set_context(None)
        if final is not None:
            _raise_if_output_unrendered(final, mandatory_list)
            final = _elide_passthrough_tree(final)
            if _has_unsourced_leaf(final):
                # A parent-less, datasource-less node that outputs a ROOT concept (a
                # base column that must come from a datasource) has no source for
                # it. This is an unresolvable query (e.g. a projection / aggregate
                # over concepts from two unconnected namespaces); fail so it raises
                # UnresolvableQueryException rather than invalid SQL.
                # Unnest-of-literal / constant leaves output only derived concepts
                # and are left alone.
                return None
            # FINAL's own re-sources host their memberships unwired until here.
            _wire_existence(final, built, ctx.feeder_cache)
            _drop_stale_resolutions(final, set())
        return final


def _drop_stale_resolutions(node: StrategyNode, seen: set[int]) -> None:
    """Clear, bottom-up, every resolution that no longer reads its parents'
    current ones: a rebuild (late membership wiring) does not reach the nodes
    that read the rebuilt node. A parent a merge folded away clears too."""
    if id(node) in seen:
        return
    seen.add(id(node))
    for parent in node.parents:
        _drop_stale_resolutions(parent, seen)
    cached = node.resolution_cache
    if cached is None:
        return
    read = {id(cached), *(id(ds) for ds in cached.datasources)}
    if any(
        parent.resolution_cache is None or id(parent.resolution_cache) not in read
        for parent in node.parents
    ):
        node.resolution_cache = None


def _raise_if_output_unrendered(
    final: StrategyNode, mandatory_list: list[BuildConcept]
) -> None:
    """A requested column the plan does not render is a wrong answer, not a
    narrower one: a group pruned of an output it could not source would
    otherwise return the statement without it."""
    missing = [
        concept.address
        for concept in mandatory_list
        if not any(output_covers(o, concept) for o in final.output_concepts)
    ]
    if missing:
        raise UnresolvableQueryException(
            f"The plan renders no column for {missing}; the group producing it"
            " could not source its inputs. This is a planner bug."
        )


def _raise_if_unbuilt_group_owed(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    unbuilt: dict[str, list[BuildConcept]],
) -> None:
    """A group that built nothing may drop out only when it owes nothing: no
    WHERE atom, no reader but the FINAL, and every output another built group
    emits (a rowset boundary over a plain key a ROOT scan already carries).
    Skipping any other would silently lose its filter or its rows."""
    delivered = {
        address
        for node in built.values()
        for output in node.output_concepts
        for address in (output.address, *output.pseudonyms)
    }
    for gid, outputs in unbuilt.items():
        atoms = _atoms_at(attrs, gid)
        readers = sorted(_readers(group_graph, gid))
        missing = sorted({o.address for o in outputs} - delivered)
        if atoms or readers or missing:
            raise UnbuiltGroupException(
                f"Group {gid} ({attrs[gid].derivation}) could not be built, and "
                f"nothing else delivers what it owes: WHERE atoms "
                f"{[str(atom) for atom in atoms]}, readers {readers}, "
                f"outputs {missing}."
            )


def _has_unsourced_leaf(final: StrategyNode) -> bool:
    for node in _strategy_nodes(final):
        if node.parents or getattr(node, "datasource", None) is not None:
            continue
        # A leaf with neither parents nor a datasource can only render what
        # literals alone produce. A ROOT output is the obvious violation, but so
        # is an aggregate over one (`sum(amt)` beside a WHERE that pruned the
        # scan away): its measure has no source either, even though the
        # aggregate is not itself ROOT. Unnest-of-literal / constant leaves
        # stay legal.
        if any(not literal_producible(concept) for concept in node.output_concepts):
            return True
    return False
