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
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import replace as dc_replace
from datetime import date, datetime
from typing import cast

from trilogy.constants import MagicConstants, logger
from trilogy.core import graph as nx
from trilogy.core.enums import (
    ComparisonOperator,
    Derivation,
    FunctionType,
    JoinType,
    Purpose,
)
from trilogy.core.exceptions import UnbuiltGroupException, UnresolvableQueryException
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import (
    BoolExpr,
    BuildAggregateWrapper,
    BuildComparison,
    BuildConcept,
    BuildConceptArgs,
    BuildDatasource,
    BuildFilterItem,
    BuildFunction,
    BuildGrain,
    BuildRowsetItem,
    BuildWhereClause,
    LooseBuildConceptList,
    generate_concept_name,
    nonstandard_grouping_lineage,
)
from trilogy.core.models.build_environment import BuildEnvironment, SpanScope
from trilogy.core.models.execute import BaseJoin
from trilogy.core.models.keyspace import Keyspace, Region
from trilogy.core.processing import plan_trace
from trilogy.core.processing.aggregate_rollup import _is_additive_aggregate
from trilogy.core.processing.condition_utility import (
    combine_condition_atoms,
    condition_implies,
    decompose_condition,
)
from trilogy.core.processing.discovery_utility import raise_if_disconnected_for
from trilogy.core.processing.grain_utility import non_null_proofs
from trilogy.core.processing.node_generators.presence_probe import is_presence_probe
from trilogy.core.processing.nodes import (
    FilterNode,
    GroupNode,
    History,
    MergeNode,
    RowsetNode,
    SelectNode,
    StrategyNode,
    UnionNode,
    WindowNode,
)
from trilogy.core.processing.nodes.base_node import NodeJoin, region_reads
from trilogy.utility import unique

from .concept_graph import _relation_mates, _statement_scoped_relation_members
from .condition_injection import (
    ConditionSources,
    condition_row_args,
    inject_condition_at_node,
)
from .constants import (
    ALL_ROWS_ADDRESS,
    FINAL_NODE_ID,
    GROUPING_DERIVATIONS,
    ROW_PRESERVING_AGGREGATE_INPUT_DERIVATIONS,
    ROW_SHAPE_BARRIER_DERIVATIONS,
    DepthLabel,
    EdgeKind,
)
from .edges import EdgeAttrs, EdgeMap, dependency_subgraph, edge_kind, remove_edge
from .extent_ownership import takes_a_value_on_padding
from .functional_dependency import build_fd_determines
from .group_graph import trace_group_graph
from .history import V4History
from .models import (
    ExtentOwnership,
    FinalAssemblyContract,
    FinalContributorContract,
    GroupAttrs,
    InputChannel,
    nulls_grouping_keys,
)
from .projection import (
    concept_satisfiable,
    filter_row_predicate,
    literal_producible,
    output_rowset_grain_keys,
    parent_output_addresses,
    renderable_addresses,
    row_lineage_arguments,
    satisfiable_outputs,
    statement_filter_population,
    widen_projection,
)
from .region_reads import inline_arguments_taking_a_value, nameable
from .source_planning import SourceRequest, plan_source

_AGGREGATING_DERIVATIONS = {
    Derivation.AGGREGATE,
    Derivation.GROUP_TO,
}


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


def _members_of(attrs: dict[str, GroupAttrs], gid: str) -> set[str]:
    """What the group computes and the keys it holds. Not what it emits: a
    ROOT's re-source at FINAL asks which merge keys are its own, and a hidden
    pass-through is not one (`test_duckdb_subset_join_pivot_axis`)."""
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
        history: History,
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

        v4_history = cast(V4History, self._history)
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
    preferred: Sequence[str] = (),
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


def _accumulated_atoms_above(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    gid: str,
) -> list[BoolExpr]:
    """Atoms applied at any STRICT ancestor of `gid` that still hold on this
    group's rows. Threaded into the node as `preexisting_conditions` so
    nullable inference (and any later optimizer) knows which rows the parent
    already filtered, without re-emitting the same WHERE on this CTE.

    A standard grouping ancestor applies its atom to the rows it aggregates;
    the groups it emits are filtered only when the atom is decided per group
    (every row arg among the group's own columns). Otherwise a descendant that
    joins those groups back onto a row stream re-admits rows the atom
    rejects, so the atom is not preexisting for it."""
    accumulated: list[BoolExpr] = []
    for anc in nx.ancestors(group_graph, gid):
        if anc == FINAL_NODE_ID:
            continue
        anc_attrs = attrs[anc]
        collapsing = anc_attrs.derivation in GROUPING_DERIVATIONS and not (
            nulls_grouping_keys(anc_attrs.grouping_mode)
        )
        columns = _members_of(attrs, anc) | set(anc_attrs.grain_components)
        for atom in anc_attrs.condition_atoms:
            if collapsing and not ({c.address for c in atom.row_arguments} <= columns):
                continue
            if atom not in accumulated:
                accumulated.append(atom)
    return accumulated


def _feeder_conditions_implied(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    feeder_gid: str,
    sibling_gid: str,
) -> bool:
    """Whether every row-reducing condition in the feeder's subtree (the atoms at
    it AND at its ancestors) is also applied in the grouping sibling's subtree.

    A redundant fact-rescan feeder can only be dropped in favor of a co-grain
    grouping sibling if the sibling's rows are filtered at least as much;
    otherwise dropping the feeder silently widens the row set. A feeder whose
    parent scan carries a post-aggregate condition the sibling does not apply
    must stay."""
    feeder = _atoms_at(attrs, feeder_gid) + _accumulated_atoms_above(
        group_graph, attrs, feeder_gid
    )
    if not feeder:
        return True
    sibling = _atoms_at(attrs, sibling_gid) + _accumulated_atoms_above(
        group_graph, attrs, sibling_gid
    )
    return all(atom in sibling for atom in feeder)


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


def _row_parents(group_graph: nx.DiGraph, group_edges: EdgeMap, gid: str) -> list[str]:
    """`gid`'s parents that feed it rows, not a subselect."""
    return [
        pgid
        for pgid in group_graph.predecessors(gid)
        if pgid != FINAL_NODE_ID
        and edge_kind(group_edges, pgid, gid) != EdgeKind.EXISTENCE
    ]


def _select_addresses(a: GroupAttrs) -> tuple[str, ...]:
    """A group the demand pass left without outputs projects every member."""
    return a.output_concepts or a.members


def _projects_parent_rows(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    gid: str,
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    mandatory_list: list[BuildConcept],
) -> bool:
    """Whether `gid`'s node would project its parents' rows as they stand: no
    WHERE, no regroup, no subselect. Read off the graph and the parents'
    nodes, the way its generator would, so the answer needs no node of
    `gid`'s own."""
    from trilogy.core.processing.v4_node_generators.basic import (  # cycle
        _grain_claim_needs_group,
    )
    from trilogy.core.processing.v4_node_generators.common import (
        outputs_with_parent_grain_keys,
    )

    a = attrs[gid]
    if a.condition_atoms or any(
        edge_kind(group_edges, pgid, gid) == EdgeKind.EXISTENCE
        for pgid in group_graph.predecessors(gid)
    ):
        return False
    outputs = satisfiable_outputs(
        [
            c
            for addr in _select_addresses(a)
            if (c := _concept_at(environment, addr)) is not None
        ],
        parents,
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
        return filter_row_predicate(outputs, parents, narrows) is None
    return len(parents) != 1 or not _grain_claim_needs_group(
        outputs_with_parent_grain_keys(outputs, parents), parents[0], environment
    )


def _aggregate_inlines(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
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
    if _built_twin_emits(attrs, built, members, skip=gid):
        return False
    parents = [
        built[pgid]
        for pgid in _row_parents(group_graph, group_edges, gid)
        if pgid in built
    ]
    if not parents or any(_contains_shape_barrier(parent) for parent in parents):
        return False
    return _projects_parent_rows(
        group_graph, group_edges, attrs, gid, parents, environment, mandatory_list
    ) and _group_renderable_from(
        attrs,
        environment,
        gid,
        {output.address for parent in parents for output in parent.output_concepts},
    )


def _inline_aggregate_inputs(
    candidates: list[tuple[str, StrategyNode]],
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    gid: str,
    environment: BuildEnvironment,
    mandatory_list: list[BuildConcept],
) -> list[tuple[str, StrategyNode]]:
    """Read, in place of each input aggregate `gid` computes inline, that
    input's own parents; to a fixed point, since those may be inlined too."""
    _raise_if_inlined_input_is_unreadable(candidates, attrs, built, gid, environment)
    while True:
        expanded: list[tuple[str, StrategyNode]] = []
        for pgid, node in candidates:
            if _aggregate_inlines(
                group_graph,
                group_edges,
                attrs,
                built,
                gid,
                pgid,
                environment,
                mandatory_list,
            ):
                expanded.extend(
                    (fgid, built[fgid])
                    for fgid in _row_parents(group_graph, group_edges, pgid)
                    if fgid in built
                )
            else:
                expanded.append((pgid, node))
        deduped = list(dict(expanded).items())
        if deduped == candidates:
            return candidates
        candidates = deduped


def _raise_if_inlined_input_is_unreadable(
    candidates: list[tuple[str, StrategyNode]],
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    gid: str,
    environment: BuildEnvironment,
) -> None:
    """The groups folded into `gid` were never built, so nothing else can
    supply their members: `gid`'s parents must render each, and no
    condition-phase twin may have materialized one since."""
    if not attrs[gid].inlined_members:
        return
    available = {o.address for _, node in candidates for o in node.output_concepts}
    unreadable = [
        address
        for address in attrs[gid].inlined_members
        if _built_twin_emits(attrs, built, {address})
        or (concept := _concept_at(environment, address)) is None
        or not concept_satisfiable(concept, available)
    ]
    if unreadable:
        raise ValueError(
            f"[v4] {gid} was planned to compute {unreadable} inline, but its "
            "parents cannot render them. This is a planner bug."
        )


def _built_twin_emits(
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    addresses: set[str],
    skip: str | None = None,
) -> bool:
    """Whether a built condition-phase (D1) group other than `skip` emits any
    of `addresses`."""
    return any(
        other != skip
        and attrs[other].depth_label == DepthLabel.D1
        and addresses & {o.address for o in node.output_concepts}
        for other, node in built.items()
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
    built: dict[str, StrategyNode],
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
            built,
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
        pgid != gid and pgid not in built
        for pgid in _row_parents(group_graph, group_edges, reader)
    ):
        return False
    return _inlined_by_every_reader(
        *_folded(group_graph, group_edges, attrs, gid),
        built,
        reader,
        environment,
        mandatory_list,
    )


def _inlined_by_every_reader(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
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
        *(
            attrs[p].output_concepts
            for p in _row_parents(group_graph, group_edges, gid)
        ),
    )
    if not set(a.output_concepts) & final_reads <= carried:
        return False
    if not all(
        _reader_inlines(
            group_graph,
            group_edges,
            attrs,
            built,
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
            group_graph, group_edges, attrs, built, twin, environment, mandatory_list
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
    parents = _row_parents(group_graph, group_edges, gid)
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
    if final.final_contract is not None:
        final.final_contract = dc_replace(
            final.final_contract,
            contributor_contracts=tuple(
                c
                for c in final.final_contract.contributor_contracts
                if c.group_id != gid
            ),
        )
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
    if not keyspace.families or keyspace.regions[0].witnesses:
        return False
    return any(
        takes_a_value_on_padding(
            m,
            _row_absent(keyspace, keyspace.keys_by_address.get(m, frozenset())),
            keyspace,
            environment,
        )
        for m in members
    )


def _row_absent(keyspace: Keyspace, keys: frozenset[str]) -> Region:
    """A base-region row on which `keys` are absent."""
    return Region(present=keyspace.regions[0].present - keys)


def _parent_nodes_for(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    gid: str,
    environment: BuildEnvironment,
    graph: ReferenceGraph,
    history: History,
    *,
    needed: set[str],
    root_requests: dict[str, RootRequest],
    mandatory_list: list[BuildConcept],
    complete_partials: bool = True,
    staged_conditions: list[BuildWhereClause] | None = None,
    feeder_cache: "_CleanFeederCache | None" = None,
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
    for pgid in group_graph.predecessors(gid):
        if pgid == FINAL_NODE_ID:
            continue
        if attrs[pgid].depth_label == DepthLabel.D1 and (
            (
                attrs[gid].derivation == Derivation.UNNEST
                and attrs[pgid].derivation
                in (
                    Derivation.BASIC,
                    Derivation.FILTER,
                    Derivation.WINDOW,
                )
            )
            or (
                attrs[gid].derivation == Derivation.WINDOW
                and attrs[pgid].derivation == Derivation.WINDOW
            )
        ):
            continue
        # Existence-kind edges feed a subselect, not the row stream;
        # `_wire_existence` wires them as side-channel parents post-build.
        # Including them here would put them in JOIN dedup and mistakenly
        # merge their row stream into this group's FROM.
        if edge_kind(group_edges, pgid, gid) == EdgeKind.EXISTENCE:
            continue
        node = built.get(pgid)
        if node is not None:
            candidates.append((pgid, node))

    if attrs[gid].derivation == Derivation.AGGREGATE:
        candidates = _inline_aggregate_inputs(
            candidates,
            group_graph,
            group_edges,
            attrs,
            built,
            gid,
            environment,
            mandatory_list,
        )
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
    slices_roots = consumer.derivation in GROUPING_DERIVATIONS or (
        _valued_on_another_fact(consumer.primary_members, environment)
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
        request = RootRequest(
            frozenset(slice_addresses),
            conditions,
            asked.scope if asked else environment.span_scope,
            preexisting=preexisting,
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
                and _feeder_conditions_implied(group_graph, attrs, pgid, other_pgid)
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
    contributor is foldable: a SelectNode, a plain (non-grouping) MergeNode
    such as a multi-table root scan, or a virt-filter FilterNode (a CASE-WHEN
    projection with no row-reducing WHERE/semijoin).

    Widen B's OUTPUT with A's outputs and B's INPUT with A's inputs (the source
    columns A consumed). `resolve_concept_map` then sources a passthrough from
    the parent (it's an input) and derives the rest inline from those inputs."""

    def crosses_unsourced_aggregate(
        concept: BuildConcept, available: set[str], seen: set[str] | None = None
    ) -> bool:
        if concept.address in available:
            return False
        seen = seen or set()
        if concept.address in seen:
            return False
        seen.add(concept.address)
        if concept.derivation == Derivation.AGGREGATE:
            return True
        return any(
            crosses_unsourced_aggregate(arg, available, seen)
            for arg in row_lineage_arguments(concept)
        )

    dropped: set[int] = set()
    for b in parents:
        if id(b) in dropped or not isinstance(b, SelectNode) or b.force_group:
            continue
        available = parent_output_addresses(b)
        b_regions = region_reads(b)
        for a in parents:
            if a is b or id(a) in dropped or not a.output_concepts:
                continue
            if region_reads(a) != b_regions:
                continue
            # Never dissolve a row-shape barrier into a row sibling. Foldable:
            # SelectNode, non-grouping MergeNode, or a row-preserving FilterNode
            # (a CASE-WHEN virt-filter with no row-reducing WHERE or semijoin).
            if a.force_group or not (
                isinstance(a, (SelectNode, MergeNode)) or _is_row_preserving_filter(a)
            ):
                continue
            if any(
                o.derivation == Derivation.ROWSET for o in b.output_concepts
            ) and any(o.derivation != Derivation.ROWSET for o in a.output_concepts):
                continue
            if any(
                crosses_unsourced_aggregate(o, available) for o in a.output_concepts
            ):
                continue
            if not all(concept_satisfiable(o, available) for o in a.output_concepts):
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
            dc_replace(
                join,
                left_node=swap.get(id(join.left_node), join.left_node),
                right_node=swap.get(id(join.right_node), join.right_node),
            )
            for join in node.node_joins
        ]
    node.resolution_cache = None
    collapsed = _elide_single_parent_passthrough(node)
    seen[node_id] = collapsed
    return collapsed


def _iter_row_lineage(concept: BuildConcept) -> Iterator[BuildConcept]:
    """`concept` and its row-lineage ancestors, each address visited once."""
    seen: set[str] = set()
    stack = [concept]
    while stack:
        current = stack.pop()
        if current.address in seen:
            continue
        seen.add(current.address)
        yield current
        stack.extend(row_lineage_arguments(current))


def _row_lineage_closure(concept: BuildConcept) -> list[BuildConcept]:
    return list(_iter_row_lineage(concept))


def _lineage_crosses_row_shape_barrier(concept: BuildConcept) -> bool:
    return any(
        c.derivation in ROW_SHAPE_BARRIER_DERIVATIONS
        for c in _iter_row_lineage(concept)
    )


def _aggregate_row_preserving_inputs(concept: BuildConcept) -> list[BuildConcept]:
    if not isinstance(concept.lineage, BuildAggregateWrapper):
        return []
    return [
        arg
        for arg in concept.lineage.function.arguments
        if isinstance(arg, BuildConcept)
        and arg.derivation in ROW_PRESERVING_AGGREGATE_INPUT_DERIVATIONS
        and not _lineage_crosses_row_shape_barrier(arg)
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
        if concept.lineage.where.existence_arguments:
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
            or _lineage_crosses_row_shape_barrier(concept)
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
                or _lineage_crosses_row_shape_barrier(arg)
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
        for region in _regions_within(keyspace, region_spans)
    )


def _regions_within(keyspace: Keyspace, spans: frozenset[str]) -> list[Region]:
    """The live extension regions whose spans all fall inside `spans`."""
    return [r for r in keyspace.live_regions if r.spans and r.spans <= spans]


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
    regions = _regions_within(keyspace, region_spans)
    named: dict[int, BuildConcept] = {}
    rewritten: list[BuildConcept] = []
    for concept in outputs:
        if concept.address not in primary_addrs or not isinstance(
            concept.lineage, BuildAggregateWrapper
        ):
            rewritten.append(concept)
            continue
        taking = [
            cast(BuildFunction, argument)
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
    environment: BuildEnvironment,
    region_spans: frozenset[str] = frozenset(),
    named: frozenset[str] = frozenset(),
) -> list[StrategyNode]:
    """Project scalar aggregate inputs without exposing the merge's join inputs.

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
    if not all(concept_satisfiable(concept, available) for concept in scalar_inputs):
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
            if not isinstance(join, BaseJoin):
                continue
            if join.join_type in (JoinType.LEFT_OUTER, JoinType.FULL):
                extended.add(join.right_datasource.identifier)
            if join.join_type in (JoinType.RIGHT_OUTER, JoinType.FULL):
                if join.left_datasource is None:
                    implicit_left_rights.append(join.right_datasource.identifier)
                else:
                    extended.add(join.left_datasource.identifier)
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
            row_parents = [
                candidate
                for candidate in parent.parents
                if candidate.resolve().grain.components
            ]
            if len(row_parents) == 1:
                return at_input_grain(row_parents[0], seen)
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


def _relation_licenses_handle(
    environment: BuildEnvironment, concept: BuildConcept
) -> bool:
    """Whether a declared relation lets a non-rowset scan stand in for a rowset
    handle. A scoped join (`subset join rs.k = l_key`) or an authored merge makes
    the handle a substitutable identity of a column the anchor binds; without
    one, a rowset is opaque: its handle's row lineage alone must not let a scan
    over the same base re-derive it (a bare shared-key read of a renamed rowset
    key is Disconnected, never an implicit join)."""
    if concept.pseudonyms:
        return True
    return _scoped_relation_member(environment, concept.address)


def _scoped_relation_member(environment: BuildEnvironment, address: str) -> bool:
    for canonical, members in environment.scoped_join_key_groups.items():
        if address == canonical or address in members:
            return True
    return False


def _mangled_rowset_content_addresses(environment: BuildEnvironment) -> set[str]:
    """Addresses of the hidden per-rowset alias concepts a rowset body's renamed
    outputs materialize (`select aid as k` stores `local._rs_k`; see
    ``SemanticState.mangle_rowset_alias``). These are internals of the rowset;
    an un-renamed output's content is the base concept itself and is excluded.
    Their auto-pseudonym back to the authored source is rename plumbing, not a
    declared relation, so it never licenses synthesis on a foreign scan."""
    out: set[str] = set()
    for concept in [
        *environment.concepts.values(),
        *environment.alias_origin_lookup.values(),
    ]:
        lineage = concept.lineage
        if isinstance(lineage, BuildRowsetItem) and lineage.content.name.startswith(
            f"_{lineage.rowset.name}_"
        ):
            out.add(lineage.content.address)
    return out


def _carried_handle_is_partial(
    environment: BuildEnvironment, concept: BuildConcept, available: set[str]
) -> bool:
    """A licensed rowset handle carried onto a non-rowset scan is a PARTIAL
    binding when the scan's own member of the licensing relation spans only
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


def _carry_needs_grain_change(concept: BuildConcept) -> bool:
    """True when synthesizing ``concept`` from a parent's row stream would need
    an aggregate/window node of its own, so it cannot be widened onto a scan.

    A ROWSET handle terminates the walk: its value arrives materialized at the
    rowset boundary rather than being recomputed here, so a derived key over one
    (`agg.period + 53`) stays carryable under the licensed-handle rules above.
    """
    stack = [concept]
    seen: set[str] = set()
    while stack:
        current = stack.pop()
        if current.address in seen:
            continue
        seen.add(current.address)
        if current.derivation in (Derivation.AGGREGATE, Derivation.WINDOW):
            return True
        if current.derivation == Derivation.ROWSET:
            continue
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
    for canonical, members in environment.scoped_join_key_groups.items():
        group = {canonical, *members}
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


def _rowset_base_join_keys(
    mandatory_list: list[BuildConcept],
    node: StrategyNode,
    feeders: list[StrategyNode],
) -> frozenset[str]:
    """Output rowset boundaries' base grain keys that BOTH the assembled
    contributor and every feeder can render.

    A FINAL-hosted gate keyed by such an address (see
    `condition_placement.PlacementReason.FINAL_ROWSET_BASE_KEY`) pairs to the
    boundary on it; without the widening the merge has no shared column and
    cross-joins, which the keyless-join guard rejects."""
    base_keys = output_rowset_grain_keys(mandatory_list)
    if not base_keys:
        return frozenset()
    available = renderable_addresses(node)
    for feeder in feeders:
        available &= renderable_addresses(feeder)
    return frozenset(base_keys & available)


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


def _carry_join_keys(
    parents: list[StrategyNode],
    join_key_concepts: list[BuildConcept],
    environment: BuildEnvironment,
) -> None:
    mangled_contents = _mangled_rowset_content_addresses(environment)

    for parent in parents:
        # A rowset domain joins the rest on its spans, which it emits already:
        # its boundary can render the body's grain keys, but those are absent
        # on the region, so carrying one widens the domain past the region's
        # members. A ROOT domain is a dimension scan and carries a key it
        # binds.
        if parent.region_spans and any(
            isinstance(node, RowsetNode) for node in _iter_strategy_nodes(parent)
        ):
            continue
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
            # substitutes the handle onto) is unaffected.
            if (
                parent_rowsets
                and isinstance(concept.lineage, BuildRowsetItem)
                and concept.lineage.rowset.name not in parent_rowsets
            ):
                continue
            # A non-rowset parent may substitute a handle only when a declared
            # relation licenses it (the anchor under `subset join rs.k = l_key`);
            # unlicensed, the synthesis silently joins a query that is
            # disconnected. A handle the parent's own inputs emit (a derivation
            # over the boundary, split from the region's domain) is passed
            # through, not synthesized. A renamed output's mangled content
            # (`_rs_k`) is equally internal: the licensed plan joins the
            # anchor's own column against the boundary's handle, never a
            # synthesized body-local.
            if not parent_rowsets:
                if (
                    isinstance(concept.lineage, BuildRowsetItem)
                    and concept.address not in parent_output_addresses(parent)
                    and not _relation_licenses_handle(environment, concept)
                ):
                    continue
                if concept.address in mangled_contents and not _scoped_relation_member(
                    environment, concept.address
                ):
                    continue
            if not concept_satisfiable(concept, available):
                continue
            # Carrying a key the parent does not already emit means SYNTHESIZING
            # it from that parent's row inputs. A grain-collapsing derivation
            # cannot render that way (it needs its own aggregate/window node),
            # so widening a plain scan onto one emits a column nothing can
            # source (`avg(amount) by part` asked of the bare `rows` scan).
            if concept.address not in available and _carry_needs_grain_change(concept):
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
      contributor (`_add_relation_axis_contributors`,
      `_add_partial_completion_contributors`) is deliberately column-invisible
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
    relation_members = {
        addr
        for canonical, members in environment.scoped_join_key_groups.items()
        for addr in (canonical, *members)
    }
    visible = [
        {o.address for o in p.output_concepts if o.address not in p.hidden_concepts}
        for p in parents
    ]
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
        for canonical, members in environment.scoped_join_key_groups.items():
            relation = {canonical, *members}
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


def _drop_unadvertised_rowset_handles(node: StrategyNode, advertised: set[str]) -> None:
    """Strip a rowset handle a ROOT scan renders only by pseudonym substitution.

    A `union join quantity = rs.return_quantity` makes the two members
    pseudonyms, so the fact scan can bind `rs.return_quantity` to its own
    `quantity` column and the bridge walk picks it up even though the group's
    concept sets never advertised it. That column is an IMPOSTOR: the handle
    names a value of the rowset body, and a consumer reading it off the anchor
    sees the anchor's own row (`count(rs.return_quantity)` counts every sales
    row instead of the matched returns). The mates the merge genuinely needs as
    a join axis ARE advertised, so keeping the contract is enough."""
    keep = [
        o
        for o in node.output_concepts
        if o.address in advertised or not isinstance(o.lineage, BuildRowsetItem)
    ]
    if len(keep) == len(node.output_concepts):
        return
    node.set_output_concepts(keep)


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
    for succ in group_graph.successors(gid):
        if succ == FINAL_NODE_ID:
            continue
        unfiltered = ancestors & set(group_graph.predecessors(succ))
        if not unfiltered:
            continue
        supplied = frozenset().union(*(_members_of(attrs, a) for a in unfiltered))
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


def _pre_merge_parents(
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    join_key_addresses: frozenset[str] = frozenset(),
    needed: set[str] | None = None,
    group_graph: nx.DiGraph | None = None,
    built: dict[str, StrategyNode] | None = None,
    force_join_type: JoinType | None = None,
    preexisting_conditions: BoolExpr | None = None,
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
    _widen_merge_join_keys(parents, environment, join_key_addresses)
    # After widening, not before: a parent that has just been handed the join
    # key it was missing needs no bridge.
    if group_graph is not None and built is not None:
        parents = _bridge_unpaired_parents(parents, group_graph, built, environment)
    seen: set[str] = set()
    all_outputs: list[BuildConcept] = []
    for p in parents:
        for o in p.output_concepts:
            if o.address not in seen:
                seen.add(o.address)
                all_outputs.append(o)
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
    )
    return [merged]


def _union_arm_parents(
    parent_builds: list[ParentBuild],
    attrs: dict[str, GroupAttrs],
    environment: BuildEnvironment,
    needed: set[str],
    group_graph: nx.DiGraph,
    built: dict[str, StrategyNode],
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


def _output_covers(output: BuildConcept, concept: BuildConcept) -> bool:
    """Whether a node output supplies `concept`, directly or via pseudonym.

    A struct field selected as `unnest_array.a` parses to the bare key
    `local.a` but is produced under its derivable pseudonym `unnest_array.a`;
    the CTE layer maps the two by pseudonym, so coverage matching must too."""
    return (
        output.address == concept.address
        or output.address in concept.pseudonyms
        or concept.address in output.pseudonyms
    )


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
        if any(_output_covers(o, m) for o in node.output_concepts):
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


def _scoped_join_mates(environment: BuildEnvironment, address: str) -> frozenset[str]:
    """The other members of the COALESCING key group `address` belongs to.

    Only a coalescing relation (`union`/`full`) fuses its members onto one axis
    that the merge emits under every member's own alias, which is what lets a
    mate answer for the address. A subset or global-merge member keeps its own
    column name, so reading it under the other member's address dangles."""
    coalescing = environment.domain_graph.coalescing_relation_members()
    if address not in coalescing:
        return frozenset()
    for canonical, members in environment.scoped_join_key_groups.items():
        group = {canonical, *members}
        if address in group:
            return frozenset(group - {address}) & coalescing
    return frozenset()


def _cover_groups_for_mandatory(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    ownership: ExtentOwnership,
) -> dict[str, list[BuildConcept]]:
    """For each mandatory concept, pick the most-downstream built group that
    actually exposes it (more built ancestors = further downstream). Returns
    `{gid: [concepts that group provides]}` preserving discovery order so
    the MergeNode renders with a stable join layout.

    A group carrying the concept's authored scoped-join MATE counts as exposing
    it: the merge coalesces the two members onto one axis, and the mate is
    surfaced under the concept's own address once the merge resolves. Without
    that, an aggregate over the completed join loses the axis to the raw
    boundary below it, which then re-enters the merge with the rows the
    aggregate's population already excluded.

    A ``~``-licensed key comes from its elected owner and nowhere else: that
    group is the one built with permission to manufacture the key's extension
    rows, so any other candidate exposes only the members the fact bound. The
    election is read here rather than re-derived, because the two answers
    diverging is what leaves a contributor dangling at render time."""
    per_group: dict[str, list[BuildConcept]] = defaultdict(list)
    for concept in mandatory_list:
        addr = concept.address
        mates = _scoped_join_mates(environment, addr)
        candidates = [
            gid
            for gid, node in built.items()
            if any(
                o.address == addr or o.address in mates for o in node.output_concepts
            )
        ]
        # Only fall back to pseudonym coverage (struct fields produced under
        # their derivable origin address) when nothing provides the concept
        # directly; a plain alias keeps its own contributor.
        if not candidates:
            candidates = [
                gid
                for gid, node in built.items()
                if any(_output_covers(o, concept) for o in node.output_concepts)
            ]
        if not candidates:
            continue
        # A dim peeled off a row stream decorates the stream's keys; its own
        # key carries every entity, not the rows the stream's WHERE kept.
        candidates.sort(
            key=lambda gid: (
                sum(1 for a in nx.ancestors(group_graph, gid) if a in built),
                addr not in attrs[gid].dim_keys,
                addr in attrs[gid].members,
            ),
            reverse=True,
        )
        winner = candidates[0]
        owner = ownership.owner_of(addr)
        # a group that READ the owner carries its rows too, and more besides
        if (
            owner is not None
            and owner in candidates
            and not any(owner in nx.ancestors(group_graph, gid) for gid in candidates)
        ):
            winner = owner
        per_group[winner].append(concept)
    return per_group


def _add_relation_axis_contributors(
    group_graph: nx.DiGraph,
    built: dict[str, StrategyNode],
    per_group: dict[str, list[BuildConcept]],
    final_contract: FinalAssemblyContract,
    environment: BuildEnvironment,
) -> None:
    """A FINAL merge grain carrying an authored relation needs every hosting
    side present as a contributor: a computed member (`union join rank
    orders.oid order by orders.amt desc = customers.rnk`) is exposed only by
    its own row-shape-barrier group, which covers no mandatory output and so
    is dropped by the mandatory cover, degrading the merge to a cross join.
    Add the built group exposing a missing hosted axis member as an axis-only
    contributor (it contributes no mandatory concepts). Edits `per_group` in
    place."""
    if len(per_group) < 2 or not environment.scoped_join_key_groups:
        return
    merge_grain = set(final_contract.merge_grain)
    if not merge_grain:
        return
    outputs_of = {
        gid: {o.address for o in node.output_concepts} for gid, node in built.items()
    }
    chosen = set(per_group)
    for canonical, members in environment.scoped_join_key_groups.items():
        relation = {canonical, *members} & merge_grain
        if len(relation) < 2:
            continue
        hosted = {addr for gid in chosen for addr in relation & outputs_of[gid]}
        if not hosted:
            continue
        for addr in sorted(relation - hosted):
            # A provider upstream of a chosen contributor already had its
            # side merged there, so it pairs nothing new: re-entering the
            # FINAL merge it would only re-admit the rows that contributor's
            # own join and population already dropped.
            merged_below = set().union(
                *(nx.ancestors(group_graph, gid) for gid in chosen)
            )
            provider = next(
                (
                    gid
                    for gid in sorted(built)
                    if gid not in chosen
                    and gid not in merged_below
                    and addr in outputs_of[gid]
                ),
                None,
            )
            if provider is not None:
                per_group.setdefault(provider, [])
                chosen.add(provider)


def _holds_statement_rows(
    reader: GroupAttrs, required: frozenset[str], environment: BuildEnvironment
) -> bool:
    """A reader of a region carries the statement's rows: a row stream does,
    an aggregate only when its grain determines the statement's."""
    if reader.derivation not in GROUPING_DERIVATIONS:
        return True
    return all(
        build_fd_determines(
            environment, reader.grain_components, address, include_empty_grain=False
        )
        for address in required
    )


def _add_region_domain_contributors(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    per_group: dict[str, list[BuildConcept]],
    environment: BuildEnvironment,
) -> None:
    """A region domain contributes ROWS (its members, NULL on everything
    absent there), so a domain whose every column a sibling also renders is
    added as a contributor of no concepts, unless a contributor already reads
    it and holds the statement's rows. An aggregate over the region holds them
    only at its own grain: aggregates by `status` and by `name` each read the
    region, but only the domain's rows pair a name with a status. A rename of
    something the domain carries (`item_desc as d`) is rendered on the domain:
    any other host holds it for matched members only."""
    required = _required_final_contract(attrs).required_grain
    for gid in sorted(built):
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
        base = built[gid]
        renames = [
            concept
            for other, concepts in per_group.items()
            if other != gid
            for concept in concepts
            if _renders_rename_of_member(concept, attrs[gid], base)
        ]
        if not renames:
            continue
        projected = SelectNode(
            output_concepts=list(base.output_concepts),
            input_concepts=list(base.output_concepts),
            environment=base.environment,
            parents=[base],
            partial_concepts=list(base.partial_concepts),
        )
        widen_projection(projected, renames)
        built[gid] = projected
        moved = {c.address for c in renames}
        for other in [o for o in per_group if o != gid]:
            per_group[other] = [c for c in per_group[other] if c.address not in moved]
            # still an output there, and a rename canonicalizes to its source:
            # left visible it reads as a COMPLETE copy of what the domain
            # carries, and the merge drops the domain as redundant. A copy:
            # the node may be a parent elsewhere that reads the rename.
            exposed = moved & {o.address for o in built[other].output_concepts}
            if exposed:
                hidden = built[other].copy()
                hidden.hidden_concepts |= exposed
                hidden.rebuild_cache()
                built[other] = hidden
        per_group[gid].extend(renames)


def _renders_rename_of_member(
    concept: BuildConcept, domain: GroupAttrs, node: StrategyNode
) -> bool:
    """`concept` renames a member of `domain` its built node can render: it
    outputs the rename itself or every argument of it."""
    if not (
        isinstance(concept.lineage, BuildFunction)
        and concept.lineage.operator == FunctionType.ALIAS
    ):
        return False
    args = {a.address for a in concept.lineage.concept_arguments}
    outputs = {c.address for c in node.output_concepts}
    return args <= set(domain.primary_members) and (
        concept.address in outputs or args <= outputs
    )


def _add_partial_completion_contributors(
    built: dict[str, StrategyNode],
    per_group: dict[str, list[BuildConcept]],
    environment: BuildEnvironment,
) -> None:
    """A mandatory scoped-join member completes through its group-mate's
    boundary: add as an axis-only contributor a built group exposing a
    COMPLETE rowset-handle mate the chosen contributors don't carry (the
    request itself happened in `_unsourced_relation_mates`). Two triggers:

    - the covering contributor leaves the member PARTIAL: its own binding
      spans only its side's domain, and only the mate's boundary carries the
      axis (rowset-anchor `subset join cust = members.mid`); the FINAL merge
      then pairs the sides and `_clear_groupmate_completed_partials` un-marks
      the member.
    - the member belongs to a COALESCING (`union`/`full`) relation: a
      demanded member projects the unified axis, so every side's boundary must
      join the merge even though it covers no mandatory output (bare-member
      projection).

    ROOT mates stay out; a datasource-bound mate completes through the
    authoritative-datasource machinery, not a boundary merge. Edits
    `per_group` in place."""
    if not environment.scoped_join_key_groups:
        return
    chosen = set(per_group)

    def _complete_outputs(gid: str) -> set[str]:
        node = built[gid]
        partial = {c.address for c in node.partial_concepts}
        return {o.address for o in node.output_concepts if o.address not in partial}

    covered: set[str] = set()
    covered_partial: set[str] = set()
    for gid, concepts in per_group.items():
        node_partial = {c.address for c in built[gid].partial_concepts}
        for concept in concepts:
            covered.add(concept.address)
            if concept.address in node_partial:
                covered_partial.add(concept.address)

    def _outputs(gid: str) -> set[str]:
        return {o.address for o in built[gid].output_concepts}

    coalescing = environment.domain_graph.coalescing_relation_members()
    for canonical, members in environment.scoped_join_key_groups.items():
        relation = {canonical, *members}
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
        rowset_mates = {
            addr
            for addr in relation - triggers
            if (c := environment.concepts.get(addr)) is not None
            and c.address == addr
            and c.derivation == Derivation.ROWSET
        }
        exposed: set[str] = set()
        for gid in chosen:
            exposed |= expose(gid)
        for addr in sorted(rowset_mates - exposed):
            provider = next(
                (
                    gid
                    for gid in sorted(built)
                    if gid not in chosen and addr in expose(gid)
                ),
                None,
            )
            if provider is not None:
                per_group.setdefault(provider, [])
                chosen.add(provider)


def _fold_descendant_contributors(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    per_group: dict[str, list[BuildConcept]],
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
    Edits `per_group` in place.

    Passthrough = add S's concepts to B's input AND output: `resolve_concept_
    map` sources an output from a parent only when it's also an input
    (`inherited`); an output that isn't an input is re-derived in B's own CTE
    (which would recompute S's aggregates from their source columns). The
    `available` guard ensures the columns actually come off B's own parents."""
    for b_gid in list(per_group.keys()):
        if b_gid not in per_group or attrs[b_gid].derivation not in (
            Derivation.BASIC,
            Derivation.WINDOW,
        ):
            continue
        b_node = built[b_gid]
        b_ancestors = nx.ancestors(group_graph, b_gid)
        available = parent_output_addresses(b_node)
        dirty = False
        for s_gid in b_ancestors:
            if s_gid not in per_group or s_gid == b_gid:
                continue
            s_concepts = per_group[s_gid]
            if not all(c.address in available for c in s_concepts):
                continue
            dirty |= widen_projection(
                b_node,
                s_concepts,
                input_candidates=s_concepts,
                available_addresses=available,
                rebuild=False,
            )
            per_group[b_gid].extend(s_concepts)
            del per_group[s_gid]
        if dirty:
            b_node.rebuild_cache()


def _promote_final_aliases_to_grouping_contributors(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    per_group: dict[str, list[BuildConcept]],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
) -> None:
    def has_grouping_output(gid: str) -> bool:
        return node_nulls_grouping_keys(built[gid])

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
            available = {output.address for output in built[gid].output_concepts}
            if not concept_satisfiable(concept, available):
                continue
            base = built[gid]
            projected = SelectNode(
                output_concepts=list(base.output_concepts),
                input_concepts=list(base.output_concepts),
                environment=environment,
                parents=[base],
                partial_concepts=list(base.partial_concepts),
            )
            widen_projection(projected, [concept])
            built[gid] = projected
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
        src_node = built[src_gid]
        if src_node.existence_concepts or src_node.force_group:
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
            if attrs[gid].derivation not in GROUPING_DERIVATIONS and not (
                has_grouping_output(gid)
            ):
                continue
            available = {output.address for output in built[gid].output_concepts}
            if not all(concept_satisfiable(c, available) for c in members):
                continue
            if not _feeder_conditions_implied(group_graph, attrs, src_gid, gid):
                continue
            base = built[gid]
            projected = SelectNode(
                output_concepts=list(base.output_concepts),
                input_concepts=list(base.output_concepts),
                environment=environment,
                parents=[base],
                partial_concepts=list(base.partial_concepts),
            )
            widen_projection(projected, members)
            built[gid] = projected
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
    return [
        c
        for address in sorted(addresses)
        if (c := _concept_at(environment, address)) is not None
    ]


def _fresh_final_root_projection(
    projected: list[BuildConcept],
    environment: BuildEnvironment,
    graph: ReferenceGraph,
    history: History,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    node = plan_source(
        SourceRequest(
            outputs=projected,
            environment=environment,
            graph=graph,
            history=history,
            conditions=conditions,
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


def _aggregate_reused_from_twin(
    address: str,
    gid: str,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
) -> bool:
    """Whether an aggregate value of `gid` is already produced by another built
    grouping group at the SAME grain, its pre-condition d1 twin. Such a value is
    read through (the GroupNode resolves it from the twin parent), not recomputed,
    so its raw inputs need not enter `gid`'s `needed` set. Grain equality gates
    out a coarser re-aggregation (avg-of-sum), which genuinely recomputes."""
    grain = attrs[gid].grain_components
    for other_gid, other_node in built.items():
        if other_gid == gid:
            continue
        other = attrs[other_gid]
        if other.derivation not in GROUPING_DERIVATIONS:
            continue
        if other.grain_components != grain:
            continue
        if any(o.address == address for o in other_node.output_concepts):
            return True
    return False


def _with_null_members(
    node: StrategyNode, spans: frozenset[str], environment: BuildEnvironment
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
        for datasource in environment.datasources.values():
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
            joins.append(NodeJoin(node, member, [key], JoinType.FULL))
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
        # rowset, so `_final_merge_grain` has nothing to declare) fall back to
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
    for canonical, members in environment.scoped_join_key_groups.items():
        relation = {canonical, *members}
        if addresses & relation:
            addresses |= relation
    return addresses


def _bridge_unpaired_parents(
    parents: list[StrategyNode],
    group_graph: nx.DiGraph,
    built: dict[str, StrategyNode],
    environment: BuildEnvironment,
) -> list[StrategyNode]:
    """Pull in the built group that pairs a contributor sharing no column with
    any sibling, so the merge below joins on an axis instead of `ON 1=1`.

    `select id, min(x) by id, min(y) by cell` leaves two aggregates, one keyed
    on `id` and one on `cell`, with nothing that maps between them: the
    row-grain projection of `{id, cell}` that fed the cell aggregate covers no
    mandatory output, so neither the cover election nor a consumer's parent
    dedup keeps it. Add it back as a join-only parent.

    A contributor that genuinely cross joins (a global aggregate) is untouched:
    nothing else renders its column, so it has no candidate."""
    if len(parents) < 2:
        return parents
    outputs = [_pairing_addresses(parent, environment) for parent in parents]
    exposed = {
        gid: _pairing_addresses(node, environment) for gid, node in built.items()
    }
    present = {id(parent) for parent in parents}
    added: list[StrategyNode] = []
    for index, own in enumerate(outputs):
        others: set[str] = set()
        for other_index, other in enumerate(outputs):
            if other_index != index:
                others |= other
        if own & others:
            continue
        candidates = [
            gid
            for gid, node in built.items()
            if id(node) not in present and exposed[gid] & own and exposed[gid] & others
        ]
        if not candidates:
            continue
        candidates.sort(
            key=lambda gid: (
                sum(1 for a in nx.ancestors(group_graph, gid) if a in built),
                gid,
            ),
            reverse=True,
        )
        bridge = built[candidates[0]]
        present.add(id(bridge))
        added.append(bridge)
        outputs.append(exposed[candidates[0]])
    return parents + added


def _filter_arg_parents(
    group_graph: nx.DiGraph,
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
        candidates = [
            gid
            for gid, node in built.items()
            if any(o.address == addr for o in node.output_concepts)
        ]
        if not candidates:
            continue
        candidates.sort(
            key=lambda gid: (
                spans <= {o.address for o in built[gid].output_concepts},
                sum(1 for a in nx.ancestors(group_graph, gid) if a in built),
            ),
            reverse=True,
        )
        gid = candidates[0]
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
    grain = {
        component
        for address in missing
        if (concept := _concept_at(environment, address)) is not None
        and concept.derivation == Derivation.AGGREGATE
        and concept.grain is not None
        for component in concept.grain.components
    }
    return missing | (grain - available)


def _required_final_contract(attrs: dict[str, GroupAttrs]) -> FinalAssemblyContract:
    contract = attrs[FINAL_NODE_ID].final_contract
    if contract is None:
        raise ValueError("FINAL contract missing; Stage 2 must declare final_contract")
    return contract


def _final_contributor_contracts(
    final_contract: FinalAssemblyContract,
    contributing: list[str],
) -> dict[str, FinalContributorContract]:
    contracts = {
        contract.group_id: contract for contract in final_contract.contributor_contracts
    }
    missing_contracts = [gid for gid in contributing if gid not in contracts]
    if missing_contracts:
        raise ValueError(
            "FINAL contributor contract missing for groups: "
            + ", ".join(sorted(missing_contracts))
        )
    return {gid: contracts[gid] for gid in contributing}


def _relevant_root_preserve_keys(
    environment: BuildEnvironment,
    output_concepts: list[BuildConcept],
    preserve_keys: frozenset[str],
    member_addresses: frozenset[str] = frozenset(),
) -> frozenset[str]:
    if not preserve_keys:
        return frozenset()
    output_addresses = {concept.address for concept in output_concepts}
    statement_members = _statement_scoped_relation_members(environment)
    relevant: set[str] = set()
    for key in preserve_keys:
        if key in output_addresses:
            relevant.add(key)
            continue
        # An AUTHORED statement-relation member is the declared merge axis: it
        # already survived the merge-grain intersection, and the FD test below
        # cannot vouch for it (a `unique` business key determines the outputs
        # only through a uniqueness the FD tables don't encode). Dropping it
        # re-loses the authored join key (`subset join cust.cid = id` with
        # only `id`-determined properties projected). ROWSET handles stay out:
        # a root scan must never claim a rowset boundary's own member.
        if key in statement_members:
            key_concept = environment.concepts.get(key)
            if key_concept is not None and key_concept.derivation != Derivation.ROWSET:
                relevant.add(key)
                continue
        # A key the ROOT group itself carries as a member is a genuine BRIDGE
        # join key even when it doesn't FD-determine any output: the group put
        # this key beside the output because a shared finer member (the fact's
        # order_id) connects them (`customer_id` beside `product_name`, bridged
        # through orders). Keeping it lets the root scan emit the connecting
        # pairs and join the sibling aggregate on it, instead of the merge
        # cross-joining ON 1=1.
        if key in member_addresses:
            relevant.add(key)
            continue
        if any(
            build_fd_determines(
                environment,
                {key},
                concept.address,
                include_empty_grain=False,
            )
            for concept in output_concepts
        ):
            relevant.add(key)
    # A composite merge grain reaches a dimension through a foreign key OFF
    # the grain (`(item, ticket)` -> customer -> region): the keys determine
    # the output jointly and none does alone, which is the composite peel
    # `_composite_determining_grain` hands this scan.
    if any(
        concept.address not in preserve_keys
        and not _fd_determined(environment, relevant, concept)
        and _fd_determined(environment, preserve_keys, concept)
        for concept in output_concepts
    ):
        return preserve_keys
    return frozenset(relevant)


def _fd_determined(
    environment: BuildEnvironment, keys: Iterable[str], concept: BuildConcept
) -> bool:
    return build_fd_determines(
        environment, set(keys), concept.address, include_empty_grain=False
    )


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
    if isinstance(node, (GroupNode, WindowNode)) or node.force_group:
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
            if any(_output_covers(o, m) for m in contract_outputs)
        ]
    if isinstance(node, MergeNode):
        # Narrow to the requested grain *before* force-grouping: a MergeNode
        # exposes its join/filter columns as outputs, so force_group alone
        # would GROUP BY the full merge grain and dedup nothing.
        node.force_group = True
        node.set_output_concepts(targets, rebuild=False, change_visibility=False)
        node.rebuild_cache()
        return node
    # An additive aggregate exposed by a row-preserving scan being grouped to a
    # coarser grain is a precomputed value at the finer grain (a
    # materialized-root rollup from a finer summary table), so re-aggregate it
    # with SUM rather than dedup it. Exact-grain materialized aggregates never
    # reach here: their scan already matches the target grain, so no group is
    # required.
    rollup = [o for o in targets if o.is_aggregate and _is_additive_aggregate(o)]
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


def _assemble_final_node(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    graph: ReferenceGraph,
    history: History,
    root_requests: dict[str, RootRequest],
    feeder_cache: "_CleanFeederCache | None" = None,
) -> StrategyNode | None:
    """Build the FINAL output node: merge the minimum set of built groups
    that together cover `mandatory_list`. When a single group already covers
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

    def _apply_final_conditions(node: StrategyNode) -> StrategyNode:
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
            built,
            _region_paired_args(filter_only_addrs, avail, spans, environment),
            spans,
        )
        row_arg_addrs = {c.address for c in condition_row_args(final_conditions)}
        row_concepts = [
            concept
            for concept in node.output_concepts
            if concept.address in row_arg_addrs
        ]
        # A membership (`x in <set>`) deferred onto FINAL is hosted on a merge
        # built here, so its subselect feeder is wired here too.
        ex_groups = _condition_existence_arg_groups(final_conditions.conditional)
        ex_concepts = _flatten_arg_groups(ex_groups)
        ex_parents = (
            _existence_parents_for(
                ex_groups, built, skip=node, feeder_cache=feeder_cache
            )
            if ex_groups
            else []
        )
        # A feeder that participates in a scoped relation must join back on
        # the relation axis, not cross-join: widen the contributor (and the
        # feeder) with the authored members each side can render: a leaf
        # scan picks up the mate it binds, the boundary its member handle.
        # Feeders with no relation stay hidden cross-join inputs.
        # A gate keyed by a base grain key of an output rowset boundary pairs
        # to the boundary on that key; widen both sides so the merge joins on
        # it instead of cross-joining.
        if arg_nodes:
            base_keys = _rowset_base_join_keys(mandatory_list, node, arg_nodes)
            if base_keys:
                _widen_merge_join_keys([node, *arg_nodes], environment, base_keys)
            # A row-level atom over the facts (`undelivered`) beside a node
            # holding a region joins it on the span: cross-joined, any fact
            # row's value would pass for every member of the region.
            if node_spans := region_reads(node):
                _widen_merge_join_keys([node, *arg_nodes], environment, node_spans)
        if arg_nodes and environment.scoped_join_key_groups:
            relation_keys: set[str] = set()
            for feeder in arg_nodes:
                feeder_outs = {o.address for o in feeder.output_concepts}
                for canonical, members in environment.scoped_join_key_groups.items():
                    relation = {canonical, *members}
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

    ownership = attrs[FINAL_NODE_ID].extent_ownership or ExtentOwnership()
    per_group = _cover_groups_for_mandatory(
        group_graph,
        attrs,
        built,
        mandatory_list,
        environment,
        ownership,
    )
    if not per_group:
        return _apply_final_conditions(
            _group_to_grain_if_required(
                next(iter(built.values())),
                mandatory_list,
                final_contract,
                environment,
            )
        )
    _add_relation_axis_contributors(
        group_graph, built, per_group, final_contract, environment
    )
    _add_partial_completion_contributors(built, per_group, environment)
    _fold_descendant_contributors(group_graph, attrs, built, per_group)
    _promote_final_aliases_to_grouping_contributors(
        group_graph, attrs, built, per_group, mandatory_list, environment
    )
    # last: the promotion above drops a contributor left with no concepts,
    # which is all a row-only domain ever has
    _add_region_domain_contributors(group_graph, attrs, built, per_group, environment)
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
        gid = contributing[0]
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
            and _push_row_condition_before_group(
                sole_node, final_conditions, environment
            )
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
                group_graph, built, filter_only_addrs - sole_avail
            )
            feeder_outs = [{o.address for o in f.output_concepts} for f in feeder_nodes]
            scoped_addrs = {
                addr
                for canonical, members in environment.scoped_join_key_groups.items()
                for addr in (canonical, *members)
            }
            relation_paired_feeders = any(outs & scoped_addrs for outs in feeder_outs)
            spans = region_reads(sole_node)
            region_paired_feeders = bool(spans) and any(
                outs & (spans | sole_avail) - mandatory_addresses
                for outs in feeder_outs
            )
        if final_probe_args or relation_paired_feeders or region_paired_feeders:
            conditioned = _apply_final_conditions(sole_node)
            # The feeder join reads the probe at ITS OWN row grain (the fact
            # side of the relation), fanning the contributor out; the merge's
            # claimed grain predates that join, so grain-satisfaction checks
            # (including MergeNode's own rowset-output carve-out) wave the
            # dedup through. Collapse explicitly to the requested outputs
            # after the filter.
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
            final_node if final_already_applied else _apply_final_conditions(final_node)
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

    # Only root scans get the grain projection: their grain is the row-level
    # source-table grain (often much wider than what a downstream merge
    # wants), and a SELECT DISTINCT-style projection is always safe.
    # Wrapping intermediate aggregates is *not* safe: a GroupNode over a
    # `sum(x)` node would re-aggregate the partial sums (OK for SUM, wrong
    # for AVG/STDDEV), and intermediate aggregates often don't even expose
    # the requested grain key (their GROUP BY is their grain, not the
    # downstream's).
    # Merge grain is defined by the grouping (aggregate/window) contributors;
    # compute it up front so root-scan wrapping can project an FD dimension at
    # this grain (keeping the join key) instead of its own key-grain. A ROWSET
    # boundary is a fixed-grain barrier too: its select grain is the key set a
    # merging dimension must join on, so a dimension scan rides it instead of
    # deduping to its own key grain and cross-joining ON 1=1.
    contracts_by_gid = _final_contributor_contracts(final_contract, contributing)
    final_merge_grain = frozenset().union(
        *(contract.projection_grain for contract in contracts_by_gid.values())
    )
    mangled_contents = _mangled_rowset_content_addresses(environment)

    # A grouping-sets sibling suppresses the FINAL dedup (`_group_to_grain_if_
    # required` passes a subtotal-bearing merge through), so a row-grain leaf
    # contributor must dedup itself before the merge instead.
    grouping_sibling = any(node_nulls_grouping_keys(built[g]) for g in contributing)

    parents: list[StrategyNode] = []
    for gid in contributing:
        node = built[gid]
        is_root = attrs[gid].derivation == Derivation.ROOT
        contributor_contract = contracts_by_gid[gid]
        preserve_keys = contributor_contract.preserve_keys & final_merge_grain
        group_concepts = list(per_group[gid])
        if is_root:
            # `final_merge_grain` is the union of what contributors ADVERTISE
            # (their projection grain), which a non-grouping contributor leaves
            # empty, so a ROOT sibling's join key would be filtered away and
            # the merge would cross-join ON 1=1. A sibling's own grain is a
            # stronger guarantee than its advertisement: a group at user grain
            # emits the user key whether or not it projects it. Preserve the
            # merge keys some sibling's grain vouches for.
            sibling_grain = {
                address
                for other in contributing
                if other != gid and other in attrs
                for address in attrs[other].grain_components
            }
            preserve_keys |= contributor_contract.preserve_keys & sibling_grain
            preserve_keys = _relevant_root_preserve_keys(
                environment,
                group_concepts,
                preserve_keys,
                frozenset(_members_of(attrs, gid)),
            )
            # A ROOT that already carries a merge key among its own concepts
            # joins its siblings on that key alone. Preserving the OTHER merge
            # keys forces the re-source below to drag in whatever fact table
            # carries them (a pure dim scan becomes a fact-dim join deduped to
            # the full output grain), and the merge then stitches on every key
            # null-safely, which under `~` partials pairs join-manufactured
            # NULLs with each other.
            # Foreign keys stay preserved only for the carrier-less case the
            # widen exists for: no own key means no join path, and the merge
            # would cross-join ON 1=1.
            # Only when the own keys determine what the ROOT delivers: a span
            # carrying `order_id` beside a `product_id` the composite
            # `(order_id, line_no)` determines repeats per line, and joining on
            # `order_id` alone pairs every line with every product of its order.
            own_join_keys = preserve_keys & (
                {concept.address for concept in group_concepts}
                | {concept.address for concept in node.usable_outputs}
            )
            if own_join_keys and all(
                concept.address in preserve_keys
                or _fd_determined(environment, own_join_keys, concept)
                for concept in group_concepts
            ):
                preserve_keys = frozenset(own_join_keys)
        # A preserved join key must survive the wrap: grouping the contributor
        # to a grain that excludes the key it was just re-sourced to carry
        # dedups that key straight back out, and the merge cross-joins anyway.
        projection_grain = (
            final_merge_grain | preserve_keys
            if is_root
            else contributor_contract.projection_grain
        )
        if is_root and preserve_keys:
            seen_group_concepts = {concept.address for concept in group_concepts}
            # Carry the merge grain's join KEYS onto the root scan, but never a
            # rowset's own handle outputs: a root that can derive those (it
            # shares the rowset's base key) would absorb the whole rowset and
            # drop its internal filter. Those handles aren't join keys; the
            # rowset stays a separate merge contributor. A renamed output's
            # mangled content (`_rs_k`) is a rowset internal the same way;
            # carrying it silently joins a disconnected query, unless a
            # declared relation licenses it.
            group_concepts.extend(
                c
                for address in sorted(preserve_keys)
                if (c := _concept_at(environment, address)) is not None
                and address not in seen_group_concepts
                and c.derivation != Derivation.ROWSET
                and (
                    address not in mangled_contents
                    or _scoped_relation_member(environment, address)
                )
            )
        if is_root:
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
            bucket_members = _members_of(attrs, gid)
            seen_group_addrs = {c.address for c in group_concepts}
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
                projected = _projection_root_concepts(group_concepts, environment)
                request = RootRequest(
                    frozenset(c.address for c in projected),
                    _wrap_atoms(satisfiable),
                    environment.span_scope,
                    preexisting=root_requests[gid].preexisting,
                )
                # Only a request the built node does not answer is planned:
                # the preserved keys and filter-only args widened it beyond
                # what the scan carries, or the scope moved.
                fresh = (
                    _fresh_final_root_projection(
                        projected, environment, graph, history, request.conditions
                    )
                    if projected
                    and len(satisfiable) == len(root_atoms)
                    and not request.answered_by(node, root_requests[gid])
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
            merge_concepts = [
                c for c in group_concepts if c not in filter_only_concepts
            ]
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
            parents.extend(wrapped)
        else:
            parents.append(node)

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
    _widen_merge_join_keys(parents, environment, final_merge_grain)
    parents = _fold_covered_contributors(
        parents,
        environment,
        final_needed | filter_only_addrs,
        {id(built[gid]) for gid, concepts in per_group.items() if concepts},
        {id(built[gid]) for gid in ownership.owner_by_span.values() if gid in built},
    )
    parents = _bridge_unpaired_parents(parents, group_graph, built, environment)
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
            _output_covers(o, c)
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
        for canonical, group_members in environment.scoped_join_key_groups.items():
            relation = {canonical, *group_members}
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
    parents = parents + arg_nodes
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
    )
    _clear_groupmate_completed_partials(merged, environment)
    # A hidden axis mate rides sides kept at their own finer row grain, and the
    # merge's claimed grain predates that fan, so grain-satisfaction checks
    # (including MergeNode's own rowset-output carve-out) wave the dedup
    # through, same trap as the probe-feeder branch above. Collapse explicitly
    # to the coalesced axis. A hidden region join key is the same story: the
    # in-place narrowing would take it off the merge that hosts by it.
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


def _apply_count_distinct_rewrites(
    outputs: list[BuildConcept], distinct_addrs: frozenset[str]
) -> list[BuildConcept]:
    """Render flagged COUNT members as COUNT(DISTINCT ...). These were folded
    onto a finer-grain sibling input stream (`aggregate_distinct_addrs`): the
    dedup their own key-grain input stream would have performed is exactly
    DISTINCT on the counted key value."""
    rewritten: list[BuildConcept] = []
    for concept in outputs:
        if concept.address in distinct_addrs and isinstance(
            concept.lineage, BuildAggregateWrapper
        ):
            function = dc_replace(
                concept.lineage.function, operator=FunctionType.COUNT_DISTINCT
            )
            concept = dc_replace(
                concept, lineage=dc_replace(concept.lineage, function=function)
            )
        rewritten.append(concept)
    return rewritten


def build_strategy_node(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    g: ReferenceGraph,
    history: History,
    complete_partials: bool = True,
    staged_conditions: list[BuildWhereClause] | None = None,
    depth: int = 0,
) -> StrategyNode | None:
    """Walk groups in topological order, dispatching each to its v4 generator
    with explicit parent nodes. Returns the most-downstream built node, or
    None if nothing built. `depth` is the nesting of this plan inside rowset
    bodies, for trace indentation."""
    from trilogy.core.processing.v4_node_generators import build_node  # cycle

    built: dict[str, StrategyNode] = {}
    root_requests: dict[str, RootRequest] = {}
    feeder_cache = _CleanFeederCache(environment, g, history)
    ownership = attrs[FINAL_NODE_ID].extent_ownership or ExtentOwnership()
    unbuilt: dict[str, list[BuildConcept]] = {}

    for gid in _topological_order(group_graph, group_edges):
        if gid == FINAL_NODE_ID:
            continue
        plan_trace.set_context(gid)
        if _inlined_by_every_reader(
            group_graph, group_edges, attrs, built, gid, environment, mandatory_list
        ):
            _fold_into_readers(group_graph, group_edges, attrs, gid)
            continue
        a = attrs[gid]
        # Scope the group's extent routing over its whole build, including the
        # consumer-side re-sources `_parent_nodes_for` plans below. A region
        # domain is the FINAL's own rows and is built under the FINAL's scope,
        # as its every reader sees it there (`_assemble_final_node`).
        environment.span_scope = (
            _final_span_scope(environment.span_scope)
            if a.extent_spans
            else _group_span_scope(environment.span_scope, ownership, gid)
        )
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
            c
            for addr in select_addrs
            if (c := _concept_at(environment, addr)) is not None
        ]
        if not outputs:
            unbuilt[gid] = outputs
            continue
        if derivation == Derivation.AGGREGATE and a.aggregate_distinct_addrs:
            outputs = _apply_count_distinct_rewrites(
                outputs, a.aggregate_distinct_addrs
            )
        primary_addrs = set(a.primary_members)
        twin_reused: dict[str, bool] = (
            {
                c.address: _aggregate_reused_from_twin(c.address, gid, attrs, built)
                for c in outputs
                if c.address in primary_addrs and c.lineage is not None
            }
            if derivation in _AGGREGATING_DERIVATIONS
            else {}
        )
        atoms = _atoms_at(attrs, gid)
        # Conjunction-coverage siblings only bind when this group recomputes
        # its aggregate over rows; a fully twin-reused value is read through,
        # so its input population is not this group's to filter, and keeping
        # the atom would resurrect the redundant fact-rescan parent through
        # its `needed` args.
        if a.conjunction_atoms and twin_reused and all(twin_reused.values()):
            atoms = [atom for atom in atoms if atom not in a.conjunction_atoms]
        injected = _wrap_atoms(atoms)
        preexisting = _wrap_atoms(_accumulated_atoms_above(group_graph, attrs, gid))
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
                if derivation in _AGGREGATING_DERIVATIONS:
                    # A post-condition aggregate that REUSES a same-grain twin
                    # (its pre-condition d1 sibling already materialized this
                    # value) reads the value through rather than recomputing,
                    # so its raw recompute inputs (the input-grain keys, measure
                    # columns) don't belong in `needed`. Pulling them keeps a
                    # redundant fact-rescan ROOT parent alive that only
                    # re-supplies grouping keys the twin already carries. Treat
                    # it like a passthrough: skip its lineage.
                    if not twin_reused.get(c.address, False):
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
        needed |= (
            set(_input_contract_join_keys(a))
            - set(a.grain_components)
            - set(a.output_concepts)
        )
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
        )
        parent_group_ids = {parent.group_id for parent in parent_builds}
        join_key_addresses = _input_contract_join_keys(a, parent_group_ids)
        if derivation == Derivation.UNION:
            parents = _union_arm_parents(
                parent_builds, attrs, environment, needed, group_graph, built
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
            # A ROOT scan hosting a null-rejecting request atom emits exactly
            # the atom's population, so the merge may claim it. A grouping
            # parent applies the atom to its INPUT rows and its output claims
            # nothing (the FINAL still gates); an `is null` atom is satisfied
            # by the padding a preserving join adds.
            applied_atoms = _wrap_atoms(
                [
                    atom
                    for parent in parent_builds
                    if attrs[parent.group_id].derivation == Derivation.ROOT
                    for atom in attrs[parent.group_id].condition_atoms
                    if non_null_proofs(atom)
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
                    and edge_kind(group_edges, build.group_id, gid)
                    == EdgeKind.CONSTRAINT
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
                    )
                outputs, named_arguments = _name_inline_arguments(
                    outputs, primary_addrs, domain_spans, environment
                )
                parents = (
                    _project_basic_aggregate_inputs(
                        outputs,
                        primary_addrs,
                        solid,
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
            )
        # ROOT scans source columns from datasources directly, not from their
        # group-graph predecessors. A `constraint`-edge predecessor (e.g. a
        # d1 aggregate feeding a HAVING-style filter on this root) is real
        # row-flow at SQL time (INNER JOIN to apply the filter) but doesn't
        # supply the root's primary scan columns. Pruning by parent outputs
        # there would strip every requested column and the root would never
        # build. A ROWSET boundary likewise sources from its own
        # recursively-planned inner select (`gen_rowset` consumes no parents,
        # permanently: a body is a statement planned in its own scope);
        # pruning it by a constraint-edge sibling would drop any handle the
        # sibling happens not to pseudonym-cover.
        if derivation not in (
            Derivation.ROOT,
            Derivation.UNNEST,
            Derivation.ROWSET,
        ):
            wanted = outputs
            outputs = satisfiable_outputs(outputs, parents)
            if not outputs:
                unbuilt[gid] = wanted
                continue
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
            and derivation in (*_AGGREGATING_DERIVATIONS, Derivation.WINDOW)
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
            and not _aggregate_inputs_are_row_preserving(
                outputs, primary_addrs, parents
            )
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
                if (
                    concept_match is not None
                    and concept_match.address not in matched_addrs
                ):
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
        # The same root in two phases (the d1 twin feeding the condition-phase
        # aggregates) asks the same question when the WHERE placed on each is
        # the same; the second reads the first's answer.
        request = (
            RootRequest(
                frozenset(c.address for c in outputs),
                condition_for_generator,
                environment.span_scope,
                preexisting,
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
                    group_graph, attrs, gid, outputs, mandatory_list, environment
                ),
                existence_source=any(
                    edge_kind(group_edges, gid, succ) == EdgeKind.EXISTENCE
                    for succ in group_graph.successors(gid)
                ),
                collapse_to_grain=all(
                    attrs[succ].derivation != Derivation.AGGREGATE
                    for succ in group_graph.successors(gid)
                ),
                complete_partials=complete_partials,
                history=history,
                g=g,
                staged_conditions=staged_conditions,
                depth=depth,
            )
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
            unbuilt[gid] = outputs
            continue
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
                node = _with_null_members(node, a.null_member_spans, environment)
            # the region contract: this node's rows are the region's own
            node.region_spans = a.extent_spans
        if derivation == Derivation.ROOT:
            _drop_unadvertised_rowset_handles(node, set(select_addrs))
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
        built[gid] = node

    _raise_if_unbuilt_group_owed(group_graph, attrs, built, unbuilt)
    # The FINAL assembly is where the owner and the extent-free branches meet;
    # it must see every span again to host the owner's rows, except the ones
    # the plan above holds.
    environment.span_scope = _final_span_scope(environment.span_scope)
    if not built:
        return None
    plan_trace.set_context("FINAL")
    final = _assemble_final_node(
        group_graph,
        attrs,
        built,
        mandatory_list,
        environment,
        g,
        history,
        root_requests,
        feeder_cache=feeder_cache,
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
        _wire_existence(final, built, feeder_cache)
    return final


def _raise_if_output_unrendered(
    final: StrategyNode, mandatory_list: list[BuildConcept]
) -> None:
    """A requested column the plan does not render is a wrong answer, not a
    narrower one: a group pruned of an output it could not source would
    otherwise return the statement without it."""
    missing = [
        concept.address
        for concept in mandatory_list
        if not any(_output_covers(o, concept) for o in final.output_concepts)
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
        readers = sorted(set(group_graph.successors(gid)) - {FINAL_NODE_ID})
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
