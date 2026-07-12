"""Stage 3/4 materialization for v4 discovery.

Stage 3 walks the group graph in topological order, hands each group's
already-built parents to its v4 generator, and stashes the resulting node.
ROOT groups delegate concrete datasource selection to `source_planning`.

Stage 4 assembles the FINAL sink by merging the minimum materialized
contributors that cover the mandatory outputs, then applies final-only
filters and output-grain deduping.

No source-concepts callback: parents are explicit, derived from the group
graph's lineage edges. Generators that haven't been ported to the v4 flat
style fall back inside `v4_node_generators.dispatch.build_node`."""

from collections import defaultdict
from dataclasses import dataclass
from typing import cast

from trilogy.constants import logger
from trilogy.core import graph as nx
from trilogy.core.enums import AggregateGroupingMode, Derivation
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import (
    BoolExpr,
    BuildAggregateWrapper,
    BuildConcept,
    BuildConceptArgs,
    BuildDatasource,
    BuildFilterItem,
    BuildGrain,
    BuildWhereClause,
    LooseBuildConceptList,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.aggregate_rollup import _is_additive_aggregate
from trilogy.core.processing.condition_utility import combine_condition_atoms
from trilogy.core.processing.node_generators.presence_probe import is_presence_probe
from trilogy.core.processing.nodes import (
    FilterNode,
    GroupNode,
    History,
    MergeNode,
    SelectNode,
    StrategyNode,
    WindowNode,
)
from trilogy.core.processing.v4_node_generators import build_node

from .condition_injection import (
    ConditionSources,
    condition_row_args,
    inject_condition_at_node,
)
from .constants import (
    FINAL_NODE_ID,
    GROUPING_DERIVATIONS,
    ROW_SHAPE_BARRIER_DERIVATIONS,
    DepthLabel,
    EdgeKind,
)
from .edges import EdgeMap, dependency_subgraph, edge_kind
from .functional_dependency import build_fd_determines
from .models import (
    FinalAssemblyContract,
    FinalContributorContract,
    GroupAttrs,
    InputChannel,
)
from .projection import (
    concept_satisfiable,
    parent_output_addresses,
    row_lineage_arguments,
    satisfiable_outputs,
    widen_projection,
)
from .source_planning import SourceRequest, plan_source
from .source_policy import STRICT_SOURCE_POLICY, SourcePolicy

_AGGREGATING_DERIVATIONS = {
    Derivation.AGGREGATE,
    Derivation.GROUP_TO,
}

_ROW_PRESERVING_AGGREGATE_INPUT_DERIVATIONS = {
    Derivation.ROOT,
    Derivation.BASIC,
    Derivation.FILTER,
}


@dataclass
class ParentBuild:
    group_id: str
    node: StrategyNode


def _concept_at(environment: BuildEnvironment, address: str) -> BuildConcept | None:
    """Resolve a (possibly pseudonym) group-member address to its concept.

    A derivable pseudonym address (e.g. the struct field `unnest_array.a`)
    resolves through `environment.concepts` to its *canonical* concept (`local.a`,
    lineage None), not the attr-access origin that actually computes it. v3
    resolves these synonyms through `alias_origin_lookup`; mirror that so the
    strategy builder builds the field's projection instead of a dead-end key.
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
    a = attrs[gid]
    return set(a.primary_members) | set(a.secondary_members)


def _atoms_at(attrs: dict[str, GroupAttrs], gid: str) -> list[BoolExpr]:
    """Atoms injected AT `gid` only. These become the WHERE for this node."""
    return list(attrs[gid].condition_atoms)


def _group_existence_concepts(
    attrs: dict[str, GroupAttrs],
    environment: BuildEnvironment,
    gid: str,
) -> list[BuildConcept]:
    """The SubselectComparison RHS concepts this group filters against —
    sourced from both the WHERE atoms injected here AND the intrinsic where of
    any FILTER concept the group computes (q08's `zips in substring(...)` lives
    in `final_zips`'s lineage, not an injected atom)."""
    out: list[BuildConcept] = []
    seen: set[str] = set()

    def _add(concepts: tuple[BuildConcept, ...]) -> None:
        for concept in concepts:
            if concept.address not in seen:
                seen.add(concept.address)
                out.append(concept)

    for atom in _atoms_at(attrs, gid):
        for arg_group in atom.existence_arguments:
            _add(arg_group)
    # Walk each primary member's lineage: a FILTER with a semijoin where
    # (q08 `_virt_filter_zips`) is often inlined into the BASIC concept that
    # wraps it (`final_zips = substring(filter, 1, 2)`) rather than built as
    # its own node, so the existence arg lives a few lineage hops down.
    visited: set[str] = set()
    stack = [environment.concepts.get(a) for a in attrs[gid].primary_members]
    while stack:
        concept = stack.pop()
        if concept is None or concept.address in visited:
            continue
        visited.add(concept.address)
        if isinstance(concept.lineage, BuildFilterItem):
            for arg_group in concept.lineage.where.existence_arguments or ():
                _add(tuple(arg_group))
        # A BASIC concept whose lineage is (or wraps) a membership comparison
        # (`x in <set>`, e.g. a projected `--x in set as flag`) carries the set
        # as a direct existence arg; without this its subselect renders against a
        # dangling CTE (INVALID_REFERENCE_BUG). Mirrors v3 gen_basic_node.
        elif isinstance(concept.lineage, BuildConceptArgs):
            for arg_group in concept.lineage.existence_arguments or ():
                _add(tuple(arg_group))
        if concept.lineage is not None:
            stack.extend(concept.lineage.concept_arguments)
    return out


def _existence_for_group(
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    environment: BuildEnvironment,
    gid: str,
) -> tuple[list[BuildConcept], list[StrategyNode]]:
    """Gather the group's existence_arguments (right-side concepts of a
    SubselectComparison) and the built groups that supply them. These become
    the host node's `existence_concepts` plus extra parents — the SQL renderer
    emits the right side as a subselect lookup against the parent CTE rather
    than joining it into the row stream."""
    existence_concepts: list[BuildConcept] = []
    existence_parents: list[StrategyNode] = []
    seen_parents: set[int] = set()
    for concept in _group_existence_concepts(attrs, environment, gid):
        existence_concepts.append(concept)
        # Find the built group that supplies this concept.
        for source_gid, source_node in built.items():
            if source_gid == gid:
                continue
            if any(o.address == concept.address for o in source_node.output_concepts):
                if id(source_node) not in seen_parents:
                    seen_parents.add(id(source_node))
                    existence_parents.append(source_node.copy())
                break
    return existence_concepts, existence_parents


def _deep_copy_node(node: StrategyNode) -> StrategyNode:
    """`copy()` shallow-shares `parents`; this recursively copies the whole
    subtree so the result shares no node object with the original tree."""
    clone = node.copy()
    clone.parents = [_deep_copy_node(p) for p in node.parents]
    return clone


class _CleanFeederCache:
    """Builds a STANDALONE source for an existence (`IN <subselect>`) concept,
    independent of the already-built strategy tree.

    When the only built group producing an existence concept is a lineage
    descendant of its own consumer (a self-referential membership like
    `week_seq in relevent_week_seq`, whose `relevent_week_seq` filter group reads
    the membership-conditioned ROOT), wiring that built node as the subselect
    feeder forms a cycle. `_existence_parents_for` would otherwise `_deep_copy_
    node` the whole conditioned subtree to break it -- acyclic but catastrophically
    verbose (q2.1: the deep copy fires per consumer and compounds into a 60k-char
    re-filter chain). The set Y in `X in Y` is by definition the UNFILTERED set, so
    re-source it from its own lineage (no outer conditions) once and share the
    result. Cached per address; returns independent copies so each consumer owns
    its parent pointer."""

    def __init__(
        self,
        environment: BuildEnvironment,
        g: ReferenceGraph,
        history: History,
        source_policy: SourcePolicy,
    ) -> None:
        self._environment = environment
        self._g = g
        self._history = history
        self._source_policy = source_policy
        self._cache: dict[str, StrategyNode | None] = {}

    def get(self, concept: BuildConcept) -> StrategyNode | None:
        if concept.address not in self._cache:
            self._cache[concept.address] = self._build(concept)
        node = self._cache[concept.address]
        return node.copy() if node is not None else None

    def _build(self, concept: BuildConcept) -> StrategyNode | None:
        from trilogy.core.processing.concept_strategies_v4 import (
            V4History,
            search_concepts,
        )

        v4_history = cast(V4History, self._history)
        search = [
            self._environment.concepts[address]
            for address in sorted({concept.address, *(concept.keys or set())})
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
            source_policy=self._source_policy,
        )
        return info.strategy_node


def _existence_parents_for(
    concepts: list[BuildConcept],
    built: dict[str, StrategyNode],
    skip: StrategyNode | None = None,
    feeder_cache: "_CleanFeederCache | None" = None,
) -> list[StrategyNode]:
    existence_parents: list[StrategyNode] = []
    seen_parents: set[int] = set()
    for concept in concepts:
        for source_node in built.values():
            if skip is not None and source_node is skip:
                continue
            if any(o.address == concept.address for o in source_node.output_concepts):
                # `copy()` shallow-shares parents, so a candidate whose subtree
                # contains `skip` would wire `skip -> candidate -> ... -> skip`, a
                # row-stream cycle that recurses forever in `resolve()`. The set in
                # `X in <candidate>` is the UNFILTERED set, so re-source it
                # standalone (no outer conditions) and share that acyclic feeder --
                # far cheaper than deep-copying the whole conditioned subtree per
                # consumer (q2.1: the deep copy compounds to 60k chars). Fall back
                # to the deep copy only when no standalone feeder can be built, so
                # the cycle is still broken (acyclic, just verbose).
                is_cyclic = skip is not None and any(
                    n is skip for n in _strategy_nodes(source_node)
                )
                if is_cyclic and feeder_cache is not None:
                    feeder = feeder_cache.get(concept)
                    if feeder is not None:
                        existence_parents.append(feeder)
                        break
                if id(source_node) not in seen_parents:
                    seen_parents.add(id(source_node))
                    if is_cyclic:
                        existence_parents.append(_deep_copy_node(source_node))
                    else:
                        existence_parents.append(source_node.copy())
                break
    return existence_parents


def _condition_existence_concepts(condition: BoolExpr | None) -> list[BuildConcept]:
    out: list[BuildConcept] = []
    seen: set[str] = set()
    if condition is None:
        return out
    for arg_group in condition.existence_arguments:
        for concept in arg_group:
            if concept.address not in seen:
                seen.add(concept.address)
                out.append(concept)
    return out


def _filter_lineage_existence_concepts(
    concepts: list[BuildConcept],
) -> list[BuildConcept]:
    out: list[BuildConcept] = []
    seen: set[str] = set()
    visited: set[str] = set()
    stack = list(concepts)
    while stack:
        concept = stack.pop()
        if concept.address in visited:
            continue
        visited.add(concept.address)
        if isinstance(concept.lineage, BuildFilterItem):
            for arg_group in concept.lineage.where.existence_arguments or ():
                for arg in arg_group:
                    if arg.address not in seen:
                        seen.add(arg.address)
                        out.append(arg)
        if concept.lineage is not None:
            stack.extend(concept.lineage.concept_arguments)
    return out


def _node_existence_concepts(node: StrategyNode) -> list[BuildConcept]:
    concepts: list[BuildConcept] = []
    seen: set[str] = set()
    for concept in _condition_existence_concepts(
        node.conditions
    ) + _filter_lineage_existence_concepts(list(node.output_concepts)):
        if concept.address not in seen:
            seen.add(concept.address)
            concepts.append(concept)
    return concepts


def _strategy_nodes(root: StrategyNode) -> list[StrategyNode]:
    seen: set[int] = set()
    nodes: list[StrategyNode] = []
    stack = [root]
    while stack:
        node = stack.pop()
        if id(node) in seen:
            continue
        seen.add(id(node))
        nodes.append(node)
        stack.extend(node.parents)
    return nodes


def _leaf_datasource_ids(node: StrategyNode) -> set[str]:
    """The concrete datasources scanned in this subtree -- its physical join
    footprint. Used to decide whether a per-consumer ROOT re-slice genuinely
    prunes a join or merely re-derives the same conditioned scan."""
    return {
        n.datasource.identifier
        for n in _strategy_nodes(node)
        if isinstance(n, SelectNode) and n.datasource is not None
    }


def _attach_existence_to_node(
    node: StrategyNode,
    concepts: list[BuildConcept],
    built: dict[str, StrategyNode],
    feeder_cache: "_CleanFeederCache | None" = None,
) -> None:
    if not concepts:
        return
    existing_concepts = {concept.address for concept in node.existence_concepts}
    node.existence_concepts = list(node.existence_concepts) + [
        concept for concept in concepts if concept.address not in existing_concepts
    ]
    existing_parent_outputs = {
        output.address for parent in node.parents for output in parent.output_concepts
    }
    node.parents = list(node.parents) + [
        parent
        for parent in _existence_parents_for(
            concepts, built, skip=node, feeder_cache=feeder_cache
        )
        if any(
            output.address not in existing_parent_outputs
            for output in parent.output_concepts
        )
    ]
    node.rebuild_cache()


def _attach_existence_sources(
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    condition_hosts: dict[str, StrategyNode],
    environment: BuildEnvironment,
    feeder_cache: "_CleanFeederCache | None" = None,
) -> None:
    for gid, host in condition_hosts.items():
        ex_concepts, ex_parents = _existence_for_group(attrs, built, environment, gid)
        if not ex_concepts:
            continue
        _attach_existence_to_node(host, ex_concepts, built, feeder_cache)
        if ex_parents:
            host.rebuild_cache()
    for root in built.values():
        for node in _strategy_nodes(root):
            _attach_existence_to_node(
                node, _node_existence_concepts(node), built, feeder_cache
            )


def _accumulated_atoms_above(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    gid: str,
) -> list[BoolExpr]:
    """Atoms applied at any STRICT ancestor of `gid`. Threaded into the node
    as `preexisting_conditions` so nullable inference (and any later
    optimizer) knows which rows the parent already filtered, without
    re-emitting the same WHERE on this CTE."""
    accumulated: list[BoolExpr] = []
    for anc in nx.ancestors(group_graph, gid):
        if anc == FINAL_NODE_ID:
            continue
        for atom in attrs[anc].condition_atoms:
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
    grouping sibling if the sibling's rows are filtered at least as much — else
    dropping the feeder silently widens the row set. q81's feeder carries only the
    metric's pre-filter (``return_address.state is not null``), which the d1 twin
    also applies, so it drops. q30.alt's feeder carries the POST-aggregate
    ``billing_customer.address.state = 'GA'`` (on its parent ROOT scan) that the
    twin does NOT apply — keep it (dropping selects non-GA customers)."""
    feeder = _atoms_at(attrs, feeder_gid) + _accumulated_atoms_above(
        group_graph, attrs, feeder_gid
    )
    if not feeder:
        return True
    sibling = _atoms_at(attrs, sibling_gid) + _accumulated_atoms_above(
        group_graph, attrs, sibling_gid
    )
    return all(atom in sibling for atom in feeder)


def _parent_nodes_for(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    gid: str,
    environment: BuildEnvironment,
    graph: ReferenceGraph,
    history: History,
    source_policy: SourcePolicy,
    *,
    needed: set[str],
) -> list[ParentBuild]:
    """Look up the already-built StrategyNodes for `gid`'s lineage
    predecessors. Topological order guarantees they exist (or that the
    generator was skipped, in which case we just skip that parent).

    Drop an ancestor predecessor X when some descendant predecessor Y
    already provides everything X would contribute to `needed` (the set
    of concepts this group actually consumes). Pass-through nodes like
    filter re-expose their parent's columns, so root often ends up
    redundant once filter is in the candidate set. Including both creates
    multi-parent ambiguity for non-merge generators — no JOIN gets
    emitted and the SQL renderer references the dropped parent by name,
    yielding a binder error."""
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
        # Existence-kind edges feed a subselect, not the row stream —
        # `_existence_for_group` wires them as side-channel parents post-
        # build. Including them here would put them in JOIN dedup and
        # mistakenly merge their row stream into this group's FROM.
        if edge_kind(group_edges, pgid, gid) == EdgeKind.EXISTENCE:
            continue
        node = built.get(pgid)
        if node is not None:
            candidates.append((pgid, node))

    if attrs[gid].derivation == Derivation.AGGREGATE:
        inline_input_addresses = _aggregate_row_preserving_input_addresses(
            [
                concept
                for addr in attrs[gid].primary_members
                if (concept := _concept_at(environment, addr)) is not None
            ]
        )
        while True:
            expanded: list[tuple[str, StrategyNode]] = []
            changed = False
            for pgid, node in candidates:
                # Don't fold a row-preserving group whose output is ALSO produced
                # by another already-built node (a condition-phase twin
                # materialized as its own CTE). Folding it here re-roots the
                # aggregate on the grandparent, but the resolver then binds the
                # folded column to that sibling CTE -- which isn't in the
                # aggregate's FROM -> dangling reference (test_or_membership: the
                # `_virt_filter` sum input is co-produced by the `in cat_qual /
                # web_qual` membership feeder CTE).
                pgid_outputs = set(attrs[pgid].primary_members)
                co_materialized = any(
                    other != pgid
                    and attrs[other].depth_label == DepthLabel.D1
                    and pgid_outputs
                    & {concept.address for concept in built_other.output_concepts}
                    for other, built_other in built.items()
                )
                row_preserving_input = (
                    attrs[pgid].derivation
                    in _ROW_PRESERVING_AGGREGATE_INPUT_DERIVATIONS
                    and attrs[pgid].derivation != Derivation.ROOT
                    and not co_materialized
                    and set(attrs[pgid].primary_members).issubset(
                        inline_input_addresses
                    )
                    and node.conditions is None
                    and not node.force_group
                    and not node.existence_concepts
                    and not _contains_shape_barrier(node)
                    and not _group_filter_has_existence(attrs, environment, pgid)
                )
                if not row_preserving_input:
                    expanded.append((pgid, node))
                    continue
                input_parents = [
                    (fgid, built[fgid])
                    for fgid in group_graph.predecessors(pgid)
                    if fgid != FINAL_NODE_ID
                    and edge_kind(group_edges, fgid, pgid) != EdgeKind.EXISTENCE
                    and fgid in built
                ]
                available = {
                    output.address
                    for _, input_parent in input_parents
                    for output in input_parent.output_concepts
                }
                if input_parents and _group_renderable_from(
                    attrs, environment, pgid, available
                ):
                    expanded.extend(input_parents)
                    changed = True
                else:
                    expanded.append((pgid, node))
            deduped = list(dict(expanded).items())
            if not changed or deduped == candidates:
                candidates = deduped
                break
            candidates = deduped

    def provides(pgid: str, node: StrategyNode) -> set[str]:
        if isinstance(node, FilterNode) and node.conditions is not None:
            return set(attrs[pgid].primary_members) & needed
        return {o.address for o in node.output_concepts} & needed

    def parent_for_consumer(pgid: str, node: StrategyNode) -> StrategyNode:
        if attrs[pgid].derivation != Derivation.ROOT:
            return node.copy()
        if attrs[gid].derivation not in GROUPING_DERIVATIONS:
            return node.copy()
        parent_outputs = {concept.address for concept in node.output_concepts}
        slice_addresses = needed & parent_outputs
        if not slice_addresses or slice_addresses == parent_outputs:
            return node.copy()
        outputs = [
            c
            for address in sorted(slice_addresses)
            if (c := _concept_at(environment, address)) is not None
        ]
        sliced = build_node(
            derivation=Derivation.ROOT,
            outputs=outputs,
            parents=[],
            environment=environment,
            conditions=_wrap_atoms(attrs[pgid].condition_atoms),
            history=history,
            g=graph,
            source_policy=source_policy,
        )
        if sliced is None:
            sliced = plan_source(
                SourceRequest(
                    outputs=outputs,
                    environment=environment,
                    graph=graph,
                    history=history,
                    conditions=_wrap_atoms(attrs[pgid].condition_atoms),
                    source_policy=source_policy,
                )
            )
        if sliced is None:
            return node.copy()
        # Adopt the narrower rebuild only when it strictly prunes the source set
        # (drops a join the slice no longer spans). Otherwise re-deriving the
        # same conditioned join just to carry fewer columns is pure CTE
        # duplication -- share the already-built ROOT and let column projection
        # narrow it (q94: a count(order_number) consumer of a filtered
        # web_sales-dim join must not re-source the whole join).
        if not (_leaf_datasource_ids(sliced) < _leaf_datasource_ids(node)):
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
            # grouping keys the sibling holds (q81's `sparkling` virt-filter
            # passthrough). A feeder that ALSO feeds raw recompute inputs has
            # columns the aggregated sibling lacks, so its `provides` is not a
            # subset and it survives. Drop only when the sibling applies every
            # row-reducing condition the feeder's subtree does — else the feeder
            # narrows rows the sibling does not (q30.alt's post-aggregate GA
            # filter) and `provides` (columns only) wouldn't catch it. Otherwise
            # require the covering sibling be a lineage descendant.
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
            if my_provides <= provides(other_pgid, other_node):
                covered_by_descendant = True
                break
        if not covered_by_descendant:
            parents.append(ParentBuild(pgid, parent_for_consumer(pgid, node)))
    return parents


def _drop_constant_only_parents(parents: list[StrategyNode]) -> list[StrategyNode]:
    """Drop a parent that supplies only constants (e.g. the `by
    __preql_internal.all_rows` grand-total marker, a `SELECT 1`). A constant is a
    literal, never a join key — merging it as a row parent only cross-joins it ON
    1=1, and the grand-total marker isn't even a needed output. Keep it only when
    it is the sole parent (a bare constant select). Mirrors v3, which drops
    ALL_ROWS_CONCEPT from the concepts it sources (group_node
    `_resolve_parent_sources`)."""
    non_constant = [
        p
        for p in parents
        if not (
            p.output_concepts
            and all(c.derivation == Derivation.CONSTANT for c in p.output_concepts)
        )
    ]
    return non_constant if non_constant else parents


def _fold_constant_parents(
    parents: list[StrategyNode], needed: set[str]
) -> list[StrategyNode]:
    """Fold a constant-only parent into a non-constant sibling instead of
    cross-joining it ON 1=1. A constant is a literal rendered inline, valid in
    any projection (aggregate/window/select), so append its needed constants to
    a sibling's outputs and drop the constant scan -- mirroring v3, which renders
    a `'abc' as label` straight in the consuming SELECT rather than as its own
    CTE. Constants not in `needed` (the `by all_rows` grand-total marker) are
    just dropped."""
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


def _fold_passthrough_parents(parents: list[StrategyNode]) -> list[StrategyNode]:
    """Absorb a parent into a row-preserving sibling that can render it.

    When a plain projection B (a non-grouping SelectNode) can render every one
    of another parent A's outputs from B's own source — each output is either
    directly available off B's parents or derivable from columns that are — B
    takes A's columns and A is dropped, instead of cross-joining two views of
    the same scan (q50: a `days_to_return` projection of the base merged back
    with the base on `1=1`; q62: two projections of one scan, one computing
    `days_to_ship`, the other `substring(warehouse.name)`, cross-joined). This
    also collapses the q77 catalog/store/web cross-joins (a grouping-key rename
    or a coalesce of one aggregate the sibling already sources).

    A real row-shape barrier A is NEVER dissolved: its rows are an aggregate,
    window, or row-reducing semijoin, not a row-wise re-derivable projection. A
    finer-grain sibling can spuriously look able to "render" a GLOBAL aggregate's
    output by recomputing the aggregate's inner expression (q22:
    `avg(acctbal ? ...)` → the bare CASE, silently dropping `avg()`). Only a
    row-PRESERVING contributor is foldable: a SelectNode, a plain (non-grouping)
    MergeNode such as a multi-table root scan (q09: the root scan folds into the
    `o_year` projection instead of self-joining on `order_id` and fanning the
    per-order sum out by lineitem count), or a virt-filter FilterNode — a
    CASE-WHEN projection with no row-reducing WHERE/semijoin (q62:
    `sum(filter row_counter where days_to_ship <= 30)`, whose `w_substr`
    dimension projection otherwise joins back on `warehouse_id` alone and fans
    out the bucket sums).

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
        for a in parents:
            if a is b or id(a) in dropped or not a.output_concepts:
                continue
            # Never dissolve a row-shape barrier into a row sibling. Foldable:
            # SelectNode, non-grouping MergeNode, or a row-preserving FilterNode
            # (a CASE-WHEN virt-filter — no row-reducing WHERE or semijoin).
            row_preserving_filter = (
                isinstance(a, FilterNode)
                and a.conditions is None
                and not a.existence_concepts
            )
            if a.force_group or not (
                isinstance(a, (SelectNode, MergeNode)) or row_preserving_filter
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
    collapsed.resolution_cache = None
    return collapsed


def _elide_passthrough_tree(
    node: StrategyNode, seen: dict[int, StrategyNode] | None = None
) -> StrategyNode:
    seen = seen or {}
    node_id = id(node)
    if node_id in seen:
        return seen[node_id]
    node.parents = [_elide_passthrough_tree(parent, seen) for parent in node.parents]
    node.resolution_cache = None
    collapsed = _elide_single_parent_passthrough(node)
    seen[node_id] = collapsed
    return collapsed


def _row_lineage_closure(concept: BuildConcept) -> list[BuildConcept]:
    seen: set[str] = set()
    output: list[BuildConcept] = []
    stack = [concept]
    while stack:
        current = stack.pop()
        if current.address in seen:
            continue
        seen.add(current.address)
        output.append(current)
        stack.extend(row_lineage_arguments(current))
    return output


def _lineage_crosses_row_shape_barrier(
    concept: BuildConcept, seen: set[str] | None = None
) -> bool:
    seen = seen or set()
    if concept.address in seen:
        return False
    seen.add(concept.address)
    if concept.derivation in ROW_SHAPE_BARRIER_DERIVATIONS:
        return True
    return any(
        _lineage_crosses_row_shape_barrier(arg, seen)
        for arg in row_lineage_arguments(concept)
    )


def _aggregate_row_preserving_inputs(concept: BuildConcept) -> list[BuildConcept]:
    if not isinstance(concept.lineage, BuildAggregateWrapper):
        return []
    return [
        arg
        for arg in concept.lineage.function.arguments
        if isinstance(arg, BuildConcept)
        and arg.derivation in _ROW_PRESERVING_AGGREGATE_INPUT_DERIVATIONS
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
            # skip input-grain normalization. Recursing into an inline function
            # arg (`sum(a - coalesce(b, 0))`) to collect its leaves wrongly
            # skipped normalization even when the parent rows weren't yet at the
            # aggregate's input grain -- miscounting (q97) and desyncing a renamed
            # ROLLUP key from its GROUP BY (q80: invalid SQL).
            if not isinstance(arg, BuildConcept):
                return False
            if (
                arg.derivation not in _ROW_PRESERVING_AGGREGATE_INPUT_DERIVATIONS
                or _lineage_crosses_row_shape_barrier(arg)
            ):
                return False
            row_preserving_inputs.append(arg)
    if not row_preserving_inputs:
        return False
    # All-ROOT inputs are raw scan columns: the parent emits rows at the
    # datasource's natural (line) grain, which is usually FINER than the
    # aggregate's input grain (e.g. `count(order_number)` reads item|order line
    # rows). Skipping normalization then aggregates the un-deduped rows and
    # over-counts (q16: 818 vs 233). Force normalization so the inputs are
    # regrouped to the aggregate's input grain first. A correctness floor until
    # a true parent-row-grain signal can prove the skip safe (which would keep
    # q23/q94 compact) -- see local_scripts notes / the v4 verbosity follow-up.
    if all(arg.derivation == Derivation.ROOT for arg in row_preserving_inputs):
        return False
    available = {
        output.address for parent in parents for output in parent.output_concepts
    }
    return all(concept_satisfiable(arg, available) for arg in row_preserving_inputs)


def _parents_already_at_input_grain(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    input_grain: frozenset[str],
    environment: BuildEnvironment,
) -> bool:
    """True when the parents already emit one row per aggregate-input-grain key,
    so the input-grain normalization GROUP is a no-op.

    This is the "true parent-row-grain signal" the all-ROOT normalization floor in
    `_aggregate_inputs_are_row_preserving` defers to: when the SELECT sources its
    rows from the dimension keyed by the input grain (q10's customer-rooted
    `count(purchasing_customer.id)` after the buyer-set filters are isolated as
    their own discovery), the rows are already unique at that grain, so deduping
    them is pure SQL bloat. Proven by resolving each parent's physical row grain
    and checking every component is functionally determined by the input-grain
    keys -- fixing the input grain fixes the row. A finer parent key that the input
    grain does NOT determine (q16's `item.id` under a per-order count) keeps the
    normalization, so a fact-line scan is still regrouped before aggregation.

    Never fires for non-standard grouping (ROLLUP/CUBE/GROUPING_SETS): those need
    the explicit normalization GROUP so a renamed grouping key stays in sync with
    its GROUP BY clause (q80 invalid-SQL hazard)."""
    if not input_grain:
        return False
    if any(
        isinstance(c.lineage, BuildAggregateWrapper)
        and c.lineage.grouping != AggregateGroupingMode.STANDARD
        for c in outputs
    ):
        return False
    keys = set(input_grain)
    for parent in parents:
        for component in parent.resolve().grain.components:
            if component in keys:
                continue
            if not build_fd_determines(
                environment, keys, component, include_empty_grain=False
            ):
                return False
    return True


def _widen_merge_join_keys(
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    join_key_addresses: frozenset[str],
) -> None:
    if len(parents) <= 1 or not join_key_addresses:
        return

    sibling_outputs: dict[str, BuildConcept] = {}
    for parent in parents:
        for concept in parent.output_concepts:
            sibling_outputs.setdefault(concept.address, concept)
    join_key_concepts: list[BuildConcept] = []
    for address in sorted(join_key_addresses):
        join_key = sibling_outputs.get(address) or _concept_at(environment, address)
        if join_key is not None:
            join_key_concepts.append(join_key)

    for parent in parents:
        if parent.force_group or not isinstance(parent, (SelectNode, MergeNode)):
            continue
        available = parent_output_addresses(parent)
        # A leaf datasource SelectNode has no parent nodes, so
        # `parent_output_addresses` is empty -- but it can still emit any column
        # its datasource binds. Include those so a partial merge key (a fact's
        # `?d1` column that canonicalizes to the declared join key) is carried as
        # the join key instead of the merge cross-joining the sibling that owns
        # the key's complete domain (a date-spine LEFT_OUTER merge: facts.d1->s1
        # vs the spine's complete s1 -> `FULL JOIN ... on 1=1` cartesian).
        ds = getattr(parent, "datasource", None)
        if isinstance(ds, BuildDatasource):
            available |= {c.address for c in ds.output_concepts}
        if not available:
            continue
        parent_outputs = {concept.address for concept in parent.output_concepts}
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
            if not concept_satisfiable(concept, available):
                continue
            carried.append(concept)
            input_candidates.extend(_row_lineage_closure(concept))
        if carried:
            widen_projection(
                parent,
                carried,
                input_candidates=input_candidates,
                available_addresses=available,
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
                parent.rebuild_cache()


def _filter_intrinsic_pushdown_safe(group_graph: nx.DiGraph, gid: str) -> bool:
    ancestors = nx.ancestors(group_graph, gid)
    if not ancestors:
        return True
    for succ in group_graph.successors(gid):
        if succ == FINAL_NODE_ID:
            continue
        if ancestors & set(group_graph.predecessors(succ)):
            return False
    return True


def _pre_merge_parents(
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    join_key_addresses: frozenset[str] = frozenset(),
) -> list[StrategyNode]:
    """Collapse a multi-parent set into a single MergeNode that auto-joins
    on shared output concepts. Non-merging generators (GroupNode for
    aggregate/group_to, WindowNode, FilterNode) emit `joins=[]` and let the
    renderer pick one parent as base, so multi-parent without a merge
    yields `Referenced table "X" not found` binder errors when the SELECT
    references the dropped parent. Wrapping here keeps the generators
    simple and the join logic in one place."""
    if len(parents) <= 1:
        return parents
    parents = _drop_constant_only_parents(parents)
    if len(parents) <= 1:
        return parents
    parents = _fold_passthrough_parents(parents)
    if len(parents) <= 1:
        return parents
    _widen_merge_join_keys(parents, environment, join_key_addresses)
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
    )
    return [merged]


def _contains_shape_barrier(node: StrategyNode) -> bool:
    if isinstance(node, (GroupNode, WindowNode)):
        return True
    if node.force_group:
        return True
    return any(_contains_shape_barrier(parent) for parent in node.parents)


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
    # being fanned out by a row-grain detail merge (q81: dims join the aggregate
    # on store_id). When every parent is a row-grain scan feeding INTO this
    # aggregate, there is no such sibling — projecting one to the group grain
    # just strips its finer row-grain join key (order.id) and the merge degrades
    # to a 1=1 cross product (q10: revenue joins root on order.id, not
    # customer.id, then the group aggregates).
    if not any(_contains_shape_barrier(parent) for parent in parents):
        return parents

    outputs_by_parent = [parent_output_addresses(parent) for parent in parents]
    projected: list[StrategyNode] = []
    for idx, parent in enumerate(parents):
        if _contains_shape_barrier(parent):
            projected.append(parent)
            continue
        parent_needed = outputs_by_parent[idx] & needed
        other_outputs = set().union(
            *(outputs for j, outputs in enumerate(outputs_by_parent) if j != idx)
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
        # When the dimension's projected grain (fd_needed) shares NO key with the
        # barrier sibling, the post-projection merge has nothing to join on and
        # cross-joins ON 1=1 — the bridge between the two is a projection-grain key
        # the sibling outputs but fd_needed dropped (an aggregate's input-grain
        # key, `ride_date` linking `start_station.id` to the inner `daily_rides`
        # aggregate). Keep that bridge by grouping to the combined grain (mirrors
        # v3's per-(dim,key) dedup CTE). Guarded on disjointness so the normal case
        # — a dimension that already shares its keys with the aggregate — is left
        # to `_wrap_for_grain` untouched. The bridge may only be DERIVABLE here
        # (the dimension carries `ride_start_time`, from which `ride_date`
        # projects), so test satisfiability.
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
            # contain an FD attribute the parent drops -- e.g. a row key like
            # `date.id` that is FD-determined by the row grain but lives only as a
            # WHERE filter applied at the scan, never as a column here (q76).
            # Grouping on it would fail input validation; keep only real outputs.
            parent_outputs = {o.address for o in parent.usable_outputs}
            grain_concepts = [
                c
                for addr in sorted((fd_needed | join_keys) & parent_outputs)
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


def _satisfiable_outputs(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
) -> list[BuildConcept]:
    """Drop outputs that no parent can actually supply. The group graph's
    secondary-members pass attaches every root to every basic on the
    optimistic theory "I can reach them"; but when a basic's direct parents
    are aggregates/windows, the roots were collapsed away and aren't in any
    parent's output. Without this filter those concepts end up in
    `output_concepts` with no source map entry, producing
    `INVALID_REFERENCE_BUG_<...>` markers in the rendered SQL.

    An output is keepable when its lineage bottoms out at parent-available
    concepts or already-kept siblings — following the chain through
    intermediate derived concepts the SelectNode will inline (q28
    `filtered_lp <- bucket_id <- quantity`, q49 `channel <- channel_label <-
    sales_channel`). Run to a fixpoint so a kept sibling unlocks others
    regardless of iteration order."""
    return satisfiable_outputs(outputs, parents)


def _topological_order(group_graph: nx.DiGraph, group_edges: EdgeMap) -> list[str]:
    """Topological order across all dependency edge kinds (lineage /
    constraint / existence). Each kind expresses a different dataflow
    relationship downstream, but all of them require the source group to
    be built before the consumer — a constraint sibling has to be
    JOIN-ready, an existence source has to be subselect-ready. Returns
    an empty list on cycle so callers bail rather than build a partial
    plan."""
    dep_graph = dependency_subgraph(group_graph, group_edges)
    try:
        return list(nx.topological_sort(dep_graph))
    except nx.NetworkXUnfeasible:
        try:
            cycle = nx.find_cycle(dep_graph)
        except nx.NetworkXNoCycle:
            cycle = None
        logger.warning("[v4] group-graph cycle, abandoning strategy build: %s", cycle)
        return []


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
    written `wrapper.a`). Sibling aliases don't list each other as pseudonyms —
    each only knows the canonical origin — so the CTE-layer pseudonym match
    can't bridge the written `wrapper.a` to the output `unnest_array.a` and the
    column drops from the SELECT. `per_group` resolved each output to the
    canonical concept, which carries the full equivalence class; expose it as a
    *hidden* output (it has no lineage of its own, so it renders via its
    canonical sibling) — the user-facing alias resolves through it, while hiding
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
        change_visibility=False,
    )
    node.hidden_concepts = set(node.hidden_concepts or set()) | {
        m.address for m in bridges
    }
    node.rebuild_cache()


def _cover_groups_for_mandatory(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    mandatory_list: list[BuildConcept],
) -> dict[str, list[BuildConcept]]:
    """For each mandatory concept, pick the most-downstream built group that
    actually exposes it (more built ancestors = further downstream). Returns
    `{gid: [concepts that group provides]}` preserving discovery order so
    the MergeNode renders with a stable join layout."""
    per_group: dict[str, list[BuildConcept]] = defaultdict(list)
    for concept in mandatory_list:
        addr = concept.address
        candidates = [
            gid
            for gid, node in built.items()
            if any(o.address == addr for o in node.output_concepts)
        ]
        # Only fall back to pseudonym coverage (struct fields produced under
        # their derivable origin address) when nothing provides the concept
        # directly — a plain alias keeps its own contributor.
        if not candidates:
            candidates = [
                gid
                for gid, node in built.items()
                if any(_output_covers(o, concept) for o in node.output_concepts)
            ]
        if not candidates:
            continue
        candidates.sort(
            key=lambda gid: (
                sum(1 for a in nx.ancestors(group_graph, gid) if a in built),
                addr in set(attrs[gid].primary_members)
                or addr in set(attrs[gid].secondary_members),
            ),
            reverse=True,
        )
        per_group[candidates[0]].append(concept)
    return per_group


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
    drop S as a separate contributor — otherwise the FINAL merge re-joins B to
    S on whatever column they happen to share, which for a rename of a
    grouping key is the value itself and fans out (q46 `bought_city`). Works
    for any basic, not just renames: B already resolved against S, we only
    widen its projection. Edits `per_group` in place.

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
        for s_gid in b_ancestors:
            if s_gid not in per_group or s_gid == b_gid:
                continue
            s_concepts = per_group[s_gid]
            if not all(c.address in available for c in s_concepts):
                continue
            widen_projection(
                b_node,
                s_concepts,
                input_candidates=s_concepts,
                available_addresses=available,
            )
            per_group[b_gid].extend(s_concepts)
            del per_group[s_gid]


def _promote_final_aliases_to_grouping_contributors(
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    per_group: dict[str, list[BuildConcept]],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
) -> None:
    def has_rollup_output(gid: str) -> bool:
        return any(
            concept.derivation == Derivation.AGGREGATE
            and isinstance(concept.lineage, BuildAggregateWrapper)
            and concept.lineage.grouping == AggregateGroupingMode.ROLLUP
            for concept in built[gid].output_concepts
        )

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
            and has_rollup_output(gid)
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
        # (grain == {itself}) may carry keys that are a COARSER parent grain it
        # was once a grouping key over (catalog_returns' billing_customer.id keyed
        # by {item.id, order_number}); expanding those drags the fact into an
        # otherwise pure dimension scan. Skip keys for such identifiers.
        if grain_components != {concept.address}:
            addresses.update(concept.keys or set())
    return [
        c
        for address in sorted(addresses)
        if (c := _concept_at(environment, address)) is not None
    ]


def _fresh_final_root_projection(
    concepts: list[BuildConcept],
    environment: BuildEnvironment,
    graph: ReferenceGraph,
    history: History,
    source_policy: SourcePolicy,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    projected = _projection_root_concepts(concepts, environment)
    if not projected:
        return None
    return plan_source(
        SourceRequest(
            outputs=projected,
            environment=environment,
            graph=graph,
            history=history,
            conditions=conditions,
            source_policy=source_policy,
        )
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


def _aggregate_reused_from_twin(
    address: str,
    gid: str,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
) -> bool:
    """Whether an aggregate value of `gid` is already produced by another built
    grouping group at the SAME grain — its pre-condition d1 twin. Such a value is
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


def _wrap_for_grain(
    parent_node: StrategyNode,
    needed_concepts: list[BuildConcept],
    environment: BuildEnvironment,
    merge_grain_components: frozenset[str] = frozenset(),
) -> list[StrategyNode]:
    """When a parent feeds a merge edge, its grain may be wider than the
    natural grain of the concepts the merge actually wants — joining the
    parent's wider-grain rows into a per-key aggregate blows up cardinality.

    For each natural-grain bucket among `needed_concepts`, emit a GroupNode
    that aggregates `parent_node` to that grain and exposes only those
    concepts plus the grain keys. Buckets whose grain already matches the
    parent's grain pass through unchanged.

    Originally root-only; generalized because intermediate aggregates can
    also sit at a wider grain than a specific concept of theirs requires
    (e.g. a `sum(...) by (a, b)` whose downstream merge only needs grain
    `{a}` for one column)."""
    if not needed_concepts:
        return [parent_node]

    # A needed concept that is NOT functionally determined by the merge grain is
    # a finer/orthogonal row key (e.g. `product_id` next to a `group(store_id) by
    # wh_id` whose merge grain is {store_id, wh_id}). Bucketing it to its own
    # natural grain shatters the scan into per-key GroupNodes that share no join
    # key, so the FINAL merge cross-joins them ON 1=1 (a forced-join disambiguator
    # selecting a group property alongside an unrelated key). Keep the parent
    # whole at its row grain; the FINAL dedup groups it down to the output grain.
    if merge_grain_components and any(
        not _fd_at_grain(concept, merge_grain_components) for concept in needed_concepts
    ):
        return [parent_node]

    parent_grain_components = (
        frozenset(parent_node.grain.components) if parent_node.grain else frozenset()
    )

    # Each concept's natural grain is the key it functionally depends on
    # (e.g. text_id is a property of customer.id, so its grain is
    # {customer.id}). `BuildGrain.from_concepts([c])` is the wrong helper
    # here — that asks "what grain do these concepts collectively require"
    # which can include the concept itself as a self-key.
    parent_outputs = {o.address for o in parent_node.usable_outputs}
    by_grain: dict[frozenset[str], list[BuildConcept]] = defaultdict(list)
    for concept in needed_concepts:
        grain_components = (
            frozenset(concept.grain.components) if concept.grain else frozenset()
        )
        # A dimension reached *through* the merge grain key (its `keys` are the
        # merge grain) is functionally determined by it and already lives in
        # this parent at that grain — e.g. `order.customer.id` (keys={order.id})
        # selected next to `sum(...) by order.id`. Deduping it to its own
        # key-grain ({order.customer.id}) drops `order.id`, so the FINAL merge
        # loses its join key and degrades to `ON 1=1` (fan-out). Project it at
        # the merge grain instead, keeping the join key. `from_concepts` folds
        # the FK key hierarchy, so reducing the concept grain together with the
        # merge grain collapses to just the merge grain iff it's determined by
        # it (transitively, e.g. nation.id -> customer.id).
        concept_keys = frozenset(concept.keys or set())
        if (
            merge_grain_components
            and grain_components.isdisjoint(merge_grain_components)
            and concept_keys
            and BuildGrain.from_concepts(
                grain_components | merge_grain_components, environment=environment
            ).components
            <= merge_grain_components
            and merge_grain_components <= parent_outputs
        ):
            grain_components = merge_grain_components
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


def _filter_arg_parents(
    group_graph: nx.DiGraph,
    built: dict[str, StrategyNode],
    missing_addrs: set[str],
) -> tuple[list[StrategyNode], list[BuildConcept]]:
    """Built groups (most-downstream) producing each `missing_addr` — a
    FINAL-deferred filter's row-arg that isn't a user output (q11's global
    `germany_total_value`, compared against a per-id aggregate). Returned as
    cross-join parents plus the concepts they supply, so the FINAL filter node
    can pull them in as hidden inputs."""
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
            key=lambda gid: sum(
                1 for a in nx.ancestors(group_graph, gid) if a in built
            ),
            reverse=True,
        )
        gid = candidates[0]
        concepts.append(
            next(o for o in built[gid].output_concepts if o.address == addr)
        )
        if gid not in seen:
            seen.add(gid)
            nodes.append(built[gid])
    return nodes, concepts


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
) -> frozenset[str]:
    if not preserve_keys:
        return frozenset()
    output_addresses = {concept.address for concept in output_concepts}
    relevant: set[str] = set()
    for key in preserve_keys:
        if key in output_addresses:
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
    return frozenset(relevant)


def _group_to_grain_if_required(
    node: StrategyNode,
    mandatory_list: list[BuildConcept],
    final_contract: FinalAssemblyContract,
    environment: BuildEnvironment,
) -> StrategyNode:
    """Dedup a non-grouping FINAL contributor to the requested output grain.

    A row-preserving contributor (a ROOT scan or plain projection) whose source
    grain is finer than the selected concepts' grain must be grouped down to
    that grain — otherwise duplicate rows at the coarser grain survive into the
    output (and inflate any downstream aggregate that reads them, e.g. q75's
    `deduped` rowset feeding two `sum`s). Mirrors v3's `group_if_required_v2`,
    which the v4 FINAL assembly otherwise skips for the single-contributor case.

    The group-required decision is made against the user-requested concepts
    only (matching v3's `group_if_required_v2`), but the GroupNode keeps any
    hidden grain keys the node exposed for sibling joins — grouping by them
    alongside the requested columns preserves those keys (and the join handle)
    without changing the dedup grain. Aggregates/windows already sit at their
    own grain and are left untouched; a MergeNode resolves its own grouping via
    `force_group`."""
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
        # exposes its join/filter columns (q82's date, warehouse, quantity,
        # manufacturer) as outputs, so force_group alone would GROUP BY the full
        # merge grain and dedup nothing. Mirrors v3's group_if_required_v2.
        node.force_group = True
        node.set_output_concepts(targets, change_visibility=False)
        node.rebuild_cache()
        return node
    # An additive aggregate exposed by a row-preserving scan being grouped to a
    # coarser grain is a precomputed value at the finer grain (a
    # materialized-root rollup from a finer summary table), so re-aggregate it
    # with SUM rather than dedup it. Exact-grain materialized aggregates never
    # reach here — their scan already matches the target grain, so no group is
    # required. Mirrors v3's `rollup_concepts` on `gen_select_merge_node`.
    rollup = [o for o in targets if o.is_aggregate and _is_additive_aggregate(o)]
    return GroupNode(
        output_concepts=targets,
        input_concepts=targets,
        environment=environment,
        parents=[node],
        partial_concepts=node.partial_concepts,
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


def _clear_groupmate_completed_partials(
    node: StrategyNode, environment: BuildEnvironment
) -> None:
    """Un-mark a scoped-join key the merge itself completes.

    A subset-side member (`subset join a.store = b.store`, a ⊆ b) is partial on
    its own contributor — it spans only the subset domain — but this merge pairs
    it with its complete group-mate via the authored equality, so the merged
    relation spans the anchor's domain and the key renders as the coalesced
    group axis (v3's completion-merge behavior). Leaving it partial trips the
    final no-complete-source guard for a value that is in fact complete here."""
    if not node.partial_concepts or not environment.scoped_join_key_groups:
        return
    complete_outputs: set[str] = set()
    for parent in node.parents:
        parent_partial = {c.address for c in parent.partial_concepts}
        complete_outputs |= {
            c.address for c in parent.output_concepts if c.address not in parent_partial
        }
    keep: list[BuildConcept] = []
    for concept in node.partial_concepts:
        # Rowset handles only — the boundary-marked subset members this pass
        # exists for. A RAW datasource-bound member left partial by its scoped
        # declaration must keep tripping the no-complete-source guard: that is
        # the deliberate author-facing error for `subset join s_brand = ...`
        # over the member's only binding (union-reproject clean-error cells).
        if concept.derivation != Derivation.ROWSET:
            keep.append(concept)
            continue
        mates: set[str] = set()
        for canonical, members in environment.scoped_join_key_groups.items():
            if concept.address in members or concept.address == canonical:
                mates |= (members | {canonical}) - {concept.address}
        if mates & complete_outputs:
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
    source_policy: SourcePolicy,
    feeder_cache: "_CleanFeederCache | None" = None,
) -> StrategyNode | None:
    """Build the FINAL output node: merge the minimum set of built groups
    that together cover `mandatory_list`. When a single group already covers
    every mandatory concept, return it as-is. Otherwise wrap the contributing
    groups in a MergeNode whose auto-join logic links them on shared output
    concepts — this is what the FINAL sink in the group graph was reserved
    for, instead of just picking one leaf and dropping the rest.

    For ROOT contributions specifically, project the root scan down to the
    needed concepts' natural grain via `_wrap_root_for_grain` so the merge
    join doesn't blow up cardinality (e.g. `text_id` at customer grain
    instead of one row per store_return)."""
    if not built:
        return None
    # A cross-arm post-merge filter (e.g. `cnt_00 <= cnt_99`) that no pre-final
    # group could host was deferred onto FINAL by `_inject_conditions`; apply it
    # as a WHERE over the assembled merge, where both columns coexist.
    final_conditions = _wrap_atoms(attrs[FINAL_NODE_ID].condition_atoms)
    final_contract = _required_final_contract(attrs)
    mandatory_addresses = {c.address for c in mandatory_list}
    # Row-args a FINAL-deferred filter needs that aren't user outputs (q11:
    # global `germany_total_value`). Their producing groups get cross-joined in
    # as hidden inputs below, else the WHERE dangles (INVALID_REFERENCE).
    filter_only_addrs: set[str] = set()
    for atom in attrs[FINAL_NODE_ID].condition_atoms:
        filter_only_addrs |= {a.address for a in atom.row_arguments}
    filter_only_addrs -= mandatory_addresses

    def _apply_final_conditions(node: StrategyNode) -> StrategyNode:
        if final_conditions is None:
            return node
        # Project only the user-requested columns. The merge below may expose
        # extra align inputs (item_sk_99/item_sk_00 folded into the align key)
        # that aren't mandatory and don't render at this layer.
        keep = [o for o in node.output_concepts if o.address in mandatory_addresses]
        avail = {o.address for o in node.output_concepts}
        arg_nodes, arg_concepts = _filter_arg_parents(
            group_graph, built, filter_only_addrs - avail
        )
        row_arg_addrs = {c.address for c in condition_row_args(final_conditions)}
        row_concepts = [
            concept
            for concept in node.output_concepts
            if concept.address in row_arg_addrs
        ]
        # A membership (`x in <set>`) deferred onto FINAL needs its subselect
        # feeder wired here -- `_attach_existence_sources` ran before assembly
        # and only saw the built groups, never this FINAL node, so without this
        # the IN-RHS concept renders against a dangling CTE (Missing source map
        # entry for `<set>`).
        ex_concepts = _condition_existence_concepts(final_conditions.conditional)
        ex_parents = (
            _existence_parents_for(
                ex_concepts, built, skip=node, feeder_cache=feeder_cache
            )
            if ex_concepts
            else []
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

    per_group = _cover_groups_for_mandatory(group_graph, attrs, built, mandatory_list)
    if not per_group:
        return _apply_final_conditions(
            _group_to_grain_if_required(
                next(iter(built.values())),
                mandatory_list,
                final_contract,
                environment,
            )
        )
    _fold_descendant_contributors(group_graph, attrs, built, per_group)
    _promote_final_aliases_to_grouping_contributors(
        attrs, built, per_group, mandatory_list, environment
    )
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
        # A FINAL-deferred presence-probe filter joins its feeder back on the
        # probe's key group. The normal path hides non-mandatory grain keys and
        # dedups to the output grain FIRST, which strips the join key and
        # degrades the feeder join to 1=1 — apply the condition over the raw
        # contributor (keys intact), then dedup the filtered rows.
        if final_probe_args:
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
                    partial_concepts=conditioned.partial_concepts,
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
        # hidden_concepts — only valid at the FINAL layer, since hiding
        # them at an intermediate group blocks downstream consumers from
        # using them as JOIN keys (MergeNode validates non-hidden parent
        # outputs only).
        # A basic riding a window-over-aggregate (q36 `i_category`/`i_class`
        # over a ROLLUP-then-rank) passes the aggregate's grain keys through as
        # row-identity / partition columns. Those aren't this basic's declared
        # grain, so add every grouping ancestor's grain to the hide candidates —
        # otherwise the carried keys (e.g. bare `ss.item.category`) leak into the
        # FINAL projection alongside their mandatory rename.
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
        conditioned = _apply_final_conditions(final_node)
        if conditioned is final_node:
            return conditioned
        # Applying a FINAL-deferred condition can wrap the contributor in a node
        # that reads it at a finer grain than the output — a membership
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
    # Wrapping intermediate aggregates is *not* safe — adding a GroupNode
    # over a `sum(x)` node would re-aggregate the partial sums (OK for SUM,
    # wrong for AVG/STDDEV), and intermediate aggregates often don't even
    # expose the requested grain key (their GROUP BY is their grain, not the
    # downstream's). Q17 surfaced both pathologies when this was generalized.
    # Merge grain is defined by the grouping (aggregate/window) contributors;
    # compute it up front so root-scan wrapping can project an FD dimension at
    # this grain (keeping the join key) instead of its own key-grain. A ROWSET
    # boundary is a fixed-grain barrier too -- its select grain is the key set a
    # merging dimension must join on (q46: the `bought` rowset's grain carries
    # `customer.id`, so the customer-address scan rides it instead of deduping to
    # its own `address.id` grain and cross-joining ON 1=1).
    contracts_by_gid = _final_contributor_contracts(final_contract, contributing)
    final_merge_grain = frozenset().union(
        *(contract.projection_grain for contract in contracts_by_gid.values())
    )

    parents: list[StrategyNode] = []
    for gid in contributing:
        node = built[gid]
        is_root = attrs[gid].derivation == Derivation.ROOT
        contributor_contract = contracts_by_gid[gid]
        preserve_keys = contributor_contract.preserve_keys & final_merge_grain
        group_concepts = list(per_group[gid])
        if is_root:
            preserve_keys = _relevant_root_preserve_keys(
                environment, group_concepts, preserve_keys
            )
        projection_grain = (
            final_merge_grain if is_root else contributor_contract.projection_grain
        )
        if is_root and preserve_keys:
            seen_group_concepts = {concept.address for concept in group_concepts}
            # Carry the merge grain's join KEYS onto the root scan, but never a
            # rowset's own handle outputs (`even_orders.order_id`) -- a root that
            # can derive those (it shares the rowset's base key) would absorb the
            # whole rowset and drop its internal filter (rowset_outer_addition).
            # Those handles aren't join keys; the rowset stays a separate merge
            # contributor.
            group_concepts.extend(
                c
                for address in sorted(preserve_keys)
                if (c := _concept_at(environment, address)) is not None
                and address not in seen_group_concepts
                and c.derivation != Derivation.ROWSET
            )
        if is_root:
            # A filter-only WHERE arg the SELECT never projects (q30.alt's
            # `billing_customer.address.state = 'GA'`, FD by this dim bucket's
            # key) is not in `group_concepts`, so `_root_atoms_satisfiable_from`
            # would drop its atom and the fresh re-source would lose the WHERE.
            # When such an arg was peeled INTO this bucket (a primary member),
            # add it to the projection so plan_source sources the dim table and
            # applies the filter (v3's `wakeful` = `customer ⋈ customer_address
            # WHERE state = 'GA'`). It isn't mandatory, so the FINAL merge
            # selects only the outputs and never leaks it. Restricted to bucket
            # members so a global-aggregate/cross-arm filter arg (handled as a
            # hidden cross-join input via `_filter_arg_parents`) is untouched.
            bucket_members = _members_of(attrs, gid)
            seen_group_addrs = {c.address for c in group_concepts}
            group_concepts.extend(
                c
                for address in sorted(
                    arg.address
                    for atom in _atoms_at(attrs, gid)
                    for arg in atom.row_arguments
                    if arg.address in bucket_members
                )
                if address not in seen_group_addrs
                and (c := _concept_at(environment, address)) is not None
            )
            root_conditions = _wrap_atoms(
                _root_atoms_satisfiable_from(_atoms_at(attrs, gid), group_concepts)
            )
            fresh = _fresh_final_root_projection(
                group_concepts,
                environment,
                graph,
                history,
                source_policy,
                # The fresh re-source must keep the root group's own WHERE
                # (e.g. `x = 1`); dropping it silently widened the scan and the
                # constant sibling's `1=1` merge returned the unfiltered rows.
                conditions=root_conditions,
            )
            if fresh is not None:
                node = fresh
            parents.extend(
                _wrap_for_grain(
                    node,
                    group_concepts,
                    environment,
                    projection_grain,
                )
            )
        else:
            parents.append(node)

    # Sibling contributors that descend from a common richer parent (e.g. q77
    # catalog: `u_id_c` renames the aggregate's grouping key while
    # `u_sales_c`/`u_profit_c` derive from that same aggregate) expose no shared
    # output key — their declared grain is the lying source-row grain — so the
    # merge would cross-join them ON 1=1. Fold any contributor whose outputs a
    # row-preserving sibling already renders off its own parents, collapsing the
    # columns into one projection instead of joining (same passthrough logic the
    # per-group `_pre_merge_parents` uses).
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

    available: set[str] = set()
    for p in parents:
        for o in p.output_concepts:
            available.add(o.address)
    outputs = [c for c in mandatory_list if c.address in available]
    # Pull in any filter-only condition arg (e.g. the global aggregate) not
    # already supplied by a contributor, as a hidden cross-join input.
    arg_nodes, arg_concepts = _filter_arg_parents(
        group_graph, built, filter_only_addrs - available
    )
    parents = parents + arg_nodes
    merge_inputs = outputs + [
        c for c in arg_concepts if c.address not in {o.address for o in outputs}
    ]
    hidden = {c.address for c in arg_concepts} - mandatory_addresses
    # A non-grouping dimension contributor only supplies FD attributes; if it
    # sits at a finer (row-level) grain it must not widen the merge grain, or it
    # fans the aggregate out (e.g. q81: customer-dims joined through
    # catalog_returns lands at returns grain). Pinning the grain lets the merge's
    # force_group collapse back to the aggregate grain. Left None when there is
    # no grouping contributor, so plain row merges keep their current behavior.
    merge_grain = (
        BuildGrain.from_concepts(final_merge_grain, environment=environment)
        if final_merge_grain
        else None
    )
    merged = MergeNode(
        input_concepts=merge_inputs,
        output_concepts=outputs,
        environment=environment,
        parents=parents,
        grain=merge_grain,
        conditions=final_conditions.conditional if final_conditions else None,
        hidden_concepts=hidden or None,
    )
    _clear_groupmate_completed_partials(merged, environment)
    # Dedup the assembled merge to the requested output grain (mirrors v3's
    # group_if_required_v2 and the single-contributor path above). A contributor
    # left whole at a finer row grain — e.g. a root scan kept at {store,wh,product}
    # to preserve a join key — otherwise leaks duplicate rows when its internal
    # keys (wh) drop out of the output grain (store_by_warehouse, product).
    # No-op when the merge already sits at the mandatory grain (the common
    # aggregate+dim case force_groups to merge_grain == output grain).
    return _group_to_grain_if_required(
        merged,
        mandatory_list,
        final_contract,
        environment,
    )


def build_strategy_node(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    g: ReferenceGraph,
    history: History,
    source_policy: SourcePolicy = STRICT_SOURCE_POLICY,
) -> StrategyNode | None:
    """Walk groups in topological order, dispatching each to its v4 generator
    with explicit parent nodes. Returns the most-downstream built node, or
    None if nothing built."""
    built: dict[str, StrategyNode] = {}
    condition_hosts: dict[str, StrategyNode] = {}

    for gid in _topological_order(group_graph, group_edges):
        if gid == FINAL_NODE_ID:
            continue
        a = attrs[gid]
        # Only the FINAL sink carries a None derivation, and it is skipped above.
        assert a.derivation is not None
        derivation = a.derivation
        # Prefer the per-group output/hidden sets computed by the backward
        # pass in `_compute_concept_sets`. The SELECT needs to project the
        # union (output ∪ hidden) so GROUP-BY keys stay in the row stream;
        # `hidden_concepts` then masks them from any downstream consumer
        # that doesn't ask for them. When the backward pass didn't run
        # (no mandatory_list supplied), `output_concepts` defaults to ()
        # — fall back to "every member" in that case.
        output_addrs: tuple[str, ...] = a.output_concepts
        hidden_addrs: tuple[str, ...] = a.hidden_concepts
        if not output_addrs and not hidden_addrs:
            output_addrs = (*a.primary_members, *a.secondary_members)
        select_addrs = (*output_addrs, *hidden_addrs)
        outputs = [
            c
            for addr in select_addrs
            if (c := _concept_at(environment, addr)) is not None
        ]
        if not outputs:
            continue
        injected = _wrap_atoms(_atoms_at(attrs, gid))
        preexisting = _wrap_atoms(_accumulated_atoms_above(group_graph, attrs, gid))
        # The "needed" set drives ancestor-dedup: a parent is kept only if
        # it contributes something to it that no descendant parent also
        # provides. Includes the output addresses themselves, the lineage
        # args of *primary* outputs (the columns this group actually
        # computes), and the inputs of any conditions applied at this
        # group. Passthroughs' lineage is intentionally NOT walked — a
        # passthrough output like `sum_sales` rides through this group
        # from an aggregate parent; if we walked its lineage we'd add
        # `sales_price` to `needed`, which ROOT provides but the aggregate
        # doesn't, and ROOT would escape dedup. The aggregate already
        # owns that lineage upstream.
        primary_addrs = set(a.primary_members)
        needed: set[str] = set()
        for c in outputs:
            needed.add(c.address)
            if c.address in primary_addrs and c.lineage is not None:
                if derivation in _AGGREGATING_DERIVATIONS:
                    # A post-condition aggregate that REUSES a same-grain twin
                    # (its pre-condition d1 sibling already materialized this
                    # value) reads the value through, not recompute — so its raw
                    # recompute inputs (the input-grain keys, measure columns)
                    # don't belong in `needed`. Pulling them keeps a redundant
                    # fact-rescan ROOT parent alive that only re-supplies grouping
                    # keys the twin already carries (q81 sparkling, q30.alt second
                    # web_returns). Treat it like a passthrough: skip its lineage.
                    if not _aggregate_reused_from_twin(c.address, gid, attrs, built):
                        _add_aggregate_needed_concepts(needed, c)
                else:
                    for arg in c.lineage.concept_arguments:
                        needed.add(arg.address)
        if injected is not None:
            for arg in condition_row_args(injected):
                _add_needed_concept(needed, arg)
        # Honor the group-planning contract's declared join keys: a bridge key
        # (e.g. `order` linking a window-derived dimension to the fact scan) is
        # not an aggregate input, so it isn't in `needed` and `_parent_nodes_for`
        # would slice it off the root scan -- leaving the merge with no shared
        # key (ON 1=1). Pull in only the EXTRA bridge keys -- those not already in
        # the group's grain/outputs -- so a grouping key (e.g. a `by rollup`
        # dimension) is never re-added to `needed` and forced into the SELECT
        # outside its GROUP BY (aligned-multi-select).
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
            source_policy,
            needed=needed,
        )
        parent_group_ids = {parent.group_id for parent in parent_builds}
        join_key_addresses = _input_contract_join_keys(a, parent_group_ids)
        parents = _apply_input_contracts(parent_builds, a, needed, environment)
        parents = _pre_merge_parents(
            parents,
            environment,
            join_key_addresses=join_key_addresses,
        )
        # ROOT scans source columns from datasources directly, not from their
        # group-graph predecessors. A `constraint`-edge predecessor (e.g. a
        # d1 aggregate feeding a HAVING-style filter on this root) is real
        # row-flow at SQL time (INNER JOIN to apply the filter) but doesn't
        # supply the root's primary scan columns. Pruning by parent outputs
        # there strips every requested column and the root never builds —
        # leaving the consumer with `INVALID_REFERENCE_BUG` against the
        # missing concepts (q11). A ROWSET boundary likewise sources from its
        # own recursively-planned inner select (`gen_rowset` ignores parents);
        # pruning it by a constraint-edge sibling silently dropped a handle the
        # sibling happened not to pseudonym-cover (subset anchor null test:
        # `a.amt` vanished from the SELECT).
        if derivation not in (
            Derivation.ROOT,
            Derivation.UNION,
            Derivation.UNNEST,
            Derivation.ROWSET,
        ):
            outputs = satisfiable_outputs(outputs, parents)
            if not outputs:
                continue
        # For aggregating derivations, peel `injected` off into a pre-filter
        # wrapper so the GroupNode itself sees no `conditions`. GroupNode's
        # non-scalar-condition path (group_node.py:199) reacts to a
        # condition that references an aggregate concept (like our
        # `cp > 1.2 * avg`) by appending the condition's row args to the
        # group's outputs — which then leak into the GROUP BY and shrink
        # every row to a unique (state, cp, avg) bucket. Wrapping in a
        # SelectNode does the WHERE first; the GroupNode then aggregates
        # the filtered rows with a clean GROUP BY at the intended grain.
        condition_for_generator = injected
        # Track whichever node ultimately owns the injected conditions. The
        # SubselectComparison (IN <subselect>) renderer reads existence
        # sources off the CTE that emits the WHERE — if we attach the
        # existence parent to a different node, the IN's right-hand side
        # has no source CTE and we emit INVALID_REFERENCE_BUG.
        condition_host_node: StrategyNode | None = None
        if injected is not None and derivation in _AGGREGATING_DERIVATIONS and parents:
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
            condition_host_node = wrapper
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
            for c in outputs:
                normalize_addrs.add(c.address)
                if c.address not in primary_addrs or c.lineage is None:
                    continue
                normalize_addrs.update(
                    arg.address for arg in c.lineage.concept_arguments
                )
            normalize_parent_output_by_addr: dict[str, BuildConcept] = {}
            for parent in parents:
                for output in parent.output_concepts:
                    normalize_parent_output_by_addr.setdefault(output.address, output)
            normalize_concepts: list[BuildConcept] = []
            for addr, concept in normalize_parent_output_by_addr.items():
                if addr in normalize_addrs:
                    normalize_concepts.append(concept)
            if normalize_concepts:
                parents = [
                    GroupNode(
                        output_concepts=normalize_concepts,
                        input_concepts=normalize_concepts,
                        environment=environment,
                        parents=parents,
                    )
                ]
        node = build_node(
            derivation=derivation,
            outputs=outputs,
            parents=parents,
            environment=environment,
            conditions=condition_for_generator,
            preexisting_conditions=preexisting,
            intrinsic_filter_pushdown=_filter_intrinsic_pushdown_safe(group_graph, gid),
            existence_source=any(
                edge_kind(group_edges, gid, succ) == EdgeKind.EXISTENCE
                for succ in group_graph.successors(gid)
            ),
            history=history,
            g=g,
            source_policy=source_policy,
        )
        logger.info(
            f"[v4] built {gid} derivation={derivation} "
            f"outputs={[o.address for o in outputs]} "
            f"parents={[type(p).__name__ for p in parents]} "
            f"-> {type(node).__name__ if node else None}"
        )
        if node is None:
            continue
        node = _elide_single_parent_passthrough(node)
        # Attach existence parents+concepts for any SubselectComparison
        # atoms at this group. Done post-build so the generators stay
        # ignorant of existence handling — the host node just learns it
        # has extra side-channel parents whose concepts render as
        # subselects rather than joins.
        #
        # The existence wiring must land on the node that actually emits
        # the WHERE referencing the IN-RHS concept. For aggregating
        # derivations we peeled the conditions off onto a SelectNode
        # wrapper (above); that wrapper is the condition host, not the
        # outer GroupNode whose `conditions=None`.
        condition_hosts[gid] = (
            condition_host_node if condition_host_node is not None else node
        )
        built[gid] = node

    if not built:
        return None
    feeder_cache = _CleanFeederCache(environment, g, history, source_policy)
    _attach_existence_sources(attrs, built, condition_hosts, environment, feeder_cache)
    final = _assemble_final_node(
        group_graph,
        attrs,
        built,
        mandatory_list,
        environment,
        g,
        history,
        source_policy,
        feeder_cache=feeder_cache,
    )
    if final is not None:
        final = _elide_passthrough_tree(final)
        if _has_unsourced_leaf(final):
            # A parent-less, datasource-less node that outputs a ROOT concept (a
            # base column that must come from a datasource) has no source for it —
            # the renderer would emit `INVALID_REFERENCE_BUG`. This is an
            # unresolvable query (e.g. a projection / aggregate over concepts from
            # two unconnected namespaces); fail so it raises
            # UnresolvableQueryException (matching v3) rather than invalid SQL.
            # Unnest-of-literal / constant leaves output only derived concepts and
            # are left alone.
            return None
        for node in _strategy_nodes(final):
            _attach_existence_to_node(
                node, _node_existence_concepts(node), built, feeder_cache
            )
    return final


def _has_unsourced_leaf(final: StrategyNode) -> bool:
    for node in _strategy_nodes(final):
        if node.parents or getattr(node, "datasource", None) is not None:
            continue
        if any(
            concept.derivation == Derivation.ROOT for concept in node.output_concepts
        ):
            return True
    return False
