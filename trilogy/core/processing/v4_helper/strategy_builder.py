"""Stage 3: walk the group graph in topological order, hand each group's
already-built parents to its v4 generator, and stash the resulting node.

No source-concepts callback: parents are explicit, derived from the group
graph's lineage edges. Generators that haven't been ported to the v4 flat
style fall back inside `v4_node_generators.dispatch.build_node`."""

from collections import defaultdict

import networkx as nx

from trilogy.constants import logger
from trilogy.core.enums import Derivation, Purpose
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildFilterItem,
    BuildGrain,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import combine_condition_atoms
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
    DEPENDENCY_EDGE_KINDS,
    EDGE_KIND_EXISTENCE,
    FINAL_NODE_ID,
    GROUPING_DERIVATIONS,
)
from .models import GroupAttrs
from .projection import (
    concept_satisfiable,
    parent_output_addresses,
    row_lineage_arguments,
    satisfiable_outputs,
    widen_projection,
)

_AGGREGATING_DERIVATIONS = {
    Derivation.AGGREGATE.value,
    Derivation.GROUP_TO.value,
}

_MERGE_JOIN_PURPOSES = {
    Purpose.KEY,
    Purpose.PROPERTY,
    Purpose.UNIQUE_PROPERTY,
}


def _wrap_atoms(atoms: list[BoolExpr]) -> BuildWhereClause | None:
    """AND-combine a list of condition atoms into a single BuildWhereClause."""
    if not atoms:
        return None
    combined = combine_condition_atoms(atoms)
    if combined is None:
        return None
    return BuildWhereClause(conditional=combined)


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


def _parent_nodes_for(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    gid: str,
    *,
    needed: set[str],
) -> list[StrategyNode]:
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
        if attrs[pgid].depth_label == "d1" and (
            (
                attrs[gid].derivation == Derivation.UNNEST.value
                and attrs[pgid].derivation
                in (
                    Derivation.BASIC.value,
                    Derivation.FILTER.value,
                    Derivation.WINDOW.value,
                )
            )
            or (
                attrs[gid].derivation == Derivation.WINDOW.value
                and attrs[pgid].derivation == Derivation.WINDOW.value
            )
        ):
            continue
        # Existence-kind edges feed a subselect, not the row stream —
        # `_existence_for_group` wires them as side-channel parents post-
        # build. Including them here would put them in JOIN dedup and
        # mistakenly merge their row stream into this group's FROM.
        if group_graph.edges[pgid, gid].get("kind") == EDGE_KIND_EXISTENCE:
            continue
        node = built.get(pgid)
        if node is not None:
            candidates.append((pgid, node))

    def provides(pgid: str, node: StrategyNode) -> set[str]:
        if isinstance(node, FilterNode) and node.conditions is not None:
            return set(attrs[pgid].primary_members) & needed
        return {o.address for o in node.output_concepts} & needed

    parents: list[StrategyNode] = []
    for pgid, node in candidates:
        my_provides = provides(pgid, node)
        covered_by_descendant = False
        for other_pgid, other_node in candidates:
            if other_pgid == pgid:
                continue
            if pgid not in nx.ancestors(group_graph, other_pgid):
                continue
            if my_provides <= provides(other_pgid, other_node):
                covered_by_descendant = True
                break
        if not covered_by_descendant:
            parents.append(node.copy())
    return parents


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


def _merge_join_key_candidate(concept: BuildConcept) -> bool:
    if concept.purpose == Purpose.KEY:
        return True
    if concept.purpose not in _MERGE_JOIN_PURPOSES:
        return False
    if concept.keys:
        return True
    if concept.grain and concept.grain.components:
        return True
    return False


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


def _widen_merge_join_keys(parents: list[StrategyNode]) -> None:
    if len(parents) <= 1:
        return

    sibling_outputs: dict[str, BuildConcept] = {}
    for parent in parents:
        for concept in parent.output_concepts:
            sibling_outputs.setdefault(concept.address, concept)

    for parent in parents:
        if parent.force_group or not isinstance(parent, (SelectNode, MergeNode)):
            continue
        available = parent_output_addresses(parent)
        if not available:
            continue
        parent_outputs = {concept.address for concept in parent.output_concepts}
        carried: list[BuildConcept] = []
        input_candidates: list[BuildConcept] = []
        for concept in sibling_outputs.values():
            if concept.address in parent_outputs:
                continue
            if not _merge_join_key_candidate(concept):
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
    parents = _fold_passthrough_parents(parents)
    if len(parents) <= 1:
        return parents
    _widen_merge_join_keys(parents)
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


def _project_dimension_parents_to_group_grain(
    parents: list[StrategyNode],
    needed: set[str],
    group_grain_components: frozenset[str],
    environment: BuildEnvironment,
) -> list[StrategyNode]:
    """Dedup dimension-side parents before they merge into a narrower group.

    A group can consume an aggregate parent plus a detail/root parent that only
    contributes FD attributes like store.name or warehouse.square_feet. If the
    detail parent is merged at row grain first, the aggregate fans out. Project
    that parent to the group's key grain before the merge; leave shape-changing
    parents and row-grain facts untouched.
    """
    if len(parents) <= 1 or not group_grain_components:
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
            if addr in environment.concepts
            and _fd_at_grain(environment.concepts[addr], group_grain_components)
        }
        fd_needed = {
            addr
            for addr in fd_candidates
            if not (addr in group_grain_components and addr in other_outputs)
        }
        non_fd_needed = parent_needed - fd_candidates
        concepts = [
            environment.concepts[addr]
            for addr in sorted(fd_needed)
            if addr in environment.concepts
        ]
        if not concepts or non_fd_needed:
            projected.append(parent)
            continue
        projected.extend(
            _wrap_for_grain(parent, concepts, environment, group_grain_components)
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


def _topological_order(group_graph: nx.DiGraph) -> list[str]:
    """Topological order across all dependency edge kinds (lineage /
    constraint / existence). Each kind expresses a different dataflow
    relationship downstream, but all of them require the source group to
    be built before the consumer — a constraint sibling has to be
    JOIN-ready, an existence source has to be subselect-ready. Returns
    an empty list on cycle so callers bail rather than build a partial
    plan."""
    dep_edges = [
        (u, v)
        for u, v, d in group_graph.edges(data=True)
        if d.get("kind") in DEPENDENCY_EDGE_KINDS
    ]
    dep_graph = group_graph.edge_subgraph(dep_edges).copy()
    for n in group_graph.nodes:
        dep_graph.add_node(n)
    try:
        return list(nx.topological_sort(dep_graph))
    except nx.NetworkXUnfeasible:
        try:
            cycle = nx.find_cycle(dep_graph)
        except nx.NetworkXNoCycle:
            cycle = None
        logger.warning("[v4] group-graph cycle, abandoning strategy build: %s", cycle)
        return []


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
        if not candidates:
            continue
        candidates.sort(
            key=lambda gid: (
                addr in set(attrs[gid].primary_members)
                or addr in set(attrs[gid].secondary_members),
                sum(1 for a in nx.ancestors(group_graph, gid) if a in built),
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
        if b_gid not in per_group or attrs[b_gid].derivation != Derivation.BASIC.value:
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

    parent_grain_components = (
        frozenset(parent_node.grain.components) if parent_node.grain else frozenset()
    )

    # Each concept's natural grain is the key it functionally depends on
    # (e.g. text_id is a property of customer.id, so its grain is
    # {customer.id}). `BuildGrain.from_concepts([c])` is the wrong helper
    # here — that asks "what grain do these concepts collectively require"
    # which can include the concept itself as a self-key.
    parent_outputs = {o.address for o in parent_node.output_concepts}
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
        # the merge grain instead, keeping the join key.
        concept_keys = frozenset(concept.keys or set())
        if (
            merge_grain_components
            and grain_components.isdisjoint(merge_grain_components)
            and concept_keys
            and concept_keys <= merge_grain_components
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
            environment.concepts[a] for a in grain_comps if a in environment.concepts
        ]
        # Dedup by address, keep concept order stable.
        outputs_by_addr: dict[str, BuildConcept] = {}
        for c in concepts + grain_concepts:
            outputs_by_addr.setdefault(c.address, c)
        outputs = list(outputs_by_addr.values())
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


def _assemble_final_node(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
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
        sources = ConditionSources(
            row_concepts=row_concepts + arg_concepts,
            row_parents=arg_nodes,
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
        return _apply_final_conditions(next(iter(built.values())))
    _fold_descendant_contributors(group_graph, attrs, built, per_group)
    contributing = list(per_group.keys())
    if len(contributing) == 1:
        gid = contributing[0]
        sole_node = built[gid]
        # The contributing group's outputs include grain keys it exposed
        # for sibling JOINs (see `_compute_concept_sets`). At the user-
        # facing FINAL projection those keys aren't part of mandatory and
        # would otherwise leak into the SELECT. Mask them with
        # hidden_concepts — only valid at the FINAL layer, since hiding
        # them at an intermediate group blocks downstream consumers from
        # using them as JOIN keys (MergeNode validates non-hidden parent
        # outputs only).
        output_addrs = {o.address for o in sole_node.output_concepts}
        grain_addrs = set(attrs[gid].grain_components)
        # A basic riding a window-over-aggregate (q36 `i_category`/`i_class`
        # over a ROLLUP-then-rank) passes the aggregate's grain keys through as
        # row-identity / partition columns. Those aren't this basic's declared
        # grain, so add every grouping ancestor's grain to the hide candidates —
        # otherwise the carried keys (e.g. bare `ss.item.category`) leak into the
        # FINAL projection alongside their mandatory rename.
        for anc in nx.ancestors(group_graph, gid):
            if anc != FINAL_NODE_ID and attrs[anc].derivation in GROUPING_DERIVATIONS:
                grain_addrs |= set(attrs[anc].grain_components)
        hide = (grain_addrs & output_addrs) - mandatory_addresses
        if hide:
            existing = set(sole_node.hidden_concepts or set())
            sole_node.hidden_concepts = existing | hide
            sole_node.rebuild_cache()
        return _apply_final_conditions(sole_node)

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
    # this grain (keeping the join key) instead of its own key-grain.
    grouping_grain_components: set[str] = set()
    for gid in contributing:
        if attrs[gid].derivation in GROUPING_DERIVATIONS:
            grouping_grain_components |= set(attrs[gid].grain_components)

    parents: list[StrategyNode] = []
    for gid in contributing:
        node = built[gid]
        is_root = attrs[gid].derivation == "root"
        if is_root:
            parents.extend(
                _wrap_for_grain(
                    node,
                    per_group[gid],
                    environment,
                    frozenset(grouping_grain_components),
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
    parents = _fold_passthrough_parents(parents)
    _widen_merge_join_keys(parents)

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
        BuildGrain.from_concepts(grouping_grain_components, environment=environment)
        if grouping_grain_components
        else None
    )
    return MergeNode(
        input_concepts=merge_inputs,
        output_concepts=outputs,
        environment=environment,
        parents=parents,
        grain=merge_grain,
        conditions=final_conditions.conditional if final_conditions else None,
        hidden_concepts=hidden or None,
    )


def build_strategy_node(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    g: ReferenceGraph,
    history: History,
) -> StrategyNode | None:
    """Walk groups in topological order, dispatching each to its v4 generator
    with explicit parent nodes. Returns the most-downstream built node, or
    None if nothing built."""
    built: dict[str, StrategyNode] = {}

    for gid in _topological_order(group_graph):
        if gid == FINAL_NODE_ID:
            continue
        a = attrs[gid]
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
            environment.concepts[addr]
            for addr in select_addrs
            if addr in environment.concepts
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
                for arg in c.lineage.concept_arguments:
                    needed.add(arg.address)
        if injected is not None:
            for arg in injected.concept_arguments:
                needed.add(arg.address)
        parents = _parent_nodes_for(group_graph, attrs, built, gid, needed=needed)
        parents = _project_dimension_parents_to_group_grain(
            parents,
            needed,
            a.grain_components,
            environment,
        )
        parents = _pre_merge_parents(parents, environment)
        # ROOT scans source columns from datasources directly, not from their
        # group-graph predecessors. A `constraint`-edge predecessor (e.g. a
        # d1 aggregate feeding a HAVING-style filter on this root) is real
        # row-flow at SQL time (INNER JOIN to apply the filter) but doesn't
        # supply the root's primary scan columns. Pruning by parent outputs
        # there strips every requested column and the root never builds —
        # leaving the consumer with `INVALID_REFERENCE_BUG` against the
        # missing concepts (q11).
        if derivation not in (
            Derivation.ROOT.value,
            Derivation.UNION.value,
            Derivation.UNNEST.value,
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
            derivation == Derivation.AGGREGATE.value
            and a.aggregate_input_grain
            and a.aggregate_input_grain != a.grain_components
            and parents
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
            history=history,
            g=g,
        )
        logger.info(
            f"[v4] built {gid} derivation={derivation} "
            f"outputs={[o.address for o in outputs]} "
            f"parents={[type(p).__name__ for p in parents]} "
            f"-> {type(node).__name__ if node else None}"
        )
        if node is not None:
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
            ex_concepts, ex_parents = _existence_for_group(
                attrs, built, environment, gid
            )
            if ex_concepts:
                host = condition_host_node if condition_host_node is not None else node
                host.parents = list(host.parents) + ex_parents
                host.existence_concepts = list(host.existence_concepts) + ex_concepts
                host.rebuild_cache()
                if host is not node:
                    node.rebuild_cache()
            built[gid] = node

    if not built:
        return None
    return _assemble_final_node(group_graph, attrs, built, mandatory_list, environment)
