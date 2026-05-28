"""Stage 3: walk the group graph in topological order, hand each group's
already-built parents to its v4 generator, and stash the resulting node.

No source-concepts callback: parents are explicit, derived from the group
graph's lineage edges. Generators that haven't been ported to the v4 flat
style fall back inside `v4_node_generators.dispatch.build_node`."""

from collections import defaultdict

import networkx as nx

from trilogy.constants import logger
from trilogy.core.enums import Derivation
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import combine_condition_atoms
from trilogy.core.processing.nodes import (
    GroupNode,
    History,
    MergeNode,
    SelectNode,
    StrategyNode,
)
from trilogy.core.processing.v4_node_generators import build_node

from .constants import FINAL_NODE_ID
from .models import GroupAttrs

_AGGREGATING_DERIVATIONS = {
    Derivation.AGGREGATE.value,
    Derivation.GROUP_TO.value,
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


def _existence_for_group(
    attrs: dict[str, GroupAttrs],
    built: dict[str, StrategyNode],
    gid: str,
) -> tuple[list[BuildConcept], list[StrategyNode]]:
    """For each atom at `gid`, gather its existence_arguments (right-side
    concepts of a SubselectComparison) and the built groups that supply
    them. These become the host node's `existence_concepts` plus extra
    parents — the SQL renderer emits the right side as a subselect lookup
    against the parent CTE rather than joining it into the row stream."""
    existence_concepts: list[BuildConcept] = []
    existence_parents: list[StrategyNode] = []
    seen_concepts: set[str] = set()
    seen_parents: set[int] = set()
    for atom in _atoms_at(attrs, gid):
        for arg_group in atom.existence_arguments:
            for concept in arg_group:
                if concept.address in seen_concepts:
                    continue
                seen_concepts.add(concept.address)
                existence_concepts.append(concept)
                # Find the built group that supplies this concept.
                for source_gid, source_node in built.items():
                    if source_gid == gid:
                        continue
                    if any(
                        o.address == concept.address
                        for o in source_node.output_concepts
                    ):
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
        # Existence-kind edges feed a subselect, not the row stream —
        # `_existence_for_group` wires them as side-channel parents post-
        # build. Including them here would put them in JOIN dedup and
        # mistakenly merge their row stream into this group's FROM.
        if group_graph.edges[pgid, gid].get("kind") == "existence":
            continue
        node = built.get(pgid)
        if node is not None:
            candidates.append((pgid, node))

    def provides(node: StrategyNode) -> set[str]:
        return {o.address for o in node.output_concepts} & needed

    parents: list[StrategyNode] = []
    for pgid, node in candidates:
        my_provides = provides(node)
        covered_by_descendant = False
        for other_pgid, other_node in candidates:
            if other_pgid == pgid:
                continue
            if pgid not in nx.ancestors(group_graph, other_pgid):
                continue
            if my_provides <= provides(other_node):
                covered_by_descendant = True
                break
        if not covered_by_descendant:
            parents.append(node.copy())
    return parents


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
    `INVALID_REFERENCE_BUG_<...>` markers in the rendered SQL."""
    if not parents:
        return outputs
    available: set[str] = set()
    for parent in parents:
        for output in parent.output_concepts:
            available.add(output.address)
    keep: list[BuildConcept] = []
    for concept in outputs:
        if concept.address in available:
            keep.append(concept)
            continue
        if concept.lineage is not None:
            args = {a.address for a in concept.lineage.concept_arguments}
            if args <= available:
                keep.append(concept)
    return keep


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
        if d.get("kind") in ("lineage", "constraint", "existence")
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
            key=lambda gid: sum(
                1 for a in nx.ancestors(group_graph, gid) if a in built
            ),
            reverse=True,
        )
        per_group[candidates[0]].append(concept)
    return per_group


def _wrap_for_grain(
    parent_node: StrategyNode,
    needed_concepts: list[BuildConcept],
    environment: BuildEnvironment,
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
    by_grain: dict[frozenset[str], list[BuildConcept]] = defaultdict(list)
    for concept in needed_concepts:
        grain_components = (
            frozenset(concept.grain.components) if concept.grain else frozenset()
        )
        by_grain[grain_components].append(concept)

    wraps: list[StrategyNode] = []
    for grain_comps, concepts in by_grain.items():
        if grain_comps == parent_grain_components or not grain_comps:
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
    per_group = _cover_groups_for_mandatory(group_graph, built, mandatory_list)
    if not per_group:
        return next(iter(built.values()))
    contributing = list(per_group.keys())
    mandatory_addresses = {c.address for c in mandatory_list}
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
        hide = (grain_addrs & output_addrs) - mandatory_addresses
        if hide:
            existing = set(sole_node.hidden_concepts or set())
            sole_node.hidden_concepts = existing | hide
            sole_node.rebuild_cache()
        return sole_node

    # Only root scans get the grain projection: their grain is the row-level
    # source-table grain (often much wider than what a downstream merge
    # wants), and a SELECT DISTINCT-style projection is always safe.
    # Wrapping intermediate aggregates is *not* safe — adding a GroupNode
    # over a `sum(x)` node would re-aggregate the partial sums (OK for SUM,
    # wrong for AVG/STDDEV), and intermediate aggregates often don't even
    # expose the requested grain key (their GROUP BY is their grain, not the
    # downstream's). Q17 surfaced both pathologies when this was generalized.
    parents: list[StrategyNode] = []
    for gid in contributing:
        node = built[gid]
        is_root = attrs[gid].derivation == "root"
        if is_root:
            parents.extend(_wrap_for_grain(node, per_group[gid], environment))
        else:
            parents.append(node)

    available: set[str] = set()
    for p in parents:
        for o in p.output_concepts:
            available.add(o.address)
    outputs = [c for c in mandatory_list if c.address in available]
    return MergeNode(
        input_concepts=outputs,
        output_concepts=outputs,
        environment=environment,
        parents=parents,
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
        parents = _parent_nodes_for(group_graph, built, gid, needed=needed)
        parents = _pre_merge_parents(parents, environment)
        # ROOT scans source columns from datasources directly, not from their
        # group-graph predecessors. A `constraint`-edge predecessor (e.g. a
        # d1 aggregate feeding a HAVING-style filter on this root) is real
        # row-flow at SQL time (INNER JOIN to apply the filter) but doesn't
        # supply the root's primary scan columns. Pruning by parent outputs
        # there strips every requested column and the root never builds —
        # leaving the consumer with `INVALID_REFERENCE_BUG` against the
        # missing concepts (q11).
        if derivation != Derivation.ROOT.value:
            outputs = _satisfiable_outputs(outputs, parents)
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
            parent_outputs = list(parents[0].output_concepts)
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
        node = build_node(
            derivation=derivation,
            outputs=outputs,
            parents=parents,
            environment=environment,
            conditions=condition_for_generator,
            preexisting_conditions=preexisting,
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
            ex_concepts, ex_parents = _existence_for_group(attrs, built, gid)
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
