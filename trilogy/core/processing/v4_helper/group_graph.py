"""Stage 2: collapse the concept graph into compatible-concept groups and
append a single FINAL sink.

Pipeline:
    assign groups via per-derivation grouping rules →
    attach secondary members → wire group-level lineage edges →
    inject condition clauses → color edges by pre/post-condition phase →
    attach FINAL sink

The assignment pass is a single uniform loop: for each derivation, look up
its rule in `group_rules.GROUPING_RULES` and let the rule produce its
buckets. The rule decides whether to merge by equality (the default,
keying on `(depth_label, grain)`) or by some other relation (BASIC merges
by grain subset; ROOT collapses everything to a single bucket). No
derivation is privileged in the orchestrator — each one's grouping logic
lives next to its rule.
"""

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Literal, overload

from trilogy.constants import logger
from trilogy.core import graph as nx
from trilogy.core.enums import Derivation
from trilogy.core.models.build import BuildConcept, BuildGrain, BuildWhereClause

from .condition_placement import plan_condition_placements
from .constants import (
    DEPENDENCY_EDGE_KINDS,
    FINAL_NODE_ID,
    GROUPING_DERIVATIONS,
    DepthLabel,
    EdgeKind,
    EdgePhase,
)
from .edges import (
    EdgeMap,
    add_edge,
    copy_edges,
    dependency_subgraph,
    edge_kind,
    edges_of_kind,
    lineage_edges,
    lineage_subgraph,
    remove_edge,
)
from .group_behaviors import Behavior, behavior_for
from .group_rules import DEFAULT_RULE, GROUPING_RULES
from .models import (
    ConceptAttrs,
    FinalAssemblyContract,
    FinalContributorContract,
    GroupAttrs,
    GroupBucket,
    GroupInputContract,
    InputChannel,
)

# depth_label used for the secondary root bucket dedicated to feeding d1
# (in-WHERE) aggregate calculations. Distinct from ``root`` so the bucket
# gets its own group id and doesn't collide with the main root bucket.
ROOT_D1_DEPTH = DepthLabel.ROOT_D1

_REGRAFTABLE_DERIVATIONS: set[Derivation] = {
    Derivation.BASIC,
    Derivation.WINDOW,
}


def _leaf_inputs(primaries: set[str], lineage_parents: dict[str, set[str]]) -> set[str]:
    """The first non-primary lineage ancestor of each primary — the columns a
    group actually consumes. Walks *through* primaries computed inside the
    group (a primary whose lineage arg is another primary, e.g. q49's chained
    rename) so the demand bottoms out on real inputs, not intermediates."""
    leaves: set[str] = set()
    for c in primaries:
        stack = list(lineage_parents.get(c, set()))
        seen: set[str] = set()
        while stack:
            p = stack.pop()
            if p in seen:
                continue
            seen.add(p)
            if p in primaries:
                stack.extend(lineage_parents.get(p, set()))
            else:
                leaves.add(p)
    return leaves


def _fd_on_key(
    concept_attrs: dict[str, ConceptAttrs], address: str, key: set[str]
) -> bool:
    """Whether `address` is functionally determined by the dimension key set
    `key`: it is a key, has no grain (constant), or its declared grain is a
    subset of the key."""
    if address in key:
        return True
    if address not in concept_attrs:
        return False
    gc = concept_attrs[address].grain_components
    return not gc or set(gc) <= key


def _group_id_for(bucket: GroupBucket) -> str:
    grain_key = "|".join(sorted(bucket.grain_components)) or "∅"
    label_prefix = f"[{bucket.label}]" if bucket.label else ""
    suffix = f":{bucket.discriminator}" if bucket.discriminator else ""
    return (
        f"grp:{label_prefix}{bucket.derivation.value}:{bucket.depth_label.value}:"
        f"{grain_key}{suffix}"
    )


def _d1_calc_subgraph(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
) -> tuple[set[str], set[str]]:
    """Identify (d1_calc_roots, d1_subgraph_nodes).

    Any concept reached via the WHERE recursion lives at a condition-phase
    label (suffix ``@condition``) and is classified d1. Such concepts —
    aggregates, filters, basics, unnests, etc. — feed pre-WHERE compute
    that must not be polluted by sibling WHERE atoms applied to the
    SELECT-side scan. We route root → condition-phase lineage edges
    through a dedicated root_d1 bucket so the condition scan stays
    independent of the blank-phase scan's WHERE atoms.

    - d1_calc_roots: blank-phase roots that feed any condition-phase node.
    - d1_subgraph_nodes: every condition-phase node. Edge routing uses
      this as the destination side of the predicate."""
    d1_subgraph: set[str] = {
        n for n in concept_graph.nodes if concept_attrs[n].depth_label == DepthLabel.D1
    }
    if not d1_subgraph:
        return set(), set()
    d1_calc_roots: set[str] = set()
    for n in d1_subgraph:
        for pred, _ in concept_graph.in_edges(n):
            if edge_kind(concept_edges, pred, n) != EdgeKind.LINEAGE:
                continue
            pa = concept_attrs[pred]
            if pa.derivation == Derivation.ROOT and pa.depth_label != DepthLabel.D1:
                d1_calc_roots.add(pred)
    return d1_calc_roots, d1_subgraph


def _add_d1_root_bucket(
    concept_attrs: dict[str, ConceptAttrs],
    buckets: dict[str, GroupBucket],
    d1_calc_roots: set[str],
) -> str | None:
    """Add an extra ROOT bucket containing just the d1-feeding roots. Returns
    the new bucket's group id (or None if no d1 calc roots)."""
    if not d1_calc_roots:
        return None
    bucket = GroupBucket(
        depth_label=ROOT_D1_DEPTH,
        derivation=Derivation.ROOT,
        grain_components=frozenset(),
    )
    for node in sorted(d1_calc_roots):
        address = concept_attrs[node].address
        bucket.primary_members.append(address)
        bucket.primary_node_ids.append(node)
        bucket.member_depths[address] = concept_attrs[node].depth_label
    gid = _group_id_for(bucket)
    buckets[gid] = bucket
    return gid


def _assign_groups(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    output_addresses: frozenset[str] = frozenset(),
) -> tuple[dict[str, str], dict[str, GroupBucket]]:
    """Group every concept by dispatching to its derivation's rule.

    Rules are invoked once per derivation, lazily, via an `ensure_assigned`
    callback. A rule that needs another derivation already bucketed (BASIC
    walks its lineage parents to compute a stop-signature, and asks for
    each parent's group id) calls `ensure_assigned(parent_derivation)` and
    we run the rule for that derivation on-demand. This avoids any
    privileging in the call order — every derivation looks the same from
    the orchestrator's view; BASIC just happens to use the callback."""
    by_derivation: dict[Derivation, list[tuple[str, ConceptAttrs]]] = defaultdict(list)
    for node in concept_graph.nodes:
        a = concept_attrs[node]
        by_derivation[a.derivation].append((node, a))

    primary_group: dict[str, str] = {}
    buckets: dict[str, GroupBucket] = {}
    assigned: set[Derivation] = set()

    def ensure_assigned(derivation: Derivation) -> None:
        if derivation in assigned:
            return
        assigned.add(derivation)
        rule = GROUPING_RULES.get(derivation, DEFAULT_RULE)
        items = by_derivation.get(derivation, [])
        for bucket in rule(
            items,
            concept_graph,
            concept_edges,
            concept_attrs,
            primary_group,
            ensure_assigned,
            output_addresses,
        ):
            group_id = _group_id_for(bucket)
            buckets[group_id] = bucket
            for node_id_member in bucket.primary_node_ids:
                primary_group[node_id_member] = group_id

    for derivation in by_derivation:
        ensure_assigned(derivation)
    return primary_group, buckets


def _attach_secondary_members(
    concept_graph: nx.DiGraph,
    concept_attrs: dict[str, ConceptAttrs],
    buckets: dict[str, GroupBucket],
) -> None:
    """Attach the concepts each grouping bucket implicitly carries beyond
    its primary set. Right now this is only the grain components of a
    GROUP-BY / PARTITION-BY bucket — they appear in the SELECT alongside
    the aggregates, so we record them as members for visualization and
    for the condition-placement pass.

    BASIC groups deliberately get nothing here: their passthrough
    capability is derived from topology in `_compute_concept_sets`
    (parent capability ∩ grain-compatible), not pre-declared. Earlier
    attempts to enumerate BASIC secondaries up front (all roots / lineage
    parents / lineage-parent grains) each leaked or starved a different
    test query."""

    def add(bucket: GroupBucket, address: str) -> None:
        if address in bucket.primary_members or address in bucket.secondary_members:
            return
        if address not in concept_attrs:
            return
        bucket.secondary_members.append(address)
        bucket.member_depths[address] = concept_attrs[address].depth_label

    for bucket in buckets.values():
        if bucket.derivation in GROUPING_DERIVATIONS:
            for grain_addr in bucket.grain_components:
                add(bucket, grain_addr)


def _lineage_leaf_addresses(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    node: str,
) -> set[str]:
    """Addresses of `node`'s lineage ancestors that have no lineage parent of
    their own — the raw inputs the concept ultimately computes from."""
    leaves: set[str] = set()
    visited: set[str] = {node}
    stack = [node]
    while stack:
        cur = stack.pop()
        preds = [
            p
            for p, _ in concept_graph.in_edges(cur)
            if edge_kind(concept_edges, p, cur) == EdgeKind.LINEAGE
        ]
        if cur != node and not preds:
            leaves.add(concept_attrs[cur].address)
        for p in preds:
            if p not in visited:
                visited.add(p)
                stack.append(p)
    return leaves


def _fold_rollup_key_dims(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    primary_group: dict[str, str],
    buckets: dict[str, GroupBucket],
) -> None:
    """Fold a BASIC dim that is purely a function of a ROLLUP/CUBE/GROUPING_SETS
    group's grouping keys into that group, so it is emitted as a column of the
    GROUP BY ROLLUP node and carries the rolled-up key values on the
    subtotal/grand-total rows.

    Otherwise such a dim (``channel <- case chan``, ``outlet <- concat('x',
    txt)``) buckets by stop-signature/grain into a leaf-grain BASIC group and is
    joined back to the rollup on the raw keys. At a subtotal row the rolled-up
    key is NULL, so the join finds no leaf match and the dim comes back NULL —
    dropping the dimension value v3 preserves (q80). Mirrors v3, which emits the
    derived dim as a non-aggregate column of the rollup group node;
    ``group_node.py`` already marks these dims nullable for the null-safe
    assembly join.

    Scoped to non-standard grouping: a standard GROUP BY key is never
    NULL-injected, so a downstream dim computed over the key join is already
    correct and folding there would only churn SQL shape."""
    rollups = [
        (gid, b)
        for gid, b in buckets.items()
        if b.derivation == Derivation.AGGREGATE
        and any(
            concept_attrs[n].grouping_mode not in (None, "standard")
            for n in b.primary_node_ids
        )
    ]
    if not rollups:
        return
    # Most specific (largest key set) first so a dim folds into the rollup whose
    # keys most tightly cover it.
    rollups.sort(key=lambda kv: (-len(kv[1].grain_components), kv[0]))
    moves: dict[str, str] = {}
    for gid, bucket in buckets.items():
        if bucket.derivation != Derivation.BASIC:
            continue
        for node in bucket.primary_node_ids:
            attrs = concept_attrs[node]
            if attrs.depth_label in (DepthLabel.D1, ROOT_D1_DEPTH):
                continue
            # A pure rename is a pseudonym of its source key; the renderer
            # resolves it to the rolled-up key column directly, so it neither
            # needs nor tolerates folding (q86: i_category renames item.category,
            # a rollup key feeding a downstream window/grouping).
            if attrs.is_rename:
                continue
            leaves = _lineage_leaf_addresses(
                concept_graph, concept_edges, concept_attrs, node
            )
            if not leaves:
                continue
            for r_gid, r_bucket in rollups:
                if r_gid == gid or r_bucket.label != bucket.label:
                    continue
                if leaves <= set(r_bucket.grain_components):
                    moves[node] = r_gid
                    break
    for node, target_gid in moves.items():
        src = buckets[primary_group[node]]
        idx = src.primary_node_ids.index(node)
        address = src.primary_members.pop(idx)
        src.primary_node_ids.pop(idx)
        src.member_depths.pop(address, None)
        tgt = buckets[target_gid]
        if node not in tgt.primary_node_ids:
            tgt.primary_node_ids.append(node)
            tgt.primary_members.append(address)
            tgt.member_depths[address] = concept_attrs[node].depth_label
        primary_group[node] = target_gid
    for gid in [
        g
        for g, b in buckets.items()
        if not b.primary_node_ids and not b.secondary_members
    ]:
        del buckets[gid]


def _materialize_group_graph(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    primary_group: dict[str, str],
    buckets: dict[str, GroupBucket],
    d1_root_gid: str | None = None,
    d1_calc_roots: set[str] | None = None,
    d1_subgraph: set[str] | None = None,
) -> tuple[nx.DiGraph, dict[str, GroupAttrs], EdgeMap]:
    """Realize the in-flight `GroupBucket` map as an nx.DiGraph plus a
    side-table of typed `GroupAttrs` keyed by group id and the typed
    `group_edges` metadata map. The graph holds topology only; downstream
    consumers read per-group state via `attrs[gid]` and edge kinds via
    `group_edges`, getting attribute access + type checking."""
    group_graph: nx.DiGraph = nx.DiGraph()
    group_edges: EdgeMap = {}
    attrs: dict[str, GroupAttrs] = {}
    for gid, bucket in buckets.items():
        members = tuple(bucket.primary_members) + tuple(bucket.secondary_members)
        attrs[gid] = GroupAttrs(
            depth_label=bucket.depth_label,
            derivation=bucket.derivation,
            grain_components=bucket.grain_components,
            label=bucket.label,
            members=members,
            primary_members=tuple(bucket.primary_members),
            secondary_members=tuple(bucket.secondary_members),
            member_depths=dict(bucket.member_depths),
            aggregate_input_grain=bucket.aggregate_input_grain,
        )
        group_graph.add_node(gid)

    # Propagate concept-level edges to the group level. Both `lineage` and
    # `constraint` edges become group predecessor relationships: lineage is
    # a computational dependency; constraint is a d1→d0 must-be-above
    # ordering that downstream consumers (the strategy walker, condition
    # placement) treat identically — both mean "this group's outputs must
    # be in the input CTE for the consumer." Without propagating
    # constraints, a d1 aggregate (e.g. `avg(price) by category`, when used
    # in a filter) ends up an island — the filter atom has nowhere to land
    # and the d0 aggregate that consumes the filtered rows never gets the
    # d1 group as a parent.
    #
    # Root edge routing: when a d1 calc node exists (an aggregate inside a
    # WHERE clause), its lineage-feeding roots are duplicated into a second
    # ROOT bucket (R_d1). Any root → d1-subgraph edge sources from R_d1 so
    # the d1 calc reads from a pristine scan; root → anything-else still
    # routes through the default R_other bucket and inherits its pushed-down
    # WHEREs. Without the split, sibling filters pollute the avg's input.
    d1_calc_roots = d1_calc_roots or set()
    d1_subgraph = d1_subgraph or set()
    # The group-edge `kind` records the strongest concept-level edge that maps
    # to it: lineage > constraint > existence. Lineage means the row stream
    # flows along this edge — JOIN partners, demand propagation, sibling-grain
    # projection. Constraint means topo ordering for filter pushdown with
    # implied JOIN. Existence means side-channel only (subselect) — the source
    # must be built and ordered before the consumer, but never JOINed in or
    # projected for sibling JOIN keys. When edges collapse, the stronger
    # guarantee wins.
    priority: dict[EdgeKind, int] = {
        EdgeKind.EXISTENCE: 0,
        EdgeKind.CONSTRAINT: 1,
        EdgeKind.LINEAGE: 2,
    }
    for (u, v), ed in concept_edges.items():
        kind = ed.kind
        if kind not in DEPENDENCY_EDGE_KINDS:
            continue
        if d1_root_gid is not None and u in d1_calc_roots and v in d1_subgraph:
            gu = d1_root_gid
        else:
            gu = primary_group[u]
        gv = primary_group[v]
        if gu == gv:
            continue
        current = edge_kind(group_edges, gu, gv)
        if current is not None:
            if priority[kind] > priority[current]:
                group_edges[(gu, gv)].kind = kind
        else:
            add_edge(group_graph, group_edges, gu, gv, kind)

    # An EXISTENCE edge only orders the subselect source before its consumer.
    # When the source group is itself a LINEAGE-descendant of the consumer
    # group, that ordering is already implied by lineage and the extra edge is
    # a pure back-edge -- it forms a cycle with no new ordering. This happens
    # when the gated row arg and the existence source's input share one node
    # (two unnests merged into one group, then `orid_2 in even_orders` where
    # even_orders descends from that same unnest). Drop those redundant edges.
    lineage_sub = lineage_subgraph(group_graph, group_edges)
    for gu, gv in edges_of_kind(group_edges, EdgeKind.EXISTENCE):
        if gv in lineage_sub and gu in lineage_sub and nx.has_path(lineage_sub, gv, gu):
            remove_edge(group_graph, group_edges, gu, gv)

    return group_graph, attrs, group_edges


def _inject_conditions(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    buckets: dict[str, GroupBucket],
    conditions: list[BuildWhereClause],
    mandatory_list: list[BuildConcept] | None = None,
) -> set[str]:
    """Apply the typed condition-placement plan to the mutable group attrs."""
    condition_group_ids: set[str] = set()
    placements = plan_condition_placements(
        group_graph, group_edges, buckets, conditions, mandatory_list
    )
    for placement in placements:
        for gid in placement.group_ids:
            if placement.atom not in attrs[gid].condition_atoms:
                attrs[gid].condition_atoms.append(placement.atom)
                attrs[gid].conditions.append(str(placement.atom))
            condition_group_ids.add(gid)
    return condition_group_ids


def _virtual_filter_scoped_columns(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    d1_gid: str,
) -> set[str]:
    """Columns already scoped by a `?` virtual-filter (`count(x ? a > b)`)
    downstream of `d1_gid`. Such a predicate is the user's per-aggregate
    selector — its source columns sit in `root_d1` only to feed that CASE-WHEN
    — so a raw outer filter on those columns must NOT be propagated onto the
    shared scan: doing so re-narrows a sibling aggregate that deliberately
    omits the predicate (q21: the unfiltered `distinct_suppliers_per_order`
    next to the late-only `late_suppliers_per_order`)."""
    addr_to_nodes: dict[str, list[str]] = {}
    for nid, ca in concept_attrs.items():
        addr_to_nodes.setdefault(ca.address, []).append(nid)
    scoped: set[str] = set()
    for desc in nx.descendants(group_graph, d1_gid):
        if attrs[desc].derivation != Derivation.FILTER:
            continue
        for addr in attrs[desc].primary_members:
            for nid in addr_to_nodes.get(addr, []):
                for parent in concept_graph.predecessors(nid):
                    if edge_kind(concept_edges, parent, nid) == EdgeKind.LINEAGE:
                        scoped.add(concept_attrs[parent].address)
    return scoped


def _propagate_raw_filters_to_d1_roots(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    datasource_columns: list[frozenset[str]],
) -> set[str]:
    """A `root_d1` bucket is a pristine DUPLICATE of a main fact scan, split off
    so a d1 calc reads rows untouched by *sibling* WHERE atoms. That split is
    right when the d1 calc is a separate population:

    - a SEMIJOIN source — `week_seq in relevent_week_seq` (q02): the source
      spans all channels, so the outer `sales_channel in (...)` must not narrow
      it (and propagating there also cycles the CTE graph, since the existence
      source feeds back into the main stream);
    - an aggregate over a DIFFERENT table than the filter — `avg(item.price) by
      category` filtered on `sales.date` (q06): the average is over the item
      dimension, which the sales-grain date filter never touches.

    But when the d1 aggregate reads the SAME fact the filter applies to —
    `avg(ss.net_profit) by item` under `ss.store.id = 1` (q44) — the filter
    belongs on its input too, or the avg covers the wrong population. So copy a
    main root's atoms (all raw root-column filters by construction) onto a
    `root_d1` only when (a) root_d1 feeds no existence source and (b) some
    single datasource carries BOTH the atom's columns and all of root_d1's scan
    columns — i.e. the filter and the aggregate genuinely share one table.
    Returns the root_d1 gids that gained atoms."""
    existence_sources = {u for u, _ in edges_of_kind(group_edges, EdgeKind.EXISTENCE)}
    d1_roots = [
        gid
        for gid in group_graph.nodes
        if gid in attrs and attrs[gid].depth_label == ROOT_D1_DEPTH
    ]
    main_roots = [
        gid
        for gid in group_graph.nodes
        if gid in attrs
        and attrs[gid].derivation == Derivation.ROOT
        and attrs[gid].depth_label == DepthLabel.ROOT
        and attrs[gid].condition_atoms
    ]
    touched: set[str] = set()
    for d1_gid in d1_roots:
        reach = nx.descendants(group_graph, d1_gid) | {d1_gid}
        if reach & existence_sources:
            continue
        d1_members = set(attrs[d1_gid].primary_members)
        # The datasource that supplies the MOST of root_d1's inputs is the table
        # the d1 aggregate actually scans (q74 → the sales fact; q06 → the item
        # table, whose `current_price` the per-category avg reads — NOT the fact,
        # even though they share the `item.id` join key). Propagate a raw filter
        # only when its columns live in THAT table — otherwise a filter on the
        # fact (`customer.id is not null`) wrongly narrows an item-level avg
        # (q06). Requiring all d1_members in one table fails for joined dim
        # attributes that aren't base columns (q74's `date.year`), so pick the
        # best-overlap table instead.
        scan_cols: frozenset[str] = frozenset()
        best = 0
        for cols in datasource_columns:
            overlap = len(d1_members & cols)
            if overlap > best:
                best = overlap
                scan_cols = cols
        if not scan_cols:
            continue
        filter_scoped = _virtual_filter_scoped_columns(
            group_graph, attrs, concept_graph, concept_edges, concept_attrs, d1_gid
        )
        for m_gid in main_roots:
            for atom in attrs[m_gid].condition_atoms:
                row_args = {a.address for a in atom.row_arguments}
                if not row_args <= scan_cols:
                    continue
                # A `?` virtual-filter downstream already scopes this predicate
                # per-aggregate; propagating it to the shared scan would narrow a
                # sibling aggregate that omits it (q21).
                if row_args and row_args <= filter_scoped:
                    continue
                if atom not in attrs[d1_gid].condition_atoms:
                    attrs[d1_gid].condition_atoms.append(atom)
                    attrs[d1_gid].conditions.append(str(atom))
                    touched.add(d1_gid)
    return touched


def _color_phases(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    condition_group_ids: set[str],
) -> set[str]:
    """Mark each edge as `pre_condition` or `post_condition`. Any edge from a
    condition-bearing group (or any descendant of one) is post-condition;
    everything earlier is pre-condition. Returns the downstream set so the
    FINAL-sink wiring can use the same coloring."""
    downstream: set[str] = set(condition_group_ids)
    for cgid in condition_group_ids:
        downstream |= nx.descendants(group_graph, cgid)
    for (u, v), attrs in group_edges.items():
        attrs.phase = (
            EdgePhase.POST_CONDITION if u in downstream else EdgePhase.PRE_CONDITION
        )
    return downstream


def _add_final_node(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    concept_graph: nx.DiGraph,
    concept_attrs: dict[str, ConceptAttrs],
    buckets: dict[str, GroupBucket],
    conditions: list[BuildWhereClause],
    mandatory_list: list[BuildConcept] | None = None,
) -> None:
    """Attach a single FINAL sink that collects every non-d1 concept, with a
    merge edge from every group. Added before `_inject_conditions` so a
    cross-arm post-merge filter (which no pre-final group can host) can land on
    it; `_color_phases` colors the merge edges afterward like the rest."""
    non_condition_members = tuple(
        n for n in concept_graph.nodes if concept_attrs[n].depth_label != DepthLabel.D1
    )
    group_graph.add_node(FINAL_NODE_ID)
    attrs[FINAL_NODE_ID] = GroupAttrs(
        depth_label=DepthLabel.FINAL,
        members=non_condition_members,
        conditions=[str(c) for c in conditions],
        final_contract=(
            FinalAssemblyContract(
                output_addresses=frozenset(c.address for c in mandatory_list),
                required_grain=frozenset(
                    BuildGrain.from_concepts(mandatory_list).components
                ),
            )
            if mandatory_list is not None
            else None
        ),
    )
    for gid in buckets:
        add_edge(group_graph, group_edges, gid, FINAL_NODE_ID, EdgeKind.MERGE)


def _rowset_join_key_addresses(
    concept: BuildConcept, mandatory_by_address: dict[str, BuildConcept]
) -> set[str]:
    key_addresses = set(concept.keys or set())
    if not key_addresses and concept.grain:
        key_addresses = set(concept.grain.components)
    if not key_addresses:
        key_addresses = {concept.address}
    output: set[str] = set()
    for key_address in key_addresses:
        key_concept = mandatory_by_address.get(key_address)
        if key_concept is None or key_concept.lineage is None:
            output.add(key_address)
            continue
        output |= {arg.address for arg in key_concept.lineage.concept_arguments}
    return output


def _final_merge_grain(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    mandatory_list: list[BuildConcept],
) -> frozenset[str]:
    mandatory_by_address = {concept.address: concept for concept in mandatory_list}
    grain: set[str] = set()
    for gid in group_graph.predecessors(FINAL_NODE_ID):
        if gid not in attrs:
            continue
        if attrs[gid].derivation in GROUPING_DERIVATIONS:
            grain |= set(attrs[gid].grain_components)
    for concept in mandatory_list:
        if concept.derivation in GROUPING_DERIVATIONS and concept.grain:
            grain |= set(concept.grain.components)
        elif concept.derivation == Derivation.ROWSET:
            grain |= _rowset_join_key_addresses(concept, mandatory_by_address)
    return frozenset(grain)


def _group_final_grain_contribution(
    attrs: dict[str, GroupAttrs], gid: str, merge_grain: frozenset[str]
) -> frozenset[str]:
    if gid not in attrs:
        return frozenset()
    if attrs[gid].derivation in GROUPING_DERIVATIONS:
        return attrs[gid].grain_components
    if attrs[gid].derivation == Derivation.ROWSET:
        return merge_grain
    return frozenset()


def _refresh_final_contract(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    mandatory_list: list[BuildConcept],
) -> None:
    if FINAL_NODE_ID not in attrs:
        return
    output_addresses = frozenset(c.address for c in mandatory_list)
    merge_grain = _final_merge_grain(group_graph, attrs, mandatory_list)
    contributors: list[FinalContributorContract] = []
    for gid in sorted(group_graph.predecessors(FINAL_NODE_ID)):
        if gid not in attrs:
            continue
        preserve_keys = (
            merge_grain if attrs[gid].derivation == Derivation.ROOT else frozenset()
        )
        contributors.append(
            FinalContributorContract(
                group_id=gid,
                output_addresses=frozenset(attrs[gid].output_concepts),
                preserve_keys=preserve_keys,
                projection_grain=_group_final_grain_contribution(
                    attrs, gid, merge_grain
                ),
            )
        )
    attrs[FINAL_NODE_ID].final_contract = FinalAssemblyContract(
        output_addresses=output_addresses,
        required_grain=frozenset(BuildGrain.from_concepts(mandatory_list).components),
        merge_grain=merge_grain,
        contributor_contracts=tuple(contributors),
    )


def _consumer_required_input_grain(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    gid: str,
) -> frozenset[str]:
    grain: set[str] = set(attrs[gid].grain_components)
    for pred in group_graph.predecessors(gid):
        if pred == FINAL_NODE_ID or pred not in attrs:
            continue
        if edge_kind(group_edges, pred, gid) == EdgeKind.EXISTENCE:
            continue
        if attrs[pred].derivation in GROUPING_DERIVATIONS:
            grain |= set(attrs[pred].grain_components)
        elif attrs[pred].derivation == Derivation.ROWSET:
            grain |= set(attrs[pred].grain_components)
    return frozenset(grain)


def _refresh_input_contracts(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
) -> None:
    for gid in group_graph.nodes:
        if gid == FINAL_NODE_ID or gid not in attrs:
            continue
        required_grain = _consumer_required_input_grain(
            group_graph, group_edges, attrs, gid
        )
        contracts: list[GroupInputContract] = []
        for pred in sorted(group_graph.predecessors(gid)):
            if pred == FINAL_NODE_ID or pred not in attrs:
                continue
            kind = edge_kind(group_edges, pred, gid)
            is_existence = kind == EdgeKind.EXISTENCE
            required_outputs = frozenset(attrs[pred].output_concepts) & frozenset(
                attrs[gid].input_concepts
            )
            contracts.append(
                GroupInputContract(
                    parent_group_id=pred,
                    consumer_group_id=gid,
                    required_outputs=required_outputs,
                    required_grain=frozenset() if is_existence else required_grain,
                    preserve_keys=frozenset() if is_existence else required_grain,
                    channel=(
                        InputChannel.EXISTENCE
                        if is_existence
                        else InputChannel.ROW_STREAM
                    ),
                    may_project_dimension=(
                        bool(required_grain)
                        and not is_existence
                        and attrs[pred].derivation not in GROUPING_DERIVATIONS
                    ),
                )
            )
        attrs[gid].input_contracts = tuple(contracts)


@dataclass
class GroupFacts:
    primary: set[str]
    grain: frozenset[str]
    derivation: Derivation | None
    native_grain: frozenset[str] = frozenset()
    behavior: Behavior | None = None


@dataclass
class GroupIOPlan:
    capability: dict[str, set[str]] = field(default_factory=dict)
    outputs: dict[str, set[str]] = field(default_factory=dict)
    inputs: dict[str, set[str]] = field(default_factory=dict)
    hidden: dict[str, set[str]] = field(default_factory=dict)

    @classmethod
    def for_groups(cls, group_graph: nx.DiGraph) -> "GroupIOPlan":
        return cls(
            capability={gid: set() for gid in group_graph.nodes},
            outputs={gid: set() for gid in group_graph.nodes},
            inputs={gid: set() for gid in group_graph.nodes},
            hidden={gid: set() for gid in group_graph.nodes},
        )


def _lineage_parents_by_address(
    concept_edges: EdgeMap, concept_attrs: dict[str, ConceptAttrs]
) -> dict[str, set[str]]:
    lineage_parents: dict[str, set[str]] = {}
    for u, v in lineage_edges(concept_edges):
        u_addr = concept_attrs[u].address
        v_addr = concept_attrs[v].address
        lineage_parents.setdefault(v_addr, set()).add(u_addr)
    return lineage_parents


def _source_grain_by_address(
    concept_graph: nx.DiGraph, concept_attrs: dict[str, ConceptAttrs]
) -> dict[str, frozenset[str]]:
    source_grain: dict[str, frozenset[str]] = {}
    for node in concept_graph.nodes:
        address = concept_attrs[node].address
        grain = concept_attrs[node].grain_components
        if grain:
            source_grain[address] = frozenset(grain)
    return source_grain


def _build_group_facts(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    buckets: dict[str, GroupBucket],
) -> dict[str, GroupFacts]:
    facts: dict[str, GroupFacts] = {}
    for gid in group_graph.nodes:
        a = attrs[gid]
        facts[gid] = GroupFacts(
            primary=set(a.primary_members),
            grain=frozenset(a.grain_components),
            derivation=a.derivation,
        )
    for gid, bucket in buckets.items():
        behavior = behavior_for(bucket.derivation)
        facts[gid].behavior = behavior
        facts[gid].native_grain = behavior.native_grain(
            bucket, concept_graph, concept_edges, concept_attrs
        )
    return facts


def _apply_grouping_parent_grain_overrides(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    facts: dict[str, GroupFacts],
    lineage_parents: dict[str, set[str]],
) -> None:
    """Correct non-grouping groups that compute pointwise over grouping parents."""
    for gid, fact in facts.items():
        if gid == FINAL_NODE_ID:
            continue
        if (
            fact.derivation == Derivation.ROOT
            or fact.derivation in GROUPING_DERIVATIONS
        ):
            continue
        leaves = _leaf_inputs(fact.primary, lineage_parents)
        if not leaves:
            continue
        grouping_preds = [
            pred
            for pred in group_graph.predecessors(gid)
            if edge_kind(group_edges, pred, gid) == EdgeKind.LINEAGE
            and facts[pred].derivation in GROUPING_DERIVATIONS
        ]
        grains = {facts[pred].grain for pred in grouping_preds}
        if len(grains) != 1:
            continue
        grouping_grain = next(iter(grains))
        if fact.grain <= grouping_grain:
            continue
        covered = set(grouping_grain)
        for pred in grouping_preds:
            covered |= facts[pred].primary
        if leaves <= covered:
            fact.grain = grouping_grain
            fact.native_grain = grouping_grain


def _topological_dependency_order(
    group_graph: nx.DiGraph, group_edges: EdgeMap
) -> list[str] | None:
    dep_graph = dependency_subgraph(group_graph, group_edges)
    try:
        return list(nx.topological_sort(dep_graph))
    except nx.NetworkXUnfeasible:
        try:
            cycle = nx.find_cycle(dep_graph)
        except nx.NetworkXNoCycle:
            cycle = None
        logger.warning(
            "[v4] group-graph lineage cycle, skipping concept-set pass: %s",
            cycle,
        )
        return None


def _compute_concept_sets(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    buckets: dict[str, GroupBucket],
    mandatory_list: list[BuildConcept],
) -> None:
    """Per-group input/output/hidden concept sets.

    The pass has three explicit pieces:
    - stable graph facts (`GroupFacts`): primary members, grain, behavior
    - forward capability: which columns each group can preserve
    - reverse demand: which outputs/inputs each group must expose
    """
    mandatory_addresses = {c.address for c in mandatory_list}
    # A struct field demanded as the canonical key (`local.a`) is produced under
    # its derivable pseudonym (`unnest_array.a`); the FINAL demand intersect must
    # match those aliases so the producing group keeps the field as an output.
    # Gate strictly on the canonical key having NO group of its own — i.e. it is
    # only reachable through the pseudonym. A plain alias (`channel_out <-
    # channel_label`) IS a group primary, so it keeps its own projection and must
    # not be matched against its alias's group.
    all_primary_members = {
        member
        for gid, a in attrs.items()
        if gid != FINAL_NODE_ID
        for member in a.primary_members
    }
    mandatory_alias_addresses = set(mandatory_addresses)
    for c in mandatory_list:
        if c.address not in all_primary_members:
            mandatory_alias_addresses.update(c.pseudonyms)
    lineage_parents = _lineage_parents_by_address(concept_edges, concept_attrs)
    source_grain_of = _source_grain_by_address(concept_graph, concept_attrs)
    facts = _build_group_facts(
        group_graph, attrs, concept_graph, concept_edges, concept_attrs, buckets
    )
    _apply_grouping_parent_grain_overrides(
        group_graph, group_edges, facts, lineage_parents
    )
    topo = _topological_dependency_order(group_graph, group_edges)
    if topo is None:
        return

    io = GroupIOPlan.for_groups(group_graph)
    for gid in topo:
        if gid == FINAL_NODE_ID:
            continue
        fact = facts[gid]
        cap: set[str] = set(fact.primary)
        if fact.behavior is None or fact.derivation == Derivation.ROOT:
            for addr in list(cap):
                cap.update(source_grain_of.get(addr, frozenset()))
            io.capability[gid] = cap
            continue
        for pgid in group_graph.predecessors(gid):
            if pgid == FINAL_NODE_ID:
                continue
            for addr in io.capability.get(pgid, set()):
                if fact.behavior.can_preserve(
                    concept_graph,
                    concept_edges,
                    concept_attrs,
                    fact.native_grain,
                    addr,
                ):
                    cap.add(addr)
        io.capability[gid] = cap

    lineage_sub = lineage_subgraph(group_graph, group_edges)

    final_condition_args: set[str] = set()
    for atom in attrs[FINAL_NODE_ID].condition_atoms:
        final_condition_args |= {a.address for a in atom.row_arguments}
    final_condition_args -= mandatory_addresses
    io.outputs[FINAL_NODE_ID] = set(mandatory_addresses)
    io.inputs[FINAL_NODE_ID] = set(mandatory_addresses) | final_condition_args

    primary_to_gid: dict[str, str] = {}
    for gid, fact in facts.items():
        if gid == FINAL_NODE_ID:
            continue
        for addr in fact.primary:
            primary_to_gid.setdefault(addr, gid)

    existence_demand: dict[str, set[str]] = {gid: set() for gid in group_graph.nodes}
    for gid in group_graph.nodes:
        if gid == FINAL_NODE_ID:
            continue
        for atom in attrs[gid].condition_atoms:
            for arg_group in atom.existence_arguments or ():
                for arg in arg_group:
                    source_gid = primary_to_gid.get(arg.address)
                    if source_gid is None or source_gid == gid:
                        continue
                    existence_demand[source_gid].add(arg.address)
    for src_gid, _ in edges_of_kind(group_edges, EdgeKind.EXISTENCE):
        existence_demand[src_gid].update(attrs[src_gid].primary_members)

    for gid in reversed(topo):
        if gid == FINAL_NODE_ID:
            continue
        fact = facts[gid]
        cap_gid = io.capability[gid]
        outs: set[str] = existence_demand.get(gid, set()) & cap_gid
        for succ in group_graph.successors(gid):
            if succ == FINAL_NODE_ID:
                mand = cap_gid & mandatory_alias_addresses
                if fact.derivation in GROUPING_DERIVATIONS:
                    mand &= fact.primary | fact.grain
                for desc in nx.descendants(lineage_sub, gid):
                    if facts[desc].derivation in GROUPING_DERIVATIONS:
                        mand -= io.outputs[desc]
                outs |= mand
                outs |= cap_gid & final_condition_args
                if fact.grain:
                    for sibling in group_graph.predecessors(succ):
                        if sibling == gid or sibling == FINAL_NODE_ID:
                            continue
                        if facts[sibling].grain == fact.grain:
                            outs |= fact.grain & cap_gid
                            break
                continue
            if edge_kind(group_edges, gid, succ) == EdgeKind.EXISTENCE:
                continue
            demanded = io.inputs.get(succ, set()) & cap_gid
            if fact.derivation in GROUPING_DERIVATIONS:
                sibling_providable: set[str] = set()
                for sib in group_graph.predecessors(succ):
                    if sib in (gid, FINAL_NODE_ID):
                        continue
                    if facts[sib].derivation in GROUPING_DERIVATIONS:
                        continue
                    sibling_providable |= io.capability.get(sib, set())
                keep = fact.primary | fact.grain
                demanded -= sibling_providable - keep
            outs |= demanded
            for sibling in group_graph.predecessors(succ):
                if sibling == gid or sibling == FINAL_NODE_ID:
                    continue
                if edge_kind(group_edges, sibling, succ) == EdgeKind.EXISTENCE:
                    continue
                outs |= facts[sibling].grain & cap_gid
        if fact.derivation in GROUPING_DERIVATIONS:
            outs |= fact.grain & cap_gid
        io.outputs[gid] = outs

        ins: set[str] = set()
        is_grouping = fact.derivation in GROUPING_DERIVATIONS
        for concept_addr in outs:
            if concept_addr in fact.primary:
                stack = list(lineage_parents.get(concept_addr, set()))
                seen_chain: set[str] = set()
                while stack:
                    parent_addr = stack.pop()
                    if parent_addr in seen_chain:
                        continue
                    seen_chain.add(parent_addr)
                    if parent_addr in fact.primary:
                        stack.extend(lineage_parents.get(parent_addr, set()))
                        continue
                    ins.add(parent_addr)
                    if is_grouping and parent_addr not in fact.grain:
                        for gc in source_grain_of.get(parent_addr, frozenset()):
                            if gc not in fact.primary:
                                ins.add(gc)
            else:
                parent_addrs = lineage_parents.get(concept_addr, set())
                if is_grouping and parent_addrs and parent_addrs <= fact.grain:
                    ins |= parent_addrs
                else:
                    ins.add(concept_addr)
        for atom in attrs[gid].condition_atoms:
            for arg in atom.row_arguments:
                if arg.address not in fact.primary:
                    ins.add(arg.address)
        io.inputs[gid] = ins

    for gid in group_graph.nodes:
        if gid == FINAL_NODE_ID:
            continue
        attrs[gid].output_concepts = tuple(sorted(io.outputs[gid]))
        attrs[gid].hidden_concepts = tuple(sorted(io.hidden[gid]))
        attrs[gid].input_concepts = tuple(sorted(io.inputs[gid]))


@overload
def build_group_graph(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    conditions: list[BuildWhereClause],
    mandatory_list: list[BuildConcept] | None = None,
    datasource_columns: list[frozenset[str]] | None = None,
    *,
    return_merged_graph: Literal[False] = False,
) -> tuple[nx.DiGraph, EdgeMap, dict[str, GroupAttrs]]: ...


@overload
def build_group_graph(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    conditions: list[BuildWhereClause],
    mandatory_list: list[BuildConcept] | None = None,
    datasource_columns: list[frozenset[str]] | None = None,
    *,
    return_merged_graph: Literal[True],
) -> tuple[nx.DiGraph, EdgeMap, dict[str, GroupAttrs], nx.DiGraph, EdgeMap]: ...


def build_group_graph(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    conditions: list[BuildWhereClause],
    mandatory_list: list[BuildConcept] | None = None,
    datasource_columns: list[frozenset[str]] | None = None,
    *,
    return_merged_graph: bool = False,
) -> (
    tuple[nx.DiGraph, EdgeMap, dict[str, GroupAttrs]]
    | tuple[nx.DiGraph, EdgeMap, dict[str, GroupAttrs], nx.DiGraph, EdgeMap]
):
    """Collapse compatible concepts into groups and append a single FINAL sink.

    Returns the topology graph, its typed `group_edges` metadata map, and a
    side-table of typed per-group attributes keyed by group id (plus the merged
    graph + its edge map when ``return_merged_graph``).

    Grouping is delegated to per-derivation rules in `group_rules.py`:
    most derivations group by equality on `(depth_label, grain)`; ROOT
    collapses to one bucket; BASIC merges by grain subset/equality.
    """
    output_addresses = frozenset(c.address for c in mandatory_list or [])
    primary_group, buckets = _assign_groups(
        concept_graph, concept_edges, concept_attrs, output_addresses
    )
    _fold_rollup_key_dims(
        concept_graph, concept_edges, concept_attrs, primary_group, buckets
    )
    d1_calc_roots, d1_subgraph = _d1_calc_subgraph(
        concept_graph, concept_edges, concept_attrs
    )
    d1_root_gid = _add_d1_root_bucket(concept_attrs, buckets, d1_calc_roots)
    _attach_secondary_members(concept_graph, concept_attrs, buckets)
    group_graph, attrs, group_edges = _materialize_group_graph(
        concept_graph,
        concept_edges,
        primary_group,
        buckets,
        d1_root_gid=d1_root_gid,
        d1_calc_roots=d1_calc_roots,
        d1_subgraph=d1_subgraph,
    )
    # FINAL must exist before injection so a cross-arm post-merge filter can
    # land on it (no pre-final group can host one); `_color_phases` then colors
    # its merge edges along with the rest.
    _add_final_node(
        group_graph,
        group_edges,
        attrs,
        concept_graph,
        concept_attrs,
        buckets,
        conditions,
        mandatory_list,
    )
    merged_group_graph = group_graph.copy()
    merged_group_edges = copy_edges(group_edges)
    if mandatory_list is not None:
        _compute_concept_sets(
            group_graph,
            group_edges,
            attrs,
            concept_graph,
            concept_edges,
            concept_attrs,
            buckets,
            mandatory_list,
        )
        changed = _regraft_group_sources(
            group_graph, group_edges, attrs, buckets, concept_attrs
        )
        if changed:
            _compute_concept_sets(
                group_graph,
                group_edges,
                attrs,
                concept_graph,
                concept_edges,
                concept_attrs,
                buckets,
                mandatory_list,
            )
    condition_group_ids = _inject_conditions(
        group_graph, group_edges, attrs, buckets, conditions, mandatory_list
    )
    condition_group_ids |= _propagate_raw_filters_to_d1_roots(
        group_graph,
        group_edges,
        attrs,
        concept_graph,
        concept_edges,
        concept_attrs,
        datasource_columns or [],
    )
    _color_phases(group_graph, group_edges, condition_group_ids)
    if mandatory_list is not None:
        _compute_concept_sets(
            group_graph,
            group_edges,
            attrs,
            concept_graph,
            concept_edges,
            concept_attrs,
            buckets,
            mandatory_list,
        )
        _refresh_input_contracts(group_graph, group_edges, attrs)
        _refresh_final_contract(group_graph, attrs, mandatory_list)
    if return_merged_graph:
        return group_graph, group_edges, attrs, merged_group_graph, merged_group_edges
    return group_graph, group_edges, attrs


def _lineage_predecessors(
    group_graph: nx.DiGraph, group_edges: EdgeMap, gid: str
) -> list[str]:
    return [
        pred
        for pred in group_graph.predecessors(gid)
        if edge_kind(group_edges, pred, gid) == EdgeKind.LINEAGE
    ]


def _best_existing_regraft_parent(
    group_graph: nx.DiGraph,
    attrs: dict[str, GroupAttrs],
    gid: str,
) -> str | None:
    current = attrs[gid]
    needed = set(current.input_concepts)
    if not needed:
        return None
    my_ancestors = nx.ancestors(group_graph, gid)
    best_gid: str | None = None
    best_score: tuple[int, int, int] = (-1, -1, -1)
    for candidate in group_graph.nodes:
        if candidate in (gid, FINAL_NODE_ID):
            continue
        cattrs = attrs[candidate]
        if cattrs.derivation in {Derivation.ROOT, Derivation.BASIC}:
            continue
        if cattrs.label != current.label:
            continue
        candidate_ancestors = nx.ancestors(group_graph, candidate)
        if not (my_ancestors & candidate_ancestors):
            continue
        if gid in candidate_ancestors:
            continue
        if not needed <= set(cattrs.output_concepts):
            continue
        score = (
            len(candidate_ancestors),
            int(cattrs.derivation in GROUPING_DERIVATIONS),
            len(cattrs.output_concepts),
        )
        if score > best_score:
            best_score = score
            best_gid = candidate
    return best_gid


def _synthetic_dimension_regraft_parent(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    buckets: dict[str, GroupBucket],
    concept_attrs: dict[str, ConceptAttrs],
    gid: str,
) -> str | None:
    current = attrs[gid]
    if current.derivation != Derivation.BASIC:
        return None
    key = set(current.grain_components)
    if not key:
        return None
    lineage_preds = _lineage_predecessors(group_graph, group_edges, gid)
    root_preds = [
        pred for pred in lineage_preds if attrs[pred].derivation == Derivation.ROOT
    ]
    if not root_preds or len(root_preds) != len(lineage_preds):
        return None
    inputs = list(current.input_concepts)
    if not inputs:
        return None
    if not all(_fd_on_key(concept_attrs, address, key) for address in inputs):
        return None
    if not any(
        key <= set(attrs[candidate].grain_components)
        for candidate in group_graph.nodes
        if candidate != FINAL_NODE_ID
        and attrs[candidate].derivation in GROUPING_DERIVATIONS
    ):
        return None

    root_gid = f"grp:root:root:dim:{'|'.join(sorted(key))}"
    if root_gid not in group_graph:
        group_graph.add_node(root_gid)
        attrs[root_gid] = GroupAttrs(
            depth_label=DepthLabel.ROOT,
            derivation=Derivation.ROOT,
            grain_components=frozenset(),
            primary_members=tuple(inputs),
            members=tuple(inputs),
        )
        bucket = GroupBucket(
            depth_label=DepthLabel.ROOT,
            derivation=Derivation.ROOT,
            grain_components=frozenset(),
        )
        bucket.primary_members = list(inputs)
        buckets[root_gid] = bucket
    for pred in root_preds:
        if group_graph.has_edge(pred, gid):
            remove_edge(group_graph, group_edges, pred, gid)
    return root_gid


def _regraft_group_sources(
    group_graph: nx.DiGraph,
    group_edges: EdgeMap,
    attrs: dict[str, GroupAttrs],
    buckets: dict[str, GroupBucket],
    concept_attrs: dict[str, ConceptAttrs],
) -> bool:
    """Topology-only repair for groups whose best row source is already built.

    This is still a grouping-phase concern: it may add group edges or a
    synthetic dimension ROOT bucket so IO demand flows through the right parent,
    but concrete datasource selection remains in `source_planning` and
    StrategyNode construction remains in `strategy_builder`.
    """
    changed = False
    for gid in list(group_graph.nodes):
        if gid == FINAL_NODE_ID:
            continue
        if attrs[gid].derivation not in _REGRAFTABLE_DERIVATIONS:
            continue
        parent_gid = _best_existing_regraft_parent(group_graph, attrs, gid)
        if parent_gid is None:
            parent_gid = _synthetic_dimension_regraft_parent(
                group_graph, group_edges, attrs, buckets, concept_attrs, gid
            )
        if parent_gid is None or group_graph.has_edge(parent_gid, gid):
            continue
        add_edge(group_graph, group_edges, parent_gid, gid, EdgeKind.LINEAGE)
        changed = True
    return changed
