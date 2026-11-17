"""Per-derivation grouping rules.

Each rule takes the list of `(node_id, node_data)` pairs for one
derivation, the concept_graph, the in-progress `primary_group` map, and an
`ensure_assigned(derivation)` callback the orchestrator passes in so a
rule can demand its dependencies be bucketed on the fly. Most rules
ignore the trailing arguments; BASIC uses them to walk lineage ancestors
and key its buckets by the set of non-BASIC stopping groups.

One registry per shape concern, lookup by derivation, fallback to a default.
"""

import hashlib
from collections import defaultdict
from collections.abc import Callable, Collection, Iterable, Sequence
from functools import partial

from trilogy.core import graph as nx
from trilogy.core.constants import ALL_ROWS_ADDRESS
from trilogy.core.enums import (
    AggregateGroupingMode,
    Derivation,
    FunctionType,
    Granularity,
    Purpose,
)

from .concept_graph import _scope_and_phase
from .constants import REPEAT_BLIND_AGGREGATES, DepthLabel, EdgeKind
from .edges import (
    EdgeMap,
    edge_kind,
    feeds_existence,
    lineage_predecessors,
    lineage_subgraph,
    lineage_successors,
)
from .models import ConceptAttrs, GroupBucket, RootReason, nulls_grouping_keys


def _sig_digest(parts: Collection[str]) -> str:
    """Digest of a set of names, order-blind. Stable across processes: group
    ids must not vary with hash salting."""
    sig_repr = "|".join(sorted(parts)) or "none"
    return hashlib.sha256(sig_repr.encode("utf-8")).hexdigest()[:12]


NodeItem = tuple[str, ConceptAttrs]
EnsureAssignedFn = Callable[[Derivation], None]
PartitionFn = Callable[
    [
        list[NodeItem],
        nx.DiGraph,
        EdgeMap,
        dict[str, ConceptAttrs],
        dict[str, str],
        EnsureAssignedFn,
        frozenset[str],
    ],
    list[GroupBucket],
]


def _bucket_for(
    depth_label: DepthLabel,
    derivation: Derivation,
    grain: frozenset[str],
    label: str = "",
) -> GroupBucket:
    return GroupBucket(
        depth_label=depth_label,
        derivation=derivation,
        grain_components=grain,
        label=label,
    )


def _apply_grouping_mode(
    bucket: GroupBucket,
    grouping_mode: AggregateGroupingMode | None,
    *extra: str,
) -> None:
    """Record an aggregate's GROUP BY mode on its bucket.

    Two things fall out of one mode and must not drift apart: the SEMANTICS
    consumers ask about (`bucket.nulls_grouping_keys`) and the IDENTITY that
    keeps a non-standard bucket from colliding with a STANDARD one of the same
    (label, depth, grain), since one CTE cannot carry both GROUP BY clauses.
    ``extra`` appends further discriminator segments for rules that also split
    on their own signature."""
    segments: list[str] = []
    if grouping_mode is not None:
        bucket.grouping_mode = grouping_mode
        if nulls_grouping_keys(grouping_mode):
            segments.append(f"grp:{grouping_mode.value}")
    segments += extra
    if segments:
        bucket.discriminator = ":".join(segments)


def _split_by_label(items: list[NodeItem]) -> dict[str, list[NodeItem]]:
    """Per-label partition. Two concepts in different sub-graphs (e.g.
    outer query vs. rowset internals) never share a bucket regardless
    of grain or derivation; they're different planning scopes."""
    by_label: dict[str, list[NodeItem]] = defaultdict(list)
    for node, data in items:
        by_label[data.label].append((node, data))
    return by_label


def _add_member(bucket: GroupBucket, node: str, data: ConceptAttrs) -> None:
    bucket.add_member(data.address, node, data.depth_label)


def _add_members(bucket: GroupBucket, items: Iterable[NodeItem]) -> None:
    for node, data in items:
        _add_member(bucket, node, data)


ShapeKey = tuple[str, DepthLabel, frozenset[str], AggregateGroupingMode | None]


def _by_shape(items: list[NodeItem]) -> dict[ShapeKey, list[NodeItem]]:
    """Items sharing `(label, depth_label, grain, grouping_mode)`, keys in
    first-seen order."""
    by_shape: dict[ShapeKey, list[NodeItem]] = defaultdict(list)
    for node, data in items:
        by_shape[
            (data.label, data.depth_label, data.grain_components, data.grouping_mode)
        ].append((node, data))
    return by_shape


def partition_by_depth_and_grain(
    items: list[NodeItem],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    primary_group: dict[str, str],
    ensure_assigned: EnsureAssignedFn,
    output_addresses: frozenset[str] = frozenset(),
) -> list[GroupBucket]:
    """Default rule: two nodes share a group iff they have the same
    ``label``, ``depth_label``, ``grain`` and ``grouping_mode``. Label
    keeps inner-rowset concepts in their own buckets; depth keeps a d1
    aggregate (filter input) and a d0 aggregate (post-filter) in
    distinct scans at the same grain; ``grouping_mode`` keeps STANDARD
    aggregates separate from ROLLUP/CUBE/GROUPING_SETS so each gets the
    GROUP BY clause it needs (one CTE can't carry both a flat GROUP BY
    and a GROUP BY ROLLUP). ``grouping_mode`` is ``None`` for non-
    aggregate concepts and harmlessly collapses to a single value
    there."""
    buckets: list[GroupBucket] = []
    for (label, depth_label, grain, grouping_mode), members in _by_shape(items).items():
        bucket = _bucket_for(depth_label, members[0][1].derivation, grain, label=label)
        _apply_grouping_mode(bucket, grouping_mode)
        _add_members(bucket, members)
        buckets.append(bucket)
    return buckets


def partition_aggregates(
    items: list[NodeItem],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    primary_group: dict[str, str],
    ensure_assigned: EnsureAssignedFn,
    output_addresses: frozenset[str] = frozenset(),
) -> list[GroupBucket]:
    """Partition aggregates by output grain and required input grain.

    Two aggregates at the same output grain can share one input stream only
    when their arguments are valid at the same row grain. For example,
    `count(customer_id)` and `sum(account_balance)` both read customer-grain
    rows and can share; `count(order_id)` and `sum(line_amount)` split when the
    latter needs line-grain rows.

    STANDARD aggregates split by ``input_grain`` (the row grain their arguments
    need). ROLLUP/CUBE/GROUPING_SETS aggregates instead co-source by *upstream
    population*: members at the same output grain and mode whose
    stop-signatures are equal or nest assemble into one combined fact and roll
    up once. Equal signatures cover differing argument sub-grains over a shared
    root (a line-grain sum beside store-grain dimensions); nesting covers a
    shared root plus same-grain derived transforms (row measures beside
    ``group(..) by order, item`` values at that same row grain). One ROLLUP CTE
    per source, rejoined on the grouping dims, is fragile (null-safety on
    rolled-up keys) and slower."""
    standard = [(n, d) for n, d in items if not nulls_grouping_keys(d.grouping_mode)]
    grouped = [(n, d) for n, d in items if nulls_grouping_keys(d.grouping_mode)]
    buckets = _partition_standard_aggregates(
        standard, concept_graph, concept_edges, concept_attrs
    )
    buckets += _partition_grouped_aggregates(
        grouped,
        concept_graph,
        concept_edges,
        concept_attrs,
        primary_group,
        ensure_assigned,
    )
    return buckets


def _arg_rowset_populations(
    node: str,
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
) -> frozenset[str]:
    """Rowset labels an aggregate's arguments read from. A rowset is an opaque
    row population: two same-grain aggregates whose arguments live in different
    rowsets cannot share one input stream; the shared scan would cross-join
    the bodies and fan each argument by the other side's cardinality (two
    global counts over an `except(...)` and a `union(...)` rowset). The walk
    stops AT a rowset member; the body behind it is a separate planning scope."""
    pops: set[str] = set()
    visited: set[str] = {node}
    stack: list[str] = [node]
    while stack:
        current = stack.pop()
        for pred in lineage_predecessors(concept_graph, concept_edges, current):
            if pred in visited:
                continue
            visited.add(pred)
            rowset = concept_attrs[pred].rowset_name
            if rowset is not None:
                pops.add(rowset)
                continue
            stack.append(pred)
    return frozenset(pops)


AggKey = tuple[str, DepthLabel, frozenset[str], frozenset[str], frozenset[str]]


def _fold_distinct_rewritable_buckets(
    entries: dict[AggKey, list[NodeItem]],
) -> dict[AggKey, set[str]]:
    """Fold a coarser-input-grain bucket whose every member is a
    distinct-rewritable COUNT into a sibling bucket whose input grain nests
    it, returning the target-key -> member-address map of counts that must
    render COUNT(DISTINCT ...) there.

    Two same-output-grain aggregates normally split when their arguments need
    different row grains (`sum(return_quantity)` reads return lines,
    `count(order_id)` reads deduped orders). But a count OF A KEY is
    DISTINCT-counting by definition, so its dedup folds into the aggregate and
    the split (sibling CTE + re-join at the output grain) is pure overhead.

    Nesting is LITERAL membership in the finer stream's row identity (its
    input grain plus the shared output grain, so an output key rides as the
    GROUP BY key beside the finer-grain rows). Deliberately NOT an FD closure:
    `post_id` FD-determines `user_id`, but a count of `user_id` beside
    post-grain sums must still count over the USER population, not the users
    reachable through posts. Nor onto a stream that binds a counted key
    partially (a `~order_id` shipments fact): it carries only a subset of the
    key's values."""
    distinct_by_key: dict[AggKey, set[str]] = defaultdict(set)
    changed = True
    while changed:
        changed = False
        for key in sorted(entries, key=repr):
            _label, _depth_label, grain, input_grain, populations = key
            if not input_grain or input_grain == grain:
                continue
            members = entries[key]
            if not all(d.aggregate_distinct_rewritable for _, d in members):
                continue
            for target in sorted(entries, key=repr):
                if target == key or target[:3] != key[:3] or target[4] != populations:
                    continue
                target_grain = target[3]
                # Only fold onto a genuine finer row stream, never onto a
                # bucket already at its output grain (no shared dedup to save,
                # and an out-grain stream rarely determines the counted key).
                if not target_grain or target_grain == target[2]:
                    continue
                if not (set(input_grain) <= set(target_grain) | set(target[2])):
                    continue
                if any(
                    d.counted_key in t.aggregate_partial_keys
                    for _, d in members
                    for _, t in entries[target]
                ):
                    continue
                entries[target].extend(members)
                distinct_by_key[target] |= {d.address for _, d in members}
                distinct_by_key[target] |= distinct_by_key.pop(key, set())
                del entries[key]
                changed = True
                break
            if changed:
                break
    return distinct_by_key


def _lineage_layers(
    members: list[NodeItem],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
) -> list[list[NodeItem]]:
    """Split same-key members into build layers so no bucket holds both a
    member and a lineage CONSUMER of it.

    Two aggregates at one output grain normally co-source when their arguments
    need the same row grain. But `count(line ? owc > 1) by order_id` where
    `owc <- count_distinct(wh) by order_id` puts both at order grain over
    line-grain rows, while the count's filtered input reads owc's OUTPUT: one
    CTE cannot compute a value and consume it. Merging them wires
    aggregate->filter->aggregate onto a single node and the group graph
    2-cycles (the strategy builder's cycle guard then raises).

    Layer i holds members whose longest chain of in-bucket lineage ancestors is
    i, so each layer's inputs are complete before it builds. This is the
    aggregate twin of `_feeds_extra_signature_group`'s producer/consumer split
    for BASIC."""
    if len(members) < 2:
        return [members]
    addrs = {node for node, _ in members}
    ancestors = {
        node: _lineage_reach(node, lineage_predecessors, concept_graph, concept_edges)
        & (addrs - {node})
        for node in addrs
    }
    if not any(ancestors.values()):
        return [members]
    # Longest-path layering, relaxation-capped so a (malformed) lineage cycle
    # terminates instead of recursing.
    level: dict[str, int] = dict.fromkeys(addrs, 0)
    for _ in range(len(addrs)):
        changed = False
        for node in addrs:
            want = max((level[a] + 1 for a in ancestors[node]), default=0)
            if want > level[node]:
                level[node] = want
                changed = True
        if not changed:
            break
    by_level: dict[int, list[NodeItem]] = defaultdict(list)
    for node, data in members:
        by_level[level[node]].append((node, data))
    return [by_level[i] for i in sorted(by_level)]


def _partition_standard_aggregates(
    items: list[NodeItem],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
) -> list[GroupBucket]:
    entries: dict[AggKey, list[NodeItem]] = {}
    for node, data in items:
        populations = _arg_rowset_populations(
            node, concept_graph, concept_edges, concept_attrs
        )
        key = (
            data.label,
            data.depth_label,
            data.grain_components,
            data.aggregate_input_grain,
            populations,
        )
        entries.setdefault(key, []).append((node, data))
    distinct_by_key = _fold_distinct_rewritable_buckets(entries)
    groups = [
        (key, layer_index, layer)
        for key, members in entries.items()
        for layer_index, layer in enumerate(
            _lineage_layers(members, concept_graph, concept_edges)
        )
    ]
    buckets: list[GroupBucket] = []
    for key, layer_index, layer, split in _split_cyclic_groups(
        groups, concept_graph, concept_edges
    ):
        label, depth_label, grain, input_grain, populations = key
        bucket = _bucket_for(depth_label, layer[0][1].derivation, grain, label=label)
        segments: list[str] = []
        if input_grain and input_grain != grain:
            segments.append("input:" + "|".join(sorted(input_grain)))
        if populations:
            segments.append("pop:" + "|".join(sorted(populations)))
        # Layer 0 keeps the unsplit group id; only later layers are
        # discriminated.
        if layer_index:
            segments.append(f"layer:{layer_index}")
        if split is not None:
            segments.append(f"split:{split}")
        if segments:
            bucket.discriminator = ":".join(segments)
        if input_grain:
            bucket.aggregate_input_grain = input_grain
        layer_addrs = {data.address for _, data in layer}
        bucket.aggregate_distinct_addrs = distinct_by_key.get(key, set()) & layer_addrs
        _add_members(bucket, layer)
        buckets.append(bucket)
    return buckets


def _split_cyclic_groups(
    groups: list[tuple[AggKey, int, list[NodeItem]]],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
) -> list[tuple[AggKey, int, list[NodeItem], int | None]]:
    """Groups that read each other through their members' lineage, split by
    each member's depth among the aggregates. `_lineage_layers` splits a
    consumer from its producer within one group; across groups, two members
    that are each fine alone can still meet: `min(h) by form`, re-aggregated
    from `min(h) by species`, beside `sum(h) by form`, which a `by species`
    filter beside `min(h) by species` reads, is a cycle of two groups."""
    members = {node for _, _, layer in groups for node, _ in layer}
    ancestors = {
        node: _lineage_reach(node, lineage_predecessors, concept_graph, concept_edges)
        & (members - {node})
        for node in members
    }
    owner = {
        node: str(i) for i, (_, _, layer) in enumerate(groups) for node, _ in layer
    }
    graph = nx.DiGraph()
    graph.add_nodes_from(owner.values())
    graph.add_edges_from(
        (owner[a], owner[node])
        for node in members
        for a in ancestors[node]
        if owner[a] != owner[node]
    )
    cyclic = {
        int(i)
        for component in nx.strongly_connected_components(graph)
        if len(component) > 1
        for i in component
    }
    if not cyclic:
        return [(key, index, layer, None) for key, index, layer in groups]
    # `ancestors` is transitive, so an ancestor always has fewer of its own
    depth: dict[str, int] = {}
    for node in sorted(members, key=lambda n: (len(ancestors[n]), n)):
        depth[node] = max((depth[a] + 1 for a in ancestors[node]), default=0)
    out: list[tuple[AggKey, int, list[NodeItem], int | None]] = []
    for i, (key, index, layer) in enumerate(groups):
        if i not in cyclic:
            out.append((key, index, layer, None))
            continue
        by_depth: dict[int, list[NodeItem]] = defaultdict(list)
        for item in layer:
            by_depth[depth.get(item[0], 0)].append(item)
        out += [(key, index, by_depth[d], d) for d in sorted(by_depth)]
    return out


def _root(uf: list[int], x: int) -> int:
    while uf[x] != x:
        uf[x] = uf[uf[x]]
        x = uf[x]
    return x


def _components(n: int, related: Iterable[tuple[int, int]]) -> list[list[int]]:
    """Connected components of indices 0..n-1 given the related pairs, each in
    index order and ordered by smallest member (group ids derive from bucket
    order)."""
    uf = list(range(n))
    for i, j in related:
        uf[_root(uf, j)] = _root(uf, i)
    groups: dict[int, list[int]] = defaultdict(list)
    for i in range(n):
        groups[_root(uf, i)].append(i)
    return list(groups.values())


def overlap_components(sets: Sequence[Collection[str]]) -> list[list[int]]:
    """Connected components of `sets` (as indices) under sharing an element."""
    first: dict[str, int] = {}
    return _components(
        len(sets),
        [
            (first.setdefault(e, i), i)
            for i, members in enumerate(sets)
            for e in members
        ],
    )


def _property_key_pairs(
    main_items: list[NodeItem], properties_only: bool = True
) -> list[tuple[str, str]]:
    """(property root, key root) pairs among `main_items`: a property and its
    declared key sit on one entity row, so they co-source even when no lineage
    joins them. A key collapsed onto another identity (scoped join, `merge
    into`) is present under the canonical address only; the mate's pseudonyms
    declare the identity. Without `properties_only`, a key determined by
    another root pairs with it too."""
    node_by_addr = {data.address: node for node, data in main_items}
    node_by_pseudonym: dict[str, str] = {}
    for node, data in main_items:
        for pseudonym in data.pseudonyms:
            node_by_pseudonym.setdefault(pseudonym, node)
    pairs: list[tuple[str, str]] = []
    for node, data in main_items:
        if properties_only and data.purpose != Purpose.PROPERTY:
            continue
        for key_addr in data.keys | data.determining_key_roots:
            key_node = node_by_addr.get(key_addr) or node_by_pseudonym.get(key_addr)
            if key_node is not None and key_node != node:
                pairs.append((node, key_node))
    return pairs


def _partition_grouped_aggregates(
    items: list[NodeItem],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    primary_group: dict[str, str],
    ensure_assigned: EnsureAssignedFn,
) -> list[GroupBucket]:
    """ROLLUP/CUBE/GROUPING_SETS bucketing: union-find merge members at the same
    (label, depth, grain, mode) whose stop-signatures are equal or nest."""
    buckets: list[GroupBucket] = []
    for (label, depth_label, grain, grouping_mode), members in _by_shape(items).items():
        sigs = [
            _stop_signature(
                node,
                Derivation.AGGREGATE,
                concept_graph,
                concept_edges,
                concept_attrs,
                primary_group,
                ensure_assigned,
            )
            for node, _ in members
        ]
        n = len(members)
        nested = [
            (i, j)
            for i in range(n)
            for j in range(i + 1, n)
            if sigs[i] <= sigs[j] or sigs[j] <= sigs[i]
        ]
        for member_indices in _components(n, nested):
            bucket = _bucket_for(depth_label, Derivation.AGGREGATE, grain, label=label)
            shared_sig: frozenset[str] = frozenset().union(
                *(sigs[i] for i in member_indices)
            )
            _apply_grouping_mode(
                bucket, grouping_mode, f"sig:{_sig_digest(shared_sig)}"
            )
            for i in member_indices:
                node, data = members[i]
                if data.aggregate_input_grain:
                    bucket.aggregate_input_grain = frozenset(
                        set(bucket.aggregate_input_grain)
                        | set(data.aggregate_input_grain)
                    )
                _add_member(bucket, node, data)
            # One pass reads one row stream. A count of a key counts at the
            # key's grain, and the stream repeats the key wherever it is
            # finer: a grouping key the counted key does not determine (its
            # rows repeat on a subtotal), or a sibling's finer or foreign
            # input (two facts joined below the pass). DISTINCT on the counted
            # value is the dedup a pass of its own would have had.
            residual = bucket.aggregate_input_grain - grain
            bucket.aggregate_distinct_addrs = {
                members[i][1].address
                for i in member_indices
                if (counted := members[i][1].counted_key) is not None
                and (
                    members[i][1].aggregate_input_grain != frozenset({counted})
                    or residual - {counted}
                )
            }
            bucket.aggregate_first_row_grains = _first_row_grains(
                [members[i][1] for i in member_indices], grain
            )
            buckets.append(bucket)
    return buckets


def _first_row_grains(
    members: list[ConceptAttrs], grain: frozenset[str]
) -> dict[str, frozenset[str]]:
    """Members of one pass whose input rows the shared stream repeats: the
    stream carries a sibling's input grain theirs does not (two facts joined
    below the pass). Each reads the first row per tuple of its own input grain,
    whose grouping keys are one row's, so the dedup never crosses a group.

    A member whose input grain is the grouping grain reads opaque values (a
    `group(..)` it re-aggregates) and is left alone, as is a grouping()
    identity's grain, which never fans the stream; a counted key is DISTINCT."""
    stream = frozenset().union(
        *(
            m.aggregate_input_grain
            for m in members
            if m.aggregate_operator
            not in (FunctionType.GROUPING, FunctionType.GROUPING_ID)
        )
    )
    return {
        m.address: m.aggregate_input_grain | grain
        for m in members
        if m.aggregate_operator not in REPEAT_BLIND_AGGREGATES
        and m.counted_key is None
        and m.aggregate_input_grain - grain
        and not stream <= m.aggregate_input_grain | grain
    }


def _relation_side_partitions(
    main_items: list[NodeItem],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
) -> list[list[NodeItem]]:
    """Partition roots by graph connectivity EXCLUDING relation edges.

    A RELATION edge is an authored join-axis equality between two DIFFERENT
    lineages (`union join rank orders.oid order by orders.amt desc =
    customers.rnk`): its sides recombine at a merge ABOVE the computed
    member, never inside one scan. So the axis must not license co-bucketing,
    and the zero-reach bailout in `_cosource_component_groups` must apply per
    side rather than swallowing both sides into one scan bucket (which
    degrades the join to a cross product; no scan can render the computed
    member as a column). Property roots sharing a declared key are FD-related
    (one entity row, one scan) and stay together even when the key itself is
    not demanded. With no relation edges in the graph this returns a single
    partition."""
    relation_pairs = {
        frozenset((u, v))
        for u, v in concept_graph.edges
        if edge_kind(concept_edges, u, v) == EdgeKind.RELATION
    }
    if len(main_items) < 2:
        return [main_items]
    roots = {node for node, _ in main_items}
    lineage = lineage_subgraph(concept_graph, concept_edges)
    if not any(_rooted_on_both_sides(pair, roots, lineage) for pair in relation_pairs):
        return [main_items]
    links = [
        (u, v)
        for u, v in concept_graph.edges
        if frozenset((u, v)) not in relation_pairs
    ]
    # A root the others determine sits on their entity row, so on their side,
    # key or not (`trees` binds `species` beside `tree_id`).
    links += _property_key_pairs(main_items, properties_only=False)
    by_key: dict[str, list[str]] = defaultdict(list)
    for node, data in main_items:
        if data.purpose == Purpose.PROPERTY:
            for key_addr in data.keys:
                by_key[key_addr].append(node)
    for key_mates in by_key.values():
        links += [(key_mates[0], other) for other in key_mates[1:]]
    adjacency: dict[str, set[str]] = defaultdict(set)
    for u, v in links:
        adjacency[u].add(v)
        adjacency[v].add(u)
    partitions: list[list[NodeItem]] = []
    assigned: set[str] = set()
    for seed, _ in main_items:
        if seed in assigned:
            continue
        component = {seed}
        frontier = [seed]
        while frontier:
            current = frontier.pop()
            for neighbor in adjacency.get(current, ()):
                if neighbor not in component:
                    component.add(neighbor)
                    frontier.append(neighbor)
        members = [item for item in main_items if item[0] in component]
        assigned.update(item[0] for item in members)
        partitions.append(members)
    if not _relation_splits(partitions, relation_pairs, roots, lineage):
        return [main_items]
    return partitions


def _side_roots(node: str, roots: set[str], lineage: nx.DiGraph) -> set[str]:
    if node in roots:
        return {node}
    return nx.ancestors(lineage, node) & roots if node in lineage else set()


def _relation_splits(
    partitions: list[list[NodeItem]],
    relation_pairs: set[frozenset[str]],
    roots: set[str],
    lineage: nx.DiGraph,
) -> bool:
    """Whether the partitions are the sides of a relation: some relation reads
    its two sides from different partitions, and every partition is a side of
    one. A partition reading no side (a bare key output beside a merged axis,
    `city` beside `unnest(...)` merged into `ecoregion_id`) is related to the
    rows by its datasource, not by any edge, so sourcing it apart cross-joins
    it; like a zero-reach root it keeps every root in one scan."""
    separated = False
    sided: set[int] = set()
    for pair in relation_pairs:
        left, right = (
            {
                i
                for i, members in enumerate(partitions)
                if any(node in sides for node, _ in members)
            }
            for sides in (_side_roots(node, roots, lineage) for node in pair)
        )
        separated |= left != right
        sided |= left | right
    return separated and len(sided) == len(partitions)


def _rooted_on_both_sides(
    pair: frozenset[str], roots: set[str], lineage: nx.DiGraph
) -> bool:
    """Whether each side of a relation reads one of these roots. A side that is
    a rowset handle is a leaf no scan sources: the roots then stay one scan,
    and sourcing finds the path to the declared key."""
    return all(_side_roots(node, roots, lineage) for node in pair)


def _is_row_stream_output(
    node: str,
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    output_addresses: frozenset[str],
) -> bool:
    """`node` is an output, or a row-level derivation of it is: its rows are
    rows of the statement, whatever aggregate reads it besides."""
    visited = {node}
    stack = [node]
    while stack:
        cur = stack.pop()
        if concept_attrs[cur].address in output_addresses:
            return True
        for nxt in lineage_successors(concept_graph, concept_edges, cur):
            if nxt in visited or concept_attrs[nxt].derivation in (
                Derivation.AGGREGATE,
                Derivation.GROUP_TO,
                Derivation.UNION,
            ):
                continue
            visited.add(nxt)
            stack.append(nxt)
    return False


def _cosource_component_groups(
    main_items: list[NodeItem],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    output_addresses: frozenset[str],
) -> list[list[NodeItem]]:
    """The co-source decision for one side-partition of root concepts: reach
    overlap unions, the zero-reach single-bucket bailout, and output-converging
    co-sourcing. See `partition_roots` for the full rules."""
    addr_of = {node: data.address for node, data in main_items}
    # Forward lineage reach per root, following only LINEAGE edges (a
    # d1->d0 ordering rides its own CONSTRAINT edge and is excluded here).
    # A UNION consumer is the arm's projection, not a shared consumer: roots
    # feeding it converge there the way output roots converge at FINAL, so it
    # is recorded as convergence (`feeds_union`) rather than reach.
    reaches: list[set[str]] = []
    feeds_union: list[bool] = []
    for node, _ in main_items:
        seen: set[str] = set()
        visited = {node}
        stack = [node]
        stacked = False
        while stack:
            cur = stack.pop()
            for nxt in lineage_successors(concept_graph, concept_edges, cur):
                if nxt in visited:
                    continue
                if concept_attrs[nxt].derivation == Derivation.UNION:
                    stacked = True
                    continue
                visited.add(nxt)
                stack.append(nxt)
                # A pure output alias is a 1:1 relabel of its own source, not
                # a shared consumer that forces a join. Counting it as reach
                # is what makes `select x as t` take the split path where
                # `select x` bails to the safe single bucket; walk through it
                # to whatever really consumes it instead.
                if not concept_attrs[nxt].is_rename:
                    seen.add(nxt)
        reaches.append(seen)
        feeds_union.append(stacked)

    # Bail out to one bucket if any root has zero reach; see partition_roots.
    can_split = bool(main_items) and all(reaches)
    if not can_split:
        return [list(main_items)] if main_items else []

    n = len(main_items)
    # Co-source roots that converge at the query output projection: each
    # output root maps to its concept-graph component id, and roots sharing
    # one merge. `reaches` holds node ids; map to addresses to test membership.
    output_component: dict[int, int] = {}
    aggregate_reach: list[bool] = []
    if output_addresses:
        output_roots = [
            i
            for i in range(n)
            if addr_of[main_items[i][0]] in output_addresses
            or any(concept_attrs[x].address in output_addresses for x in reaches[i])
        ]
        # Only co-source output-converging roots that lie in the same
        # weakly-connected component of the concept graph. Roots in
        # different components (unrelated models, no join/merge path) only
        # meet at a cross-product of single-row aggregates; forcing them
        # into one scan yields an unsourceable disconnected root group
        # (`select sum(av), sum(bv)` over two unrelated models).
        # A membership (`x in set`) is a side-channel subselect, never a join,
        # so its EXISTENCE edge relates no two models.
        undirected = concept_graph.to_undirected()
        undirected.remove_edges_from(
            (u, v)
            for (u, v), edge in concept_edges.items()
            if edge.kind == EdgeKind.EXISTENCE
        )
        # A PROPERTY root and its KEY root are FD-related even when the
        # lineage graph never joins them (a pure two-alias projection
        # `select cust_id as x, cname as y` has one BASIC per root and
        # no shared consumer); the table binding both is what relates
        # them, and splitting them cross-joins ON 1=1 (cartesian rows).
        # Properties only: two KEYS related through a fact FK (user_id
        # on posts) must NOT co-source; `count(user_id)` reads the
        # users table, not the post FK column's deduped domain.
        for node, key_node in _property_key_pairs(main_items):
            undirected.add_edge(node, key_node)
        # A PROPERTY root and another root BOUND BY THE SAME DATASOURCE sit on
        # one physical row stream (`select group_id as g, nullable_amount as v`:
        # the fact binds its FK column beside its property; each root feeds
        # only its own rename, so no shared consumer relates them and splitting
        # cross-joins ON 1=1 with any WHERE applied to just one leg). Only when
        # NEITHER root feeds a grain-collapsing consumer: an aggregate defines
        # its own input domain (`count(user_id)` counts the users table, not
        # the fact FK column's fan) and must keep its independent source.
        aggregate_reach = [
            any(
                concept_attrs[x].derivation
                in (Derivation.AGGREGATE, Derivation.GROUP_TO)
                for x in reaches[i]
            )
            for i in range(n)
        ]
        for i in range(n):
            node_i, data_i = main_items[i]
            if data_i.purpose != Purpose.PROPERTY or not data_i.datasource_bindings:
                continue
            if aggregate_reach[i]:
                continue
            for j in range(n):
                if i == j or aggregate_reach[j]:
                    continue
                node_j, data_j = main_items[j]
                if data_i.datasource_bindings & data_j.datasource_bindings:
                    undirected.add_edge(node_i, node_j)
        # A WHERE row argument filters every scan of the select's rows: a root
        # bound beside it is read under it, aggregate or not. Without a shared
        # consumer (a summary serving one aggregate, the trees another) the
        # split would read the other scan unfiltered
        # (`Rollup.where_filters_every_scan`). A dimension's column filters
        # every scan binding its key (`tree_form` the trees, through `species`).
        for i in range(n):
            node_i, data_i = main_items[i]
            if not data_i.where_row_arg or not data_i.datasource_bindings:
                continue
            determinants = data_i.keys | data_i.determining_key_roots
            bindings = data_i.datasource_bindings.union(
                *(
                    data_k.datasource_bindings
                    for _, data_k in main_items
                    if data_k.address in determinants
                )
            )
            for j in range(n):
                node_j, data_j = main_items[j]
                if i != j and bindings & data_j.datasource_bindings:
                    undirected.add_edge(node_i, node_j)
        comp_of: dict[str, int] = {}
        for ci, comp in enumerate(nx.connected_components(undirected)):
            for node in comp:
                comp_of[node] = ci
        for i in output_roots:
            output_component[i] = comp_of.get(main_items[i][0], -1 - i)

    # Two output roots whose reach is a ROW STREAM (`upper(name)` beside
    # `quantity * 2`) recombine at FINAL as one row stream, and the axis that
    # pairs them (the fact's FK) is a datasource fact no lineage edge shows:
    # sourced apart, the merge has nothing to join on (`ON 1=1`). Co-source
    # them whatever their component, as the direct spelling `name, quantity`
    # is (a leaf output has no reach, so it never splits). An aggregate
    # defines its own input domain and keeps its independent source, unless
    # its input is a row of the statement besides (`customer_id` beside
    # `count(customer_id) by *`).
    row_stream = [
        i in output_component
        and (
            not aggregate_reach[i]
            or _is_row_stream_output(
                main_items[i][0],
                concept_graph,
                concept_edges,
                concept_attrs,
                output_addresses,
            )
        )
        for i in range(n)
    ]
    related = [
        (i, j)
        for i in range(n)
        for j in range(i + 1, n)
        if reaches[i] & reaches[j]
        or (i in output_component and output_component[i] == output_component.get(j))
        or (row_stream[i] and row_stream[j])
        or (feeds_union[i] and feeds_union[j])
    ]
    return [
        [main_items[i] for i in member_indices]
        for member_indices in _components(n, related)
    ]


def _feeds_only_global_aggregates(
    node: str,
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    output_addresses: frozenset[str],
) -> bool:
    """Every path from `node` ends at a `by *` aggregate, through row-level
    BASIC/FILTER steps only: its rows exist only to be collapsed into a
    constant of the query, which the aggregate sources on its own."""
    visited = {node}
    stack = [node]
    while stack:
        cur = stack.pop()
        succs = list(concept_graph.successors(cur))
        if not succs or concept_attrs[cur].address in output_addresses:
            return False
        for nxt in succs:
            if edge_kind(concept_edges, cur, nxt) != EdgeKind.LINEAGE:
                return False
            data = concept_attrs[nxt]
            if data.derivation == Derivation.AGGREGATE:
                # only a written `by *` is a constant of the query; a bare
                # total is the select's own aggregate over its rows
                # (`select sum(pred)`, `SelectGrain.bare_is_select_grain`)
                if (
                    not set(data.grain_components) <= {ALL_ROWS_ADDRESS}
                    or not data.fixed_grain
                ):
                    return False
                continue
            if data.derivation not in (Derivation.BASIC, Derivation.FILTER):
                return False
            if nxt not in visited:
                visited.add(nxt)
                stack.append(nxt)
    return True


def _global_aggregate_inputs(
    main_items: list[NodeItem],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    output_addresses: frozenset[str],
) -> list[NodeItem]:
    """Roots read only by `by *` aggregates, on no table the other roots read.
    Left in the row stream they cross-join their whole table onto its rows
    (#953); the aggregate's one-row result is all the rows need. Under a WHERE
    see `_peel_exact_under_where`."""
    peeled = [
        (n, d)
        for n, d in main_items
        if not (d.scan_filtered or d.relation_only)
        and d.datasource_bindings
        and _feeds_only_global_aggregates(
            n, concept_graph, concept_edges, concept_attrs, output_addresses
        )
    ]
    peeled_nodes = {n for n, _ in peeled}
    rest = [(n, d) for n, d in main_items if n not in peeled_nodes]
    rest_bindings = {b for _, d in rest for b in d.datasource_bindings}
    if not rest or any(d.datasource_bindings & rest_bindings for _, d in peeled):
        return []
    if not _peel_exact_under_where(
        peeled, rest, concept_graph, concept_edges, concept_attrs, output_addresses
    ):
        return []
    return peeled


def _peel_exact_under_where(
    peeled: list[NodeItem],
    rest: list[NodeItem],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    output_addresses: frozenset[str],
) -> bool:
    """A WHERE filters every scan of the select's rows, and a select's
    aggregate reads the rows it kept. When no atom reads a peeled table and it
    shares no column the row stream could join it on, those rows are the
    peeled table's whole whenever the row stream kept a row, and none
    otherwise: an existence gate. A selected value that reads the total
    beside the kept rows holds it already (no kept row, no value), so when
    every one does the peel reads the table once instead of once per kept row
    (#972). A value of the total alone keeps the scan in the row stream."""
    where_args = [n for n, d in concept_attrs.items() if d.where_row_arg]
    if not where_args:
        return True
    filtered = set(where_args).union(
        *(nx.ancestors(concept_graph, n) for n in where_args)
    )
    peeled_bindings = frozenset().union(*(d.datasource_bindings for _, d in peeled))
    if filtered & {n for n, _ in peeled} or any(
        concept_attrs[n].datasource_bindings & peeled_bindings for n in filtered
    ):
        return False
    peeled_columns = frozenset().union(*(d.datasource_columns for _, d in peeled))
    if any(d.datasource_columns & peeled_columns for _, d in rest):
        return False
    reads_total, reads_rows = (
        _selected_readers(
            [n for n, _ in items],
            concept_graph,
            concept_edges,
            concept_attrs,
            output_addresses,
        )
        for items in (peeled, rest)
    )
    return bool(reads_total) and reads_total <= reads_rows


def _selected_readers(
    nodes: list[str],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    output_addresses: frozenset[str],
) -> set[str]:
    """The selected columns whose lineage reads any of `nodes`.
    `output_addresses` holds the WHERE's row arguments too; those are not
    selected values."""
    seen = set(nodes).union(
        *(
            _lineage_reach(n, lineage_successors, concept_graph, concept_edges)
            for n in nodes
        )
    )
    return {
        n
        for n in seen
        if concept_attrs[n].address in output_addresses
        and not concept_attrs[n].where_row_arg
    }


def _holds(
    members: list[NodeItem],
    concept_attrs: dict[str, ConceptAttrs],
    test: Callable[[ConceptAttrs], bool],
) -> bool:
    return any(test(concept_attrs[node]) for node, _ in members)


def _filtered_scans_with_filter_columns(
    components: list[list[NodeItem]], concept_attrs: dict[str, ConceptAttrs]
) -> list[list[NodeItem]]:
    """A scan-filtered root reads the WHERE on its own scan, so it is sourced
    with the WHERE's row columns: split from them, its scan hosts no atom and
    reads every row (`min(min(height) by tree_form)` beside `min(min(height)
    by city)` under `city = 'A'`)."""
    filtered = [_holds(m, concept_attrs, lambda a: a.scan_filtered) for m in components]
    filters = [_holds(m, concept_attrs, lambda a: a.where_row_arg) for m in components]
    if not any(f and not w for f, w in zip(filtered, filters)):
        return components
    merged = [m for m, f, w in zip(components, filtered, filters) if f or w]
    if len(merged) < 2:
        return components
    rest = [m for m, f, w in zip(components, filtered, filters) if not (f or w)]
    return [[item for members in merged for item in members], *rest]


def partition_roots(
    items: list[NodeItem],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    primary_group: dict[str, str],
    ensure_assigned: EnsureAssignedFn,
    output_addresses: frozenset[str] = frozenset(),
) -> list[GroupBucket]:
    """Partition root concepts into independent scan buckets per label.

    Two roots co-source iff their downstream lineage reach overlaps: a
    shared consumer (aggregate, basic, etc.) means the planner has to
    join them into one row stream. Disjoint reach means the two roots
    feed entirely separate sub-plans that only meet again at a later
    cross-product merge (e.g. two global aggregates joined 1=1), so
    they can be sourced independently.

    Splitting is only safe when the concept graph proves independence:
    every non-existence root has non-zero reach. A zero-reach root is a
    leaf SELECT output or an inline WHERE filter arg (``date.year=2024``)
    whose membership in a row stream is implicit: the d1 condition node
    isn't materialized in the concept graph and the datasource FK that
    ties it to a fact table lives elsewhere. With no signal to place it,
    everything stays in one bucket so no filter is dropped.

    Reach is also extended to the query's output projection: roots whose
    lineage lands in ``output_addresses`` all recombine at the FINAL node,
    so they are co-sourced even when their concept-graph reach is disjoint.
    Every output column is a separate leaf here (the projection that unifies
    them is only added to the group graph later), so without this they would
    split and the FINAL merge would degrade to a ``1=1`` cross product (four
    customer attributes, each feeding only its own SELECT alias).

    Existence-only roots (concepts only referenced as existence_args)
    always stay in their own buckets so the existence wiring picks them
    up as side-channel sources.
    """
    buckets: list[GroupBucket] = []
    for label_value, sub_items in _split_by_label(items).items():
        existence_only_nodes = [(n, d) for n, d in sub_items if d.existence_only]
        main_items = [(n, d) for n, d in sub_items if not d.existence_only]
        # Single-row roots (a grand-total precomputed value, a constant, a `<*>`
        # watermark) join by cross product, never by a key. Co-bucketing one with
        # keyed roots from a different scan yields an unsourceable group (no join
        # key links a grand-total table to a dimension), and a zero-reach
        # single-row output would otherwise force the
        # conservative single-bucket bailout to swallow every keyed root. Pull
        # them out: same-source single rows still share one scan (one bucket
        # together), and the FINAL node cross-joins them onto the keyed plan.
        # A scan-filtered root reads a finer table under the WHERE, so it is
        # no single-row scan: it stays with the filter columns.
        single_row_items = [
            (n, d)
            for n, d in main_items
            if d.granularity == Granularity.SINGLE_ROW and not d.scan_filtered
        ]
        main_items = [
            (n, d)
            for n, d in main_items
            if d.granularity != Granularity.SINGLE_ROW or d.scan_filtered
        ]

        coarse_items = [(n, d) for n, d in main_items if d.coarse_rolled]
        main_items = [(n, d) for n, d in main_items if not d.coarse_rolled]
        global_inputs = _global_aggregate_inputs(
            main_items, concept_graph, concept_edges, concept_attrs, output_addresses
        )
        peeled_nodes = {n for n, _ in global_inputs}
        main_items = [(n, d) for n, d in main_items if n not in peeled_nodes]

        component_groups: list[list[NodeItem]] = []
        for side_items in _relation_side_partitions(
            main_items, concept_graph, concept_edges
        ):
            component_groups.extend(
                _cosource_component_groups(
                    side_items,
                    concept_graph,
                    concept_edges,
                    concept_attrs,
                    output_addresses,
                )
            )

        component_groups = _filtered_scans_with_filter_columns(
            component_groups, concept_attrs
        )
        kind = "split" if len(component_groups) > 1 else None
        buckets += [
            _component_bucket(label_value, members, kind)
            for members in component_groups
        ]
        buckets += [
            _component_bucket(label_value, members, "global_input")
            for members in _cosource_component_groups(
                global_inputs,
                concept_graph,
                concept_edges,
                concept_attrs,
                output_addresses,
            )
        ]
        coarse_by_grain: dict[frozenset[str], list[NodeItem]] = defaultdict(list)
        for node, data in coarse_items:
            coarse_by_grain[data.grain_components].append((node, data))
        for grain, members in coarse_by_grain.items():
            bucket = _root_bucket(
                label_value,
                RootReason.ENTITY,
                members,
                f"dim:{'|'.join(sorted(grain))}",
            )
            bucket.dim_keys = grain
            buckets.append(bucket)
        if single_row_items:
            # Distinct id from the keyed `grp:root:root:∅` bucket so they stay
            # separate scans the FINAL node cross-joins.
            buckets.append(
                _root_bucket(
                    label_value, RootReason.SINGLE_ROW, single_row_items, "single_row"
                )
            )
        buckets += [
            _root_bucket(
                label_value,
                RootReason.EXISTENCE,
                [(node, data)],
                f"existence:{data.address}",
            )
            for node, data in existence_only_nodes
        ]
    return buckets


def _component_bucket(
    label: str, members: list[NodeItem], kind: str | None
) -> GroupBucket:
    """A co-sourced scan: the label's one row stream, or under `kind` one of
    several, told apart by its members."""
    if kind is None:
        return _root_bucket(label, RootReason.ROW_STREAM, members)
    digest = _sig_digest([data.address for _, data in members])
    return _root_bucket(label, RootReason.COMPONENT, members, f"{kind}:{digest}")


def _root_bucket(
    label: str,
    reason: RootReason,
    members: list[NodeItem],
    discriminator: str = "",
) -> GroupBucket:
    bucket = _bucket_for(DepthLabel.ROOT, Derivation.ROOT, frozenset(), label=label)
    bucket.reason = reason
    bucket.discriminator = discriminator
    _add_members(bucket, members)
    return bucket


def _stop_signature(
    node: str,
    recurse_through: Derivation,
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    primary_group: dict[str, str],
    ensure_assigned: EnsureAssignedFn,
) -> frozenset[str]:
    """Walk lineage ancestors upward, recursing through nodes whose
    derivation matches ``recurse_through`` and stopping at any other
    derivation. Returns the set of primary-group ids the stopping nodes
    belong to. Two nodes with the same stop-set read from the same
    upstream transforms, regardless of how many same-derivation hops sit
    between them.

    Triggers ``ensure_assigned(derivation)`` on each stop so we can look
    up its primary_group id without privileging non-recursing derivations
    in the orchestrator's call order."""
    sig: set[str] = set()
    visited: set[str] = {node}
    stack: list[str] = [node]
    while stack:
        current = stack.pop()
        for pred in lineage_predecessors(concept_graph, concept_edges, current):
            if pred in visited:
                continue
            visited.add(pred)
            pred_derivation = concept_attrs[pred].derivation
            if pred_derivation == recurse_through:
                stack.append(pred)
                continue
            ensure_assigned(pred_derivation)
            gid = primary_group.get(pred)
            if gid is not None:
                sig.add(gid)
    return frozenset(sig)


def _feeds_extra_signature_group(
    node: str,
    extra_gids: frozenset[str],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    primary_group: dict[str, str],
) -> bool:
    """True if `node` is a lineage ANCESTOR of any concept in `extra_gids`,
    i.e. `node` feeds a (non-BASIC, barrier) group the larger-signature node
    consumes. Subset-nest-merging would then place a producer and a consumer
    of that barrier in one bucket, which 2-cycles through the barrier (a BASIC
    feeds a window that a second BASIC reads; a naive nest would merge both
    into one group). Walks lineage out-edges (parent -> child = input ->
    consumer)."""
    targets = {n for n, g in primary_group.items() if g in extra_gids}
    return bool(targets) and not targets.isdisjoint(
        _lineage_reach(node, lineage_successors, concept_graph, concept_edges)
    )


def _can_merge_nested_signatures(
    left: frozenset[str], right: frozenset[str], root_gids: set[str]
) -> bool:
    if not left or not right:
        return False
    if left <= right:
        smaller = left
    elif right <= left:
        smaller = right
    else:
        return False
    return not smaller & root_gids


def _lineage_reach(
    node: str,
    step: Callable[[nx.DiGraph, EdgeMap, str], Iterable[str]],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
) -> frozenset[str]:
    reach: set[str] = set()
    stack = [node]
    while stack:
        for nxt in step(concept_graph, concept_edges, stack.pop()):
            if nxt not in reach:
                reach.add(nxt)
                stack.append(nxt)
    return frozenset(reach)


def _drop_bypassed_nests(
    merged: list[tuple[int, int]],
    nodes: list[str],
    sigs: list[frozenset[str]],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
) -> list[tuple[int, int]]:
    """Split a bucket whose producer reaches another member through a concept
    the bucket leaves out: the bucket would feed and read that concept's group
    (`b_all` by `*` beside `b_all * x`, with a `case ... b_all` at a coarser
    grain between them). The producer's nested-signature merges go first, its
    equal-signature ones only if those leave the bypass in place. Repeated to
    fixpoint, since a split can strand another member's path."""
    if not any(sigs[i] != sigs[j] for i, j in merged):
        return merged
    paired = {k for pair in merged for k in pair}
    down = {
        k: _lineage_reach(nodes[k], lineage_successors, concept_graph, concept_edges)
        for k in paired
    }
    up = {
        k: _lineage_reach(nodes[k], lineage_predecessors, concept_graph, concept_edges)
        for k in paired
    }
    while True:
        producers = _bypassing_producers(merged, nodes, down, up)
        if not producers:
            return merged
        incident = [pair for pair in merged if set(pair) & producers]
        nested = [pair for pair in incident if sigs[pair[0]] != sigs[pair[1]]]
        dropped = set(nested or incident)
        merged = [pair for pair in merged if pair not in dropped]


def _bypassing_producers(
    merged: list[tuple[int, int]],
    nodes: list[str],
    down: dict[int, frozenset[str]],
    up: dict[int, frozenset[str]],
) -> set[int]:
    out: set[int] = set()
    for component in _components(len(nodes), merged):
        if len(component) < 2:
            continue
        names = {nodes[k] for k in component}
        for a in component:
            for b in component:
                if a != b and nodes[b] in down[a] and (down[a] & up[b]) - names:
                    out.add(a)
    return out


def _partition_by_signature_and_grain(
    items: list[NodeItem],
    own_derivation: Derivation,
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    primary_group: dict[str, str],
    ensure_assigned: EnsureAssignedFn,
    extra_signature: Callable[[str], frozenset[str]] | None = None,
    allow_signature_subset: bool = False,
    signature_exempt: frozenset[str] = frozenset(),
    merge_terminal_siblings: bool = False,
    output_addresses: frozenset[str] = frozenset(),
) -> list[GroupBucket]:
    """Generic signature+grain bucketing. Used for derivations whose
    upstream identity should split buckets even when row-shape (depth /
    grain) matches: BASIC (rename chains, derived columns) and FILTER
    (specialized basics, same scan-compatibility story).

    Within a label, two nodes share a bucket iff their stop-signatures
    are equal AND one's grain is a subset of the other's; the bucket grain
    is the union, so a chain of derivations at progressively finer grains
    still co-sources when they share an upstream.

    ``signature_exempt`` nodes waive only the signature test (the grain
    test still applies): see `partition_filters_by_signature`.

    ``merge_terminal_siblings`` waives the grain test for an equal-signature
    pair nothing downstream reads and whose grain keys are all statement
    outputs: FINAL already relates those keys, so one projection at the union
    grain renders both without asking the upstream for a new key relation
    (a day key beside a warehouse key would force the fact that links them)."""
    buckets: list[GroupBucket] = []
    for label_value, sub_items in _split_by_label(items).items():
        n = len(sub_items)
        nodes = [node for node, _ in sub_items]
        sigs = []
        for node in nodes:
            sig = _stop_signature(
                node,
                own_derivation,
                concept_graph,
                concept_edges,
                concept_attrs,
                primary_group,
                ensure_assigned,
            )
            if extra_signature is not None:
                sig |= extra_signature(node)
            sigs.append(sig)
        # after the signatures: reading a stop is what assigns its group
        root_gids = (
            {
                gid
                for node, gid in primary_group.items()
                if concept_attrs[node].derivation == Derivation.ROOT
            }
            if allow_signature_subset
            else set()
        )
        grains = [data.grain_components for _, data in sub_items]
        terminal = [
            merge_terminal_siblings
            and data.grain_components <= output_addresses
            and not lineage_successors(concept_graph, concept_edges, node)
            for node, data in sub_items
        ]
        pins = [data.grain_pin_anchors for _, data in sub_items]
        merged: list[tuple[int, int]] = []
        for i in range(n):
            for j in range(i + 1, n):
                # a pinned value evaluates on the select's rows, which a
                # sibling at its reads' grain may be kept off (`solid_groups`)
                if pins[i] != pins[j]:
                    continue
                signatures_match = sigs[i] == sigs[j] or (
                    nodes[i] in signature_exempt and nodes[j] in signature_exempt
                )
                signatures_nest = (
                    allow_signature_subset
                    and _can_merge_nested_signatures(sigs[i], sigs[j], root_gids)
                )
                if not signatures_match and not signatures_nest:
                    continue
                if signatures_nest and not signatures_match:
                    smaller_idx = i if sigs[i] <= sigs[j] else j
                    extra = (sigs[i] | sigs[j]) - sigs[smaller_idx]
                    if _feeds_extra_signature_group(
                        nodes[smaller_idx],
                        extra,
                        concept_graph,
                        concept_edges,
                        primary_group,
                    ):
                        continue
                if (
                    grains[i] <= grains[j]
                    or grains[j] <= grains[i]
                    or (sigs[i] == sigs[j] and terminal[i] and terminal[j])
                ):
                    merged.append((i, j))
        merged = _drop_bypassed_nests(merged, nodes, sigs, concept_graph, concept_edges)

        for member_indices in _components(n, merged):
            merged_grain: frozenset[str] = frozenset().union(
                *(grains[i] for i in member_indices)
            )
            depths = {sub_items[i][1].depth_label for i in member_indices}
            group_depth = (
                DepthLabel.D1 if DepthLabel.D1 in depths else next(iter(depths))
            )
            shared_sig: frozenset[str] = frozenset().union(
                *(sigs[i] for i in member_indices)
            )
            # Stable signature representation: hash the sorted stop-set so
            # two component-equal sigs produce the same discriminator and
            # two disjoint sigs produce different ones. Group ids include
            # the discriminator so colliding (label, depth, grain) buckets
            # stay distinct.
            discriminator = f"sig:{_sig_digest(shared_sig)}"
            if pin := pins[member_indices[0]]:
                discriminator += f":pin:{'|'.join(sorted(pin))}"
            bucket = _bucket_for(
                depth_label=group_depth,
                derivation=own_derivation,
                grain=merged_grain,
                label=label_value,
            )
            bucket.discriminator = discriminator
            _add_members(bucket, [sub_items[i] for i in member_indices])
            buckets.append(bucket)
    return buckets


def partition_windows(
    items: list[NodeItem],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    primary_group: dict[str, str],
    ensure_assigned: EnsureAssignedFn,
    output_addresses: frozenset[str] = frozenset(),
) -> list[GroupBucket]:
    """Same-grain windows co-source, except when one reads another's output.
    SQL forbids a window function inside another window's OVER clause
    (`sum(x) over (order by rank() over (...))` is a parser error), so a window
    whose partition/order lineage includes a sibling window needs its own CTE
    over the producer's. Layering is the aggregate rule's producer/consumer
    split applied to the default depth+grain partition."""
    buckets: list[GroupBucket] = []
    for (label, depth_label, grain, grouping_mode), members in _by_shape(items).items():
        for index, layer in enumerate(
            _lineage_layers(members, concept_graph, concept_edges)
        ):
            bucket = _bucket_for(
                depth_label, layer[0][1].derivation, grain, label=label
            )
            # Layer 0 keeps the undiscriminated group id; only later layers are
            # discriminated.
            _apply_grouping_mode(
                bucket, grouping_mode, *(() if index == 0 else (f"wlayer:{index}",))
            )
            _add_members(bucket, layer)
            buckets.append(bucket)
    return buckets


def partition_basics_by_signature(
    items: list[NodeItem],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    primary_group: dict[str, str],
    ensure_assigned: EnsureAssignedFn,
    output_addresses: frozenset[str] = frozenset(),
) -> list[GroupBucket]:
    """Group BASICs by `(label, stop-signature, grain-subset)`. See
    `_partition_by_signature_and_grain` for the full story; BASIC's
    stop walks through other BASICs and stops at any non-BASIC."""
    return _partition_by_signature_and_grain(
        items,
        Derivation.BASIC,
        concept_graph,
        concept_edges,
        concept_attrs,
        primary_group,
        ensure_assigned,
        allow_signature_subset=True,
        merge_terminal_siblings=True,
        output_addresses=output_addresses,
    )


def partition_filters_by_signature(
    items: list[NodeItem],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    primary_group: dict[str, str],
    ensure_assigned: EnsureAssignedFn,
    output_addresses: frozenset[str] = frozenset(),
) -> list[GroupBucket]:
    """FILTERs are specialized BASICs: same scan-compatibility story.
    Two filters that look identical by depth/grain but read from
    disjoint upstreams (one over a basic chain, one over customer roots)
    should not be co-sourced; their disjoint parent groups would form a
    back-edge through any shared downstream consumer.

    That back-edge risk needs a downstream consumer to route through.
    A FILTER that is a projected output AND a lineage sink has none (its
    only consumer is the FINAL merge, which is a sink), so such filters
    recombine into one row anyway and are exempted from the signature
    split (they still must be grain-comparable). Mirrors `partition_roots`'
    output-convergence co-sourcing. Without it, a membership subselect
    requesting several cross-grain `_virt_filter` concepts (HAVING-derived
    existence feeders) sources one CTE per concept plus a merge, instead of
    one feeder CTE."""

    # A FILTER concept that is itself a semijoin RHS (an existence SOURCE, with
    # an outgoing EXISTENCE edge: `pcid in store_buyers`) is functionally an
    # independent sub-query. Co-bucketing two of them forces one shared scan
    # carrying two mutually-exclusive predicates, which can only render as
    # row-preserving CASE columns, blocking predicate pushdown and dim pruning.
    # Give each its own bucket so it sources as a single-predicate WHERE
    # sub-query (mirrors the ROOT `existence:` solos).
    solo_items: list[NodeItem] = []
    shared_items: list[NodeItem] = []
    for node, data in items:
        if feeds_existence(concept_graph, concept_edges, node):
            solo_items.append((node, data))
        else:
            shared_items.append((node, data))

    # `output_addresses` folds in condition args, so a D1 (WHERE-recursion)
    # filter can carry a projected address without being a projected OUTPUT;
    # exempting one would waive the signature split for a node that does have a
    # downstream consumer.
    output_sinks = frozenset(
        node
        for node, data in shared_items
        if concept_attrs[node].address in output_addresses
        and data.depth_label != DepthLabel.D1
        and concept_graph.out_degree(node) == 0
    )

    buckets = _partition_by_signature_and_grain(
        shared_items,
        Derivation.FILTER,
        concept_graph,
        concept_edges,
        concept_attrs,
        primary_group,
        ensure_assigned,
        extra_signature=partial(
            _existence_signature, concept_graph, concept_edges, concept_attrs
        ),
        signature_exempt=output_sinks,
    )
    for node, data in solo_items:
        solo = _bucket_for(
            depth_label=data.depth_label,
            derivation=Derivation.FILTER,
            grain=data.grain_components,
            label=data.label,
        )
        solo.discriminator = f"existence:{concept_attrs[node].address}"
        _add_member(solo, node, data)
        buckets.append(solo)
    return buckets


def _existence_signature(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    node: str,
) -> frozenset[str]:
    return frozenset(
        f"exist:{concept_attrs[pred].address}"
        for pred, _ in concept_graph.in_edges(node)
        if edge_kind(concept_edges, pred, node) == EdgeKind.EXISTENCE
    )


def _partition_by_scope(
    items: list[NodeItem],
    derivation: Derivation,
    key_of: Callable[[str, ConceptAttrs], tuple[object, str | None]],
) -> list[GroupBucket]:
    """One bucket per `(scope, key_of(...))`, never per depth: the members are
    one population whichever phase reads them. Keyed on the SCOPE, not the
    full label, so the `@condition` phase suffix does not split a SELECT read
    from a WHERE read. The bucket surfaces at its most-downstream member depth
    so it produces the SELECT output AND carries the WHERE, and its grain is
    the union of its members' (same rows, so a wider grain only widens
    ride-through within the one CTE). `key_of` also names the discriminator."""
    members_by_key: dict[tuple, list[NodeItem]] = defaultdict(list)
    discriminators: dict[tuple, str | None] = {}
    for node, data in items:
        key, discriminator = key_of(node, data)
        full_key = (_scope_and_phase(data.label)[0], key, data.grouping_mode)
        members_by_key[full_key].append((node, data))
        discriminators[full_key] = discriminator
    buckets: list[GroupBucket] = []
    for full_key, members in members_by_key.items():
        scope, _, grouping_mode = full_key
        depths = {d.depth_label for _, d in members}
        depth_label = (
            DepthLabel.STAR
            if DepthLabel.STAR in depths
            else DepthLabel.D0 if DepthLabel.D0 in depths else DepthLabel.D1
        )
        bucket = _bucket_for(depth_label, derivation, frozenset(), label=scope)
        if (discriminator := discriminators[full_key]) is not None:
            bucket.discriminator = discriminator
        _apply_grouping_mode(bucket, grouping_mode)
        for node, data in members:
            bucket.grain_components |= data.grain_components
            _add_member(bucket, node, data)
        buckets.append(bucket)
    return buckets


def _rowset_key(node: str, data: ConceptAttrs) -> tuple[object, str | None]:
    assert data.rowset_name is not None
    return data.rowset_name, f"rowset:{data.rowset_name}"


def _constant_key(node: str, data: ConceptAttrs) -> tuple[object, str | None]:
    return data.grain_components, None


def _union_arms_key(
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    node: str,
    data: ConceptAttrs,
) -> tuple[object, str | None]:
    arms = frozenset(
        _scope_and_phase(concept_attrs[pred].label)[0]
        for pred in lineage_predecessors(concept_graph, concept_edges, node)
    )
    return arms, "|".join(sorted(arms))


def partition_rowsets(
    items: list[NodeItem],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    primary_group: dict[str, str],
    ensure_assigned: EnsureAssignedFn,
    output_addresses: frozenset[str] = frozenset(),
) -> list[GroupBucket]:
    """Every handle of one rowset shares a row population (the rowset is one
    sub-query, planned in full by `gen_rowset`), so they all bucket into a
    single boundary group by rowset identity: never per-column grain (which
    the FINAL node would have to rejoin on grain keys, degrading to `1=1` for
    grain-mismatched contributors) and never per-depth (an arm's `count(...)
    by dims` reads the d0 dim handles; a split d1 handle group strands its
    per-arm filter at FINAL and the aggregate counts unfiltered rows)."""
    return _partition_by_scope(items, Derivation.ROWSET, _rowset_key)


def partition_constants(
    items: list[NodeItem],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    primary_group: dict[str, str],
    ensure_assigned: EnsureAssignedFn,
    output_addresses: frozenset[str] = frozenset(),
) -> list[GroupBucket]:
    """A constant is the same value at every grain and phase, so a constant
    referenced in both the SELECT (d*) and a WHERE (d1) is one population.
    Split by depth, a constant-only WHERE strands on the upstream d1
    condition-feeder group whose filtered output the downstream SELECT
    constant simply rebuilds (`gen_constant` ignores parents), dropping the
    filter (`select today where date_add(current_date(), day, -30) < today`).
    Keyed by grain too."""
    return _partition_by_scope(items, Derivation.CONSTANT, _constant_key)


def partition_unions(
    items: list[NodeItem],
    concept_graph: nx.DiGraph,
    concept_edges: EdgeMap,
    concept_attrs: dict[str, ConceptAttrs],
    primary_group: dict[str, str],
    ensure_assigned: EnsureAssignedFn,
    output_addresses: frozenset[str] = frozenset(),
) -> list[GroupBucket]:
    """Sibling unions over one arm family stack into ONE UnionNode: key on
    the arm scopes, never grain. The family's members can differ in grain
    (`union(amt, zero)` is at `k1`, `union(k1, k2)` at `k1|k2`), and a
    per-grain split would hand the FINAL two stacked sources to join on
    nothing. The arm scopes are the labels of the union's lineage parents
    (see `union_arms`)."""
    return _partition_by_scope(
        items,
        Derivation.UNION,
        partial(_union_arms_key, concept_graph, concept_edges, concept_attrs),
    )


# Per-derivation registry. Any derivation not in here uses the default rule.
GROUPING_RULES: dict[Derivation, PartitionFn] = {
    Derivation.ROOT: partition_roots,
    Derivation.BASIC: partition_basics_by_signature,
    Derivation.FILTER: partition_filters_by_signature,
    Derivation.ROWSET: partition_rowsets,
    Derivation.AGGREGATE: partition_aggregates,
    Derivation.CONSTANT: partition_constants,
    Derivation.WINDOW: partition_windows,
    Derivation.UNION: partition_unions,
}

DEFAULT_RULE: PartitionFn = partition_by_depth_and_grain
