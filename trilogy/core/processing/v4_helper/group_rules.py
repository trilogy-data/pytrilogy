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
from collections.abc import Callable, Iterable

from trilogy.core import graph as nx
from trilogy.core.enums import (
    AggregateGroupingMode,
    Derivation,
    Granularity,
    Purpose,
)

from .concept_graph import _scope_and_phase
from .constants import DepthLabel, EdgeKind
from .edges import EdgeMap, edge_kind
from .models import ConceptAttrs, GroupBucket, nulls_grouping_keys


def _sig_digest(sig_repr: str) -> str:
    """Stable across processes — group ids must not vary with hash salting."""
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
    (label, depth, grain) — one CTE cannot carry both GROUP BY clauses.
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
    of grain or derivation — they're different planning scopes."""
    by_label: dict[str, list[NodeItem]] = defaultdict(list)
    for node, data in items:
        by_label[data.label].append((node, data))
    return by_label


def _add_member(bucket: GroupBucket, node: str, data: ConceptAttrs) -> None:
    address = data.address
    bucket.primary_members.append(address)
    bucket.primary_node_ids.append(node)
    bucket.member_depths[address] = data.depth_label


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
    by_key: dict[
        tuple[str, DepthLabel, frozenset[str], AggregateGroupingMode | None],
        GroupBucket,
    ] = {}
    for node, data in items:
        depth_label = data.depth_label
        derivation = data.derivation
        grain = data.grain_components
        label = data.label
        grouping_mode = data.grouping_mode
        key = (label, depth_label, grain, grouping_mode)
        bucket = by_key.get(key)
        if bucket is None:
            bucket = _bucket_for(depth_label, derivation, grain, label=label)
            _apply_grouping_mode(bucket, grouping_mode)
            by_key[key] = bucket
        _add_member(bucket, node, data)
    return list(by_key.values())


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
    population*: members at the same output grain and mode whose stop-signatures
    are equal or nest assemble into one combined fact and roll up once. Equal
    signatures cover differing argument sub-grains over a shared root (q70:
    line-grain ``total_sum`` rides with store-grain dimensions); nesting covers a
    shared root plus same-grain derived transforms (q18: agg1-5 read row measures
    while agg6/7 read ``group(..) by order_number, item.id`` values at that same
    row grain). Splitting them into one ROLLUP CTE per source — rejoined on the
    grouping dims — is fragile (null-safety on rolled-up keys) and slower."""
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
    rowsets cannot share one input stream — the shared scan would cross-join
    the bodies and fan each argument by the other side's cardinality (the two
    global counts over an `except(...)` and a `union(...)` rowset). The walk
    stops AT a rowset member; the body behind it is a separate planning scope."""
    pops: set[str] = set()
    visited: set[str] = {node}
    stack: list[str] = [node]
    while stack:
        current = stack.pop()
        for pred, _ in concept_graph.in_edges(current):
            if edge_kind(concept_edges, pred, current) != EdgeKind.LINEAGE:
                continue
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
    concept_attrs: dict[str, ConceptAttrs],
) -> dict[AggKey, set[str]]:
    """Fold a coarser-input-grain bucket whose every member is a
    distinct-rewritable COUNT into a sibling bucket whose input grain nests
    it, returning the target-key -> member-address map of counts that must
    render COUNT(DISTINCT ...) there.

    Two same-output-grain aggregates normally split when their arguments need
    different row grains (`sum(return_quantity)` reads return lines,
    `count(order_id)` reads deduped orders — q83). But a count OF A KEY is
    DISTINCT-counting by definition, so its dedup folds into the aggregate and
    the split (sibling CTE + re-join at the output grain) is pure overhead.

    Nesting is LITERAL membership in the finer stream's row identity (its
    input grain plus the shared output grain — q83's `item.id` rides as the
    GROUP BY key beside the `item.sk`-grain rows). Deliberately NOT an FD
    closure: `post_id` FD-determines `user_id`, but a count of `user_id`
    beside post-grain sums must still count over the USER population, not the
    users reachable through posts."""
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
                # Only fold onto a genuine finer row stream — never onto a
                # bucket already at its output grain (no shared dedup to save,
                # and an out-grain stream rarely determines the counted key).
                if not target_grain or target_grain == target[2]:
                    continue
                if not (set(input_grain) <= set(target_grain) | set(target[2])):
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
    ancestors: dict[str, set[str]] = {}
    for node, _ in members:
        found: set[str] = set()
        visited: set[str] = {node}
        stack: list[str] = [node]
        while stack:
            current = stack.pop()
            for pred, _child in concept_graph.in_edges(current):
                if edge_kind(concept_edges, pred, current) != EdgeKind.LINEAGE:
                    continue
                if pred in visited:
                    continue
                visited.add(pred)
                if pred in addrs:
                    found.add(pred)
                stack.append(pred)
        ancestors[node] = found
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
    distinct_by_key = _fold_distinct_rewritable_buckets(entries, concept_attrs)
    buckets: list[GroupBucket] = []
    for key, members in entries.items():
        label, depth_label, grain, input_grain, populations = key
        layers = _lineage_layers(members, concept_graph, concept_edges)
        for layer_index, layer in enumerate(layers):
            bucket = _bucket_for(
                depth_label, layer[0][1].derivation, grain, label=label
            )
            segments: list[str] = []
            if input_grain and input_grain != grain:
                segments.append("input:" + "|".join(sorted(input_grain)))
            if populations:
                segments.append("pop:" + "|".join(sorted(populations)))
            # Layer 0 keeps the unsplit group id, so unaffected queries plan
            # byte-identically to before layering existed.
            if layer_index:
                segments.append(f"layer:{layer_index}")
            if segments:
                bucket.discriminator = ":".join(segments)
            if input_grain:
                bucket.aggregate_input_grain = input_grain
            layer_addrs = {data.address for _, data in layer}
            bucket.aggregate_distinct_addrs = (
                distinct_by_key.get(key, set()) & layer_addrs
            )
            for node, data in layer:
                _add_member(bucket, node, data)
            buckets.append(bucket)
    return buckets


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


def _property_key_pairs(main_items: list[NodeItem]) -> list[tuple[str, str]]:
    """(property root, key root) pairs among `main_items`: a property and its
    declared key sit on one entity row, so they co-source even when no lineage
    joins them. A key collapsed onto another identity (scoped join, `merge
    into`) is present under the canonical address only; the mate's pseudonyms
    declare the identity."""
    node_by_addr = {data.address: node for node, data in main_items}
    node_by_pseudonym: dict[str, str] = {}
    for node, data in main_items:
        for pseudonym in data.pseudonyms:
            node_by_pseudonym.setdefault(pseudonym, node)
    pairs: list[tuple[str, str]] = []
    for node, data in main_items:
        if data.purpose != Purpose.PROPERTY:
            continue
        for key_addr in data.keys:
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
    by_shape: dict[
        tuple[str, DepthLabel, frozenset[str], AggregateGroupingMode | None],
        list[NodeItem],
    ] = defaultdict(list)
    for node, data in items:
        by_shape[
            (data.label, data.depth_label, data.grain_components, data.grouping_mode)
        ].append((node, data))
    for (label, depth_label, grain, grouping_mode), members in by_shape.items():
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
            sig_repr = "|".join(sorted(shared_sig)) or "none"
            _apply_grouping_mode(bucket, grouping_mode, f"sig:{_sig_digest(sig_repr)}")
            for i in member_indices:
                node, data = members[i]
                if data.aggregate_input_grain:
                    bucket.aggregate_input_grain = frozenset(
                        set(bucket.aggregate_input_grain)
                        | set(data.aggregate_input_grain)
                    )
                _add_member(bucket, node, data)
            buckets.append(bucket)
    return buckets


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
    degrades the join to a cross product — no scan can render the computed
    member as a column). Property roots sharing a declared key are FD-related
    (one entity row, one scan) and stay together even when the key itself is
    not demanded. With no relation edges in the graph this returns a single
    partition, leaving the existing behavior untouched."""
    relation_pairs = {
        frozenset((u, v))
        for u, v in concept_graph.edges
        if edge_kind(concept_edges, u, v) == EdgeKind.RELATION
    }
    if not relation_pairs or len(main_items) < 2:
        return [main_items]
    adjacency: dict[str, set[str]] = defaultdict(set)
    for u, v in concept_graph.edges:
        if frozenset((u, v)) in relation_pairs:
            continue
        adjacency[u].add(v)
        adjacency[v].add(u)
    for node, key_node in _property_key_pairs(main_items):
        adjacency[node].add(key_node)
        adjacency[key_node].add(node)
    by_key: dict[str, list[str]] = defaultdict(list)
    for node, data in main_items:
        if data.purpose == Purpose.PROPERTY:
            for key_addr in data.keys:
                by_key[key_addr].append(node)
    for key_mates in by_key.values():
        for other in key_mates[1:]:
            adjacency[key_mates[0]].add(other)
            adjacency[other].add(key_mates[0])
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
    return partitions


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
    # d1→d0 ordering rides its own CONSTRAINT edge and is excluded here).
    reaches: list[set[str]] = []
    for node, _ in main_items:
        seen: set[str] = set()
        visited = {node}
        stack = [node]
        while stack:
            cur = stack.pop()
            for nxt in concept_graph.successors(cur):
                if nxt in visited:
                    continue
                if edge_kind(concept_edges, cur, nxt) != EdgeKind.LINEAGE:
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

    # Bail out to one bucket if any root has zero reach — see partition_roots.
    can_split = bool(main_items) and all(reaches)
    if not can_split:
        return [list(main_items)] if main_items else []

    n = len(main_items)
    # Co-source roots that converge at the query output projection: each
    # output root maps to its concept-graph component id, and roots sharing
    # one merge. `reaches` holds node ids; map to addresses to test membership.
    output_component: dict[int, int] = {}
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
        undirected = concept_graph.to_undirected()
        # A PROPERTY root and its KEY root are FD-related even when the
        # lineage graph never joins them (a pure two-alias projection
        # `select cust_id as x, cname as y` has one BASIC per root and
        # no shared consumer) — the table binding both is what relates
        # them, and splitting them cross-joins ON 1=1 (cartesian rows).
        # Properties only: two KEYS related through a fact FK (user_id
        # on posts) must NOT co-source — `count(user_id)` reads the
        # users table, not the post FK column's deduped domain.
        for node, key_node in _property_key_pairs(main_items):
            undirected.add_edge(node, key_node)
        # A PROPERTY root and another root BOUND BY THE SAME DATASOURCE sit on
        # one physical row stream (`select group_id as g, nullable_amount as v`
        # — the fact binds its FK column beside its property; each root feeds
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
        comp_of: dict[str, int] = {}
        for ci, comp in enumerate(nx.connected_components(undirected)):
            for node in comp:
                comp_of[node] = ci
        for i in output_roots:
            output_component[i] = comp_of.get(main_items[i][0], -1 - i)

    related = [
        (i, j)
        for i in range(n)
        for j in range(i + 1, n)
        if reaches[i] & reaches[j]
        or (i in output_component and output_component[i] == output_component.get(j))
    ]
    return [
        [main_items[i] for i in member_indices]
        for member_indices in _components(n, related)
    ]


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

    Two roots co-source iff their downstream lineage reach overlaps — a
    shared consumer (aggregate, basic, etc.) means the planner has to
    join them into one row stream. Disjoint reach means the two roots
    feed entirely separate sub-plans that only meet again at a later
    cross-product merge (e.g. two global aggregates joined 1=1), so
    they can be sourced independently.

    Splitting is only safe when the concept graph proves independence:
    every non-existence root has non-zero reach. A zero-reach root is a
    leaf SELECT output or an inline WHERE filter arg (``date.year=2024``)
    whose membership in a row stream is implicit — the d1 condition node
    isn't materialized in the concept graph and the datasource FK that
    ties it to a fact table lives elsewhere. With no signal to place it,
    we conservatively keep everything in one bucket: matches the prior
    behavior and avoids dropping filters (q08).

    Reach is also extended to the query's output projection: roots whose
    lineage lands in ``output_addresses`` all recombine at the FINAL node,
    so they are co-sourced even when their concept-graph reach is disjoint.
    Every output column is a separate leaf here (the projection that unifies
    them is only added to the group graph later), so without this they would
    split and the FINAL merge would degrade to a ``1=1`` cross product (q04:
    four customer attributes, each feeding only its own SELECT alias).

    Existence-only roots (concepts only referenced as existence_args)
    always stay in their own buckets so the existence wiring picks them
    up as side-channel sources (q16).
    """
    if not items:
        return []
    buckets: list[GroupBucket] = []
    for label_value, sub_items in _split_by_label(items).items():
        if not sub_items:
            continue
        addr_of = {node: data.address for node, data in sub_items}
        existence_only_nodes = [(n, d) for n, d in sub_items if d.existence_only]
        main_items = [(n, d) for n, d in sub_items if not d.existence_only]
        # Single-row roots (a grand-total precomputed value, a constant, a `<*>`
        # watermark) join by cross product, never by a key. Co-bucketing one with
        # keyed roots from a different scan yields an unsourceable group — no join
        # key links e.g. a `flight_count` grand-total table to a `carrier` dim,
        # and a zero-reach single-row output would otherwise force the
        # conservative single-bucket bailout to swallow every keyed root. Pull
        # them out: same-source single rows still share one scan (one bucket
        # together), and the FINAL node cross-joins them onto the keyed plan.
        single_row_items = [
            (n, d) for n, d in main_items if d.granularity == Granularity.SINGLE_ROW
        ]
        main_items = [
            (n, d) for n, d in main_items if d.granularity != Granularity.SINGLE_ROW
        ]

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

        multi = len(component_groups) > 1
        for members in component_groups:
            bucket = _bucket_for(
                depth_label=DepthLabel.ROOT,
                derivation=Derivation.ROOT,
                grain=frozenset(),
                label=label_value,
            )
            if multi:
                sig_repr = "|".join(sorted(addr_of[node] for node, _ in members))
                bucket.discriminator = f"split:{_sig_digest(sig_repr)}"
            for node, data in members:
                _add_member(bucket, node, data)
            buckets.append(bucket)

        if single_row_items:
            single_row_bucket = _bucket_for(
                depth_label=DepthLabel.ROOT,
                derivation=Derivation.ROOT,
                grain=frozenset(),
                label=label_value,
            )
            # Distinct id from the keyed `grp:root:root:∅` bucket so they stay
            # separate scans the FINAL node cross-joins.
            single_row_bucket.discriminator = "single_row"
            for node, data in single_row_items:
                _add_member(single_row_bucket, node, data)
            buckets.append(single_row_bucket)

        for node, data in existence_only_nodes:
            solo = _bucket_for(
                depth_label=DepthLabel.ROOT,
                derivation=Derivation.ROOT,
                grain=frozenset(),
                label=label_value,
            )
            solo.discriminator = f"existence:{addr_of[node]}"
            _add_member(solo, node, data)
            buckets.append(solo)
    return buckets


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
        for pred, _ in concept_graph.in_edges(current):
            if edge_kind(concept_edges, pred, current) != EdgeKind.LINEAGE:
                continue
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
    concept_attrs: dict[str, ConceptAttrs],
    primary_group: dict[str, str],
) -> bool:
    """True if `node` is a lineage ANCESTOR of any concept in `extra_gids` --
    i.e. `node` feeds a (non-BASIC, barrier) group the larger-signature node
    consumes. Subset-nest-merging would then place a producer and a consumer of
    that barrier in one bucket, which 2-cycles through the barrier (q2.1: the
    `*_sales` BASIC feeds the window the `*_increase` round-BASIC reads, so the
    naive nest merged both into one week_seq group). Walks lineage out-edges
    (parent -> child = input -> consumer)."""
    if not extra_gids:
        return False
    targets = {n for n, g in primary_group.items() if g in extra_gids}
    if not targets:
        return False
    visited: set[str] = {node}
    stack: list[str] = [node]
    while stack:
        cur = stack.pop()
        for _, succ in concept_graph.out_edges(cur):
            if edge_kind(concept_edges, cur, succ) != EdgeKind.LINEAGE:
                continue
            if succ in targets:
                return True
            if succ not in visited:
                visited.add(succ)
                stack.append(succ)
    return False


def _can_merge_nested_signatures(left: frozenset[str], right: frozenset[str]) -> bool:
    if not left or not right:
        return False
    if left <= right:
        smaller = left
    elif right <= left:
        smaller = right
    else:
        return False
    return not any(gid.startswith("grp:root") for gid in smaller)


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
) -> list[GroupBucket]:
    """Generic signature+grain bucketing. Used for derivations whose
    upstream identity should split buckets even when row-shape (depth /
    grain) matches: BASIC (rename chains, derived columns) and FILTER
    (specialized basics — same scan-compatibility story).

    Within a label, two nodes share a bucket iff their stop-signatures
    are equal AND one's grain is a subset of the other's. The grain-
    subset union preserves the historical "widen to the superset" merge
    so a chain of derivations at progressively finer grains still
    co-sources when they share an upstream.

    ``signature_exempt`` nodes waive only the signature test (the grain
    test still applies): see `partition_filters_by_signature`."""
    if not items:
        return []
    buckets: list[GroupBucket] = []
    for label_value, sub_items in _split_by_label(items).items():
        n = len(sub_items)
        if not n:
            continue
        sigs = []
        for node, _ in sub_items:
            sig = set(
                _stop_signature(
                    node,
                    own_derivation,
                    concept_graph,
                    concept_edges,
                    concept_attrs,
                    primary_group,
                    ensure_assigned,
                )
            )
            if extra_signature is not None:
                sig |= set(extra_signature(node))
            sigs.append(frozenset(sig))
        grains = [sub_items[i][1].grain_components for i in range(n)]
        merged: list[tuple[int, int]] = []
        for i in range(n):
            for j in range(i + 1, n):
                signatures_match = sigs[i] == sigs[j] or (
                    sub_items[i][0] in signature_exempt
                    and sub_items[j][0] in signature_exempt
                )
                signatures_nest = (
                    allow_signature_subset
                    and _can_merge_nested_signatures(sigs[i], sigs[j])
                )
                if not signatures_match and not signatures_nest:
                    continue
                if signatures_nest and not signatures_match:
                    smaller_idx = i if sigs[i] <= sigs[j] else j
                    extra = (sigs[i] | sigs[j]) - sigs[smaller_idx]
                    if _feeds_extra_signature_group(
                        sub_items[smaller_idx][0],
                        extra,
                        concept_graph,
                        concept_edges,
                        concept_attrs,
                        primary_group,
                    ):
                        continue
                if grains[i] <= grains[j] or grains[j] <= grains[i]:
                    merged.append((i, j))

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
            sig_repr = "|".join(sorted(shared_sig)) or "none"
            discriminator = f"sig:{_sig_digest(sig_repr)}"
            bucket = _bucket_for(
                depth_label=group_depth,
                derivation=own_derivation,
                grain=merged_grain,
                label=label_value,
            )
            bucket.discriminator = discriminator
            for i in member_indices:
                node, data = sub_items[i]
                _add_member(bucket, node, data)
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
    by_key: dict[
        tuple[str, DepthLabel, frozenset[str], AggregateGroupingMode | None],
        list[NodeItem],
    ] = defaultdict(list)
    for node, data in items:
        by_key[
            (data.label, data.depth_label, data.grain_components, data.grouping_mode)
        ].append((node, data))
    for (label, depth_label, grain, grouping_mode), members in by_key.items():
        for index, layer in enumerate(
            _lineage_layers(members, concept_graph, concept_edges)
        ):
            if not layer:
                continue
            bucket = _bucket_for(
                depth_label, layer[0][1].derivation, grain, label=label
            )
            # Layer 0 keeps the historical (undiscriminated) group id so the
            # single-layer case -- every query without window-over-window -- is
            # byte-identical to the default rule.
            _apply_grouping_mode(
                bucket, grouping_mode, *(() if index == 0 else (f"wlayer:{index}",))
            )
            for node, data in layer:
                _add_member(bucket, node, data)
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
    `_partition_by_signature_and_grain` for the full story — BASIC's
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
    """FILTERs are specialized BASICs — same scan-compatibility story.
    Two filters that look identical by depth/grain but read from
    disjoint upstreams (e.g. q08's `_virt_filter_zips` over a basic
    chain vs. `_virt_filter_id` over customer roots) should not be
    co-sourced; their disjoint parent groups would form a back-edge
    through any shared downstream consumer.

    That back-edge risk needs a downstream consumer to route through.
    A FILTER that is a projected output AND a lineage sink has none —
    its only consumer is the FINAL merge, which is a sink — so such
    filters recombine into one row anyway and are exempted from the
    signature split (they still must be grain-comparable). Mirrors
    `partition_roots`' output-convergence co-sourcing. Without it, a
    membership subselect requesting several cross-grain `_virt_filter`
    concepts (HAVING-derived existence feeders) sources one CTE per
    concept plus a merge, instead of one feeder CTE."""

    def existence_signature(node: str) -> frozenset[str]:
        return frozenset(
            f"exist:{concept_attrs[pred].address}"
            for pred, _ in concept_graph.in_edges(node)
            if edge_kind(concept_edges, pred, node) == EdgeKind.EXISTENCE
        )

    # A FILTER concept that is itself a semijoin RHS -- an existence SOURCE, with
    # an outgoing EXISTENCE edge (`pcid in store_buyers`) -- is functionally an
    # independent sub-query. Co-bucketing two of them (q10's `store_buyers` +
    # `webcat_buyers`) forces one shared scan carrying two mutually-exclusive
    # predicates, which can only render as row-preserving CASE columns -- blocking
    # predicate pushdown and dim pruning. Give each its own bucket so it sources as
    # a single-predicate WHERE sub-query (mirrors the ROOT `existence:` solos).
    solo_items: list[NodeItem] = []
    shared_items: list[NodeItem] = []
    for node, data in items:
        if any(
            edge_kind(concept_edges, node, succ) == EdgeKind.EXISTENCE
            for succ in concept_graph.successors(node)
        ):
            solo_items.append((node, data))
        else:
            shared_items.append((node, data))

    # `output_addresses` folds in condition args, so a D1 (WHERE-recursion)
    # filter can carry a projected address without being a projected OUTPUT --
    # exempting one would waive the signature split for a node that does have a
    # downstream consumer. Excluded structurally rather than relying on the
    # corpus never producing one.
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
        extra_signature=existence_signature,
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
    single boundary group by rowset identity — never per-column grain (which
    the FINAL node would have to rejoin on grain keys, degrading to `1=1` for
    grain-mismatched contributors) and never per-DEPTH.

    Keyed by `(label, rowset_name)` only — deliberately NOT including depth. A
    rowset referenced in both the SELECT (d0) and a WHERE (d1) is still one
    population; splitting d0/d1 stranded a consumer's filter from its scan (q64:
    an arm's `count(...) by dims` read the d0 dim handles while the per-arm
    `marital != ...` filter sat in a separate d1 handle group, so the filter
    fell through to FINAL and the aggregate counted unfiltered rows). The
    bucket depth is d1 only if every member is d1 (a pure condition-feeder
    rowset); any d0 member makes it a d0 boundary that produces SELECT output.
    Grain is the union of members' grains (same rows, so a wider grain only
    widens ride-through within the one CTE)."""
    by_key: dict[tuple[str, str], GroupBucket] = {}
    members_by_key: dict[tuple[str, str], list[NodeItem]] = defaultdict(list)
    for node, data in items:
        assert data.rowset_name is not None
        # Key on SCOPE, not the full label: the label's "@condition" phase
        # suffix would otherwise split a rowset's SELECT (blank-phase) handles
        # from its WHERE (condition-phase) handles into two groups (q64).
        scope = _scope_and_phase(data.label)[0]
        members_by_key[(scope, data.rowset_name)].append((node, data))
    for (scope, rowset_name), members in members_by_key.items():
        depth_label = (
            DepthLabel.D1
            if all(d.depth_label == DepthLabel.D1 for _, d in members)
            else DepthLabel.D0
        )
        bucket = _bucket_for(depth_label, Derivation.ROWSET, frozenset(), label=scope)
        bucket.discriminator = f"rowset:{rowset_name}"
        for node, data in members:
            bucket.grain_components |= data.grain_components
            _add_member(bucket, node, data)
        by_key[(scope, rowset_name)] = bucket
    return list(by_key.values())


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

    The default rule keys on depth, which splits those into two groups; a
    constant-only WHERE then strands on the upstream d1 condition-feeder group
    whose filtered output the downstream SELECT constant simply rebuilds
    (`gen_constant` ignores parents), dropping the filter entirely
    (`select today where date_add(current_date(), day, -30) < today`). Key on
    `(scope, grain, grouping_mode)` — never depth, and on the scope rather than
    the full label so the `@condition` phase suffix doesn't re-split them — and
    surface the bucket at its most-downstream member depth so it produces the
    SELECT output AND carries the WHERE. Mirrors `partition_rowsets`."""
    ConstantKey = tuple[str, frozenset[str], AggregateGroupingMode | None]
    by_key: dict[ConstantKey, GroupBucket] = {}
    members_by_key: dict[ConstantKey, list[NodeItem]] = defaultdict(list)
    for node, data in items:
        scope = _scope_and_phase(data.label)[0]
        members_by_key[(scope, data.grain_components, data.grouping_mode)].append(
            (node, data)
        )
    for key, members in members_by_key.items():
        scope, grain, grouping_mode = key
        depths = {d.depth_label for _, d in members}
        depth_label = (
            DepthLabel.STAR
            if DepthLabel.STAR in depths
            else DepthLabel.D0 if DepthLabel.D0 in depths else DepthLabel.D1
        )
        bucket = _bucket_for(depth_label, Derivation.CONSTANT, grain, label=scope)
        _apply_grouping_mode(bucket, grouping_mode)
        for node, data in members:
            _add_member(bucket, node, data)
        by_key[key] = bucket
    return list(by_key.values())


# Per-derivation registry. Any derivation not in here uses the default rule.
GROUPING_RULES: dict[Derivation, PartitionFn] = {
    Derivation.ROOT: partition_roots,
    Derivation.BASIC: partition_basics_by_signature,
    Derivation.FILTER: partition_filters_by_signature,
    Derivation.ROWSET: partition_rowsets,
    Derivation.AGGREGATE: partition_aggregates,
    Derivation.CONSTANT: partition_constants,
    Derivation.WINDOW: partition_windows,
}

DEFAULT_RULE: PartitionFn = partition_by_depth_and_grain
