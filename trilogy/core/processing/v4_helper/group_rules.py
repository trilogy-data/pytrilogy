"""Per-derivation grouping rules.

Each rule takes the list of `(node_id, node_data)` pairs for one
derivation, the concept_graph, the in-progress `primary_group` map, and an
`ensure_assigned(derivation)` callback the orchestrator passes in so a
rule can demand its dependencies be bucketed on the fly. Most rules
ignore the trailing arguments; BASIC uses them to walk lineage ancestors
and key its buckets by the set of non-BASIC stopping groups.

One registry per shape concern, lookup by derivation, fallback to a default.
"""

from collections import defaultdict
from typing import Callable

from trilogy.core import graph as nx
from trilogy.core.enums import Derivation, Granularity

from .concept_graph import _scope_and_phase
from .constants import DepthLabel, EdgeKind
from .edges import EdgeMap, edge_kind
from .models import ConceptAttrs, GroupBucket

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
        tuple[str, DepthLabel, frozenset[str], str | None],
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
            # Encode grouping mode in the bucket discriminator so distinct
            # modes map to distinct group ids (and survive the topo/edge
            # walks below as separate nodes).
            if grouping_mode and grouping_mode != "standard":
                bucket.discriminator = f"grp:{grouping_mode}"
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
    standard = [
        (n, d) for n, d in items if not d.grouping_mode or d.grouping_mode == "standard"
    ]
    grouped = [
        (n, d) for n, d in items if d.grouping_mode and d.grouping_mode != "standard"
    ]
    buckets = _partition_standard_aggregates(standard)
    buckets += _partition_grouped_aggregates(
        grouped,
        concept_graph,
        concept_edges,
        concept_attrs,
        primary_group,
        ensure_assigned,
    )
    return buckets


def _partition_standard_aggregates(items: list[NodeItem]) -> list[GroupBucket]:
    by_key: dict[
        tuple[str, DepthLabel, frozenset[str], frozenset[str]], GroupBucket
    ] = {}
    for node, data in items:
        grain = data.grain_components
        input_grain = data.aggregate_input_grain
        key = (data.label, data.depth_label, grain, input_grain)
        bucket = by_key.get(key)
        if bucket is None:
            bucket = _bucket_for(
                data.depth_label, data.derivation, grain, label=data.label
            )
            if input_grain and input_grain != grain:
                bucket.discriminator = "input:" + "|".join(sorted(input_grain))
            by_key[key] = bucket
        if input_grain:
            bucket.aggregate_input_grain = frozenset(
                set(bucket.aggregate_input_grain) | set(input_grain)
            )
        _add_member(bucket, node, data)
    return list(by_key.values())


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
        tuple[str, DepthLabel, frozenset[str], str | None], list[NodeItem]
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
        uf = list(range(n))

        def find(x: int, _uf=uf) -> int:
            while _uf[x] != x:
                _uf[x] = _uf[_uf[x]]
                x = _uf[x]
            return x

        for i in range(n):
            for j in range(i + 1, n):
                if sigs[i] <= sigs[j] or sigs[j] <= sigs[i]:
                    uf[find(j)] = find(i)

        components: dict[int, list[int]] = defaultdict(list)
        for i in range(n):
            components[find(i)].append(i)
        for member_indices in components.values():
            bucket = _bucket_for(depth_label, Derivation.AGGREGATE, grain, label=label)
            shared_sig: frozenset[str] = frozenset().union(
                *(sigs[i] for i in member_indices)
            )
            sig_repr = "|".join(sorted(shared_sig)) or "none"
            disc = [f"grp:{grouping_mode}", f"sig:{abs(hash(sig_repr)) % (16**6):06x}"]
            bucket.discriminator = ":".join(disc)
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

        # Forward lineage reach per root, following only LINEAGE edges (a
        # d1→d0 ordering rides its own CONSTRAINT edge and is excluded here).
        reaches: list[set[str]] = []
        for node, _ in main_items:
            seen: set[str] = set()
            stack = [node]
            while stack:
                cur = stack.pop()
                for nxt in concept_graph.successors(cur):
                    if nxt in seen:
                        continue
                    if edge_kind(concept_edges, cur, nxt) != EdgeKind.LINEAGE:
                        continue
                    seen.add(nxt)
                    stack.append(nxt)
            reaches.append(seen)

        # Bail out to one bucket if any root has zero reach — see docstring.
        can_split = bool(main_items) and all(reaches)

        if can_split:
            n = len(main_items)
            parent = list(range(n))

            def find(x: int) -> int:
                while parent[x] != x:
                    parent[x] = parent[parent[x]]
                    x = parent[x]
                return x

            def union(a: int, b: int) -> None:
                ra, rb = find(a), find(b)
                if ra != rb:
                    parent[rb] = ra

            for i in range(n):
                for j in range(i + 1, n):
                    if reaches[i] & reaches[j]:
                        union(i, j)

            # Co-source roots that converge at the query output projection.
            # `reaches` holds node ids; map to addresses to test membership.
            if output_addresses:
                output_roots = [
                    i
                    for i in range(n)
                    if addr_of[main_items[i][0]] in output_addresses
                    or any(
                        concept_attrs[x].address in output_addresses for x in reaches[i]
                    )
                ]
                # Only co-source output-converging roots that lie in the same
                # weakly-connected component of the concept graph. Roots in
                # different components (unrelated models, no join/merge path) only
                # meet at a cross-product of single-row aggregates; forcing them
                # into one scan yields an unsourceable disconnected root group
                # (`select sum(av), sum(bv)` over two unrelated models).
                undirected = concept_graph.to_undirected()
                # A property root and its KEY root are FD-related even when the
                # lineage graph never joins them (a pure two-alias projection
                # `select cust_id as x, cname as y` has one BASIC per root and
                # no shared consumer) — the table binding both is what relates
                # them, and splitting them cross-joins ON 1=1 (cartesian rows).
                node_by_addr = {data.address: node for node, data in main_items}
                for node, data in main_items:
                    for key_addr in data.keys:
                        key_node = node_by_addr.get(key_addr)
                        if key_node is not None and key_node != node:
                            undirected.add_edge(node, key_node)
                comp_of: dict[str, int] = {}
                for ci, comp in enumerate(nx.connected_components(undirected)):
                    for node in comp:
                        comp_of[node] = ci
                by_component: dict[int, list[int]] = defaultdict(list)
                for i in output_roots:
                    by_component[comp_of.get(main_items[i][0], -1 - i)].append(i)
                for component_roots in by_component.values():
                    for k in range(1, len(component_roots)):
                        union(component_roots[0], component_roots[k])

            components: dict[int, list[NodeItem]] = defaultdict(list)
            for i, item in enumerate(main_items):
                components[find(i)].append(item)
        else:
            components = defaultdict(list)
            if main_items:
                components[0] = list(main_items)

        multi = len(components) > 1
        for members in components.values():
            bucket = _bucket_for(
                depth_label=DepthLabel.ROOT,
                derivation=Derivation.ROOT,
                grain=frozenset(),
                label=label_value,
            )
            if multi:
                sig_repr = "|".join(sorted(addr_of[node] for node, _ in members))
                bucket.discriminator = f"split:{abs(hash(sig_repr)) % (16**6):06x}"
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
) -> list[GroupBucket]:
    """Generic signature+grain bucketing. Used for derivations whose
    upstream identity should split buckets even when row-shape (depth /
    grain) matches: BASIC (rename chains, derived columns) and FILTER
    (specialized basics — same scan-compatibility story).

    Within a label, two nodes share a bucket iff their stop-signatures
    are equal AND one's grain is a subset of the other's. The grain-
    subset union preserves the historical "widen to the superset" merge
    so a chain of derivations at progressively finer grains still
    co-sources when they share an upstream."""
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
        uf = list(range(n))

        def find(x: int, _uf=uf) -> int:
            while _uf[x] != x:
                _uf[x] = _uf[_uf[x]]
                x = _uf[x]
            return x

        def union(a: int, b: int, _uf=uf) -> None:
            ra, rb = find(a, _uf), find(b, _uf)
            if ra != rb:
                _uf[rb] = ra

        for i in range(n):
            for j in range(i + 1, n):
                signatures_match = sigs[i] == sigs[j]
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
                    union(i, j)

        components: dict[int, list[int]] = defaultdict(list)
        for i in range(n):
            components[find(i)].append(i)

        for member_indices in components.values():
            merged_grain: frozenset[str] = frozenset().union(
                *(grains[i] for i in member_indices)
            )
            depths = {sub_items[i][1].depth_label for i in member_indices}
            group_depth = (
                DepthLabel.D1 if DepthLabel.D1 in depths else next(iter(depths))
            )
            shared_sig = sigs[member_indices[0]]
            # Stable signature representation: hash the sorted stop-set so
            # two component-equal sigs produce the same discriminator and
            # two disjoint sigs produce different ones. Group ids include
            # the discriminator so colliding (label, depth, grain) buckets
            # stay distinct.
            sig_repr = "|".join(sorted(shared_sig)) or "none"
            discriminator = f"sig:{abs(hash(sig_repr)) % (16**6):06x}"
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
    through any shared downstream consumer."""

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

    buckets = _partition_by_signature_and_grain(
        shared_items,
        Derivation.FILTER,
        concept_graph,
        concept_edges,
        concept_attrs,
        primary_group,
        ensure_assigned,
        extra_signature=existence_signature,
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
    by_key: dict[tuple[str, frozenset[str], str | None], GroupBucket] = {}
    members_by_key: dict[tuple[str, frozenset[str], str | None], list[NodeItem]] = (
        defaultdict(list)
    )
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
        if grouping_mode and grouping_mode != "standard":
            bucket.discriminator = f"grp:{grouping_mode}"
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
}

DEFAULT_RULE: PartitionFn = partition_by_depth_and_grain
