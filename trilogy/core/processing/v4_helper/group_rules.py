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

import networkx as nx

from trilogy.core.enums import Derivation

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
    """
    by_key: dict[
        tuple[
            str,
            DepthLabel,
            frozenset[str],
            str | None,
            frozenset[str],
            frozenset[str],
        ],
        GroupBucket,
    ] = {}
    for node, data in items:
        depth_label = data.depth_label
        derivation = data.derivation
        grain = data.grain_components
        label = data.label
        grouping_mode = data.grouping_mode
        input_grain = data.aggregate_input_grain
        source_sig = (
            _stop_signature(
                node,
                Derivation.AGGREGATE,
                concept_graph,
                concept_edges,
                concept_attrs,
                primary_group,
                ensure_assigned,
            )
            if grouping_mode and grouping_mode != "standard"
            else frozenset()
        )
        key_input_grain = (
            input_grain if not grouping_mode or grouping_mode == "standard" else grain
        )
        key = (
            label,
            depth_label,
            grain,
            grouping_mode,
            key_input_grain,
            source_sig,
        )
        bucket = by_key.get(key)
        if bucket is None:
            bucket = _bucket_for(depth_label, derivation, grain, label=label)
            disc: list[str] = []
            if grouping_mode and grouping_mode != "standard":
                disc.append(f"grp:{grouping_mode}")
                sig_repr = "|".join(sorted(source_sig)) or "none"
                disc.append(f"sig:{abs(hash(sig_repr)) % (16**6):06x}")
            if (
                (not grouping_mode or grouping_mode == "standard")
                and input_grain
                and input_grain != grain
            ):
                disc.append("input:" + "|".join(sorted(input_grain)))
            if disc:
                bucket.discriminator = ":".join(disc)
            by_key[key] = bucket
        if input_grain:
            bucket.aggregate_input_grain = frozenset(
                set(bucket.aggregate_input_grain) | set(input_grain)
            )
        _add_member(bucket, node, data)
    return list(by_key.values())


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
                for k in range(1, len(output_roots)):
                    union(output_roots[0], output_roots[k])

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

    return _partition_by_signature_and_grain(
        items,
        Derivation.FILTER,
        concept_graph,
        concept_edges,
        concept_attrs,
        primary_group,
        ensure_assigned,
        extra_signature=existence_signature,
    )


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


# Per-derivation registry. Any derivation not in here uses the default rule.
GROUPING_RULES: dict[Derivation, PartitionFn] = {
    Derivation.ROOT: partition_roots,
    Derivation.BASIC: partition_basics_by_signature,
    Derivation.FILTER: partition_filters_by_signature,
    Derivation.ROWSET: partition_rowsets,
    Derivation.AGGREGATE: partition_aggregates,
}

DEFAULT_RULE: PartitionFn = partition_by_depth_and_grain
