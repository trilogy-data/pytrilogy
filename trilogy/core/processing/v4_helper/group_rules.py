"""Per-derivation grouping rules.

Each rule takes the list of `(node_id, node_data)` pairs for one
derivation, the concept_graph, the in-progress `primary_group` map, and an
`ensure_assigned(derivation)` callback the orchestrator passes in so a
rule can demand its dependencies be bucketed on the fly. Most rules
ignore the trailing arguments; BASIC uses them to walk lineage ancestors
and key its buckets by the set of non-BASIC stopping groups.

Parallels `factory_dispatch.py`: one registry per shape concern, lookup by
derivation, fallback to a default.
"""

from collections import defaultdict
from typing import Any, Callable

import networkx as nx

from trilogy.core.enums import Derivation

from .models import GroupBucket

NodeItem = tuple[str, dict[str, Any]]
EnsureAssignedFn = Callable[[str], None]
PartitionFn = Callable[
    [list[NodeItem], nx.DiGraph, dict[str, str], EnsureAssignedFn],
    list[GroupBucket],
]


def _bucket_for(
    depth_label: str,
    derivation: str,
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
        by_label[data.get("label", "")].append((node, data))
    return by_label


def _add_member(bucket: GroupBucket, node: str, data: dict[str, Any]) -> None:
    address = data.get("address", node)
    bucket.primary_members.append(address)
    bucket.primary_node_ids.append(node)
    bucket.member_depths[address] = data.get("depth_label", "d*")


def partition_by_depth_and_grain(
    items: list[NodeItem],
    concept_graph: nx.DiGraph,
    primary_group: dict[str, str],
    ensure_assigned: EnsureAssignedFn,
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
        tuple[str, str, frozenset[str], str | None],
        GroupBucket,
    ] = {}
    for node, data in items:
        depth_label = data.get("depth_label", "d*")
        derivation = data.get("derivation", "")
        grain = frozenset(data.get("grain_components", ()))
        label = data.get("label", "")
        grouping_mode = data.get("grouping_mode")
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


def partition_roots(
    items: list[NodeItem],
    concept_graph: nx.DiGraph,
    primary_group: dict[str, str],
    ensure_assigned: EnsureAssignedFn,
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
        addr_of = {node: data.get("address", node) for node, data in sub_items}
        existence_only_nodes = [(n, d) for n, d in sub_items if d.get("existence_only")]
        main_items = [(n, d) for n, d in sub_items if not d.get("existence_only")]

        # Forward lineage reach per root. d1→d0 constraint edges are
        # carried as is_constraint=True flags on lineage edges, so the
        # single-kind filter still picks them up.
        reaches: list[set[str]] = []
        for node, _ in main_items:
            seen: set[str] = set()
            stack = [node]
            while stack:
                cur = stack.pop()
                for nxt in concept_graph.successors(cur):
                    if nxt in seen:
                        continue
                    if concept_graph.edges[cur, nxt].get("kind") != "lineage":
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

            components: dict[int, list[tuple[str, dict]]] = defaultdict(list)
            for i, item in enumerate(main_items):
                components[find(i)].append(item)
        else:
            components = defaultdict(list)
            if main_items:
                components[0] = list(main_items)

        multi = len(components) > 1
        for members in components.values():
            bucket = _bucket_for(
                depth_label="root",
                derivation=Derivation.ROOT.value,
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
                depth_label="root",
                derivation=Derivation.ROOT.value,
                grain=frozenset(),
                label=label_value,
            )
            solo.discriminator = f"existence:{addr_of[node]}"
            _add_member(solo, node, data)
            buckets.append(solo)
    return buckets


def _stop_signature(
    node: str,
    recurse_through: str,
    concept_graph: nx.DiGraph,
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
        for pred, _, ed in concept_graph.in_edges(current, data=True):
            if ed.get("kind") != "lineage":
                continue
            if pred in visited:
                continue
            visited.add(pred)
            pred_derivation = concept_graph.nodes[pred].get("derivation", "")
            if pred_derivation == recurse_through:
                stack.append(pred)
                continue
            ensure_assigned(pred_derivation)
            gid = primary_group.get(pred)
            if gid is not None:
                sig.add(gid)
    return frozenset(sig)


def _partition_by_signature_and_grain(
    items: list[NodeItem],
    own_derivation: str,
    concept_graph: nx.DiGraph,
    primary_group: dict[str, str],
    ensure_assigned: EnsureAssignedFn,
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
        sigs = [
            _stop_signature(
                node,
                own_derivation,
                concept_graph,
                primary_group,
                ensure_assigned,
            )
            for node, _ in sub_items
        ]
        grains = [
            frozenset(sub_items[i][1].get("grain_components", ())) for i in range(n)
        ]
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
                if sigs[i] != sigs[j]:
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
            depths = {sub_items[i][1].get("depth_label", "d*") for i in member_indices}
            group_depth = "d1" if "d1" in depths else next(iter(depths))
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
    primary_group: dict[str, str],
    ensure_assigned: EnsureAssignedFn,
) -> list[GroupBucket]:
    """Group BASICs by `(label, stop-signature, grain-subset)`. See
    `_partition_by_signature_and_grain` for the full story — BASIC's
    stop walks through other BASICs and stops at any non-BASIC."""
    return _partition_by_signature_and_grain(
        items,
        Derivation.BASIC.value,
        concept_graph,
        primary_group,
        ensure_assigned,
    )


def partition_filters_by_signature(
    items: list[NodeItem],
    concept_graph: nx.DiGraph,
    primary_group: dict[str, str],
    ensure_assigned: EnsureAssignedFn,
) -> list[GroupBucket]:
    """FILTERs are specialized BASICs — same scan-compatibility story.
    Two filters that look identical by depth/grain but read from
    disjoint upstreams (e.g. q08's `_virt_filter_zips` over a basic
    chain vs. `_virt_filter_id` over customer roots) should not be
    co-sourced; their disjoint parent groups would form a back-edge
    through any shared downstream consumer."""
    return _partition_by_signature_and_grain(
        items,
        Derivation.FILTER.value,
        concept_graph,
        primary_group,
        ensure_assigned,
    )


# Per-derivation registry. Any derivation not in here uses the default rule.
GROUPING_RULES: dict[str, PartitionFn] = {
    Derivation.ROOT.value: partition_roots,
    Derivation.BASIC.value: partition_basics_by_signature,
    Derivation.FILTER.value: partition_filters_by_signature,
}

DEFAULT_RULE: PartitionFn = partition_by_depth_and_grain
