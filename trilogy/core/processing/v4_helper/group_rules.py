"""Per-derivation grouping rules.

Each rule takes the list of `(node_address, node_data)` pairs for one
derivation and returns one `GroupBucket` per scan-compatible cluster. The
default rule is equality on `(depth_label, grain)` — that captures "same
row shape, same role in placement, deliverable in one scan." Derivations
with special semantics (ROOT collapses all together; BASIC accepts subset
grains as compatible) register their own partition function.

Parallels `factory_dispatch.py`: one registry per shape concern, lookup by
derivation, fallback to a default.
"""

from collections import defaultdict
from typing import Any, Callable

from trilogy.core.enums import Derivation

from .models import GroupBucket

NodeItem = tuple[str, dict[str, Any]]
PartitionFn = Callable[[list[NodeItem]], list[GroupBucket]]


def _bucket_for(
    depth_label: str,
    derivation: str,
    grain: frozenset[str],
) -> GroupBucket:
    return GroupBucket(
        depth_label=depth_label,
        derivation=derivation,
        grain_components=grain,
    )


def _add_member(bucket: GroupBucket, node: str, data: dict[str, Any]) -> None:
    bucket.primary_members.append(node)
    bucket.member_depths[node] = data.get("depth_label", "d*")


def partition_by_depth_and_grain(items: list[NodeItem]) -> list[GroupBucket]:
    """Default rule: two nodes share a group iff they have the same
    `depth_label` and the same `grain`. Keeps `depth_label` in the key so a
    d1 aggregate (input to a filter) and a d0 aggregate (post-filter
    re-aggregation) stay in distinct scans even at the same grain."""
    by_key: dict[tuple[str, frozenset[str]], GroupBucket] = {}
    for node, data in items:
        depth_label = data.get("depth_label", "d*")
        derivation = data.get("derivation", "")
        grain = frozenset(data.get("grain_components", ()))
        key = (depth_label, grain)
        bucket = by_key.get(key)
        if bucket is None:
            bucket = _bucket_for(depth_label, derivation, grain)
            by_key[key] = bucket
        _add_member(bucket, node, data)
    return list(by_key.values())


def partition_roots(items: list[NodeItem]) -> list[GroupBucket]:
    """Roots can always be co-sourced (joinable across datasources in a single
    base scan), so collapse every root concept into one bucket."""
    if not items:
        return []
    bucket = _bucket_for(
        depth_label="root",
        derivation=Derivation.ROOT.value,
        grain=frozenset(),
    )
    for node, data in items:
        _add_member(bucket, node, data)
    return [bucket]


def partition_basics_by_subset_grain(items: list[NodeItem]) -> list[GroupBucket]:
    """Two basics share a scan iff one's grain is a subset of the other's;
    connected components in that relation become the groups. The group's
    grain is the union (widest member); its `depth_label` lifts to `d1` if
    any member is `d1`, so a condition input still pins the group above
    every d0 barrier."""
    if not items:
        return []

    n = len(items)
    uf = list(range(n))

    def find(x: int) -> int:
        while uf[x] != x:
            uf[x] = uf[uf[x]]
            x = uf[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            uf[rb] = ra

    grains = [frozenset(items[i][1].get("grain_components", ())) for i in range(n)]
    for i in range(n):
        for j in range(i + 1, n):
            if grains[i] <= grains[j] or grains[j] <= grains[i]:
                union(i, j)

    components: dict[int, list[int]] = defaultdict(list)
    for i in range(n):
        components[find(i)].append(i)

    buckets: list[GroupBucket] = []
    for member_indices in components.values():
        merged_grain: frozenset[str] = frozenset().union(
            *(grains[i] for i in member_indices)
        )
        depths = {items[i][1].get("depth_label", "d*") for i in member_indices}
        group_depth = "d1" if "d1" in depths else next(iter(depths))
        bucket = _bucket_for(
            depth_label=group_depth,
            derivation=Derivation.BASIC.value,
            grain=merged_grain,
        )
        for i in member_indices:
            node, data = items[i]
            _add_member(bucket, node, data)
        buckets.append(bucket)
    return buckets


# Per-derivation registry. Any derivation not in here uses the default rule.
GROUPING_RULES: dict[str, PartitionFn] = {
    Derivation.ROOT.value: partition_roots,
    Derivation.BASIC.value: partition_basics_by_subset_grain,
}

DEFAULT_RULE: PartitionFn = partition_by_depth_and_grain
