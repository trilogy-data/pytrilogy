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
    bucket.primary_members.append(node)
    bucket.member_depths[node] = data.get("depth_label", "d*")


def partition_by_depth_and_grain(items: list[NodeItem]) -> list[GroupBucket]:
    """Default rule: two nodes share a group iff they have the same
    ``label``, ``depth_label`` and ``grain``. Label keeps inner-rowset
    concepts in their own buckets; depth keeps a d1 aggregate (filter
    input) and a d0 aggregate (post-filter) in distinct scans at the
    same grain."""
    by_key: dict[tuple[str, str, frozenset[str]], GroupBucket] = {}
    for node, data in items:
        depth_label = data.get("depth_label", "d*")
        derivation = data.get("derivation", "")
        grain = frozenset(data.get("grain_components", ()))
        label = data.get("label", "")
        key = (label, depth_label, grain)
        bucket = by_key.get(key)
        if bucket is None:
            bucket = _bucket_for(depth_label, derivation, grain, label=label)
            by_key[key] = bucket
        _add_member(bucket, node, data)
    return list(by_key.values())


def partition_roots(items: list[NodeItem]) -> list[GroupBucket]:
    """Roots can always be co-sourced (joinable across datasources in a single
    base scan), but only within a label. Each sub-graph (outer query plus
    each rowset's inner walk) gets its own root bucket — their scans are
    independent."""
    if not items:
        return []
    buckets: list[GroupBucket] = []
    for label_value, sub_items in _split_by_label(items).items():
        if not sub_items:
            continue
        bucket = _bucket_for(
            depth_label="root",
            derivation=Derivation.ROOT.value,
            grain=frozenset(),
            label=label_value,
        )
        for node, data in sub_items:
            _add_member(bucket, node, data)
        buckets.append(bucket)
    return buckets


def partition_basics_by_subset_grain(items: list[NodeItem]) -> list[GroupBucket]:
    """Two basics share a scan iff they have the same ``label`` and one's
    grain is a subset of the other's; connected components in that
    relation become the groups. The group's grain is the union (widest
    member); its ``depth_label`` lifts to ``d1`` if any member is ``d1``
    so a condition input still pins the group above every d0 barrier.

    The label split is what keeps outer query BASICs separate from
    rowset-internal BASICs even when their grains happen to overlap —
    without it, q05's outer renames and the rowset's derive-clause
    BASICs merge into one bucket and form a group-level cycle through
    the rowset boundary."""
    if not items:
        return []
    buckets: list[GroupBucket] = []
    for label_value, sub_items in _split_by_label(items).items():
        n = len(sub_items)
        if not n:
            continue
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

        grains = [
            frozenset(sub_items[i][1].get("grain_components", ())) for i in range(n)
        ]
        for i in range(n):
            for j in range(i + 1, n):
                if grains[i] <= grains[j] or grains[j] <= grains[i]:
                    union(i, j)

        components: dict[int, list[int]] = defaultdict(list)
        for i in range(n):
            components[find(i)].append(i)

        for member_indices in components.values():
            merged_grain: frozenset[str] = frozenset().union(
                *(grains[i] for i in member_indices)
            )
            depths = {
                sub_items[i][1].get("depth_label", "d*") for i in member_indices
            }
            group_depth = "d1" if "d1" in depths else next(iter(depths))
            bucket = _bucket_for(
                depth_label=group_depth,
                derivation=Derivation.BASIC.value,
                grain=merged_grain,
                label=label_value,
            )
            for i in member_indices:
                node, data = sub_items[i]
                _add_member(bucket, node, data)
            buckets.append(bucket)
    return buckets


# Per-derivation registry. Any derivation not in here uses the default rule.
GROUPING_RULES: dict[str, PartitionFn] = {
    Derivation.ROOT.value: partition_roots,
    Derivation.BASIC.value: partition_basics_by_subset_grain,
}

DEFAULT_RULE: PartitionFn = partition_by_depth_and_grain
