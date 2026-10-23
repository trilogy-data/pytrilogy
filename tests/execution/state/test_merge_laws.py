"""Snapshot merge laws (formal/Trilogy/StateMerge.lean), checked on random fan-outs.

Workers each rebuild the slices they own and publish a scoped delta; the
coordinator folds them into a base. Each case requires:

- the merge does not depend on the order the deltas are passed in
- replaying the deltas changes nothing
- an untrimmed summary never contradicts the slices it sits beside
- over an untrimmed base, the merged counts and status are the truth, unless a
  trimmed whole-table probe was folded (its silence cannot say which slices it
  dropped; ``_merge_partition_summary`` bounds that)
"""

import random

import pytest

from trilogy.execution.state.snapshot import (
    DatasourceState,
    PartitionColumn,
    PartitionState,
    PhysicalAssetState,
    StateSnapshot,
    cap_partitions,
    cap_snapshot,
    merge_snapshots,
    scope_to_partitions,
    summarize_partitions,
)

IDS = [f"d={i:02d}" for i in range(8)]


def _probe(truth: dict[str, str], ts: int, cap: int | None = None) -> StateSnapshot:
    parts = [
        PartitionState(
            partition_id=pid,
            observed=status != "missing",
            expected=True,
            status="fresh" if status == "fresh" else "stale",
        )
        for pid, status in sorted(truth.items())
    ]
    summary = summarize_partitions(parts, "reconciled")
    if cap is not None:
        parts, summary = cap_partitions(parts, cap, summary)
    ds_state = DatasourceState(
        datasource_id="t",
        partition_by=[PartitionColumn(column="d", concept_address="d")],
        partitions=parts,
        partition_summary=summary,
        status="stale" if summary.stale else "fresh",
    )
    return StateSnapshot(
        snapshot_ts=f"2024-01-01T00:00:{ts:02d}+00:00",
        assets=[PhysicalAssetState(address="t", datasources=[ds_state])],
    )


def _record(snapshot: StateSnapshot) -> DatasourceState:
    return snapshot.assets[0].datasources[0]


def _comparable(snapshot: StateSnapshot) -> dict:
    return snapshot.model_dump(exclude={"snapshot_ts"})


def _fan_out(rng: random.Random):
    ids = rng.sample(IDS, rng.randint(2, 8))
    current = {i: rng.choice(["fresh", "stale", "missing"]) for i in ids}
    base_kind = rng.choice(["whole", "whole", "capped", "absent"])
    if base_kind == "absent":
        base = StateSnapshot(snapshot_ts="2024-01-01T00:00:00+00:00", assets=[])
    else:
        base = _probe(current, 0, 2 if base_kind == "capped" else None)
    workers = rng.randint(1, 3)
    deltas = []
    for w in range(workers):
        mine = ids[w::workers]
        if rng.random() < 0.2:
            deltas.append(scope_to_partitions(_probe(current, 2 * w + 1), set(mine)))
        for pid in mine:
            current[pid] = "fresh"
        if (new := rng.choice(IDS)) not in current:
            current[new] = "fresh"
            mine = [*mine, new]
        delta = scope_to_partitions(_probe(current, 2 * w + 2), set(mine))
        deltas.append(cap_snapshot(delta, rng.choice([None, 1, 3])))
        if rng.random() < 0.2:
            deltas.append(_probe(current, 2 * w + 2, rng.choice([None, 2])))
    return base, base_kind, deltas, current


def check_fan_out(seed: int) -> list[str]:
    rng = random.Random(seed)
    base, base_kind, deltas, truth = _fan_out(rng)
    orders = [
        deltas,
        deltas[::-1],
        *(rng.sample(deltas, len(deltas)) for _ in range(4)),
    ]
    merges = [merge_snapshots(base, *order) for order in orders]
    failures = []
    if any(_comparable(m) != _comparable(merges[0]) for m in merges):
        failures.append("depends on delta order")
    merged = merges[0]
    if _comparable(merge_snapshots(merged, *deltas)) != _comparable(merged):
        failures.append("replay moved the merge")
    record = _record(merged)
    listed = sum(p.status == "stale" for p in record.partitions)
    summary = record.partition_summary
    if summary is not None and not summary.truncated and summary.stale != listed:
        failures.append(f"summary says {summary.stale} stale, list holds {listed}")
    exact = not any(
        _record(d).partitions_complete and _record(d).partition_summary.truncated
        for d in deltas
    )
    if base_kind == "whole" and exact:
        stale = sum(v != "fresh" for v in truth.values())
        if summary is None or (summary.total, summary.stale) != (len(truth), stale):
            failures.append(f"summary {summary} != {len(truth)} slices, {stale} stale")
        if (record.status == "stale") != bool(stale):
            failures.append(f"status {record.status} with {stale} stale")
    return failures


@pytest.mark.parametrize("seed", range(60))
def test_a_fan_out_merges_to_the_truth(seed):
    assert check_fan_out(seed) == []


def test_capping_a_scoped_delta_keeps_every_owned_slice():
    delta = scope_to_partitions(_probe(dict.fromkeys(IDS, "fresh"), 1), set(IDS))
    assert len(_record(cap_snapshot(delta, 1)).partitions) == len(IDS)


def test_a_record_only_scoped_deltas_reached_carries_no_count():
    empty = StateSnapshot(snapshot_ts="2024-01-01T00:00:00+00:00", assets=[])
    probed = _probe(
        {**dict.fromkeys(IDS[:4], "stale"), **dict.fromkeys(IDS[4:], "fresh")}, 1
    )
    first = scope_to_partitions(probed, set(IDS[:4]))
    second = scope_to_partitions(_probe(dict.fromkeys(IDS, "fresh"), 2), set(IDS[4:]))
    for merged in (
        merge_snapshots(empty, first),
        merge_snapshots(empty, first, second),
    ):
        record = _record(merged)
        assert record.partition_summary is None
        assert record.status == "stale"
    assert len(_record(merge_snapshots(empty, second)).partitions) == 4
    assert _record(merge_snapshots(empty, second)).status == "fresh"


def test_the_later_claim_on_a_slice_wins():
    stale = scope_to_partitions(_probe({"d=00": "stale"}, 1), {"d=00"})
    fresh = scope_to_partitions(_probe({"d=00": "fresh"}, 2), {"d=00"})
    base = _probe({"d=00": "stale"}, 0)
    for order in ((stale, fresh), (fresh, stale)):
        assert _record(merge_snapshots(base, *order)).status == "fresh"


def test_a_count_never_drifts_from_the_slices_beside_it():
    ids = IDS[:3]
    base = _probe(dict.fromkeys(ids, "stale"), 0)
    trimmed = _probe({"d=00": "stale", "d=01": "fresh", "d=02": "stale"}, 1, cap=1)
    fixed = scope_to_partitions(_probe(dict.fromkeys(ids, "fresh"), 2), set(ids))
    summary = _record(merge_snapshots(base, trimmed, fixed)).partition_summary
    assert (summary.total, summary.stale) == (3, 0)
