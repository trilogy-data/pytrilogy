"""Tests for per-partition state on partitioned datasources.

Covers the three pieces the partitioned-refresh contract rests on: probing the
observed/expected slices, judging each one, and merging per-partition deltas
back into a base snapshot without workers clobbering each other.
"""

from datetime import date, datetime

import pytest

from trilogy import Dialects
from trilogy.core.models.datasource import UpdateKey, UpdateKeyType
from trilogy.execution.state import (
    BaseStateStore,
    PartitionObservation,
    build_datasource_state,
    build_partition_states,
    merge_into_snapshot,
    merge_snapshots,
    partition_id,
    probe_expected_partitions,
    probe_observed_partitions,
    render_partition_value,
    scope_to_partitions,
    stale_partitions,
)
from trilogy.execution.state.partitions import NULL_PARTITION_TOKEN, is_partitioned

MODEL = """
key order_id int;
property order_id.order_date date;
property order_id.region string;
property order_id.updated_at datetime;

root datasource raw_orders (
    order_id: order_id,
    order_date: order_date,
    region: region,
    updated_at: updated_at
)
grain (order_id)
query '''
SELECT 1 as order_id, DATE '2024-01-01' as order_date, 'north' as region,
       TIMESTAMP '2024-01-05 06:00:00' as updated_at
UNION ALL
SELECT 2, DATE '2024-01-02', 'north', TIMESTAMP '2024-01-05 06:01:00'
UNION ALL
SELECT 3, DATE '2024-01-03', 'south', TIMESTAMP '2024-01-05 06:02:00'
''';

auto order_count <- count(order_id) by order_date, region;
auto max_updated_at <- max(updated_at) by order_date, region;

datasource daily_orders (
    order_date: order_date,
    region: region,
    order_count: order_count,
    max_updated_at: max_updated_at
)
grain (order_date, region)
address daily_orders
freshness by max_updated_at
partition by order_date
;

CREATE IF NOT EXISTS DATASOURCE daily_orders;
"""

BUILD_ONE_DAY = """
RAW_SQL('''
INSERT INTO daily_orders
SELECT DATE '2024-01-01', 'north', 1, TIMESTAMP '2024-01-05 06:00:00'
''');
"""


@pytest.fixture
def executor():
    ex = Dialects.DUCK_DB.default_executor()
    ex.execute_text(MODEL)
    return ex


def _ds(executor):
    return executor.environment.datasources["daily_orders"]


def _roots(executor):
    return {
        d.identifier for d in executor.environment.datasources.values() if d.is_root
    }


def test_render_partition_value_is_canonical():
    assert render_partition_value(date(2024, 1, 3)) == "2024-01-03"
    assert render_partition_value(datetime(2024, 1, 3, 4, 5)) == "2024-01-03T04:05:00"
    assert render_partition_value(None) == NULL_PARTITION_TOKEN
    assert render_partition_value(True) == "true"


def test_partition_id_follows_declared_order():
    assert (
        partition_id({"order_date": date(2024, 1, 3), "region": "north"})
        == "order_date=2024-01-03/region=north"
    )


def test_roots_are_never_partitioned_assets(executor):
    assert is_partitioned(_ds(executor))
    assert not is_partitioned(executor.environment.datasources["raw_orders"])


def test_empty_table_observes_no_slices_but_expects_three(executor):
    ds = _ds(executor)
    assert probe_observed_partitions(ds, executor) == []
    expected = probe_expected_partitions(ds, executor, _roots(executor))
    assert {obs.id for obs in expected} == {
        "order_date=2024-01-01",
        "order_date=2024-01-02",
        "order_date=2024-01-03",
    }


def test_missing_slice_is_stale_and_present_slice_is_fresh(executor):
    executor.execute_text(BUILD_ONE_DAY)
    ds = _ds(executor)
    states = build_partition_states(
        ds,
        probe_observed_partitions(ds, executor),
        probe_expected_partitions(ds, executor, _roots(executor)),
    )
    by_id = {s.partition_id: s for s in states}
    assert by_id["order_date=2024-01-01"].status == "fresh"
    assert by_id["order_date=2024-01-01"].row_count == 1
    assert by_id["order_date=2024-01-02"].status == "stale"
    assert by_id["order_date=2024-01-02"].stale_reason == "partition missing"
    assert by_id["order_date=2024-01-02"].observed is False
    assert by_id["order_date=2024-01-02"].expected is True


def test_slice_behind_on_its_own_watermark_is_stale(executor):
    ds = _ds(executor)
    observed = [
        PartitionObservation(
            values={"order_date": date(2024, 1, 1)},
            row_count=1,
            keys={
                "max_updated_at": UpdateKey(
                    concept_name="max_updated_at",
                    type=UpdateKeyType.UPDATE_TIME,
                    value=datetime(2024, 1, 5, 6, 0),
                )
            },
        )
    ]
    expected = [
        PartitionObservation(
            values={"order_date": date(2024, 1, 1)},
            keys={
                "max_updated_at": UpdateKey(
                    concept_name="max_updated_at",
                    type=UpdateKeyType.UPDATE_TIME,
                    value=datetime(2024, 1, 6, 9, 30),
                )
            },
        )
    ]
    (state,) = build_partition_states(ds, observed, expected)
    assert state.status == "stale"
    assert "behind" in (state.stale_reason or "")
    assert [w.value for w in state.observed_watermarks] == ["2024-01-05 06:00:00"]
    assert [w.value for w in state.expected_watermarks] == ["2024-01-06 09:30:00"]


def test_slice_present_but_not_expected_is_not_stale(executor):
    ds = _ds(executor)
    observed = [
        PartitionObservation(values={"order_date": date(2023, 12, 31)}, row_count=4)
    ]
    (state,) = build_partition_states(ds, observed, [])
    assert state.status == "fresh"
    assert state.expected is False


def test_store_caches_and_invalidates_partition_probes(executor):
    store = BaseStateStore()
    first = store.partition_asset(executor.environment, executor, "daily_orders")
    assert first is not None
    assert (
        store.partition_asset(executor.environment, executor, "daily_orders") is first
    )
    store.invalidate_address(executor.environment, "daily_orders")
    assert (
        store.partition_asset(executor.environment, executor, "daily_orders")
        is not first
    )
    assert store.partition_asset(executor.environment, executor, "raw_orders") is None


def _snapshot(executor, partitions):
    ds = _ds(executor)
    return merge_into_snapshot(
        [
            (
                "daily_orders",
                build_datasource_state(ds, None, None, partitions=partitions),
            )
        ]
    )


def _slices(executor, statuses: dict[str, str]):
    ds = _ds(executor)
    return build_partition_states(
        ds,
        [
            PartitionObservation(values={"order_date": date.fromisoformat(day)})
            for day, status in statuses.items()
            if status != "stale"
        ],
        [
            PartitionObservation(values={"order_date": date.fromisoformat(day)})
            for day in statuses
        ],
    )


def test_datasource_is_stale_when_any_slice_is(executor):
    snapshot = _snapshot(
        executor, _slices(executor, {"2024-01-01": "fresh", "2024-01-02": "stale"})
    )
    (asset,) = snapshot.assets
    assert asset.status == "stale"
    assert [p.partition_id for _, _, p in stale_partitions(snapshot)] == [
        "order_date=2024-01-02"
    ]


def test_scoped_delta_keeps_only_its_own_slices(executor):
    full = _snapshot(
        executor,
        _slices(executor, {"2024-01-01": "stale", "2024-01-02": "stale"}),
    )
    scoped = scope_to_partitions(full, {"order_date=2024-01-01"})
    ds_state = scoped.assets[0].datasources[0]
    assert [p.partition_id for p in ds_state.partitions] == ["order_date=2024-01-01"]
    assert ds_state.partitions_complete is False
    # The source snapshot is untouched — scoping returns a copy.
    assert len(full.assets[0].datasources[0].partitions) == 2


def test_merge_folds_deltas_in_any_order(executor):
    base = _snapshot(
        executor,
        _slices(
            executor,
            {"2024-01-01": "stale", "2024-01-02": "stale", "2024-01-03": "stale"},
        ),
    )
    built = _snapshot(
        executor,
        _slices(
            executor,
            {"2024-01-01": "fresh", "2024-01-02": "fresh", "2024-01-03": "stale"},
        ),
    )
    first = scope_to_partitions(built, {"order_date=2024-01-01"})
    second = scope_to_partitions(built, {"order_date=2024-01-02"})

    forward = merge_snapshots(base, first, second)
    reverse = merge_snapshots(base, second, first)
    for merged in (forward, reverse):
        ds_state = merged.assets[0].datasources[0]
        assert {p.partition_id: p.status for p in ds_state.partitions} == {
            "order_date=2024-01-01": "fresh",
            "order_date=2024-01-02": "fresh",
            "order_date=2024-01-03": "stale",
        }
        assert merged.assets[0].status == "stale"
    # Replaying a delta changes nothing.
    assert (
        merge_snapshots(forward, first).assets[0].datasources[0].partitions
        == forward.assets[0].datasources[0].partitions
    )


def test_merge_recomputes_status_from_merged_slices(executor):
    base = _snapshot(executor, _slices(executor, {"2024-01-01": "stale"}))
    built = scope_to_partitions(
        _snapshot(executor, _slices(executor, {"2024-01-01": "fresh"})),
        {"order_date=2024-01-01"},
    )
    merged = merge_snapshots(base, built)
    ds_state = merged.assets[0].datasources[0]
    assert ds_state.status == "fresh"
    assert ds_state.stale_reason is None
    assert merged.summary.fresh == 1


def test_merge_keys_datasources_by_id_not_script(executor):
    """A delta comes from the per-partition build script, the base from the
    model — the same asset must not be filed twice."""
    base = _snapshot(executor, _slices(executor, {"2024-01-01": "stale"}))
    base.assets[0].datasources[0].script = "model.preql"
    delta = scope_to_partitions(
        _snapshot(executor, _slices(executor, {"2024-01-01": "fresh"})),
        {"order_date=2024-01-01"},
    )
    delta.assets[0].datasources[0].script = "build_partition.preql"
    merged = merge_snapshots(base, delta)
    assert len(merged.assets[0].datasources) == 1
    assert merged.assets[0].datasources[0].script == "model.preql"


def test_unpartitioned_datasource_carries_no_partition_state(executor):
    ds = executor.environment.datasources["raw_orders"]
    state = build_datasource_state(ds, None, None)
    assert state.partition_by == []
    assert state.partitions == []
    assert state.partitions_complete is True
