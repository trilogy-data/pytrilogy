"""Tests for per-partition state on partitioned datasources.

Covers the three pieces the partitioned-refresh contract rests on: probing the
observed/expected slices, judging each one, and merging per-partition deltas
back into a base snapshot without workers clobbering each other.
"""

from datetime import date, datetime
from unittest.mock import patch

import pytest

from trilogy import Dialects
from trilogy.core.models.core import DataType
from trilogy.core.models.datasource import UpdateKey, UpdateKeyType
from trilogy.execution.state import (
    MAX_REPORTED_PARTITIONS,
    BaseStateStore,
    PartitionObservation,
    RefreshKind,
    RefreshPolicy,
    build_datasource_state,
    build_partition_states,
    cap_snapshot,
    create_refresh_plan,
    execute_refresh_plan,
    merge_into_snapshot,
    merge_snapshots,
    parse_partition_selector,
    parse_partition_value,
    partition_id,
    probe_expected_partitions,
    probe_observed_partitions,
    refresh_stale_assets,
    render_partition_value,
    scope_to_partitions,
    selected_slice,
    selector_partition_ids,
    stale_partitions,
    summarize_partitions,
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
    states, _ = build_partition_states(
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
    (state,), _ = build_partition_states(ds, observed, expected)
    assert state.status == "stale"
    assert "behind" in (state.stale_reason or "")
    assert [w.value for w in state.observed_watermarks] == ["2024-01-05 06:00:00"]
    assert [w.value for w in state.expected_watermarks] == ["2024-01-06 09:30:00"]


def test_slice_present_but_not_expected_is_not_stale(executor):
    ds = _ds(executor)
    observed = [
        PartitionObservation(values={"order_date": date(2023, 12, 31)}, row_count=4)
    ]
    (state,), _ = build_partition_states(ds, observed, [])
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
    """A whole-asset snapshot: the given slices are the complete set, so the
    summary is derived from them rather than passed in."""
    ds = _ds(executor)
    return merge_into_snapshot(
        [
            (
                "daily_orders",
                build_datasource_state(
                    ds,
                    None,
                    None,
                    partitions=partitions,
                    partition_summary=summarize_partitions(partitions, "reconciled"),
                ),
            )
        ]
    )


def _partition_kwargs(ds, observed, expected):
    """``partitions=``/``partition_summary=`` for build_datasource_state."""
    partitions, summary = build_partition_states(ds, observed, expected)
    return {"partitions": partitions, "partition_summary": summary}


def _slices(executor, statuses: dict[str, str]):
    """Just the slice list — the summary has its own tests."""
    ds = _ds(executor)
    states, _ = build_partition_states(
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
    return states


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


HOLE_MODEL = """
key id int;
property id.d date;
property id.upd datetime;

root datasource src (id: id, d: d, upd: upd)
grain (id)
query '''
SELECT 1 as id, DATE '2024-01-01' as d, TIMESTAMP '2024-01-01 00:00:00' as upd
UNION ALL SELECT 2, DATE '2024-01-02', TIMESTAMP '2024-01-02 00:00:00'
UNION ALL SELECT 3, DATE '2024-01-03', TIMESTAMP '2024-01-03 00:00:00'
''';

auto mx <- max(upd) by d;

datasource facts (d: d, mx: mx)
grain (d)
address facts
freshness by mx
partition by d;

CREATE IF NOT EXISTS DATASOURCE facts;
"""

# d1 twice, d3 once, d2 missing. The duplicate does not move MAX(mx), so d1 stays
# fresh — it survives only if the refresh genuinely leaves that slice alone.
SEED_WITH_HOLE = """
RAW_SQL('''
INSERT INTO facts VALUES
  (DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00'),
  (DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00'),
  (DATE '2024-01-03', TIMESTAMP '2024-01-03 00:00:00')
''');
"""


@pytest.fixture
def hole_executor():
    ex = Dialects.DUCK_DB.default_executor()
    ex.execute_text(HOLE_MODEL)
    ex.execute_text(SEED_WITH_HOLE)
    return ex


def _counts(executor) -> dict:
    return dict(
        executor.execute_raw_sql("SELECT d, count(*) FROM facts GROUP BY 1").fetchall()
    )


def test_a_hole_in_the_range_is_stale_even_when_the_table_max_is_current(
    hole_executor,
):
    """The whole reason slices reach `is_stale`: the missing day's rows are OLDER
    than the table's MAX, so the table-level comparison reports fresh."""
    store = BaseStateStore()
    asset = store.is_stale(hole_executor.environment, hole_executor, "facts")
    assert asset is not None
    assert [obs.id for obs in asset.partitions] == ["d=2024-01-02"]
    assert "1 stale partition(s): d=2024-01-02" == asset.reason
    # Slices and incremental filters are mutually exclusive.
    assert not asset.filters.keys


def test_refresh_fills_the_hole_without_rebuilding_its_neighbours(hole_executor):
    assert _counts(hole_executor) == {date(2024, 1, 1): 2, date(2024, 1, 3): 1}
    result = refresh_stale_assets(hole_executor)
    assert result.refreshed_count == 1
    assert _counts(hole_executor) == {
        date(2024, 1, 1): 2,  # untouched — the duplicate survived
        date(2024, 1, 2): 1,  # filled
        date(2024, 1, 3): 1,
    }


def test_refresh_is_idempotent_once_the_hole_is_filled(hole_executor):
    refresh_stale_assets(hole_executor)
    after = _counts(hole_executor)
    store = BaseStateStore()
    assert store.is_stale(hole_executor.environment, hole_executor, "facts") is None
    assert _counts(hole_executor) == after


def _filter_sql(executor, slices) -> str:
    return executor.update_datasource(
        executor.environment.datasources["facts"], partitions=slices, dry_run=True
    )


def test_slice_filter_is_an_in_list_for_a_single_key(hole_executor):
    slices = [
        PartitionObservation(values={"d": date(2024, 1, 2)}),
        PartitionObservation(values={"d": date(2024, 1, 4)}),
    ]
    sql = _filter_sql(hole_executor, slices)
    # One membership test, not an OR of per-slice equalities (the list literal's
    # bracket style is the dialect's business).
    assert '"src"."d" in ' in sql.lower()
    assert "2024-01-02" in sql and "2024-01-04" in sql


def test_slice_filter_selects_a_null_slice_with_is_null(hole_executor):
    """`= NULL` matches nothing, so a NULL slice would stay stale forever."""
    sql = _filter_sql(hole_executor, [PartitionObservation(values={"d": None})])
    assert "is null" in sql.lower()


def test_empty_slice_list_refreshes_nothing(hole_executor):
    """`no stale slices` must never render as `no filter` — that would rebuild
    the whole table."""
    before = _counts(hole_executor)
    assert (
        hole_executor.update_datasource(
            hole_executor.environment.datasources["facts"], partitions=[]
        )
        is None
    )
    assert _counts(hole_executor) == before


def test_slice_filter_chunks_to_stay_within_statement_limits(
    hole_executor, monkeypatch
):
    monkeypatch.setattr("trilogy.executor.MAX_PARTITION_FILTER_VALUES", 2)
    slices = [
        PartitionObservation(values={"d": date(2024, 2, day)}) for day in range(1, 6)
    ]
    sql = _filter_sql(hole_executor, slices)
    # 5 slices at 2 per statement = 3 persists, each with its own staging table.
    assert sql.count("CREATE TEMPORARY TABLE") == 3


def _seeded_store(snapshot):
    from trilogy.execution.state import SnapshotStateStore

    return SnapshotStateStore(snapshot)


def test_state_input_seeds_partitions_without_probing(hole_executor, monkeypatch):
    """`--state-input` means "trust this snapshot" — for slices exactly as much
    as for watermarks. A seeded run must not touch the warehouse for either."""
    snapshot = merge_into_snapshot(
        [
            (
                "facts",
                build_datasource_state(
                    hole_executor.environment.datasources["facts"],
                    None,
                    None,
                    **_partition_kwargs(
                        hole_executor.environment.datasources["facts"],
                        [
                            PartitionObservation(
                                values={"d": date(2024, 1, 1)}, row_count=7
                            )
                        ],
                        [PartitionObservation(values={"d": date(2024, 1, 1)})],
                    ),
                ),
            )
        ]
    )

    def explode(*args, **kwargs):
        raise AssertionError("seeded run probed the warehouse for partitions")

    monkeypatch.setattr(
        "trilogy.execution.state.state_store.probe_observed_partitions", explode
    )
    monkeypatch.setattr(
        "trilogy.execution.state.state_store.probe_expected_partitions", explode
    )
    store = _seeded_store(snapshot)
    observed, expected = store.partition_asset(
        hole_executor.environment, hole_executor, "facts"
    )
    assert [obs.id for obs in observed] == ["d=2024-01-01"]
    assert [obs.id for obs in expected] == ["d=2024-01-01"]
    # Values come back TYPED, not as their rendered strings — the refresh filter
    # needs real values to compare and to build a WHERE.
    assert observed[0].values == {"d": date(2024, 1, 1)}
    assert observed[0].row_count == 7


def test_seeded_partitions_drive_the_staleness_verdict(hole_executor, monkeypatch):
    """Seeding is only meaningful if the seeded slices are what gets judged."""
    ds = hole_executor.environment.datasources["facts"]
    snapshot = merge_into_snapshot(
        [
            (
                "facts",
                build_datasource_state(
                    ds,
                    None,
                    None,
                    **_partition_kwargs(
                        ds,
                        [],  # nothing observed
                        [PartitionObservation(values={"d": date(2024, 1, 9)})],
                    ),
                ),
            )
        ]
    )
    monkeypatch.setattr(
        "trilogy.execution.state.state_store.probe_observed_partitions",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("probed")),
    )
    store = _seeded_store(snapshot)
    asset = store.is_stale(hole_executor.environment, hole_executor, "facts")
    assert asset is not None
    assert [obs.id for obs in asset.partitions] == ["d=2024-01-09"]


def test_seeding_falls_back_to_a_probe_when_the_snapshot_has_no_slices(hole_executor):
    """An asset absent from the snapshot is probed normally, as for watermarks."""
    store = _seeded_store(merge_into_snapshot([]))
    sides = store.partition_asset(hole_executor.environment, hole_executor, "facts")
    assert sides is not None
    assert [obs.id for obs in sides[0]] == ["d=2024-01-01", "d=2024-01-03"]


def test_unpartitioned_datasource_carries_no_partition_state(executor):
    ds = executor.environment.datasources["raw_orders"]
    state = build_datasource_state(ds, None, None)
    assert state.partition_by == []
    assert state.partitions == []
    assert state.partitions_complete is True
    assert state.partition_summary is None


# --- the cap, and the counts that have to outlive it -------------------------


def _days(start_day: int, count: int) -> list[PartitionObservation]:
    """``count`` consecutive daily slices from an ordinal, one row each."""
    return [
        PartitionObservation(
            values={"order_date": date.fromordinal(start_day + i)}, row_count=1
        )
        for i in range(count)
    ]


def test_summary_counts_cover_the_whole_set_not_the_capped_list(executor):
    """The reason the summary exists: a 200-slice payload must still be able to
    say how many of 500 slices are behind."""
    base = date(2024, 1, 1).toordinal()
    expected = _days(base, 500)
    observed = _days(base, 200)  # the first 200 days exist; 300 are missing
    states, summary = build_partition_states(
        _ds(executor), observed, expected, limit=MAX_REPORTED_PARTITIONS
    )

    assert summary.total == 500
    assert summary.stale == 300
    assert summary.missing == 300
    assert summary.reported == 200
    assert summary.truncated is True
    assert summary.level == "reconciled"
    assert len(states) == 200


def test_cap_keeps_the_stale_slices(executor):
    """Stale slices are the actionable set, so they survive the cap — a consumer
    that reads the head of the list is reading the backfill queue."""
    base = date(2024, 1, 1).toordinal()
    states, summary = build_partition_states(
        _ds(executor), _days(base, 200), _days(base, 260), limit=200
    )
    assert summary.stale == 60
    assert sum(1 for s in states if s.status == "stale") == 60
    assert summary.reported == 200


def test_the_builder_does_not_cap(executor):
    """A snapshot is complete as computed; the cap is a boundary step."""
    base = date(2024, 1, 1).toordinal()
    states, summary = build_partition_states(
        _ds(executor), _days(base, 300), _days(base, 300)
    )
    assert len(states) == 300
    assert summary.truncated is False
    assert summary.reported == 300


def test_cap_snapshot_trims_at_the_boundary(executor):
    """The budget applies to a finished snapshot, so a consumer that outgrows
    it just stops asking."""
    day = date(2024, 1, 1).toordinal()
    snapshot = _probed_snapshot(executor, _days(day, 300), _days(day, 500))
    assert len(snapshot.assets[0].datasources[0].partitions) == 500

    capped = cap_snapshot(snapshot, 200)
    ds_state = capped.assets[0].datasources[0]
    assert len(ds_state.partitions) == 200
    assert ds_state.partition_summary.total == 500
    assert ds_state.partition_summary.reported == 200
    assert ds_state.partition_summary.truncated is True
    # Whatever the budget, the actionable slices are the ones that survive.
    assert sum(1 for p in ds_state.partitions if p.status == "stale") == 200

    # None is "no budget"; 0 keeps the counts and drops every slice.
    assert cap_snapshot(snapshot, None) is snapshot
    summary_only = cap_snapshot(snapshot, 0).assets[0].datasources[0]
    assert summary_only.partitions == []
    assert summary_only.partition_summary.stale == 200


def test_resolve_partition_limit_reads_flag_then_env(monkeypatch):
    from trilogy.scripts.state import resolve_partition_limit

    monkeypatch.delenv("TRILOGY_STATE_MAX_PARTITIONS", raising=False)
    assert resolve_partition_limit(None) is None, "uncapped unless asked"
    assert resolve_partition_limit("all") is None
    assert resolve_partition_limit("0") == 0
    assert resolve_partition_limit(str(MAX_REPORTED_PARTITIONS)) == 200

    monkeypatch.setenv("TRILOGY_STATE_MAX_PARTITIONS", "20")
    assert resolve_partition_limit(None) == 20
    assert resolve_partition_limit("50") == 50, "the flag wins over the env"
    assert resolve_partition_limit("all") is None, "and can turn the env off"

    with pytest.raises(ValueError, match="expected an integer or 'all'"):
        resolve_partition_limit("lots")
    with pytest.raises(ValueError, match="use 'all' for no limit"):
        resolve_partition_limit("-1")


def test_unresolved_expectation_is_scan_not_reconciled(executor):
    """probe_expected_partitions swallows an unresolvable plan and returns [].
    Reporting that as `reconciled` would make `missing: 0` read as a clean bill
    of health when nothing was actually asked for."""
    base = date(2024, 1, 1).toordinal()
    _, summary = build_partition_states(_ds(executor), _days(base, 3), [])
    assert summary.level == "scan"
    assert summary.missing == 0
    assert summary.total == 3


def test_capped_datasource_still_reports_stale(executor):
    """The verdict comes off the summary, so a table 300 slices behind cannot
    read as fresh just because they did not fit in the payload."""
    base = date(2024, 1, 1).toordinal()
    partitions, summary = build_partition_states(
        _ds(executor), _days(base, 200), _days(base, 500), limit=MAX_REPORTED_PARTITIONS
    )
    state = build_datasource_state(
        _ds(executor), None, None, partitions=partitions, partition_summary=summary
    )
    assert state.status == "stale"
    assert state.stale_reason == "300 of 500 partitions stale"


def _probed_snapshot(executor, observed, expected):
    """A whole-asset probe, carrying every slice it found."""
    ds = _ds(executor)
    return merge_into_snapshot(
        [
            (
                "daily_orders",
                build_datasource_state(
                    ds, None, None, **_partition_kwargs(ds, observed, expected)
                ),
            )
        ]
    )


def _capped_snapshot(executor, observed, expected):
    """The same probe as a size-budgeted consumer receives it."""
    return cap_snapshot(
        _probed_snapshot(executor, observed, expected), MAX_REPORTED_PARTITIONS
    )


def test_a_truncated_complete_probe_overlays_instead_of_replacing(executor):
    """A truncated probe's silence about a slice is "did not fit", not "gone".
    Replacing would shrink the accumulated work list to one payload's worth."""
    day = date(2024, 1, 1).toordinal()
    base = _capped_snapshot(executor, _days(day, 500), _days(day, 500))
    assert len(base.assets[0].datasources[0].partitions) == 200

    # A later probe of the same table: 40 slices have gone missing.
    delta = _capped_snapshot(executor, _days(day + 40, 460), _days(day, 500))
    ds_state = merge_snapshots(base, delta).assets[0].datasources[0]

    assert ds_state.partition_summary.total == 500
    assert len(ds_state.partitions) > 200, "the union outlives one payload"
    assert ds_state.partition_summary.reported == len(ds_state.partitions)
    # Every slice the latest probe found missing is in the work list.
    stale = {p.partition_id for p in ds_state.partitions if p.status == "stale"}
    assert len(stale) == 40


def test_an_untruncated_complete_probe_still_replaces(executor):
    """Absence in a whole list is the only signal a slice was dropped from the
    table, so it must keep propagating where nothing was trimmed."""
    day = date(2024, 1, 1).toordinal()
    base = _capped_snapshot(executor, _days(day, 5), _days(day, 5))
    delta = _capped_snapshot(executor, _days(day, 3), _days(day, 3))
    ds_state = merge_snapshots(base, delta).assets[0].datasources[0]
    assert len(ds_state.partitions) == 3


def test_a_truncated_probe_drops_stale_entries_it_proved_healthy(executor):
    """It kept stale slices first, so when it carried every stale one, a base
    entry it does not mention is provably no longer stale — dispatching a run to
    rediscover that is waste the record can avoid."""
    day = date(2024, 1, 1).toordinal()
    base = _capped_snapshot(executor, _days(day + 1, 499), _days(day, 500))
    assert [
        p.partition_id
        for p in base.assets[0].datasources[0].partitions
        if p.status == "stale"
    ]

    healthy = _capped_snapshot(executor, _days(day, 500), _days(day, 500))
    ds_state = merge_snapshots(base, healthy).assets[0].datasources[0]
    assert ds_state.partition_summary.stale == 0
    assert not [p for p in ds_state.partitions if p.status == "stale"]
    assert ds_state.status == "fresh"


def test_scoped_delta_keeps_the_summary_but_narrows_reported(executor):
    """An aggregate is not a per-slice claim, and dropping it would leave a
    fan-out of targeted runs never reporting totals at all."""
    snapshot = _snapshot(
        executor,
        _slices(
            executor,
            {"2024-01-01": "stale", "2024-01-02": "stale", "2024-01-03": "fresh"},
        ),
    )
    scoped = scope_to_partitions(snapshot, {"order_date=2024-01-01"})
    summary = scoped.assets[0].datasources[0].partition_summary
    assert summary is not None
    assert summary.total == 3, "the table still has three slices"
    assert summary.reported == 1, "this worker speaks for one of them"
    assert summary.stale == 2
    assert summary.truncated is True


def test_partition_selector_resolves_to_ids_off_the_snapshot(executor):
    """`--partition` speaks concept addresses, scope_to_partitions ids."""
    snapshot = _snapshot(
        executor, _slices(executor, {"2024-01-01": "stale", "2024-01-02": "fresh"})
    )
    assert selector_partition_ids(snapshot, {"local.order_date": "2024-01-02"}) == {
        "order_date=2024-01-02"
    }
    # ISO spellings of the same day normalize to the same slice.
    assert selector_partition_ids(snapshot, {"local.order_date": "20240102"}) == {
        "order_date=2024-01-02"
    }
    # A value matching no recorded slice still names one: an empty result means
    # "do not scope", and an unscoped snapshot claims the whole table.
    assert selector_partition_ids(snapshot, {"local.order_date": "2024-1-2"}) == {
        "order_date=2024-1-2"
    }
    # A concept the datasource is not partitioned on names nothing.
    assert selector_partition_ids(snapshot, {"local.region": "north"}) == set()


DATETIME_MODEL = """
key id int;
property id.ts datetime;
property id.v int;

root datasource src (id: id, ts: ts, v: v)
grain (id)
query '''SELECT 1 as id, TIMESTAMP '2024-01-03 00:00:00' as ts, 5 as v''';

auto mx <- max(v) by ts;

datasource ts_facts (ts: ts, mx: mx)
grain (ts)
address ts_facts
freshness by mx
partition by ts;
"""


def test_selector_scopes_the_slice_a_datetime_column_actually_recorded():
    """The flag is spelled as a date; a datetime-typed column records the slice
    as an instant. Scoping has to name the id the refresh wrote, or the delta
    reports on no slice at all and the backfill never records progress."""
    ex = Dialects.DUCK_DB.default_executor()
    ex.execute_text(DATETIME_MODEL)
    ds = ex.environment.datasources["ts_facts"]

    written = selected_slice(ds, ex.environment, {"local.ts": "2024-01-03"})
    assert written.id == "ts=2024-01-03T00:00:00"

    snapshot = merge_into_snapshot(
        [
            (
                "ts_facts",
                build_datasource_state(
                    ds, None, None, **_partition_kwargs(ds, [written], [written])
                ),
            )
        ]
    )
    assert written.id in selector_partition_ids(snapshot, {"local.ts": "2024-01-03"})


def test_merge_adjusts_summary_counts_for_the_slices_a_delta_owned(executor):
    """A backfill that fixes one of N stale slices must leave a readable N-1
    behind, not a recount of whatever survived the cap."""
    base = _snapshot(
        executor,
        _slices(
            executor,
            {"2024-01-01": "stale", "2024-01-02": "stale", "2024-01-03": "fresh"},
        ),
    )
    assert base.assets[0].datasources[0].partition_summary.stale == 2

    fixed = scope_to_partitions(
        _snapshot(
            executor,
            _slices(
                executor,
                {"2024-01-01": "fresh", "2024-01-02": "stale", "2024-01-03": "fresh"},
            ),
        ),
        {"order_date=2024-01-01"},
    )
    merged = merge_snapshots(base, fixed)
    summary = merged.assets[0].datasources[0].partition_summary
    assert summary.stale == 1
    assert summary.total == 3
    assert merged.assets[0].datasources[0].status == "stale"


def test_a_snapshot_written_before_summaries_still_merges(executor):
    """`schema_version` was not bumped, so files predating `partition_summary`
    are still valid input. The verdict then has to come off the slice list, as
    it did before the counts existed."""
    base = _snapshot(
        executor, _slices(executor, {"2024-01-01": "stale", "2024-01-02": "fresh"})
    )
    old = _snapshot(executor, _slices(executor, {"2024-01-01": "fresh"}))
    for asset in (*base.assets, *old.assets):
        for ds_state in asset.datasources:
            ds_state.partition_summary = None
    old.assets[0].datasources[0].partitions_complete = False

    merged = merge_snapshots(base, old).assets[0].datasources[0]
    assert merged.partition_summary is None
    assert merged.status == "fresh"
    assert {p.partition_id: p.status for p in merged.partitions} == {
        "order_date=2024-01-01": "fresh",
        "order_date=2024-01-02": "fresh",
    }


def test_summary_counts_do_not_depend_on_merge_order(executor):
    """Each worker's delta carries a whole-table probe taken at a different
    moment. Preferring the delta's aggregate would make the merged counts — and
    the status derived from them — a function of which file was folded last."""
    base = _snapshot(
        executor, _slices(executor, {"2024-01-01": "stale", "2024-01-02": "stale"})
    )
    # The worker that finished first still saw its neighbour stale.
    early = scope_to_partitions(
        _snapshot(
            executor, _slices(executor, {"2024-01-01": "fresh", "2024-01-02": "stale"})
        ),
        {"order_date=2024-01-01"},
    )
    late = scope_to_partitions(
        _snapshot(
            executor, _slices(executor, {"2024-01-01": "fresh", "2024-01-02": "fresh"})
        ),
        {"order_date=2024-01-02"},
    )
    forward = merge_snapshots(base, early, late).assets[0].datasources[0]
    reverse = merge_snapshots(base, late, early).assets[0].datasources[0]
    assert forward.partition_summary.stale == reverse.partition_summary.stale == 0
    assert forward.status == reverse.status == "fresh"
    # And the aggregate never contradicts the slices in its own record.
    assert not [p for p in reverse.partitions if p.status == "stale"]


def test_a_scoped_delta_can_add_a_slice_the_base_never_had(executor):
    """A worker that just built a brand-new slice grows the table. The base's
    counts stand for everything else, so the new one is added rather than
    triggering a recount from a payload that speaks for one slice."""
    base = _snapshot(
        executor, _slices(executor, {"2024-01-01": "stale", "2024-01-02": "fresh"})
    )
    assert base.assets[0].datasources[0].partition_summary.total == 2

    fresh_slice = scope_to_partitions(
        _snapshot(executor, _slices(executor, {"2024-01-03": "stale"})),
        {"order_date=2024-01-03"},
    )
    merged = merge_snapshots(base, fresh_slice).assets[0].datasources[0]

    assert merged.partition_summary.total == 3
    assert merged.partition_summary.stale == 2
    assert merged.partition_summary.missing == 2
    assert merged.partition_summary.first == "order_date=2024-01-01"
    assert merged.partition_summary.last == "order_date=2024-01-03"
    assert len(merged.partitions) == 3


# --- `refresh --partition`: naming the slice a run owns ----------------------


def test_parse_partition_selector_accepts_repeats_and_commas():
    assert parse_partition_selector(["a.b=1", "c.d=2,e.f=3"]) == {
        "a.b": "1",
        "c.d": "2",
        "e.f": "3",
    }


def test_parse_partition_selector_rejects_a_bare_value():
    with pytest.raises(ValueError, match="expected <concept.address>=<value>"):
        parse_partition_selector(["2026-07-30"])
    # A continuation cannot reach across flags, or a malformed second flag would
    # be swallowed into the first flag's value.
    with pytest.raises(ValueError, match="expected <concept.address>=<value>"):
        parse_partition_selector(["a.b=1", "oops"])


def test_parse_partition_selector_keeps_a_comma_bearing_value():
    """Commas separate pairs, so a value holding one would otherwise be
    truncated to everything before it — silently selecting a different slice."""
    assert parse_partition_selector(["a.b=north,south"]) == {"a.b": "north,south"}
    assert parse_partition_selector(["a.b=x,y", "c.d=2"]) == {"a.b": "x,y", "c.d": "2"}


def test_parse_partition_selector_rejects_a_conflicting_repeat():
    """Two values for one concept name a range, not the slice a run owns."""
    with pytest.raises(ValueError, match="names a.b twice"):
        parse_partition_selector(["a.b=1", "a.b=2"])
    # Repeating the same value is not a conflict.
    assert parse_partition_selector(["a.b=1", "a.b=1"]) == {"a.b": "1"}


def test_policy_owns_its_selector():
    """One policy is shared by every managed node on the refresh thread pool."""
    source = {"local.order_date": "2024-01-03"}
    policy = RefreshPolicy(partition_selector=source)
    source["local.order_date"] = "2024-01-04"
    assert policy.partition_selector == {"local.order_date": "2024-01-03"}
    with pytest.raises(TypeError):
        policy.partition_selector["local.order_date"] = "2024-01-05"  # type: ignore[index]


def test_policy_is_hashable_despite_holding_a_mapping():
    """It advertises frozen, so anything that treats it as a value must work —
    the generated __hash__ would raise on the read-only mapping."""
    policy = RefreshPolicy(
        force_sources=frozenset({"a"}), partition_selector={"local.d": "2024-01-03"}
    )
    twin = RefreshPolicy(
        force_sources=frozenset({"a"}), partition_selector={"local.d": "2024-01-03"}
    )
    assert len({policy, twin}) == 1
    assert hash(policy) != hash(RefreshPolicy())


def test_selected_slice_types_the_value_from_the_model(executor):
    ds = _ds(executor)
    obs = selected_slice(ds, executor.environment, {"local.order_date": "2024-01-02"})
    # Typed, not the string: partition_filter builds a real comparison from it.
    assert obs.values == {"order_date": date(2024, 1, 2)}
    assert obs.id == "order_date=2024-01-02"


NON_TEMPORAL_MODEL = """
key order_id int;
property order_id.yr int;
property order_id.ratio float;
property order_id.live bool;
property order_id.amt int;

root datasource raw (order_id: order_id, yr: yr, ratio: ratio, live: live, amt: amt)
grain (order_id)
query '''SELECT 1 as order_id, 2024 as yr, 0.5 as ratio, true as live, 10 as amt
UNION ALL SELECT 2, 2025, 1.5, false, 20''';

auto total <- sum(amt) by yr, ratio, live;

datasource by_kind (yr: yr, ratio: ratio, live: live, total: total)
grain (yr, ratio, live)
address by_kind
incremental by yr
partition by yr, ratio, live;
"""


def test_selector_types_non_temporal_partition_values():
    """Dates are the common key, but the parse is shared with snapshot restore
    and has to type the rest — a string on an int column reaches partition_filter
    and renders a comparison that cannot match."""
    ex = Dialects.DUCK_DB.default_executor()
    ex.execute_text(NON_TEMPORAL_MODEL)
    ds = ex.environment.datasources["by_kind"]

    obs = selected_slice(
        ds,
        ex.environment,
        {"local.yr": "2025", "local.ratio": "1.5", "local.live": "false"},
    )
    assert obs.values == {"yr": 2025, "ratio": 1.5, "live": False}


def test_unparseable_partition_value_degrades_to_the_string():
    """It still compares consistently, and the refresh fails loudly at execute
    rather than quietly writing the wrong slice."""
    ex = Dialects.DUCK_DB.default_executor()
    ex.execute_text(NON_TEMPORAL_MODEL)
    ds = ex.environment.datasources["by_kind"]

    obs = selected_slice(
        ex.environment.datasources["by_kind"],
        ex.environment,
        {"local.yr": "not-an-int", "local.ratio": "x", "local.live": "false"},
    )
    assert obs.values == {"yr": "not-an-int", "ratio": "x", "live": False}
    assert ds.partition_by


def test_null_partition_token_restores_as_none(executor):
    """A NULL slice is a real slice; round-tripping it as the literal token
    would make it a distinct, never-matching value."""
    ds = _ds(executor)
    (state,), _ = build_partition_states(
        ds, [PartitionObservation(values={"order_date": None}, row_count=2)], []
    )
    assert state.values == {"order_date": NULL_PARTITION_TOKEN}
    assert parse_partition_value(NULL_PARTITION_TOKEN, DataType.DATE) is None


def test_selected_slice_is_none_for_a_datasource_it_does_not_name(executor):
    """A directory holds many assets; a selector speaks only for the ones keyed
    on the concept it names. Not applying is normal, not an error."""
    ds = _ds(executor)
    assert selected_slice(ds, executor.environment, {"local.region": "north"}) is None
    root = executor.environment.datasources["raw_orders"]
    assert selected_slice(root, executor.environment, {"local.order_date": "x"}) is None


def test_partial_multi_column_key_is_rejected(executor):
    """Naming one of two key columns identifies a range, not a slice. Widening a
    targeted refresh into a partial rebuild silently is the failure to avoid."""
    ex = Dialects.DUCK_DB.default_executor()
    ex.execute_text(
        MODEL.replace("partition by order_date", "partition by order_date, region")
    )
    ds = ex.environment.datasources["daily_orders"]
    with pytest.raises(ValueError, match="only part of"):
        selected_slice(ds, ex.environment, {"local.order_date": "2024-01-02"})


def test_selector_targets_the_slice_even_when_nothing_looks_stale(executor):
    """A tick that owns a day must load that day. The slice may be absent from
    state entirely, so consulting staleness first would find nothing to do."""
    executor.execute_text(BUILD_ONE_DAY)
    plan = create_refresh_plan(
        executor,
        policy=RefreshPolicy(partition_selector={"local.order_date": "2024-01-01"}),
    )
    targeted = [a for a in plan.refresh_assets if a.datasource_id == "daily_orders"]
    assert len(targeted) == 1, "the asset is planned exactly once, not twice"
    assert [p.id for p in targeted[0].partitions] == ["order_date=2024-01-01"]
    assert "partition order_date=2024-01-01 requested" == targeted[0].reason


def test_selector_refresh_writes_only_its_own_slice(executor):
    """End to end: the targeted refresh fills its slice and leaves neighbours
    untouched."""
    executor.execute_text(BUILD_ONE_DAY)
    before = executor.execute_raw_sql(
        "SELECT order_date, order_count FROM daily_orders ORDER BY 1"
    ).fetchall()
    assert [str(r[0]) for r in before] == ["2024-01-01"]

    plan = create_refresh_plan(
        executor,
        policy=RefreshPolicy(partition_selector={"local.order_date": "2024-01-03"}),
    )
    execute_refresh_plan(executor, plan)

    after = executor.execute_raw_sql(
        "SELECT order_date FROM daily_orders ORDER BY 1"
    ).fetchall()
    # 2024-01-02 is stale in source and deliberately still absent: it was not
    # the slice this run owned.
    assert [str(r[0]) for r in after] == ["2024-01-01", "2024-01-03"]


def test_a_targeted_slice_survives_the_post_script_re_evaluation(executor):
    """`execute_refresh_plan` re-decides staleness for SQL assets once a
    script-kind refresh has run. A targeted slice must not be re-decided — it may
    look fresh (this one already exists) and would be dropped from the plan,
    which is the sentinel `StaleAsset.explicit` replaced a reason-string check to
    prevent."""
    executor.execute_text(BUILD_ONE_DAY)
    raw = executor.environment.datasources["raw_orders"]
    executor.environment.datasources["raw_orders"] = raw.model_copy(
        update={"freshness_probe": "/fake/probe.py", "refresh_script": "/fake/ref.py"}
    )
    policy = RefreshPolicy(
        force_sources=frozenset({"raw_orders"}),
        partition_selector={"local.order_date": "2024-01-01"},
    )

    with patch(
        "trilogy.execution.state.state_store.run_freshness_probe", return_value=True
    ), patch("trilogy.execution.state.state_store.run_refresh_script"):
        plan = create_refresh_plan(executor, policy=policy)
        assert any(
            a.kind == RefreshKind.SCRIPT for a in plan.refresh_assets
        ), "the script asset is what turns on re-evaluation"
        targeted = next(
            a for a in plan.refresh_assets if a.datasource_id == "daily_orders"
        )
        assert targeted.explicit is True
        assert [p.id for p in targeted.partitions] == ["order_date=2024-01-01"]

        result = execute_refresh_plan(executor, plan)

    assert result.refreshed_count == 2, "the script AND the slice it did not re-decide"
    rows = executor.execute_raw_sql(
        "SELECT order_date, order_count FROM daily_orders ORDER BY 1"
    ).fetchall()
    assert [str(r[0]) for r in rows] == ["2024-01-01"], "its slice, and only its slice"


def test_complete_delta_replaces_the_summary(executor):
    base = _snapshot(
        executor, _slices(executor, {"2024-01-01": "stale", "2024-01-02": "stale"})
    )
    complete = _snapshot(
        executor, _slices(executor, {"2024-01-01": "fresh", "2024-01-02": "fresh"})
    )
    merged = merge_snapshots(base, complete)
    summary = merged.assets[0].datasources[0].partition_summary
    assert summary.stale == 0
    assert merged.assets[0].datasources[0].status == "fresh"


# --- what the expected-side probe may and may not absorb ---------------------


def test_expected_probe_absorbs_an_unresolvable_model():
    """Hiding non-roots can leave the partition key underivable — the model
    answering "no expectation", not a failure.

    Unmocked on purpose: a rootless model reaches this for real, which is what
    pins the exception the planner actually raises into UNRESOLVABLE_ERRORS. An
    injected one only asserts that whatever it injected is caught.
    """
    ex = Dialects.DUCK_DB.default_executor()
    ex.execute_text(
        MODEL.replace("root datasource raw_orders", "datasource raw_orders")
    )
    ds = ex.environment.datasources["daily_orders"]

    assert not any(d.is_root for d in ex.environment.datasources.values())
    assert probe_expected_partitions(ds, ex, set()) == []


def test_expected_probe_does_not_absorb_a_warehouse_failure(executor):
    """An empty expected side makes every slice look fresh, so swallowing a
    broken connection here would report a healthy table for an unreachable one.

    Injected: a real connection failure is not reproducible on demand.
    """
    ds = _ds(executor)
    with patch.object(
        executor,
        "execute_ephemeral",
        side_effect=RuntimeError("connection reset by peer"),
    ), pytest.raises(RuntimeError, match="connection reset"):
        probe_expected_partitions(ds, executor, set())


def test_expected_probe_restores_hidden_datasources_after_a_failure(executor):
    """The probe mutates the environment; a raise must not leave it stripped."""
    ds = _ds(executor)
    before = dict(executor.environment.datasources)
    with patch.object(
        executor, "execute_ephemeral", side_effect=RuntimeError("x")
    ), pytest.raises(RuntimeError):
        probe_expected_partitions(ds, executor, set())
    assert executor.environment.datasources == before
