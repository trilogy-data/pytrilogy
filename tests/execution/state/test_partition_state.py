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
    RefreshPolicy,
    build_datasource_state,
    build_partition_states,
    create_refresh_plan,
    execute_refresh_plan,
    merge_into_snapshot,
    merge_snapshots,
    parse_partition_selector,
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
    ((state,), _) = build_partition_states(ds, observed, expected)
    assert state.status == "stale"
    assert "behind" in (state.stale_reason or "")
    assert [w.value for w in state.observed_watermarks] == ["2024-01-05 06:00:00"]
    assert [w.value for w in state.expected_watermarks] == ["2024-01-06 09:30:00"]


def test_slice_present_but_not_expected_is_not_stale(executor):
    ds = _ds(executor)
    observed = [
        PartitionObservation(values={"order_date": date(2023, 12, 31)}, row_count=4)
    ]
    ((state,), _) = build_partition_states(ds, observed, [])
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
    states, summary = build_partition_states(_ds(executor), observed, expected)

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
        _ds(executor), _days(base, 200), _days(base, 260)
    )
    assert summary.stale == 60
    assert sum(1 for s in states if s.status == "stale") == 60
    assert summary.reported == 200


def test_limit_none_disables_the_cap(executor):
    base = date(2024, 1, 1).toordinal()
    states, summary = build_partition_states(
        _ds(executor), _days(base, 300), _days(base, 300), limit=None
    )
    assert len(states) == 300
    assert summary.truncated is False
    assert summary.reported == 300


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
        _ds(executor), _days(base, 200), _days(base, 500)
    )
    state = build_datasource_state(
        _ds(executor), None, None, partitions=partitions, partition_summary=summary
    )
    assert state.status == "stale"
    assert state.stale_reason == "300 of 500 partitions stale"


def test_scoped_delta_keeps_the_summary_but_narrows_reported(executor):
    """A partial writer cannot be trusted about a slice it did not own, but an
    aggregate is not a per-slice claim — and if scoping dropped it, a fan-out
    where every run is targeted would never report totals at all."""
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
    """`--partition` speaks concept addresses; scope_to_partitions speaks
    partition ids. partition_by carries both halves of the mapping."""
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
    # An unparseable value selects nothing rather than guessing — under-matching
    # costs detail, over-matching would claim a slice the run never wrote.
    assert selector_partition_ids(snapshot, {"local.order_date": "2024-1-2"}) == {
        "order_date=2024-1-2"
    }
    # A concept the datasource is not partitioned on names nothing.
    assert selector_partition_ids(snapshot, {"local.region": "north"}) == set()


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


def test_selected_slice_types_the_value_from_the_model(executor):
    ds = _ds(executor)
    obs = selected_slice(ds, executor.environment, {"local.order_date": "2024-01-02"})
    # Typed, not the string: partition_filter builds a real comparison from it.
    assert obs.values == {"order_date": date(2024, 1, 2)}
    assert obs.id == "order_date=2024-01-02"


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
    ex.execute_text(MODEL.replace("partition by order_date", "partition by order_date, region"))
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
    targeted = [
        a for a in plan.refresh_assets if a.datasource_id == "daily_orders"
    ]
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
