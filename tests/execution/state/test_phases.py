"""Phase recording: what a refresh found before it changed anything.

The planning probe is the only look at pre-execution state — the post-run
snapshot re-probes and can only ever see the end state. The recorder keeps
that first look (both sides of the comparison) plus the plan's verdicts, so
the snapshot can emit a begin phase a reader can audit.
"""

from datetime import datetime
from unittest.mock import MagicMock

from trilogy import Dialects
from trilogy.core.models.datasource import UpdateKey, UpdateKeyType
from trilogy.execution.state import (
    DatasourceWatermark,
    PhaseRecorder,
    RefreshPlan,
    StaleAsset,
    build_datasource_state,
    create_refresh_plan,
    get_phase_recorder,
    phase_recording,
)
from trilogy.execution.state.watermarks import RefreshKind

SCRIPT = """
key item_id int;
property item_id.updated_at datetime;

root datasource source_items (
    item_id: item_id,
    updated_at: updated_at
)
grain (item_id)
query '''
SELECT 1 as item_id, TIMESTAMP '2024-01-10 12:00:00' as updated_at
''';

datasource target_items (
    item_id: item_id,
    updated_at: updated_at
)
grain (item_id)
address target_items_table
incremental by updated_at;

CREATE IF NOT EXISTS DATASOURCE target_items;

RAW_SQL('''
INSERT INTO target_items_table
SELECT 1 as item_id, TIMESTAMP '2024-01-10 12:00:00' as updated_at
''');
"""


def _executor():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(SCRIPT)
    return executor


def _watermark(value) -> DatasourceWatermark:
    return DatasourceWatermark(
        keys={
            "updated_at": UpdateKey(
                concept_name="updated_at",
                type=UpdateKeyType.INCREMENTAL_KEY,
                value=value,
            )
        }
    )


def _plan(watermarks: dict, concept_max: dict, stale: list) -> RefreshPlan:
    return RefreshPlan(
        stale_assets=stale,
        forced_assets=[],
        watermarks=watermarks,
        concept_max_watermarks=concept_max,
        root_assets=1,
        all_assets=2,
    )


def _env(*datasources) -> MagicMock:
    env = MagicMock()
    env.datasources = {ds.identifier: ds for ds in datasources}
    return env


def _ds(identifier: str, *, is_root: bool, refreshable: bool = False) -> MagicMock:
    ds = MagicMock()
    ds.identifier = identifier
    ds.is_root = is_root
    ds.is_refreshable_root = refreshable
    ds.is_managed = (not is_root) or refreshable
    return ds


def test_ambient_recorder_is_scoped_and_restored():
    assert get_phase_recorder() is None
    with phase_recording() as recorder:
        assert get_phase_recorder() is recorder
    assert get_phase_recorder() is None


def test_first_probe_wins_over_later_reprobes():
    """A refresh re-plans and the post-run snapshot probes again; neither may
    overwrite the state the run actually started from."""
    recorder = PhaseRecorder()
    target = _ds("target_items", is_root=False)
    env = _env(target)

    recorder.record_plan(env, _plan({"target_items": _watermark("2024-01-10")}, {}, []))
    recorder.record_plan(env, _plan({"target_items": _watermark("2024-02-01")}, {}, []))

    begin = recorder.begin_for("target_items")
    assert begin is not None
    assert begin.watermark.keys["updated_at"].value == "2024-01-10"


def test_freeze_rejects_later_records():
    """The snapshot's own probe runs after execution; a datasource never
    probed during the run must not acquire a fake begin phase from it."""
    recorder = PhaseRecorder()
    env = _env(_ds("target_items", is_root=False))

    recorder.freeze()
    recorder.record_plan(env, _plan({"target_items": _watermark("2024-02-01")}, {}, []))

    assert recorder.begin_for("target_items") is None
    assert recorder.plan_for("target_items") is None


def test_begin_captures_both_sides_of_the_comparison():
    """Observed alone can't be judged — the expected values the probe compared
    against are captured with it so a reader re-derives the verdict."""
    recorder = PhaseRecorder()
    env = _env(_ds("target_items", is_root=False))
    concept_max = {
        "updated_at": UpdateKey(
            concept_name="updated_at",
            type=UpdateKeyType.INCREMENTAL_KEY,
            value=datetime(2024, 2, 1),
        )
    }

    recorder.record_plan(
        env,
        _plan(
            {"target_items": _watermark(datetime(2024, 1, 10))},
            concept_max,
            [],
        ),
    )

    begin = recorder.begin_for("target_items")
    assert begin is not None
    assert begin.concept_max["updated_at"].value == datetime(2024, 2, 1)
    assert begin.probed_at


def test_expected_side_is_snapshotted_not_aliased():
    """A later plan rebuilds concept_max in place; begin must keep what THIS
    probe compared against."""
    recorder = PhaseRecorder()
    env = _env(_ds("target_items", is_root=False))
    concept_max: dict = {}

    recorder.record_plan(
        env, _plan({"target_items": _watermark("2024-01-10")}, concept_max, [])
    )
    concept_max["updated_at"] = UpdateKey(
        concept_name="updated_at", type=UpdateKeyType.INCREMENTAL_KEY, value="later"
    )

    begin = recorder.begin_for("target_items")
    assert begin is not None
    assert begin.concept_max == {}


def test_stale_asset_verdict_is_recorded_verbatim():
    recorder = PhaseRecorder()
    env = _env(_ds("target_items", is_root=False))
    stale = StaleAsset(
        datasource_id="target_items",
        reason="incremental key 'updated_at' behind",
        kind=RefreshKind.SQL,
    )

    recorder.record_plan(env, _plan({}, {}, [stale]))

    verdict = recorder.plan_for("target_items")
    assert verdict is not None
    assert verdict.judged_stale is True
    assert verdict.reason == "incremental key 'updated_at' behind"
    assert verdict.kind == "sql"
    assert verdict.forced is False


def test_fresh_managed_datasource_gets_a_not_stale_verdict():
    """Absence of a verdict means "never planned"; a datasource the plan looked
    at and passed must say so explicitly."""
    recorder = PhaseRecorder()
    env = _env(_ds("target_items", is_root=False))

    recorder.record_plan(env, _plan({}, {}, []))

    verdict = recorder.plan_for("target_items")
    assert verdict is not None
    assert verdict.judged_stale is False
    assert verdict.kind == "sql"


def test_plain_roots_get_no_verdict():
    """The plan never judges an unmanaged root — it is the expected side, not
    something trilogy refreshes."""
    recorder = PhaseRecorder()
    env = _env(_ds("source_items", is_root=True))

    recorder.record_plan(env, _plan({}, {}, []))

    assert recorder.plan_for("source_items") is None


def test_refreshable_root_gets_a_script_kind_verdict():
    recorder = PhaseRecorder()
    env = _env(_ds("raw", is_root=True, refreshable=True))

    recorder.record_plan(env, _plan({}, {}, []))

    verdict = recorder.plan_for("raw")
    assert verdict is not None
    assert verdict.kind == "script"


def test_skipped_datasources_get_no_verdict():
    """Owned by another script in a multi-script run — that owner's plan
    records them, and this one must not claim them."""
    recorder = PhaseRecorder()
    env = _env(_ds("target_items", is_root=False))

    recorder.record_plan(env, _plan({}, {}, []), skipped={"target_items"})

    assert recorder.plan_for("target_items") is None


def test_create_refresh_plan_feeds_the_ambient_recorder():
    """The seam that makes any of this reach a snapshot: planning is where the
    pre-execution look happens."""
    executor = _executor()
    with phase_recording() as recorder:
        create_refresh_plan(executor)

    assert recorder.begin_for("target_items") is not None
    assert recorder.plan_for("target_items") is not None


def test_snapshot_emits_begin_and_end_from_the_recorder():
    executor = _executor()
    ds = executor.environment.datasources["target_items"]
    with phase_recording():
        create_refresh_plan(executor)
        state = build_datasource_state(ds, _watermark(datetime(2024, 2, 1)), None)

    assert [o.phase for o in state.observations] == ["begin", "end"]
    assert state.plan is not None


def test_snapshot_without_a_recorder_is_end_only():
    executor = _executor()
    ds = executor.environment.datasources["target_items"]

    state = build_datasource_state(ds, _watermark(datetime(2024, 2, 1)), None)

    assert [o.phase for o in state.observations] == ["end"]
    assert state.plan is None
