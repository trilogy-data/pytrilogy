"""Tests for loading persisted asset state back into a state store.

``trilogy/execution/state/persistence.py`` is the read half of the state file
contract: a ``StateSnapshot`` written by ``--state-file`` / ``trilogy state``
rehydrates into a ``SnapshotStateStore`` whose observations are matched onto the
current environment by PHYSICAL ADDRESS, so a snapshot written by one model is
consumable by a different model pointing at the same tables. Roots are never
seeded — they are the expected side of every staleness comparison.
"""

from datetime import date, datetime
from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.datasource import UpdateKey, UpdateKeyType
from trilogy.execution.state import (
    BaseStateStore,
    DatasourceWatermark,
    SnapshotStateStore,
    StateSnapshot,
    address_type_of,
    build_datasource_state,
    create_refresh_plan,
    managed_states_by_address,
    merge_into_snapshot,
    new_state_store,
    read_state_snapshot,
    resolve_state_input,
    restore_watermark_value,
    snapshot_store_factory,
    stable_asset_key,
    state_store_factory,
    watermarks_for_datasource,
)
from trilogy.execution.state.persistence import ENV_STATE_INPUT
from trilogy.execution.state.snapshot import WatermarkValue

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

# Same physical address (target_items_table), different datasource name.
RENAMED_SCRIPT = SCRIPT.replace("target_items ", "warehouse_items ").replace(
    "DATASOURCE target_items;", "DATASOURCE warehouse_items;"
)

# Same physical address AND same physical columns, different concept name.
RENAMED_CONCEPT_SCRIPT = (
    SCRIPT.replace("item_id.updated_at", "item_id.loaded_at")
    .replace(": updated_at", ": loaded_at")
    .replace("incremental by updated_at", "incremental by loaded_at")
)


def _executor(script: str = SCRIPT):
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(script)
    return executor


def _snapshot_for(
    executor, watermarks: dict[str, DatasourceWatermark]
) -> StateSnapshot:
    entries = [
        (
            ds.safe_address,
            build_datasource_state(ds, watermarks.get(ds.identifier), None),
        )
        for ds in executor.environment.datasources.values()
    ]
    return merge_into_snapshot(entries)


def _watermark(value) -> DatasourceWatermark:
    return DatasourceWatermark(
        keys={
            "local.updated_at": UpdateKey(
                concept_name="local.updated_at",
                type=UpdateKeyType.INCREMENTAL_KEY,
                value=value,
            )
        }
    )


@pytest.mark.parametrize(
    "value",
    [
        datetime(2024, 1, 10, 12, 0, 0),
        date(2024, 1, 10),
        17,
        3.5,
        "abc123",
        True,
        None,
    ],
)
def test_watermark_value_round_trips(value):
    executor = _executor()
    snapshot = _snapshot_for(executor, {"target_items": _watermark(value)})
    ds_state = managed_states_by_address(snapshot)["target_items_table"]
    ds = executor.environment.datasources["target_items"]
    restored = watermarks_for_datasource(ds_state, ds)
    assert restored.keys["local.updated_at"].value == value
    assert restored.keys["local.updated_at"].type == UpdateKeyType.INCREMENTAL_KEY
    # concept_name mirrors the emitted key: the reader's own concept address.
    assert restored.keys["local.updated_at"].concept_name == "local.updated_at"


def test_unparseable_temporal_degrades_to_string():
    entry = WatermarkValue(
        key="updated_at",
        type="incremental_key",
        value="not-a-date",
        value_type="datetime",
    )
    assert restore_watermark_value(entry) == "not-a-date"


def test_roots_are_never_seeded():
    """A root's watermark is the expected side of every comparison — reusing a
    recorded one would hide an upstream that has since moved."""
    executor = _executor()
    snapshot = _snapshot_for(
        executor,
        {
            "target_items": _watermark(datetime(2024, 1, 10, 12, 0)),
            "source_items": _watermark(datetime(2024, 1, 10, 12, 0)),
        },
    )
    by_address = managed_states_by_address(snapshot)
    assert set(by_address) == {"target_items_table"}


def test_seeds_across_a_renamed_datasource():
    """Matching is by physical address, so a different model consuming the same
    table adopts the recorded state under its own datasource id."""
    writer = _executor()
    snapshot = _snapshot_for(
        writer, {"target_items": _watermark(datetime(2024, 1, 10, 12, 0))}
    )

    reader = _executor(RENAMED_SCRIPT)
    store = SnapshotStateStore(snapshot)
    seeded = store.seeded_watermarks(reader.environment)
    assert set(seeded) == {"warehouse_items"}
    assert seeded["warehouse_items"].keys["local.updated_at"].value == datetime(
        2024, 1, 10, 12, 0
    )


def test_rekeys_onto_a_renamed_concept():
    """Watermark keys are concept addresses, so a reader that renamed its
    concepts would never match the writer's keys. The shared physical column
    bridges them — without this, the seeded value is silently never compared."""
    writer = _executor()
    snapshot = _snapshot_for(
        writer, {"target_items": _watermark(datetime(2024, 1, 10, 12, 0))}
    )

    reader = _executor(RENAMED_CONCEPT_SCRIPT)
    ds_state = managed_states_by_address(snapshot)["target_items_table"]
    restored = watermarks_for_datasource(
        ds_state, reader.environment.datasources["target_items"]
    )

    assert set(restored.keys) == {"local.loaded_at"}
    assert restored.keys["local.loaded_at"].value == datetime(2024, 1, 10, 12, 0)


def test_unmatched_column_keeps_the_recorded_key():
    """No shared column means no translation is possible; the recorded key
    passes through rather than being silently dropped."""
    writer = _executor()
    snapshot = _snapshot_for(
        writer, {"target_items": _watermark(datetime(2024, 1, 10, 12, 0))}
    )
    ds_state = managed_states_by_address(snapshot)["target_items_table"]
    reader = _executor(SCRIPT.replace("updated_at:", "other_col:"))

    restored = watermarks_for_datasource(
        ds_state, reader.environment.datasources["target_items"]
    )
    assert set(restored.keys) == {"local.updated_at"}


def test_seeded_watermark_drives_the_plan():
    """The recorded observation is used instead of a warehouse probe: a value
    behind the live root makes the asset stale without ever querying it."""
    executor = _executor()
    snapshot = _snapshot_for(
        executor, {"target_items": _watermark(datetime(2020, 1, 1, 0, 0))}
    )

    store = SnapshotStateStore(snapshot)
    plan = create_refresh_plan(executor, state_store=store)

    assert {a.datasource_id for a in plan.stale_assets} == {"target_items"}
    assert "behind" in plan.stale_assets[0].reason


def test_unseeded_assets_still_probe_live():
    """A snapshot covering nothing relevant degrades to normal behaviour."""
    executor = _executor()
    empty = StateSnapshot()

    plan = create_refresh_plan(executor, state_store=SnapshotStateStore(empty))

    # Live probe finds target_items matching the root -> fresh.
    assert plan.stale_assets == []
    assert plan.watermarks["target_items"].keys["local.updated_at"].value == datetime(
        2024, 1, 10, 12, 0
    )


def test_seeding_does_not_resurrect_after_invalidate():
    """invalidate_address drops entries so post-refresh evaluation re-reads the
    warehouse; re-seeding there would restore the pre-refresh value."""
    executor = _executor()
    snapshot = _snapshot_for(
        executor, {"target_items": _watermark(datetime(2020, 1, 1, 0, 0))}
    )
    store = SnapshotStateStore(snapshot)
    store.watermark_all_assets(executor.environment, executor)
    assert store.watermarks["target_items"].keys["local.updated_at"].value == datetime(
        2020, 1, 1, 0, 0
    )

    store.invalidate_address(executor.environment, "target_items_table")
    store.watermark_all_assets(executor.environment, executor)

    assert store.watermarks["target_items"].keys["local.updated_at"].value == datetime(
        2024, 1, 10, 12, 0
    )


FILE_SCRIPT = """
key ev_id int;
property ev_id.ev_ts datetime;

datasource target_events (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
file `{target}`
incremental by ev_ts;
"""


def _file_executor(project_root: Path, target: Path):
    """A script at the project root binding a file under it — the layout the
    CLI enforces, where ``env.working_path`` (the script's directory) is the
    same root the writer relativized against."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.environment.working_path = str(project_root)
    executor.parse_text(FILE_SCRIPT.format(target=target.as_posix()))
    return executor


def test_file_asset_seeds_from_a_different_checkout(tmp_path):
    """The writer's key is project-relative, the reader's ds.safe_address is
    absolute and points somewhere else entirely. Matching happens on the key
    body recomputed against the reader's own root — the whole reason keys are
    relative."""
    writer_root = tmp_path / "checkout_a"
    (writer_root / "data").mkdir(parents=True)
    writer_target = writer_root / "data" / "out.parquet"
    writer = _file_executor(writer_root, writer_target)
    ds = writer.environment.datasources["target_events"]

    snapshot = merge_into_snapshot(
        [
            (
                stable_asset_key(ds.safe_address, address_type_of(ds), writer_root),
                build_datasource_state(
                    ds,
                    DatasourceWatermark(
                        keys={
                            "ev_ts": UpdateKey(
                                concept_name="ev_ts",
                                type=UpdateKeyType.INCREMENTAL_KEY,
                                value=datetime(2024, 1, 15, 12, 0),
                            )
                        }
                    ),
                    None,
                ),
            )
        ]
    )
    assert snapshot.assets[0].address == "data/out.parquet"

    reader_root = tmp_path / "checkout_b"
    (reader_root / "data").mkdir(parents=True)
    reader = _file_executor(reader_root, reader_root / "data" / "out.parquet")

    seeded = SnapshotStateStore(snapshot).seeded_watermarks(reader.environment)
    assert seeded["target_events"].keys["ev_ts"].value == datetime(2024, 1, 15, 12, 0)


def test_file_asset_outside_the_project_does_not_cross_match(tmp_path):
    """A path that can't be relativized keeps its absolute form, so it must NOT
    match a same-named file in a different checkout — that would seed one
    project's state onto another's asset."""
    writer_root = tmp_path / "checkout_a"
    writer_root.mkdir(parents=True)
    outside = tmp_path / "shared" / "out.parquet"
    outside.parent.mkdir(parents=True)
    writer = _file_executor(writer_root, outside)
    ds = writer.environment.datasources["target_events"]
    key = stable_asset_key(ds.safe_address, address_type_of(ds), writer_root)
    assert key == ds.safe_address  # unrelativizable -> absolute

    snapshot = merge_into_snapshot(
        [
            (
                key,
                build_datasource_state(
                    ds,
                    DatasourceWatermark(
                        keys={
                            "ev_ts": UpdateKey(
                                concept_name="ev_ts",
                                type=UpdateKeyType.INCREMENTAL_KEY,
                                value=datetime(2024, 1, 15, 12, 0),
                            )
                        }
                    ),
                    None,
                ),
            )
        ]
    )

    elsewhere = tmp_path / "checkout_b" / "data"
    elsewhere.mkdir(parents=True)
    reader = _file_executor(elsewhere.parent, elsewhere / "out.parquet")

    seeded = SnapshotStateStore(snapshot).seeded_watermarks(reader.environment)
    assert seeded == {}


def test_read_state_snapshot(tmp_path):
    executor = _executor()
    snapshot = _snapshot_for(
        executor, {"target_items": _watermark(datetime(2024, 1, 10, 12, 0))}
    )
    path = tmp_path / "snap.json"
    path.write_text(snapshot.model_dump_json(indent=2), encoding="utf-8")

    assert read_state_snapshot(path).model_dump() == snapshot.model_dump()


def test_read_state_snapshot_ignores_unknown_fields(tmp_path):
    """Consumers must ignore unknown fields — a file from a newer trilogy loads."""
    path = tmp_path / "snap.json"
    path.write_text(
        '{"schema_version": 99, "future_field": 1, "assets": []}', encoding="utf-8"
    )
    assert read_state_snapshot(path).schema_version == 99


def test_resolve_state_input_precedence(tmp_path, monkeypatch):
    monkeypatch.delenv(ENV_STATE_INPUT, raising=False)
    assert resolve_state_input(None) is None

    monkeypatch.setenv(ENV_STATE_INPUT, str(tmp_path / "from_env.json"))
    assert resolve_state_input(None) == tmp_path / "from_env.json"
    # Explicit flag beats the environment.
    assert resolve_state_input(str(tmp_path / "flag.json")) == tmp_path / "flag.json"


def test_ambient_factory_scopes_and_restores():
    executor = _executor()
    snapshot = _snapshot_for(
        executor, {"target_items": _watermark(datetime(2020, 1, 1, 0, 0))}
    )

    assert isinstance(new_state_store(), BaseStateStore)
    with state_store_factory(snapshot_store_factory(snapshot)):
        store = new_state_store()
        assert isinstance(store, SnapshotStateStore)
        # Each call gets its own store — managed nodes mutate independently.
        assert new_state_store() is not store
    assert not isinstance(new_state_store(), SnapshotStateStore)


def test_ambient_factory_reaches_create_refresh_plan():
    executor = _executor()
    snapshot = _snapshot_for(
        executor, {"target_items": _watermark(datetime(2020, 1, 1, 0, 0))}
    )
    with state_store_factory(snapshot_store_factory(snapshot)):
        plan = create_refresh_plan(executor)
    assert {a.datasource_id for a in plan.stale_assets} == {"target_items"}


def test_explicit_store_beats_ambient_factory():
    executor = _executor()
    snapshot = _snapshot_for(
        executor, {"target_items": _watermark(datetime(2020, 1, 1, 0, 0))}
    )
    with state_store_factory(snapshot_store_factory(snapshot)):
        plan = create_refresh_plan(executor, state_store=BaseStateStore())
    # The explicit in-memory store probed live, so nothing is behind.
    assert plan.stale_assets == []
