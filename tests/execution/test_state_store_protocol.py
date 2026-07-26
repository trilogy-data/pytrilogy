"""Tests for the StateStore protocol and state_store injection into planning.

``StateStore`` (``trilogy/execution/state/state_store.py``) is the contract
the refresh planner/executor requires; ``BaseStateStore`` is the default
in-memory implementation. Alternate backends are injected via the
``state_store=`` parameter on ``create_refresh_plan`` /
``execute_refresh_plan`` / ``refresh_stale_assets``. Pre-seeded watermarks on
an injected store must be respected (``watermark_all_assets`` skips
already-present datasource ids) so an orchestrator-managed store can avoid
re-probing the warehouse.
"""

from datetime import datetime

from trilogy import Dialects
from trilogy.core.models.datasource import UpdateKey, UpdateKeyType
from trilogy.execution.state import (
    BaseStateStore,
    DatasourceWatermark,
    StateStore,
    create_refresh_plan,
)

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


class PreSeededStore(BaseStateStore):
    """A store whose per-asset probe must never run: watermarks are supplied
    up front (the remote/orchestrator-managed store scenario)."""

    def __init__(self) -> None:
        super().__init__()
        self.watermark_asset_calls = 0

    def watermark_asset(self, datasource, executor) -> DatasourceWatermark:
        self.watermark_asset_calls += 1
        raise AssertionError(
            f"watermark_asset should not be called for pre-seeded store "
            f"(datasource={datasource.identifier})"
        )


def test_base_state_store_satisfies_protocol():
    assert isinstance(BaseStateStore(), StateStore)


def test_preseeded_store_short_circuits_asset_probing():
    """create_refresh_plan(state_store=...) must respect pre-seeded watermarks:
    non-root datasources already present in ``store.watermarks`` are never
    re-probed (watermark_asset raises here to prove it). Root concept-max
    computation may still query the warehouse — that is expected."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(SCRIPT)

    store = PreSeededStore()
    seeded = DatasourceWatermark(
        keys={
            "updated_at": UpdateKey(
                concept_name="updated_at",
                type=UpdateKeyType.INCREMENTAL_KEY,
                value=datetime(2024, 1, 10, 12, 0, 0),
            )
        }
    )
    # Pre-populate every non-root datasource (only target_items here).
    store.watermarks["target_items"] = seeded

    plan = create_refresh_plan(executor, state_store=store)

    assert store.watermark_asset_calls == 0
    # The custom store's pre-seeded watermarks flow into the plan.
    assert plan.watermarks["target_items"] is seeded
    # Seeded value matches the root max, so nothing is stale.
    assert plan.stale_assets == []
    # Root concept-max computation ran against the custom store.
    assert "updated_at" in plan.concept_max_watermarks


def test_recording_store_is_used_for_staleness_decisions():
    """The injected store is the store — a stale-looking pre-seeded watermark
    must drive the plan without any warehouse re-probe of that asset."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(SCRIPT)

    store = PreSeededStore()
    store.watermarks["target_items"] = DatasourceWatermark(
        keys={
            "updated_at": UpdateKey(
                concept_name="updated_at",
                type=UpdateKeyType.INCREMENTAL_KEY,
                value=datetime(2020, 1, 1, 0, 0, 0),  # far behind the root
            )
        }
    )

    plan = create_refresh_plan(executor, state_store=store)

    assert store.watermark_asset_calls == 0
    stale_ids = {a.datasource_id for a in plan.stale_assets}
    assert stale_ids == {"target_items"}
    reason = plan.stale_assets[0].reason
    assert "updated_at" in reason and "behind" in reason
