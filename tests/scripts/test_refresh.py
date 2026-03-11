from unittest.mock import patch

import pytest

from trilogy import Dialects
from trilogy.core.models.datasource import UpdateKey, UpdateKeyType
from trilogy.execution.state import (
    DatasourceWatermark,
    StaleAsset,
    refresh_stale_assets,
)
from trilogy.scripts.dependency import ScriptNode
from trilogy.scripts.display import (
    is_rich_available,
    set_rich_mode,
    show_refresh_plan,
    show_stale_assets,
    show_watermarks,
)
from trilogy.scripts.refresh import _prompt_approval, execute_script_for_refresh
from trilogy.scripts.single_execution import execute_refresh_mode

requires_rich = pytest.mark.skipif(
    not is_rich_available(), reason="rich is not installed"
)


def _sample_watermarks() -> dict[str, DatasourceWatermark]:
    return {
        "orders": DatasourceWatermark(
            keys={
                "order_date": UpdateKey(
                    concept_name="order_date",
                    type=UpdateKeyType.INCREMENTAL_KEY,
                    value="2024-01-15",
                )
            }
        ),
        "users": DatasourceWatermark(keys={}),
    }


def _sample_stale_assets() -> list[StaleAsset]:
    return [
        StaleAsset(
            datasource_id="target_orders",
            reason="incremental key 'order_date' behind: 2024-01-10 < 2024-01-15",
        ),
    ]


def test_show_watermarks_empty(capsys):
    """Test formatting when watermarks dict is empty."""
    with set_rich_mode(False):
        watermarks: dict[str, DatasourceWatermark] = {}
        show_watermarks(watermarks)
    captured = capsys.readouterr()
    assert "Watermarks:" in captured.out


def test_show_watermarks_no_keys(capsys):
    """Test formatting when datasource has no watermark keys."""
    with set_rich_mode(False):
        watermarks = {"ds1": DatasourceWatermark(keys={})}
        show_watermarks(watermarks)
    captured = capsys.readouterr()
    assert "Watermarks:" in captured.out
    assert "ds1: (no watermarks)" in captured.out


def test_show_watermarks_with_keys(capsys):
    """Test formatting datasources with watermark keys."""
    with set_rich_mode(False):
        watermarks = {
            "orders": DatasourceWatermark(
                keys={
                    "order_date": UpdateKey(
                        concept_name="order_date",
                        type=UpdateKeyType.INCREMENTAL_KEY,
                        value="2024-01-15",
                    )
                }
            )
        }
        show_watermarks(watermarks)
    captured = capsys.readouterr()
    assert "Watermarks:" in captured.out
    assert "orders.order_date: 2024-01-15 (incremental_key)" in captured.out


def test_show_watermarks_multiple_datasources(capsys):
    """Test formatting multiple datasources with various watermark configurations."""
    with set_rich_mode(False):
        watermarks = {
            "users": DatasourceWatermark(keys={}),
            "events": DatasourceWatermark(
                keys={
                    "event_time": UpdateKey(
                        concept_name="event_time",
                        type=UpdateKeyType.UPDATE_TIME,
                        value="2024-06-01T12:00:00",
                    ),
                    "event_id": UpdateKey(
                        concept_name="event_id",
                        type=UpdateKeyType.INCREMENTAL_KEY,
                        value=12345,
                    ),
                }
            ),
        }
        show_watermarks(watermarks)
    captured = capsys.readouterr()
    assert "Watermarks:" in captured.out
    assert "users: (no watermarks)" in captured.out
    assert "events.event_id: 12345 (incremental_key)" in captured.out
    assert "events.event_time: 2024-06-01T12:00:00 (update_time)" in captured.out


def test_show_watermarks_sorted_output(capsys):
    """Test that datasources are printed in sorted order."""
    with set_rich_mode(False):
        watermarks = {
            "zebra": DatasourceWatermark(keys={}),
            "alpha": DatasourceWatermark(keys={}),
            "beta": DatasourceWatermark(keys={}),
        }
        show_watermarks(watermarks)
    captured = capsys.readouterr()
    alpha_pos = captured.out.find("alpha")
    beta_pos = captured.out.find("beta")
    zebra_pos = captured.out.find("zebra")
    assert alpha_pos < beta_pos < zebra_pos


@requires_rich
def test_show_watermarks_rich(capsys):
    with set_rich_mode(True):
        show_watermarks(_sample_watermarks())
    captured = capsys.readouterr()
    assert "Datasource Watermarks" in captured.out
    assert "orders" in captured.out
    assert "order_date" in captured.out
    assert "2024-01-15" in captured.out
    assert "users" in captured.out
    assert "(no watermarks)" in captured.out


def test_show_stale_assets_plain(capsys):
    with set_rich_mode(False):
        show_stale_assets(_sample_stale_assets())
    captured = capsys.readouterr()
    assert "Stale Assets to Refresh" in captured.out
    assert "target_orders" in captured.out
    assert "behind" in captured.out


@requires_rich
def test_show_stale_assets_rich(capsys):
    with set_rich_mode(True):
        show_stale_assets(_sample_stale_assets())
    captured = capsys.readouterr()
    assert "Stale Assets to Refresh" in captured.out
    assert "target_orders" in captured.out


def test_show_refresh_plan_plain(capsys):
    with set_rich_mode(False):
        show_refresh_plan(_sample_stale_assets(), _sample_watermarks())
    captured = capsys.readouterr()
    assert "Watermarks:" in captured.out
    assert "Stale Assets to Refresh" in captured.out
    assert "target_orders" in captured.out


@requires_rich
def test_show_refresh_plan_rich(capsys):
    with set_rich_mode(True):
        show_refresh_plan(_sample_stale_assets(), _sample_watermarks())
    captured = capsys.readouterr()
    assert "Datasource Watermarks" in captured.out
    assert "Stale Assets to Refresh" in captured.out


def test_prompt_approval_accepted(capsys):
    with set_rich_mode(False), patch("click.confirm", return_value=True):
        result = _prompt_approval(_sample_stale_assets(), _sample_watermarks())
    assert result is True


def test_prompt_approval_declined(capsys):
    with set_rich_mode(False), patch("click.confirm", return_value=False):
        result = _prompt_approval(_sample_stale_assets(), _sample_watermarks())
    assert result is False


def test_refresh_stale_assets_on_approval_declined():
    """on_approval returning False should skip refresh."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(
        """
        key item_id int;
        property item_id.name string;
        property item_id.updated_at datetime;

        root datasource source_items (
            item_id: item_id,
            name: name,
            updated_at: updated_at
        )
        grain (item_id)
        query '''
        SELECT 1 as item_id, 'Widget' as name, TIMESTAMP '2024-01-10 12:00:00' as updated_at
        '''
        freshness by updated_at;

        datasource target_items (
            item_id: item_id,
            name: name,
            updated_at: updated_at
        )
        grain (item_id)
        address target_items_table
        freshness by updated_at;

        CREATE IF NOT EXISTS DATASOURCE target_items;
        """
    )

    result = refresh_stale_assets(
        executor,
        on_approval=lambda assets, wm: False,
        force_sources={"target_items"},
    )
    assert result.stale_count == 1
    assert result.refreshed_count == 0


def test_refresh_stale_assets_on_approval_accepted():
    """on_approval returning True should proceed with refresh."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(
        """
        key item_id int;
        property item_id.name string;
        property item_id.updated_at datetime;

        root datasource source_items (
            item_id: item_id,
            name: name,
            updated_at: updated_at
        )
        grain (item_id)
        query '''
        SELECT 1 as item_id, 'Widget' as name, TIMESTAMP '2024-01-10 12:00:00' as updated_at
        '''
        freshness by updated_at;

        datasource target_items (
            item_id: item_id,
            name: name,
            updated_at: updated_at
        )
        grain (item_id)
        address target_items_table
        freshness by updated_at;

        CREATE IF NOT EXISTS DATASOURCE target_items;
        """
    )

    result = refresh_stale_assets(
        executor,
        on_approval=lambda assets, wm: True,
        force_sources={"target_items"},
    )
    assert result.stale_count == 1
    assert result.refreshed_count == 1


def test_refresh_stale_assets_forced():
    """Test that force_sources forces rebuild regardless of staleness."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(
        """
        key item_id int;
        property item_id.name string;
        property item_id.updated_at datetime;

        root datasource source_items (
            item_id: item_id,
            name: name,
            updated_at: updated_at
        )
        grain (item_id)
        query '''
        SELECT 1 as item_id, 'Widget' as name, TIMESTAMP '2024-01-10 12:00:00' as updated_at
        '''
        freshness by updated_at;

        datasource target_items (
            item_id: item_id,
            name: name,
            updated_at: updated_at
        )
        grain (item_id)
        address target_items_table
        freshness by updated_at;

        CREATE IF NOT EXISTS DATASOURCE target_items;

        RAW_SQL('''
        INSERT INTO target_items_table
        SELECT 1 as item_id, 'Widget' as name, TIMESTAMP '2024-01-10 12:00:00' as updated_at
        ''');
        """
    )

    refreshed_assets: list[tuple[str, str]] = []

    def track_refresh(asset_id: str, reason: str) -> None:
        refreshed_assets.append((asset_id, reason))

    # Without force, target_items should NOT be refreshed (it's up to date)
    result = refresh_stale_assets(executor, on_refresh=track_refresh)
    assert result.stale_count == 0
    assert result.refreshed_count == 0
    assert len(refreshed_assets) == 0

    # With force, target_items SHOULD be refreshed
    result = refresh_stale_assets(
        executor, on_refresh=track_refresh, force_sources={"target_items"}
    )
    assert result.stale_count == 1
    assert result.refreshed_count == 1
    assert len(refreshed_assets) == 1
    assert refreshed_assets[0][0] == "target_items"
    assert refreshed_assets[0][1] == "forced rebuild"


STALE_SCRIPT = """\
key item_id int;
property item_id.name string;
property item_id.updated_at datetime;

root datasource source_items (
    item_id: item_id,
    name: name,
    updated_at: updated_at
)
grain (item_id)
query '''
SELECT 1 as item_id, 'Widget' as name, TIMESTAMP '2024-01-10 12:00:00' as updated_at
'''
freshness by updated_at;

datasource target_items (
    item_id: item_id,
    name: name,
    updated_at: updated_at
)
grain (item_id)
address target_items_table
freshness by updated_at;

CREATE IF NOT EXISTS DATASOURCE target_items;
"""


def test_execute_script_for_refresh_with_watermarks(tmp_path, capsys):
    """Cover on_watermarks callback and refreshed success path."""
    script = tmp_path / "test.preql"
    script.write_text(STALE_SCRIPT)
    executor = Dialects.DUCK_DB.default_executor()
    node = ScriptNode(path=script)

    with set_rich_mode(False):
        stats = execute_script_for_refresh(
            executor,
            node,
            print_watermarks=True,
            force_sources=frozenset({"target_items"}),
        )

    assert stats.update_count == 1
    captured = capsys.readouterr()
    assert "Watermarks:" in captured.out
    assert "Refreshed 1 asset(s)" in captured.out


def test_execute_script_for_refresh_interactive_declined(tmp_path, capsys):
    """Cover interactive=True with user declining approval."""
    script = tmp_path / "test.preql"
    script.write_text(STALE_SCRIPT)
    executor = Dialects.DUCK_DB.default_executor()
    node = ScriptNode(path=script)

    with set_rich_mode(False), patch("click.confirm", return_value=False):
        stats = execute_script_for_refresh(
            executor,
            node,
            force_sources=frozenset({"target_items"}),
            interactive=True,
        )

    assert stats.update_count == 0
    captured = capsys.readouterr()
    assert "Refresh skipped by user" in captured.out


def test_execute_refresh_mode_with_watermarks(capsys):
    """Cover show_watermarks call in single_execution.execute_refresh_mode."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(STALE_SCRIPT)

    with set_rich_mode(False):
        result = execute_refresh_mode(
            executor,
            force_sources={"target_items"},
            print_watermarks=True,
        )

    assert result.refreshed_count == 1
    captured = capsys.readouterr()
    assert "Watermarks:" in captured.out


def test_refresh_stale_assets_excludes_peer_stale_from_planner():
    """When multiple datasources are stale, each refresh hides the not-yet-refreshed
    peers from the query planner so it cannot route through stale/missing sources."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(
        """
        key rec_id int;
        property rec_id.rec_ts datetime;

        root datasource rec_root (
            rec_id: rec_id,
            rec_ts: rec_ts
        )
        grain (rec_id)
        query '''SELECT 1 as rec_id, TIMESTAMP '2024-01-20' as rec_ts'''
        incremental by rec_ts;

        datasource rec_stage (
            rec_id: rec_id,
            rec_ts: rec_ts
        )
        grain (rec_id)
        address rec_stage_table
        incremental by rec_ts;

        datasource rec_final (
            rec_id: rec_id,
            rec_ts: rec_ts
        )
        grain (rec_id)
        address rec_final_table
        incremental by rec_ts;
        """
    )

    # Snapshot which datasources are visible in the environment at each update call.
    env_at_each_refresh: list[set[str]] = []
    original_update = executor.update_datasource

    def tracking_update(datasource, keys=None):
        env_at_each_refresh.append(set(executor.environment.datasources.keys()))
        return original_update(datasource, keys)

    executor.update_datasource = tracking_update  # type: ignore[method-assign]

    result = refresh_stale_assets(executor)

    assert result.refreshed_count == 2
    assert len(env_at_each_refresh) == 2

    first_visible = env_at_each_refresh[0]
    second_visible = env_at_each_refresh[1]

    assert "rec_root" in first_visible
    assert "rec_root" in second_visible

    # During the first refresh, exactly one of the two stale peers is visible
    # (the other is hidden to prevent the planner routing through it).
    stale_visible_first = first_visible & {"rec_stage", "rec_final"}
    assert (
        len(stale_visible_first) == 1
    ), f"Expected one stale datasource visible during first refresh, got: {stale_visible_first}"

    # During the second refresh the first peer has been refreshed and is restored,
    # so both stale datasources are present again.
    stale_visible_second = second_visible & {"rec_stage", "rec_final"}
    assert (
        len(stale_visible_second) == 2
    ), f"Expected both stale datasources visible during second refresh, got: {stale_visible_second}"
