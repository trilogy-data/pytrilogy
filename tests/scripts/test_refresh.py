from unittest.mock import patch

import pytest

from trilogy import Dialects
from trilogy.core.models.datasource import UpdateKey, UpdateKeyType
from trilogy.execution.state.state_store import (
    DatasourceWatermark,
    StaleAsset,
    refresh_stale_assets,
)
from trilogy.scripts.display import (
    is_rich_available,
    set_rich_mode,
    show_refresh_plan,
    show_stale_assets,
    show_watermarks,
)
from trilogy.scripts.refresh import _prompt_approval

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
    executor.execute_text("""
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
        """)

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
    executor.execute_text("""
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
        """)

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
    executor.execute_text("""
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
        """)

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
