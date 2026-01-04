from trilogy import Dialects
from trilogy.core.models.datasource import UpdateKey, UpdateKeyType
from trilogy.execution.state.state_store import (
    DatasourceWatermark,
    refresh_stale_assets,
)
from trilogy.scripts.refresh import _format_watermarks


def test_format_watermarks_empty(capsys):
    """Test formatting when watermarks dict is empty."""
    watermarks: dict[str, DatasourceWatermark] = {}
    _format_watermarks(watermarks)
    captured = capsys.readouterr()
    assert "Watermarks:" in captured.out


def test_format_watermarks_no_keys(capsys):
    """Test formatting when datasource has no watermark keys."""
    watermarks = {"ds1": DatasourceWatermark(keys={})}
    _format_watermarks(watermarks)
    captured = capsys.readouterr()
    assert "Watermarks:" in captured.out
    assert "ds1: (no watermarks)" in captured.out


def test_format_watermarks_with_keys(capsys):
    """Test formatting datasources with watermark keys."""
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
    _format_watermarks(watermarks)
    captured = capsys.readouterr()
    assert "Watermarks:" in captured.out
    assert "orders.order_date: 2024-01-15 (incremental_key)" in captured.out


def test_format_watermarks_multiple_datasources(capsys):
    """Test formatting multiple datasources with various watermark configurations."""
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
    _format_watermarks(watermarks)
    captured = capsys.readouterr()
    assert "Watermarks:" in captured.out
    assert "users: (no watermarks)" in captured.out
    assert "events.event_id: 12345 (incremental_key)" in captured.out
    assert "events.event_time: 2024-06-01T12:00:00 (update_time)" in captured.out


def test_format_watermarks_sorted_output(capsys):
    """Test that datasources are printed in sorted order."""
    watermarks = {
        "zebra": DatasourceWatermark(keys={}),
        "alpha": DatasourceWatermark(keys={}),
        "beta": DatasourceWatermark(keys={}),
    }
    _format_watermarks(watermarks)
    captured = capsys.readouterr()
    # Check order in output
    alpha_pos = captured.out.find("alpha")
    beta_pos = captured.out.find("beta")
    zebra_pos = captured.out.find("zebra")
    assert alpha_pos < beta_pos < zebra_pos


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
