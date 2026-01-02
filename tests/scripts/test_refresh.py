from trilogy.core.models.datasource import UpdateKey, UpdateKeyType
from trilogy.execution.state.state_store import DatasourceWatermark
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
