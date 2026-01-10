import pytest

from trilogy.core.enums import ChartType
from trilogy.core.statements.author import ChartConfig
from trilogy.rendering.terminal_renderer import PLOTEXT_AVAILABLE


@pytest.mark.skipif(not PLOTEXT_AVAILABLE, reason="plotext not installed")
def test_terminal_bar_chart():
    from trilogy.rendering.terminal_renderer import TerminalRenderer

    config = ChartConfig(
        chart_type=ChartType.BAR,
        x_fields=["category"],
        y_fields=["value"],
    )
    data = [
        {"category": "a", "value": 10},
        {"category": "b", "value": 20},
        {"category": "c", "value": 30},
    ]

    renderer = TerminalRenderer()
    output = renderer.render(config, data)

    assert isinstance(output, str)
    assert len(output) > 0


@pytest.mark.skipif(not PLOTEXT_AVAILABLE, reason="plotext not installed")
def test_terminal_line_chart():
    from trilogy.rendering.terminal_renderer import TerminalRenderer

    config = ChartConfig(
        chart_type=ChartType.LINE,
        x_fields=["x"],
        y_fields=["y"],
    )
    data = [
        {"x": 1, "y": 10},
        {"x": 2, "y": 20},
        {"x": 3, "y": 30},
    ]

    renderer = TerminalRenderer()
    output = renderer.render(config, data)

    assert isinstance(output, str)
    assert len(output) > 0


@pytest.mark.skipif(not PLOTEXT_AVAILABLE, reason="plotext not installed")
def test_terminal_point_chart():
    from trilogy.rendering.terminal_renderer import TerminalRenderer

    config = ChartConfig(
        chart_type=ChartType.POINT,
        x_fields=["x"],
        y_fields=["y"],
    )
    data = [
        {"x": 1, "y": 10},
        {"x": 2, "y": 20},
        {"x": 3, "y": 15},
    ]

    renderer = TerminalRenderer()
    output = renderer.render(config, data)

    assert isinstance(output, str)
    assert len(output) > 0


@pytest.mark.skipif(not PLOTEXT_AVAILABLE, reason="plotext not installed")
def test_terminal_multiple_y_fields():
    from trilogy.rendering.terminal_renderer import TerminalRenderer

    config = ChartConfig(
        chart_type=ChartType.LINE,
        x_fields=["x"],
        y_fields=["y1", "y2"],
    )
    data = [
        {"x": 1, "y1": 10, "y2": 15},
        {"x": 2, "y1": 20, "y2": 25},
        {"x": 3, "y1": 30, "y2": 35},
    ]

    renderer = TerminalRenderer()
    output = renderer.render(config, data)

    assert isinstance(output, str)
    assert len(output) > 0


@pytest.mark.skipif(not PLOTEXT_AVAILABLE, reason="plotext not installed")
def test_terminal_barh_chart():
    from trilogy.rendering.terminal_renderer import TerminalRenderer

    config = ChartConfig(
        chart_type=ChartType.BARH,
        x_fields=["category"],
        y_fields=["value"],
    )
    data = [
        {"category": "a", "value": 10},
        {"category": "b", "value": 20},
    ]

    renderer = TerminalRenderer()
    output = renderer.render(config, data)

    assert isinstance(output, str)
    assert len(output) > 0


@pytest.mark.skipif(not PLOTEXT_AVAILABLE, reason="plotext not installed")
def test_terminal_unsupported_chart_type():
    from trilogy.rendering.terminal_renderer import TerminalRenderer

    config = ChartConfig(
        chart_type=ChartType.HEATMAP,  # Not supported in terminal
        x_fields=["x"],
        y_fields=["y"],
    )
    data = [{"x": 1, "y": 10}]

    renderer = TerminalRenderer()
    output = renderer.render(config, data)

    assert "not supported" in output.lower()


def test_plotext_not_available_raises():
    """Test that TerminalRenderer raises ImportError when plotext not available."""
    import trilogy.rendering.terminal_renderer as tr

    original = tr.PLOTEXT_AVAILABLE
    try:
        tr.PLOTEXT_AVAILABLE = False
        with pytest.raises(ImportError):
            tr.TerminalRenderer()
    finally:
        tr.PLOTEXT_AVAILABLE = original
