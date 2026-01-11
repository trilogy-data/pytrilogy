import pytest

from trilogy.core.enums import ChartType
from trilogy.core.statements.author import ChartConfig
from trilogy.rendering.terminal_renderer import TerminalRenderer


def test_terminal_bar_chart():
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


def test_terminal_line_chart():
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


def test_terminal_point_chart():
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


def test_terminal_multiple_y_fields():
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


def test_terminal_barh_chart():
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


def test_terminal_area_chart():
    config = ChartConfig(
        chart_type=ChartType.AREA,
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


def test_terminal_unsupported_chart_type():
    config = ChartConfig(
        chart_type=ChartType.HEATMAP,
        x_fields=["x"],
        y_fields=["y"],
    )
    data = [{"x": 1, "y": 10}]

    renderer = TerminalRenderer()
    output = renderer.render(config, data)

    assert "not supported" in output.lower()


def test_terminal_to_spec():
    config = ChartConfig(
        chart_type=ChartType.BAR,
        x_fields=["category"],
        y_fields=["value"],
    )
    data = [{"category": "a", "value": 10}]

    renderer = TerminalRenderer()
    spec = renderer.to_spec(config, data)

    assert spec["type"] == "terminal"
    assert "output" in spec
    assert isinstance(spec["output"], str)


def test_terminal_bar_missing_fields():
    config = ChartConfig(
        chart_type=ChartType.BAR,
        x_fields=[],
        y_fields=[],
    )
    data = [{"category": "a", "value": 10}]

    renderer = TerminalRenderer()
    output = renderer.render(config, data)

    # Should still return something (empty chart)
    assert isinstance(output, str)


def test_terminal_barh_missing_fields():
    config = ChartConfig(
        chart_type=ChartType.BARH,
        x_fields=[],
        y_fields=[],
    )
    data = [{"category": "a", "value": 10}]

    renderer = TerminalRenderer()
    output = renderer.render(config, data)

    assert isinstance(output, str)


def test_terminal_line_missing_fields():
    config = ChartConfig(
        chart_type=ChartType.LINE,
        x_fields=[],
        y_fields=[],
    )
    data = [{"x": 1, "y": 10}]

    renderer = TerminalRenderer()
    output = renderer.render(config, data)

    assert isinstance(output, str)


def test_terminal_point_missing_fields():
    config = ChartConfig(
        chart_type=ChartType.POINT,
        x_fields=[],
        y_fields=[],
    )
    data = [{"x": 1, "y": 10}]

    renderer = TerminalRenderer()
    output = renderer.render(config, data)

    assert isinstance(output, str)


def test_terminal_area_missing_fields():
    config = ChartConfig(
        chart_type=ChartType.AREA,
        x_fields=[],
        y_fields=[],
    )
    data = [{"x": 1, "y": 10}]

    renderer = TerminalRenderer()
    output = renderer.render(config, data)

    assert isinstance(output, str)


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


def test_print_chart_terminal_renders_chart(capsys):
    """Test print_chart_terminal renders a chart to terminal."""
    from trilogy.scripts.display import print_chart_terminal

    config = ChartConfig(
        chart_type=ChartType.BAR,
        x_fields=["category"],
        y_fields=["value"],
    )
    data = [
        {"category": "a", "value": 10},
        {"category": "b", "value": 20},
    ]

    result = print_chart_terminal(data, config)
    assert result is True

    captured = capsys.readouterr()
    assert len(captured.out) > 0


def test_print_chart_terminal_empty_data(capsys):
    """Test print_chart_terminal handles empty data."""
    from trilogy.scripts.display import print_chart_terminal

    config = ChartConfig(
        chart_type=ChartType.BAR,
        x_fields=["x"],
        y_fields=["y"],
    )

    result = print_chart_terminal([], config)
    assert result is True


def test_print_chart_terminal_plotext_unavailable(capsys):
    """Test print_chart_terminal when plotext is not available."""
    import trilogy.rendering.terminal_renderer as tr
    from trilogy.scripts.display import print_chart_terminal

    original = tr.PLOTEXT_AVAILABLE
    try:
        tr.PLOTEXT_AVAILABLE = False
        config = ChartConfig(
            chart_type=ChartType.BAR,
            x_fields=["x"],
            y_fields=["y"],
        )
        result = print_chart_terminal([{"x": 1, "y": 2}], config)
        assert result is False
    finally:
        tr.PLOTEXT_AVAILABLE = original
