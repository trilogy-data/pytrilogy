import pytest

from trilogy.core.enums import ChartPlaceKind, ChartType
from trilogy.core.statements.author import ChartPlacement
from trilogy.core.statements.execute import (
    ProcessedChartLayer,
    ProcessedChartStatement,
)
from trilogy.rendering.terminal_renderer import TerminalRenderer


def _statement(
    layer_type: ChartType,
    x_fields: list[str] | None = None,
    y_fields: list[str] | None = None,
    placements: list[ChartPlacement] | None = None,
) -> ProcessedChartStatement:
    layer = ProcessedChartLayer(
        layer_type=layer_type,
        x_fields=x_fields or [],
        y_fields=y_fields or [],
    )
    return ProcessedChartStatement(layers=[layer], placements=placements or [])


def test_terminal_bar_chart():
    statement = _statement(ChartType.BAR, ["category"], ["value"])
    data = [
        [
            {"category": "a", "value": 10},
            {"category": "b", "value": 20},
            {"category": "c", "value": 30},
        ]
    ]

    output = TerminalRenderer().render(statement, data)

    assert isinstance(output, str)
    assert len(output) > 0


def test_terminal_line_chart():
    statement = _statement(ChartType.LINE, ["x"], ["y"])
    data = [[{"x": 1, "y": 10}, {"x": 2, "y": 20}, {"x": 3, "y": 30}]]

    output = TerminalRenderer().render(statement, data)

    assert isinstance(output, str)
    assert len(output) > 0


def test_terminal_point_chart():
    statement = _statement(ChartType.POINT, ["x"], ["y"])
    data = [[{"x": 1, "y": 10}, {"x": 2, "y": 20}, {"x": 3, "y": 15}]]

    output = TerminalRenderer().render(statement, data)

    assert isinstance(output, str)
    assert len(output) > 0


def test_terminal_multiple_y_fields():
    statement = _statement(ChartType.LINE, ["x"], ["y1", "y2"])
    data = [
        [
            {"x": 1, "y1": 10, "y2": 15},
            {"x": 2, "y1": 20, "y2": 25},
            {"x": 3, "y1": 30, "y2": 35},
        ]
    ]

    output = TerminalRenderer().render(statement, data)

    assert isinstance(output, str)
    assert len(output) > 0


def test_terminal_barh_chart():
    statement = _statement(ChartType.BARH, ["value"], ["category"])
    data = [[{"category": "a", "value": 10}, {"category": "b", "value": 20}]]

    output = TerminalRenderer().render(statement, data)

    assert isinstance(output, str)
    assert len(output) > 0


def test_terminal_area_chart():
    statement = _statement(ChartType.AREA, ["x"], ["y"])
    data = [[{"x": 1, "y": 10}, {"x": 2, "y": 20}, {"x": 3, "y": 15}]]

    output = TerminalRenderer().render(statement, data)

    assert isinstance(output, str)
    assert len(output) > 0


def test_terminal_unsupported_chart_type():
    statement = _statement(ChartType.HEATMAP, ["x"], ["y"])
    data = [[{"x": 1, "y": 10}]]

    output = TerminalRenderer().render(statement, data)

    assert "not supported" in output.lower()


def test_terminal_to_spec():
    statement = _statement(ChartType.BAR, ["category"], ["value"])
    data = [[{"category": "a", "value": 10}]]

    spec = TerminalRenderer().to_spec(statement, data)

    assert spec["type"] == "terminal"
    assert "output" in spec
    assert isinstance(spec["output"], str)


@pytest.mark.parametrize(
    "chart_type",
    [ChartType.BAR, ChartType.BARH, ChartType.LINE, ChartType.POINT, ChartType.AREA],
)
def test_terminal_missing_fields(chart_type):
    statement = _statement(chart_type, [], [])
    data = [[{"x": 1, "y": 10}]]

    output = TerminalRenderer().render(statement, data)

    assert isinstance(output, str)


def test_terminal_placements():
    statement = _statement(
        ChartType.LINE,
        ["x"],
        ["y"],
        placements=[
            ChartPlacement(kind=ChartPlaceKind.HLINE, value=5),
            ChartPlacement(kind=ChartPlaceKind.VLINE, value=2),
        ],
    )
    data = [[{"x": 1, "y": 10}, {"x": 2, "y": 20}]]

    output = TerminalRenderer().render(statement, data)

    assert isinstance(output, str)
    assert len(output) > 0


def test_plotext_not_available_raises():
    import trilogy.rendering.terminal_renderer as tr

    original = tr.PLOTEXT_AVAILABLE
    try:
        tr.PLOTEXT_AVAILABLE = False
        with pytest.raises(ImportError):
            tr.TerminalRenderer()
    finally:
        tr.PLOTEXT_AVAILABLE = original


def test_print_chart_terminal_renders_chart(capsys):
    from trilogy.scripts.display import print_chart_terminal

    statement = _statement(ChartType.BAR, ["category"], ["value"])
    data = [[{"category": "a", "value": 10}, {"category": "b", "value": 20}]]

    result = print_chart_terminal(data, statement)
    assert result is True

    captured = capsys.readouterr()
    assert len(captured.out) > 0


def test_print_chart_terminal_empty_data():
    from trilogy.scripts.display import print_chart_terminal

    statement = _statement(ChartType.BAR, ["x"], ["y"])

    result = print_chart_terminal([[]], statement)
    assert result is True


def test_print_chart_terminal_plotext_unavailable():
    import trilogy.rendering.terminal_renderer as tr
    from trilogy.scripts.display import print_chart_terminal

    original = tr.PLOTEXT_AVAILABLE
    try:
        tr.PLOTEXT_AVAILABLE = False
        statement = _statement(ChartType.BAR, ["x"], ["y"])
        result = print_chart_terminal([[{"x": 1, "y": 2}]], statement)
        assert result is False
    finally:
        tr.PLOTEXT_AVAILABLE = original
