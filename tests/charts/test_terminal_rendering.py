import pytest

from trilogy.core.enums import ChartPlaceKind, ChartType
from trilogy.core.models.author import ConceptRef
from trilogy.core.models.core import DataType
from trilogy.core.statements.author import ChartPlacement
from trilogy.core.statements.execute import (
    ProcessedChartLayer,
    ProcessedChartStatement,
    ProcessedQuery,
)
from trilogy.rendering.terminal_renderer import TerminalRenderer


def _query_for(fields: dict[str, DataType]) -> ProcessedQuery:
    output_columns = [
        ConceptRef(address=f"local.{name}", datatype=dt) for name, dt in fields.items()
    ]
    return ProcessedQuery(output_columns=output_columns, ctes=[], base=None)  # type: ignore[arg-type]


def _statement(
    layer_type: ChartType,
    x_fields: list[str] | None = None,
    y_fields: list[str] | None = None,
    placements: list[ChartPlacement] | None = None,
    column_types: dict[str, DataType] | None = None,
) -> ProcessedChartStatement:
    layer = ProcessedChartLayer(
        layer_type=layer_type,
        x_fields=x_fields or [],
        y_fields=y_fields or [],
        query=_query_for(column_types) if column_types else None,
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


@pytest.mark.parametrize(
    "chart_type",
    [ChartType.LINE, ChartType.POINT, ChartType.AREA],
)
def test_terminal_categorical_x_axis(chart_type):
    statement = _statement(
        chart_type,
        ["name"],
        ["count"],
        column_types={"name": DataType.STRING, "count": DataType.INTEGER},
    )
    data = [
        [
            {"name": "United", "count": 10},
            {"name": "Delta", "count": 20},
            {"name": "American", "count": 15},
        ]
    ]

    output = TerminalRenderer().render(statement, data)

    assert isinstance(output, str)
    assert len(output) > 0


def test_terminal_multi_layer_subplots():
    layer_a = ProcessedChartLayer(
        layer_type=ChartType.BARH,
        x_fields=["value"],
        y_fields=["category"],
        query=_query_for({"value": DataType.INTEGER, "category": DataType.STRING}),
    )
    layer_b = ProcessedChartLayer(
        layer_type=ChartType.LINE,
        x_fields=["value"],
        y_fields=["category"],
        query=_query_for({"value": DataType.INTEGER, "category": DataType.STRING}),
    )
    statement = ProcessedChartStatement(layers=[layer_a, layer_b])
    data = [
        [{"category": "A", "value": 10}, {"category": "B", "value": 20}],
        [{"category": "A", "value": 15}, {"category": "B", "value": 25}],
    ]

    output = TerminalRenderer().render(statement, data)

    assert isinstance(output, str)
    assert len(output) > 0


@pytest.mark.parametrize(
    "chart_type",
    [ChartType.LINE, ChartType.POINT, ChartType.AREA],
)
def test_terminal_numeric_x_axis_with_metadata(chart_type):
    statement = _statement(
        chart_type,
        ["x"],
        ["y"],
        column_types={"x": DataType.FLOAT, "y": DataType.INTEGER},
    )
    data = [[{"x": 0.5, "y": 10}, {"x": 1.5, "y": 20}, {"x": 2.5, "y": 30}]]

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
