import json

import pytest

pytest.importorskip("altair")

from trilogy.core.enums import ChartPlaceKind, ChartType
from trilogy.core.statements.author import ChartPlacement
from trilogy.core.statements.execute import (
    ProcessedChartLayer,
    ProcessedChartStatement,
)
from trilogy.rendering.altair_renderer import AltairRenderer


def _statement(
    layer_type: ChartType,
    x_fields: list[str] | None = None,
    y_fields: list[str] | None = None,
    color_field: str | None = None,
    size_field: str | None = None,
    placements: list[ChartPlacement] | None = None,
    hide_legend: bool = False,
    layers: int = 1,
) -> ProcessedChartStatement:
    layer = ProcessedChartLayer(
        layer_type=layer_type,
        x_fields=x_fields or [],
        y_fields=y_fields or [],
        color_field=color_field,
        size_field=size_field,
    )
    return ProcessedChartStatement(
        layers=[layer] * layers,
        placements=placements or [],
        hide_legend=hide_legend,
    )


def _data() -> list[list[dict]]:
    return [[{"x": 1, "y": 10}, {"x": 2, "y": 20}, {"x": 3, "y": 30}]]


@pytest.mark.parametrize(
    "chart_type",
    [ChartType.BAR, ChartType.LINE, ChartType.POINT, ChartType.AREA],
)
def test_altair_chart_types(chart_type):
    statement = _statement(chart_type, ["x"], ["y"])
    chart = AltairRenderer().render(statement, _data())
    assert chart is not None


def test_altair_barh_uses_literal_axes():
    statement = _statement(
        ChartType.BARH,
        x_fields=["value"],
        y_fields=["category"],
        color_field="region",
        size_field="count",
    )
    data = [[{"value": 10, "category": "a", "region": "n", "count": 3}]]
    chart = AltairRenderer().render(statement, data)
    spec = chart.to_dict()
    # barh maps axes literally: x_axis -> x, y_axis -> y (no swap)
    assert spec["encoding"]["x"]["field"] == "value"
    assert spec["encoding"]["y"]["field"] == "category"
    assert spec["encoding"]["color"]["field"] == "region"
    assert spec["encoding"]["size"]["field"] == "count"


def test_altair_headline_renders_number_and_title():
    statement = _statement(ChartType.HEADLINE, x_fields=["total_revenue"])
    data = [[{"total_revenue": 437000.0}]]
    spec = AltairRenderer().render(statement, data).to_dict()
    assert "layer" in spec
    assert all(layer["mark"]["type"] == "text" for layer in spec["layer"])
    assert "TOTAL REVENUE" in json.dumps(spec)


def test_altair_headline_requires_x_axis():
    statement = _statement(ChartType.HEADLINE)
    with pytest.raises(ValueError, match="headline"):
        AltairRenderer().render(statement, [[]])


def test_altair_size_field_encoded():
    statement = _statement(ChartType.POINT, ["x"], ["y"], size_field="weight")
    data = [[{"x": 1, "y": 10, "weight": 5}]]
    chart = AltairRenderer().render(statement, data)
    spec = chart.to_dict()
    assert spec["encoding"]["size"]["field"] == "weight"


def test_altair_placements_hline_vline():
    statement = _statement(
        ChartType.LINE,
        ["x"],
        ["y"],
        placements=[
            ChartPlacement(kind=ChartPlaceKind.HLINE, value=5),
            ChartPlacement(kind=ChartPlaceKind.VLINE, value=2),
        ],
    )
    chart = AltairRenderer().render(statement, _data())
    assert chart is not None
    spec = chart.to_dict()
    # layered chart with base + 2 placements
    assert len(spec["layer"]) == 3


def test_altair_hide_legend():
    statement = _statement(
        ChartType.BAR, ["x"], ["y"], color_field="cat", hide_legend=True
    )
    data = [[{"x": 1, "y": 10, "cat": "a"}]]
    chart = AltairRenderer().render(statement, data)
    spec = chart.to_dict()
    assert spec.get("config", {}).get("legend", {}).get("disable") is True


def test_altair_to_spec_returns_dict():
    statement = _statement(ChartType.BAR, ["x"], ["y"])
    spec = AltairRenderer().to_spec(statement, _data())
    assert isinstance(spec, dict)
    assert "mark" in spec or "layer" in spec


def test_altair_layer_count_mismatch_raises():
    statement = _statement(ChartType.BAR, ["x"], ["y"], layers=2)
    with pytest.raises(ValueError, match="Layer data count"):
        AltairRenderer().render(statement, _data())


def test_altair_empty_layers_returns_none():
    statement = ProcessedChartStatement(layers=[])
    assert AltairRenderer().render(statement, []) is None


def test_altair_unimplemented_chart_type_raises():
    statement = _statement(ChartType.HEATMAP, ["x"], ["y"])
    with pytest.raises(NotImplementedError):
        AltairRenderer().render(statement, _data())


def test_altair_unavailable_raises():
    import trilogy.rendering.altair_renderer as ar

    original = ar.ALTAIR_AVAILABLE
    try:
        ar.ALTAIR_AVAILABLE = False
        with pytest.raises(ImportError):
            ar.AltairRenderer()
    finally:
        ar.ALTAIR_AVAILABLE = original
