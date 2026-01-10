import pytest

from trilogy.core.enums import ChartType
from trilogy.core.statements.author import ChartConfig
from trilogy.rendering.altair_renderer import ALTAIR_AVAILABLE, AltairRenderer


@pytest.mark.skipif(not ALTAIR_AVAILABLE, reason="Altair not installed")
def test_altair_bar_chart():
    config = ChartConfig(
        chart_type=ChartType.BAR,
        x_fields=["category"],
        y_fields=["value"],
    )
    data = [
        {"category": "a", "value": 1},
        {"category": "b", "value": 2},
        {"category": "c", "value": 3},
    ]

    renderer = AltairRenderer()
    chart = renderer.render(config, data)

    assert chart is not None
    spec = renderer.to_spec(config, data)
    assert "mark" in spec
    assert spec["mark"]["type"] == "bar"


@pytest.mark.skipif(not ALTAIR_AVAILABLE, reason="Altair not installed")
def test_altair_line_chart():
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

    renderer = AltairRenderer()
    renderer.render(config, data)
    spec = renderer.to_spec(config, data)

    assert spec["mark"]["type"] == "line"


@pytest.mark.skipif(not ALTAIR_AVAILABLE, reason="Altair not installed")
def test_altair_chart_with_color():
    config = ChartConfig(
        chart_type=ChartType.BAR,
        x_fields=["category"],
        y_fields=["value"],
        color_field="region",
    )
    data = [
        {"category": "a", "value": 1, "region": "north"},
        {"category": "b", "value": 2, "region": "south"},
    ]

    renderer = AltairRenderer()
    spec = renderer.to_spec(config, data)

    assert "color" in spec["encoding"]
