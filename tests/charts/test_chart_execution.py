import pytest

from trilogy.core.enums import ChartType
from trilogy.core.statements.author import ChartConfig
from trilogy.rendering.altair_renderer import AltairRenderer


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


def test_altair_barh_chart():
    config = ChartConfig(
        chart_type=ChartType.BARH,
        x_fields=["category"],
        y_fields=["value"],
    )
    data = [
        {"category": "a", "value": 10},
        {"category": "b", "value": 20},
    ]

    renderer = AltairRenderer()
    spec = renderer.to_spec(config, data)

    assert spec["mark"]["type"] == "bar"
    # barh swaps x and y encoding
    assert "x" in spec["encoding"]
    assert "y" in spec["encoding"]


def test_altair_barh_chart_with_color():
    config = ChartConfig(
        chart_type=ChartType.BARH,
        x_fields=["category"],
        y_fields=["value"],
        color_field="region",
    )
    data = [
        {"category": "a", "value": 10, "region": "north"},
        {"category": "b", "value": 20, "region": "south"},
    ]

    renderer = AltairRenderer()
    spec = renderer.to_spec(config, data)

    assert "color" in spec["encoding"]


def test_altair_point_chart():
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

    renderer = AltairRenderer()
    spec = renderer.to_spec(config, data)

    assert spec["mark"]["type"] == "point"


def test_altair_point_chart_with_size():
    config = ChartConfig(
        chart_type=ChartType.POINT,
        x_fields=["x"],
        y_fields=["y"],
        size_field="size",
    )
    data = [
        {"x": 1, "y": 10, "size": 100},
        {"x": 2, "y": 20, "size": 200},
    ]

    renderer = AltairRenderer()
    spec = renderer.to_spec(config, data)

    assert "size" in spec["encoding"]


def test_altair_point_chart_with_color():
    config = ChartConfig(
        chart_type=ChartType.POINT,
        x_fields=["x"],
        y_fields=["y"],
        color_field="category",
    )
    data = [
        {"x": 1, "y": 10, "category": "a"},
        {"x": 2, "y": 20, "category": "b"},
    ]

    renderer = AltairRenderer()
    spec = renderer.to_spec(config, data)

    assert "color" in spec["encoding"]


def test_altair_area_chart():
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

    renderer = AltairRenderer()
    spec = renderer.to_spec(config, data)

    assert spec["mark"]["type"] == "area"


def test_altair_area_chart_with_color():
    config = ChartConfig(
        chart_type=ChartType.AREA,
        x_fields=["x"],
        y_fields=["y"],
        color_field="series",
    )
    data = [
        {"x": 1, "y": 10, "series": "a"},
        {"x": 2, "y": 20, "series": "b"},
    ]

    renderer = AltairRenderer()
    spec = renderer.to_spec(config, data)

    assert "color" in spec["encoding"]


def test_altair_line_chart_with_color():
    config = ChartConfig(
        chart_type=ChartType.LINE,
        x_fields=["x"],
        y_fields=["y"],
        color_field="series",
    )
    data = [
        {"x": 1, "y": 10, "series": "a"},
        {"x": 2, "y": 20, "series": "a"},
        {"x": 1, "y": 15, "series": "b"},
        {"x": 2, "y": 25, "series": "b"},
    ]

    renderer = AltairRenderer()
    spec = renderer.to_spec(config, data)

    assert "color" in spec["encoding"]


def test_altair_hide_legend():
    config = ChartConfig(
        chart_type=ChartType.BAR,
        x_fields=["category"],
        y_fields=["value"],
        color_field="region",
        hide_legend=True,
    )
    data = [
        {"category": "a", "value": 1, "region": "north"},
        {"category": "b", "value": 2, "region": "south"},
    ]

    renderer = AltairRenderer()
    spec = renderer.to_spec(config, data)

    # hide_legend applies configure_legend(disable=True)
    assert "config" in spec
    assert spec["config"]["legend"]["disable"] is True


def test_altair_unsupported_chart_type():
    config = ChartConfig(
        chart_type=ChartType.HEATMAP,
        x_fields=["x"],
        y_fields=["y"],
    )
    data = [{"x": 1, "y": 10}]

    renderer = AltairRenderer()
    with pytest.raises(NotImplementedError, match="not yet implemented"):
        renderer.render(config, data)


def test_altair_bar_no_x_field():
    config = ChartConfig(
        chart_type=ChartType.BAR,
        x_fields=[],
        y_fields=["value"],
    )
    data = [{"value": 10}]

    renderer = AltairRenderer()
    spec = renderer.to_spec(config, data)

    # Should still render, just without x encoding
    assert "x" not in spec["encoding"]


def test_altair_bar_no_y_field():
    config = ChartConfig(
        chart_type=ChartType.BAR,
        x_fields=["category"],
        y_fields=[],
    )
    data = [{"category": "a"}]

    renderer = AltairRenderer()
    spec = renderer.to_spec(config, data)

    # Should still render, just without y encoding
    assert "y" not in spec["encoding"]


def test_altair_not_available_raises():
    """Test that AltairRenderer raises ImportError when altair not available."""
    import trilogy.rendering.altair_renderer as ar

    original = ar.ALTAIR_AVAILABLE
    try:
        ar.ALTAIR_AVAILABLE = False
        with pytest.raises(ImportError):
            ar.AltairRenderer()
    finally:
        ar.ALTAIR_AVAILABLE = original
