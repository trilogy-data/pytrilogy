from typing import Any

from trilogy.core.enums import ChartPlaceKind, ChartType
from trilogy.core.statements.execute import (
    ProcessedChartLayer,
    ProcessedChartStatement,
)
from trilogy.rendering.base import BaseRenderer, prettify_label

try:
    import altair as alt
    import pandas as pd

    ALTAIR_AVAILABLE = True
except ImportError:
    ALTAIR_AVAILABLE = False
    alt = None  # type: ignore[assignment]
    pd = None  # type: ignore[assignment]


class AltairRenderer(BaseRenderer):
    def __init__(self):
        if not ALTAIR_AVAILABLE:
            raise ImportError(
                "Altair is required for chart rendering. "
                "Install `altair` via pip/uv to use."
            )

    def render(
        self,
        statement: ProcessedChartStatement,
        layer_data: list[list[dict]],
    ) -> Any:
        if len(layer_data) != len(statement.layers):
            raise ValueError("Layer data count does not match layer count")
        layer_charts: list[Any] = []
        for layer, data in zip(statement.layers, layer_data):
            layer_charts.append(self._render_layer(layer, data))
        for placement in statement.placements:
            layer_charts.append(self._render_placement(placement))
        if not layer_charts:
            return None
        chart = layer_charts[0] if len(layer_charts) == 1 else alt.layer(*layer_charts)
        if statement.hide_legend:
            chart = chart.configure_legend(disable=True)
        return chart

    def to_spec(
        self,
        statement: ProcessedChartStatement,
        layer_data: list[list[dict]],
    ) -> dict:
        return self.render(statement, layer_data).to_dict()

    def _render_layer(self, layer: ProcessedChartLayer, data: list[dict]) -> Any:
        df = pd.DataFrame(data)
        builders = {
            ChartType.BAR: self._bar,
            ChartType.BARH: self._barh,
            ChartType.LINE: self._line,
            ChartType.POINT: self._point,
            ChartType.AREA: self._area,
        }
        build = builders.get(layer.layer_type)
        if build is None:
            raise NotImplementedError(
                f"Chart type '{layer.layer_type.value}' not yet implemented"
            )
        return build(df, layer)

    def _render_placement(self, placement) -> Any:
        datum_key = "y" if placement.kind == ChartPlaceKind.HLINE else "x"
        encoding = {datum_key: alt.datum(placement.value)}
        return alt.Chart().mark_rule().encode(**encoding)

    def _encode(self, layer: ProcessedChartLayer) -> dict[str, Any]:
        encoding: dict[str, Any] = {}
        if layer.x_fields:
            field = layer.x_fields[0]
            encoding["x"] = alt.X(field, title=prettify_label(field))
        if layer.y_fields:
            field = layer.y_fields[0]
            encoding["y"] = alt.Y(field, title=prettify_label(field))
        if layer.color_field:
            encoding["color"] = alt.Color(
                layer.color_field, title=prettify_label(layer.color_field)
            )
        if layer.size_field:
            encoding["size"] = alt.Size(
                layer.size_field, title=prettify_label(layer.size_field)
            )
        return encoding

    def _bar(self, data: Any, layer: ProcessedChartLayer) -> Any:
        return alt.Chart(data).mark_bar().encode(**self._encode(layer))

    def _barh(self, data: Any, layer: ProcessedChartLayer) -> Any:
        encoding: dict[str, Any] = {}
        if layer.y_fields:
            field = layer.y_fields[0]
            encoding["x"] = alt.X(field, title=prettify_label(field))
        if layer.x_fields:
            field = layer.x_fields[0]
            encoding["y"] = alt.Y(field, title=prettify_label(field))
        if layer.color_field:
            encoding["color"] = alt.Color(
                layer.color_field, title=prettify_label(layer.color_field)
            )
        if layer.size_field:
            encoding["size"] = alt.Size(
                layer.size_field, title=prettify_label(layer.size_field)
            )
        return alt.Chart(data).mark_bar().encode(**encoding)

    def _line(self, data: Any, layer: ProcessedChartLayer) -> Any:
        return alt.Chart(data).mark_line().encode(**self._encode(layer))

    def _point(self, data: Any, layer: ProcessedChartLayer) -> Any:
        return alt.Chart(data).mark_point().encode(**self._encode(layer))

    def _area(self, data: Any, layer: ProcessedChartLayer) -> Any:
        return alt.Chart(data).mark_area().encode(**self._encode(layer))
