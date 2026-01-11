from typing import Any

from trilogy.core.enums import ChartType
from trilogy.core.statements.author import ChartConfig
from trilogy.rendering.base import BaseRenderer

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
                "Install with: pip install altair pandas"
            )

    def render(self, config: ChartConfig, data: list[dict]) -> Any:
        # Use pandas DataFrame for better type inference
        df_data = pd.DataFrame(data)

        chart_map = {
            ChartType.BAR: self._bar_chart,
            ChartType.LINE: self._line_chart,
            ChartType.POINT: self._point_chart,
            ChartType.AREA: self._area_chart,
            ChartType.BARH: self._barh_chart,
        }

        chart_fn = chart_map.get(config.chart_type)
        if chart_fn is None:
            raise NotImplementedError(
                f"Chart type '{config.chart_type.value}' not yet implemented"
            )

        return chart_fn(df_data, config)

    def to_spec(self, config: ChartConfig, data: list[dict]) -> dict:
        chart = self.render(config, data)
        return chart.to_dict()

    def _apply_common_options(self, chart: Any, config: ChartConfig) -> Any:
        if config.hide_legend:
            chart = chart.configure_legend(disable=True)
        return chart

    def _bar_chart(self, data: Any, config: ChartConfig) -> Any:
        x = config.x_fields[0] if config.x_fields else None
        y = config.y_fields[0] if config.y_fields else None

        encoding = {}
        if x:
            encoding["x"] = x
        if y:
            encoding["y"] = y
        if config.color_field:
            encoding["color"] = config.color_field

        chart = alt.Chart(data).mark_bar().encode(**encoding)
        return self._apply_common_options(chart, config)

    def _barh_chart(self, data: Any, config: ChartConfig) -> Any:
        x = config.x_fields[0] if config.x_fields else None
        y = config.y_fields[0] if config.y_fields else None

        encoding = {}
        if x:
            encoding["y"] = x
        if y:
            encoding["x"] = y
        if config.color_field:
            encoding["color"] = config.color_field

        chart = alt.Chart(data).mark_bar().encode(**encoding)
        return self._apply_common_options(chart, config)

    def _line_chart(self, data: Any, config: ChartConfig) -> Any:
        x = config.x_fields[0] if config.x_fields else None
        y = config.y_fields[0] if config.y_fields else None

        encoding = {}
        if x:
            encoding["x"] = x
        if y:
            encoding["y"] = y
        if config.color_field:
            encoding["color"] = config.color_field

        chart = alt.Chart(data).mark_line().encode(**encoding)
        return self._apply_common_options(chart, config)

    def _point_chart(self, data: Any, config: ChartConfig) -> Any:
        x = config.x_fields[0] if config.x_fields else None
        y = config.y_fields[0] if config.y_fields else None

        encoding = {}
        if x:
            encoding["x"] = x
        if y:
            encoding["y"] = y
        if config.color_field:
            encoding["color"] = config.color_field
        if config.size_field:
            encoding["size"] = config.size_field

        chart = alt.Chart(data).mark_point().encode(**encoding)
        return self._apply_common_options(chart, config)

    def _area_chart(self, data: Any, config: ChartConfig) -> Any:
        x = config.x_fields[0] if config.x_fields else None
        y = config.y_fields[0] if config.y_fields else None

        encoding = {}
        if x:
            encoding["x"] = x
        if y:
            encoding["y"] = y
        if config.color_field:
            encoding["color"] = config.color_field

        chart = alt.Chart(data).mark_area().encode(**encoding)
        return self._apply_common_options(chart, config)
