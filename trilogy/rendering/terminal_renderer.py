from typing import Any

from trilogy.core.enums import ChartType
from trilogy.core.statements.author import ChartConfig
from trilogy.rendering.base import BaseRenderer

try:
    import plotext as plt

    PLOTEXT_AVAILABLE = True
except ImportError:
    PLOTEXT_AVAILABLE = False
    plt = None  # type: ignore[assignment]


class TerminalRenderer(BaseRenderer):
    def __init__(self):
        if not PLOTEXT_AVAILABLE:
            raise ImportError(
                "plotext is required for terminal chart rendering. "
                "Install with: pip install plotext"
            )

    def render(self, config: ChartConfig, data: list[dict]) -> str:
        plt.clear_figure()
        plt.theme("clear")

        chart_map = {
            ChartType.BAR: self._bar_chart,
            ChartType.LINE: self._line_chart,
            ChartType.POINT: self._point_chart,
            ChartType.AREA: self._area_chart,
            ChartType.BARH: self._barh_chart,
        }

        chart_fn = chart_map.get(config.chart_type)
        if chart_fn is None:
            return f"Chart type '{config.chart_type.value}' not supported in terminal"

        chart_fn(data, config)
        return plt.build()

    def to_spec(self, config: ChartConfig, data: list[dict]) -> dict:
        return {"type": "terminal", "output": self.render(config, data)}

    def _bar_chart(self, data: list[dict], config: ChartConfig) -> Any:
        x_field = config.x_fields[0] if config.x_fields else None
        y_field = config.y_fields[0] if config.y_fields else None

        if not x_field or not y_field:
            return

        x_vals = [row.get(x_field) for row in data]
        y_vals = [row.get(y_field) for row in data]

        plt.bar(x_vals, y_vals)
        plt.xlabel(x_field)
        plt.ylabel(y_field)

    def _barh_chart(self, data: list[dict], config: ChartConfig) -> Any:
        x_field = config.x_fields[0] if config.x_fields else None
        y_field = config.y_fields[0] if config.y_fields else None

        if not x_field or not y_field:
            return

        x_vals = [row.get(x_field) for row in data]
        y_vals = [row.get(y_field) for row in data]

        plt.bar(x_vals, y_vals, orientation="horizontal")
        plt.xlabel(y_field)
        plt.ylabel(x_field)

    def _line_chart(self, data: list[dict], config: ChartConfig) -> Any:
        x_field = config.x_fields[0] if config.x_fields else None
        y_fields = config.y_fields if config.y_fields else []

        if not x_field or not y_fields:
            return

        x_vals = [row.get(x_field) for row in data]

        for y_field in y_fields:
            y_vals = [row.get(y_field) for row in data]
            plt.plot(x_vals, y_vals, label=y_field)

        plt.xlabel(x_field)
        if len(y_fields) == 1:
            plt.ylabel(y_fields[0])

    def _point_chart(self, data: list[dict], config: ChartConfig) -> Any:
        x_field = config.x_fields[0] if config.x_fields else None
        y_field = config.y_fields[0] if config.y_fields else None

        if not x_field or not y_field:
            return

        x_vals = [row.get(x_field) for row in data]
        y_vals = [row.get(y_field) for row in data]

        plt.scatter(x_vals, y_vals)
        plt.xlabel(x_field)
        plt.ylabel(y_field)

    def _area_chart(self, data: list[dict], config: ChartConfig) -> Any:
        x_field = config.x_fields[0] if config.x_fields else None
        y_field = config.y_fields[0] if config.y_fields else None

        if not x_field or not y_field:
            return

        x_vals = [row.get(x_field) for row in data]
        y_vals = [row.get(y_field) for row in data]

        plt.plot(x_vals, y_vals, fillx=True)
        plt.xlabel(x_field)
        plt.ylabel(y_field)
