from typing import Any

from trilogy.core.enums import ChartPlaceKind, ChartType
from trilogy.core.statements.execute import (
    ProcessedChartLayer,
    ProcessedChartStatement,
)
from trilogy.rendering.base import BaseRenderer, prettify_label

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

    def render(
        self,
        statement: ProcessedChartStatement,
        layer_data: list[list[dict]],
    ) -> str:
        plt.clear_figure()
        plt.theme("clear")

        builders = {
            ChartType.BAR: self._bar,
            ChartType.BARH: self._barh,
            ChartType.LINE: self._line,
            ChartType.POINT: self._point,
            ChartType.AREA: self._area,
        }

        for layer, data in zip(statement.layers, layer_data):
            build = builders.get(layer.layer_type)
            if build is None:
                return (
                    f"Chart type '{layer.layer_type.value}' not supported in terminal"
                )
            build(data, layer)

        for placement in statement.placements:
            self._placement(placement)

        return plt.build()

    def to_spec(
        self,
        statement: ProcessedChartStatement,
        layer_data: list[list[dict]],
    ) -> dict:
        return {"type": "terminal", "output": self.render(statement, layer_data)}

    def _placement(self, placement: Any) -> None:
        value = placement.value
        if placement.kind == ChartPlaceKind.HLINE:
            plt.horizontal_line(value)
        elif placement.kind == ChartPlaceKind.VLINE:
            plt.vertical_line(value)

    def _bar(self, data: list[dict], layer: ProcessedChartLayer) -> None:
        x = layer.x_fields[0] if layer.x_fields else None
        y = layer.y_fields[0] if layer.y_fields else None
        if not x or not y:
            return
        plt.bar([row.get(x) for row in data], [row.get(y) for row in data])
        plt.xlabel(prettify_label(x))
        plt.ylabel(prettify_label(y))

    def _barh(self, data: list[dict], layer: ProcessedChartLayer) -> None:
        x = layer.x_fields[0] if layer.x_fields else None
        y = layer.y_fields[0] if layer.y_fields else None
        if not x or not y:
            return
        rev = list(reversed(data))
        plt.bar(
            [row.get(y) for row in rev],
            [row.get(x) for row in rev],
            orientation="horizontal",
        )
        plt.xlabel(prettify_label(x))
        plt.ylabel(prettify_label(y))

    def _line(self, data: list[dict], layer: ProcessedChartLayer) -> None:
        x = layer.x_fields[0] if layer.x_fields else None
        ys = layer.y_fields or []
        if not x or not ys:
            return
        xs = [row.get(x) for row in data]
        for y in ys:
            plt.plot(xs, [row.get(y) for row in data], label=prettify_label(y))
        plt.xlabel(prettify_label(x))
        if len(ys) == 1:
            plt.ylabel(prettify_label(ys[0]))

    def _point(self, data: list[dict], layer: ProcessedChartLayer) -> None:
        x = layer.x_fields[0] if layer.x_fields else None
        y = layer.y_fields[0] if layer.y_fields else None
        if not x or not y:
            return
        plt.scatter([row.get(x) for row in data], [row.get(y) for row in data])
        plt.xlabel(prettify_label(x))
        plt.ylabel(prettify_label(y))

    def _area(self, data: list[dict], layer: ProcessedChartLayer) -> None:
        x = layer.x_fields[0] if layer.x_fields else None
        y = layer.y_fields[0] if layer.y_fields else None
        if not x or not y:
            return
        plt.plot(
            [row.get(x) for row in data],
            [row.get(y) for row in data],
            fillx=True,
        )
        plt.xlabel(prettify_label(x))
        plt.ylabel(prettify_label(y))
