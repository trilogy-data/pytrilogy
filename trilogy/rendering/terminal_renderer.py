from typing import Any

from trilogy.core.enums import ChartPlaceKind, ChartType
from trilogy.core.models.core import DataType
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


_NUMERIC_DATA_TYPES = {
    DataType.INTEGER,
    DataType.BIGINT,
    DataType.FLOAT,
    DataType.NUMERIC,
    DataType.NUMBER,
    DataType.UNIX_SECONDS,
}


def _is_numeric_axis(datatype: Any) -> bool:
    """Plotext treats numeric axes positionally; everything else needs xticks."""
    if datatype is None:
        return False
    resolved = getattr(datatype, "data_type", datatype)
    return resolved in _NUMERIC_DATA_TYPES


def _field_datatype(layer: ProcessedChartLayer, field_name: str) -> Any:
    if layer.query is None:
        return None
    for col in layer.query.output_columns:
        if col.safe_address == field_name:
            return col.datatype
    return None


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

        layers = list(zip(statement.layers, layer_data))
        use_subplots = len(layers) > 1
        if use_subplots:
            plt.subplots(len(layers), 1)

        for idx, (layer, data) in enumerate(layers):
            build = builders.get(layer.layer_type)
            if build is None:
                return (
                    f"Chart type '{layer.layer_type.value}' not supported in terminal"
                )
            if use_subplots:
                plt.subplot(idx + 1, 1)
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
        plot_xs, x_labels = self._resolve_axis(data, x, layer)
        for y in ys:
            plot_ys, y_labels = self._resolve_axis(data, y, layer)
            plt.plot(plot_xs, plot_ys, label=prettify_label(y))
            if y_labels is not None and len(ys) == 1:
                plt.yticks(plot_ys, y_labels)
        if x_labels is not None:
            plt.xticks(plot_xs, x_labels)
        plt.xlabel(prettify_label(x))
        if len(ys) == 1:
            plt.ylabel(prettify_label(ys[0]))

    def _point(self, data: list[dict], layer: ProcessedChartLayer) -> None:
        x = layer.x_fields[0] if layer.x_fields else None
        y = layer.y_fields[0] if layer.y_fields else None
        if not x or not y:
            return
        plot_xs, x_labels = self._resolve_axis(data, x, layer)
        plot_ys, y_labels = self._resolve_axis(data, y, layer)
        plt.scatter(plot_xs, plot_ys)
        if x_labels is not None:
            plt.xticks(plot_xs, x_labels)
        if y_labels is not None:
            plt.yticks(plot_ys, y_labels)
        plt.xlabel(prettify_label(x))
        plt.ylabel(prettify_label(y))

    def _area(self, data: list[dict], layer: ProcessedChartLayer) -> None:
        x = layer.x_fields[0] if layer.x_fields else None
        y = layer.y_fields[0] if layer.y_fields else None
        if not x or not y:
            return
        plot_xs, x_labels = self._resolve_axis(data, x, layer)
        plot_ys, y_labels = self._resolve_axis(data, y, layer)
        plt.plot(plot_xs, plot_ys, fillx=True)
        if x_labels is not None:
            plt.xticks(plot_xs, x_labels)
        if y_labels is not None:
            plt.yticks(plot_ys, y_labels)
        plt.xlabel(prettify_label(x))
        plt.ylabel(prettify_label(y))

    @staticmethod
    def _resolve_axis(
        data: list[dict],
        field_name: str,
        layer: ProcessedChartLayer,
    ) -> tuple[list[Any], list[str] | None]:
        raw = [row.get(field_name) for row in data]
        if _is_numeric_axis(_field_datatype(layer, field_name)):
            return raw, None
        positions = list(range(len(raw)))
        labels = ["" if v is None else str(v) for v in raw]
        return positions, labels
