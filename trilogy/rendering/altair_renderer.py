from decimal import Decimal
from typing import Any

from trilogy.core.enums import ChartPlaceKind, ChartType
from trilogy.core.statements.execute import (
    ProcessedChartLayer,
    ProcessedChartStatement,
)
from trilogy.rendering.base import BaseRenderer, prettify_label
from trilogy.rendering.rich_types import (
    axis_label_expr,
    currency_symbol,
    field_datatype,
    format_currency,
)

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

    def _coerce_decimals(self, df: Any) -> Any:
        """DuckDB returns DECIMAL as Decimal; cast to float so Altair infers a
        quantitative type and the resulting spec is JSON-serializable."""
        for col in df.columns:
            if (
                df[col].dtype == object
                and df[col].map(lambda v: isinstance(v, Decimal)).any()
            ):
                df[col] = df[col].astype(float)
        return df

    def _render_layer(self, layer: ProcessedChartLayer, data: list[dict]) -> Any:
        df = self._coerce_decimals(pd.DataFrame(data))
        builders = {
            ChartType.BAR: self._bar,
            ChartType.BARH: self._bar,
            ChartType.LINE: self._line,
            ChartType.POINT: self._point,
            ChartType.AREA: self._area,
            ChartType.HEADLINE: self._headline,
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

    def _field_axis(self, layer: ProcessedChartLayer, field: str) -> Any:
        """A currency-formatting axis for the field, else Altair's default."""
        expr = axis_label_expr(field_datatype(layer, field))
        return alt.Axis(labelExpr=expr) if expr else alt.Undefined

    def _encode(self, layer: ProcessedChartLayer) -> dict[str, Any]:
        encoding: dict[str, Any] = {}
        if layer.x_fields:
            field = layer.x_fields[0]
            encoding["x"] = alt.X(
                field,
                title=prettify_label(field),
                axis=self._field_axis(layer, field),
            )
        if layer.y_fields:
            field = layer.y_fields[0]
            encoding["y"] = alt.Y(
                field,
                title=prettify_label(field),
                axis=self._field_axis(layer, field),
            )
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
        # barh maps here too: axes are literal, so binding a quantitative field
        # to x_axis yields horizontal bars without any axis swap.
        return alt.Chart(data).mark_bar().encode(**self._encode(layer))

    def _line(self, data: Any, layer: ProcessedChartLayer) -> Any:
        return alt.Chart(data).mark_line().encode(**self._encode(layer))

    def _point(self, data: Any, layer: ProcessedChartLayer) -> Any:
        return alt.Chart(data).mark_point().encode(**self._encode(layer))

    def _area(self, data: Any, layer: ProcessedChartLayer) -> Any:
        return alt.Chart(data).mark_area().encode(**self._encode(layer))

    def _headline(self, data: Any, layer: ProcessedChartLayer) -> Any:
        """Render the x_axis value(s) as large numbers under a soft-gray title.

        Sized for a single KPI value; the font shrinks if a query returns
        several rows so they still fit side by side.
        """
        if not layer.x_fields:
            raise ValueError("A headline chart requires an x_axis binding.")
        field = layer.x_fields[0]
        title = (prettify_label(field) or field).upper()
        positioned = data.reset_index(drop=True).assign(_headline_pos=range(len(data)))
        count = max(len(positioned), 1)
        font_size = max(22, min(52, round(165 / count)))
        symbol = currency_symbol(field_datatype(layer, field))
        if symbol is not None:
            positioned = positioned.assign(
                _headline_value=[
                    format_currency(v, symbol) if pd.notna(v) else ""
                    for v in positioned[field]
                ]
            )
            text_encoding = alt.Text("_headline_value:N")
        else:
            text_encoding = alt.Text(f"{field}:Q", format=",.4~f")
        number = (
            alt.Chart(positioned)
            .mark_text(
                fontSize=font_size,
                fontWeight=700,
                color="#1f1f1c",
                baseline="middle",
                dy=8,
            )
            .encode(
                x=alt.X("_headline_pos:O", axis=None),
                text=text_encoding,
            )
        )
        label = (
            alt.Chart(pd.DataFrame({"_headline_title": [title]}))
            .mark_text(
                align="left",
                x=0,
                fontSize=12,
                fontWeight=600,
                color="#8a8a83",
                baseline="middle",
                dy=-28,
            )
            .encode(text=alt.Text("_headline_title:N"))
        )
        # content-driven width so the report can scale the headline to fit
        return alt.layer(number, label).properties(width=count * 240, height=100)
