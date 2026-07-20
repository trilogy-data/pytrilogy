from datetime import date
from decimal import Decimal
from typing import Any

from trilogy.core.enums import ChartPlaceKind, ChartType, ScaleType
from trilogy.core.models.core import DataType, TraitDataType
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
    is_hex_color,
    is_integer_date_part,
    is_numeric,
)
from trilogy.rendering.theme import DEFAULT_THEME, Theme

try:
    import altair as alt
    import pandas as pd

    ALTAIR_AVAILABLE = True
except ImportError:
    ALTAIR_AVAILABLE = False
    alt = None  # type: ignore[assignment]
    pd = None  # type: ignore[assignment]


class AltairRenderer(BaseRenderer):
    def __init__(self, theme: Theme | None = None):
        if not ALTAIR_AVAILABLE:
            raise ImportError(
                "Altair is required for chart rendering. "
                "Install `altair` via pip/uv to use."
            )
        # Text colors bake into headline marks at build time (Vega config can't
        # differentiate the two text layers), so the renderer needs the theme.
        self.theme = theme or DEFAULT_THEME

    def render(
        self,
        statement: ProcessedChartStatement,
        layer_data: list[list[dict]],
    ) -> Any:
        if len(layer_data) != len(statement.layers):
            raise ValueError("Layer data count does not match layer count")
        trellised = any(
            layer.x_trellis_field or layer.y_trellis_field for layer in statement.layers
        )
        annotated = any(layer.annotation_field for layer in statement.layers)
        if trellised and (
            len(statement.layers) > 1 or statement.placements or annotated
        ):
            # Vega-Lite forbids facet channels inside layered specs.
            raise ValueError(
                "Trellis roles cannot be combined with multiple layers,"
                " place rules, or annotations."
            )
        scales = (statement.scale_x, statement.scale_y)
        layer_charts: list[Any] = []
        for layer, data in zip(statement.layers, layer_data):
            layer_charts.append(self._render_layer(layer, data, scales))
        for placement in statement.placements:
            layer_charts.extend(self._render_placement(placement))
        if not layer_charts:
            return None
        chart = layer_charts[0] if len(layer_charts) == 1 else alt.layer(*layer_charts)
        if statement.show_title:
            title = self._statement_title(statement)
            if title:
                chart = chart.properties(title=title)
        if statement.hide_legend:
            chart = chart.configure_legend(disable=True)
        return chart

    @staticmethod
    def _statement_title(statement: ProcessedChartStatement) -> str | None:
        """A title from the first layer's value-axis field (y, else x)."""
        for layer in statement.layers:
            for fields in (layer.y_fields, layer.x_fields):
                if fields:
                    return prettify_label(fields[0])
        return None

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

    def _coerce_temporals(self, df: Any) -> Any:
        """DuckDB returns DATE as datetime.date, which pandas keeps as object
        dtype and Altair can't JSON-serialize; cast to datetime64."""
        for col in df.columns:
            if (
                df[col].dtype == object
                and df[col].map(lambda v: isinstance(v, date)).any()
            ):
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df

    def _render_layer(
        self,
        layer: ProcessedChartLayer,
        data: list[dict],
        scales: tuple[ScaleType | None, ScaleType | None] = (None, None),
    ) -> Any:
        df = self._coerce_temporals(self._coerce_decimals(pd.DataFrame(data)))
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
        return build(df, layer, scales)

    def _render_placement(self, placement) -> list[Any]:
        datum_key = "y" if placement.kind == ChartPlaceKind.HLINE else "x"
        encoding = {datum_key: alt.datum(placement.value)}
        charts = [alt.Chart().mark_rule().encode(**encoding)]
        if placement.label:
            # pin the label to the rule on its axis; pixel-position the other.
            # hline: just above the line at the left edge; vline: right of the
            # line near the top.
            is_hline = placement.kind == ChartPlaceKind.HLINE
            other_key = "x" if is_hline else "y"
            charts.append(
                alt.Chart()
                .mark_text(
                    align="left",
                    baseline="bottom" if is_hline else "top",
                    dx=4,
                    dy=-4 if is_hline else 0,
                    fontSize=11,
                    color=self.theme.text_muted,
                )
                .encode(
                    **{
                        datum_key: alt.datum(placement.value),
                        other_key: alt.value(4),
                    },
                    text=alt.value(placement.label),
                )
            )
        return charts

    _TEMPORAL_TYPES = {DataType.DATE, DataType.DATETIME, DataType.TIMESTAMP}

    def _field_type(self, layer: ProcessedChartLayer, field: str) -> Any:
        """Vega-Lite type from the concept's declared datatype, so encoding
        doesn't depend on pandas dtype inference; Altair's default otherwise."""
        datatype = field_datatype(layer, field)
        if datatype is None:
            return alt.Undefined
        if is_numeric(datatype):
            return "quantitative"
        base = datatype.type if isinstance(datatype, TraitDataType) else datatype
        if base in self._TEMPORAL_TYPES:
            return "temporal"
        if base in (DataType.STRING, DataType.BOOL):
            return "nominal"
        return alt.Undefined

    @staticmethod
    def _time_label_format(values: Any) -> str:
        """A time-label format matched to the data's granularity, judged by the
        smallest gap between distinct values."""
        stamps = pd.to_datetime(pd.Series(values), errors="coerce").dropna()
        gaps = stamps.drop_duplicates().sort_values().diff().dropna()
        step = gaps.min() if len(gaps) else pd.Timedelta(days=1)
        if step < pd.Timedelta(hours=1):
            return "%H:%M"
        if step < pd.Timedelta(days=1):
            return "%b %d %H:%M"
        if step < pd.Timedelta(days=28):
            return "%b %d"
        if step < pd.Timedelta(days=365):
            return "%b %Y"
        return "%Y"

    def _encode(
        self,
        layer: ProcessedChartLayer,
        data: Any = None,
        category_axis: str | None = None,
        scales: tuple[ScaleType | None, ScaleType | None] = (None, None),
    ) -> dict[str, Any]:
        # Bar order follows the query's ORDER BY when it has one; without one,
        # sort by category value so output stays deterministic.
        ordered = layer.query is not None and layer.query.order_by is not None
        encoding: dict[str, Any] = {}
        for channel, cls, fields, scale_type in (
            ("x", alt.X, layer.x_fields, scales[0]),
            ("y", alt.Y, layer.y_fields, scales[1]),
        ):
            if not fields:
                continue
            field = fields[0]
            field_type = self._field_type(layer, field)
            sort: Any = alt.Undefined
            axis_kwargs: dict[str, Any] = {}
            datatype = field_datatype(layer, field)
            currency_expr = axis_label_expr(datatype)
            if currency_expr:
                axis_kwargs["labelExpr"] = currency_expr
            elif is_integer_date_part(datatype):
                # Vega's default quantitative axis shows a year as `2,020.5`;
                # date parts want bare integer ticks.
                axis_kwargs["format"] = "d"
                axis_kwargs["tickMinStep"] = 1
            if channel == category_axis:
                sort = None if ordered else "ascending"
                # A bar's category axis is discrete: encode as ordinal so bars
                # band to the step at any density. A continuous scale gives
                # fixed-width slivers (Vega's 5px default) or overlap instead.
                if field_type == "temporal":
                    field_type = "ordinal"
                    if data is not None and field in data:
                        fmt = self._time_label_format(data[field])
                        axis_kwargs["labelExpr"] = (
                            f"timeFormat(toDate(datum.value), '{fmt}')"
                        )
                elif field_type == "quantitative":
                    field_type = "ordinal"
            # `set scale_x|scale_y:` applies to continuous value axes only —
            # log/sqrt is meaningless on a banded category axis.
            scale = (
                alt.Scale(type=scale_type)
                if scale_type and field_type == "quantitative"
                else alt.Undefined
            )
            encoding[channel] = cls(
                field,
                title=prettify_label(field),
                axis=alt.Axis(**axis_kwargs) if axis_kwargs else alt.Undefined,
                type=field_type,
                sort=sort,
                scale=scale,
            )
        if layer.color_field:
            encoding["color"] = alt.Color(
                layer.color_field,
                title=prettify_label(layer.color_field),
                scale=self._hex_color_scale(layer, data),
            )
        if layer.size_field:
            encoding["size"] = alt.Size(
                layer.size_field, title=prettify_label(layer.size_field)
            )
        if layer.group_field:
            # grouped bars sit side by side within each category band; on
            # continuous marks the group splits the series without a legend
            if layer.layer_type in (ChartType.BAR, ChartType.BARH):
                offset_cls = (
                    alt.YOffset if layer.layer_type == ChartType.BARH else alt.XOffset
                )
                offset_channel = (
                    "yOffset" if layer.layer_type == ChartType.BARH else "xOffset"
                )
                encoding[offset_channel] = offset_cls(
                    layer.group_field, title=prettify_label(layer.group_field)
                )
            else:
                encoding["detail"] = alt.Detail(layer.group_field)
        if layer.x_trellis_field:
            encoding["column"] = alt.Column(
                layer.x_trellis_field, title=prettify_label(layer.x_trellis_field)
            )
        if layer.y_trellis_field:
            encoding["row"] = alt.Row(
                layer.y_trellis_field, title=prettify_label(layer.y_trellis_field)
            )
        if layer.geo_field:
            raise NotImplementedError(
                "The 'geo' chart role is not yet implemented in the Altair" " renderer."
            )
        return encoding

    _HEX_FALLBACK = "#999999"

    def _hex_color_scale(self, layer: ProcessedChartLayer, data: Any) -> Any:
        """Explicit category→color mapping when the query outputs a `::hex`
        column alongside the color field: each color-field member maps to the
        hex code found on its rows, so authors control series colors from data.
        """
        if layer.query is None or data is None or layer.color_field not in data:
            return alt.Undefined
        hex_field = next(
            (
                c.safe_address
                for c in layer.query.output_columns
                if is_hex_color(c.datatype) and c.safe_address in data
            ),
            None,
        )
        if hex_field is None:
            return alt.Undefined
        lookup: dict[Any, str] = {}
        for category, hex_value in zip(data[layer.color_field], data[hex_field]):
            if pd.notna(hex_value):
                lookup.setdefault(category, str(hex_value))
        domain = sorted(data[layer.color_field].dropna().unique().tolist(), key=str)
        return alt.Scale(
            domain=domain,
            range=[lookup.get(category, self._HEX_FALLBACK) for category in domain],
        )

    def _with_annotation(
        self,
        chart: Any,
        data: Any,
        layer: ProcessedChartLayer,
        encoding: dict[str, Any],
    ) -> Any:
        """Overlay per-mark text labels bound to the `annotation` role."""
        if not layer.annotation_field:
            return chart
        horizontal = layer.layer_type == ChartType.BARH
        text = (
            alt.Chart(data)
            .mark_text(
                fontSize=11,
                color=self.theme.text_secondary,
                align="left" if horizontal else "center",
                baseline="middle" if horizontal else "bottom",
                dx=4 if horizontal else 0,
                dy=0 if horizontal else -4,
            )
            .encode(
                **{k: v for k, v in encoding.items() if k in ("x", "y")},
                text=alt.Text(layer.annotation_field),
            )
        )
        return alt.layer(chart, text)

    def _bar(
        self,
        data: Any,
        layer: ProcessedChartLayer,
        scales: tuple[ScaleType | None, ScaleType | None] = (None, None),
    ) -> Any:
        # barh maps here too: axes are literal, so binding a quantitative field
        # to x_axis yields horizontal bars without any axis swap.
        category_axis = "y" if layer.layer_type == ChartType.BARH else "x"
        encoding = self._encode(
            layer, data=data, category_axis=category_axis, scales=scales
        )
        chart = alt.Chart(data).mark_bar().encode(**encoding)
        return self._with_annotation(chart, data, layer, encoding)

    def _line(
        self,
        data: Any,
        layer: ProcessedChartLayer,
        scales: tuple[ScaleType | None, ScaleType | None] = (None, None),
    ) -> Any:
        encoding = self._encode(layer, data=data, scales=scales)
        chart = alt.Chart(data).mark_line().encode(**encoding)
        return self._with_annotation(chart, data, layer, encoding)

    def _point(
        self,
        data: Any,
        layer: ProcessedChartLayer,
        scales: tuple[ScaleType | None, ScaleType | None] = (None, None),
    ) -> Any:
        encoding = self._encode(layer, data=data, scales=scales)
        chart = alt.Chart(data).mark_point().encode(**encoding)
        return self._with_annotation(chart, data, layer, encoding)

    @staticmethod
    def _stack_order_ranks(data: Any, layer: ProcessedChartLayer) -> dict[Any, int]:
        """Bottom→top stack rank per color category, largest-(coverage, total)
        first: series spanning the whole x-domain form a stable base, while
        transient ones float to the top, where appearing or disappearing only
        moves the top edge instead of reflowing every band above (which linear
        interpolation would draw as diagonals crossing neighboring bands)."""
        x, y = layer.x_fields[0], layer.y_fields[0]
        agg = data.groupby(layer.color_field).agg(
            coverage=(x, "nunique"), total=(y, "sum")
        )
        ordered = agg.sort_values(
            ["coverage", "total"], ascending=False, kind="stable"
        ).index
        return {category: rank for rank, category in enumerate(ordered)}

    def _area(
        self,
        data: Any,
        layer: ProcessedChartLayer,
        scales: tuple[ScaleType | None, ScaleType | None] = (None, None),
    ) -> Any:
        order: Any = alt.Undefined
        if layer.color_field and layer.x_fields and layer.y_fields and len(data):
            ranks = self._stack_order_ranks(data, layer)
            # null categories get no rank from groupby; stack them on top
            data = data.assign(
                _stack_order=data[layer.color_field].map(ranks).fillna(len(ranks))
            )
            order = alt.Order("_stack_order:Q", sort="ascending")
        encoding = self._encode(layer, data=data, scales=scales)
        chart = alt.Chart(data).mark_area().encode(order=order, **encoding)
        return self._with_annotation(chart, data, layer, encoding)

    def _headline(
        self,
        data: Any,
        layer: ProcessedChartLayer,
        scales: tuple[ScaleType | None, ScaleType | None] = (None, None),
    ) -> Any:
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
        elif is_integer_date_part(field_datatype(layer, field)):
            text_encoding = alt.Text(f"{field}:Q", format="d")
        else:
            text_encoding = alt.Text(f"{field}:Q", format=",.4~f")
        number = (
            alt.Chart(positioned)
            .mark_text(
                fontSize=font_size,
                fontWeight=700,
                color=self.theme.text_primary,
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
                color=self.theme.text_muted,
                baseline="middle",
                dy=-28,
            )
            .encode(text=alt.Text("_headline_title:N"))
        )
        # content-driven width so the report can scale the headline to fit
        return alt.layer(number, label).properties(width=count * 240, height=100)
