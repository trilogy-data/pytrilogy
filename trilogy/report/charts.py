"""Apply report theming to an executed Altair chart."""

from __future__ import annotations

from typing import Any

from trilogy.core.enums import ChartType
from trilogy.rendering.theme import Layout, Theme


def style_chart(
    chart: Any,
    theme: Theme,
    layout: Layout,
    interactive: bool,
    columns: int = 1,
    chart_type: ChartType | None = None,
) -> Any:
    """Resize and restyle an Altair chart to match the report theme.

    `columns` is the number of side-by-side charts in the row (1 = full width).
    Headline charts get a shorter fixed height. Interactive charts size to their
    container; static charts get explicit px. Axes are thinned, gridlines muted,
    and colors drawn from the theme.
    """
    is_headline = chart_type == ChartType.HEADLINE
    properties: dict[str, Any] = {"background": "transparent"}
    if is_headline:
        # Width is content-driven (set by the renderer): the headline keeps its
        # natural size and the column scales the whole SVG via CSS max-width.
        properties["height"] = layout.headline_height
    else:
        width_px, height_px = layout.chart_box(columns)
        properties["width"] = "container" if interactive else width_px
        properties["height"] = height_px
        # axis charts need breathing room; a headline is just centered text
        properties["padding"] = {"top": 16, "left": 12, "right": 16, "bottom": 8}
    return (
        chart.properties(**properties)
        .configure_view(stroke=None)
        .configure_axis(
            grid=True,
            gridColor="rgba(0,0,0,0.05)",
            gridWidth=1,
            domain=False,
            ticks=False,
            labelColor=theme.text_muted,
            labelFont=theme.font_stack,
            labelFontSize=layout.caption_size,
            labelFontWeight=500,
            labelPadding=8,
            titleColor=theme.text_secondary,
            titleFont=theme.font_stack,
            titleFontSize=layout.caption_size,
            titleFontWeight=500,
            titlePadding=16,
        )
        .configure_mark(color=theme.chart_palette[0])
        .configure_bar(cornerRadiusEnd=4)
        .configure_line(strokeWidth=2.5)
        .configure_area(opacity=0.15)
        .configure_range(category=list(theme.chart_palette))
    )
