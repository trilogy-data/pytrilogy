"""Apply report theming to an executed Altair chart."""

from __future__ import annotations

from typing import Any

from trilogy.core.enums import ChartType
from trilogy.rendering.chart_theme import theme_chart
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
    container; static charts get explicit px. Color/axis/font theming is the
    shared `theme_chart` step.
    """
    is_headline = chart_type == ChartType.HEADLINE
    properties: dict[str, Any] = {}
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
    return theme_chart(
        chart.properties(**properties), theme, label_font_size=layout.caption_size
    )
