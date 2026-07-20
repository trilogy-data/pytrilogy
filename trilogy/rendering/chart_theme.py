"""Apply a Theme's colors, fonts, and chrome to an Altair chart.

This is the sizing-free half of chart styling, shared by every path that emits
a chart — `trilogy render` composes it with report layout in `style_chart`;
`copy into png|svg|... from chart` applies it directly so exported assets match
the theme. Charts render on a transparent background so the host page owns the
surround.
"""

from __future__ import annotations

from typing import Any

from trilogy.rendering.theme import Appearance, Theme

_LIGHT_GRID = "rgba(0,0,0,0.05)"
_DARK_GRID = "rgba(255,255,255,0.08)"


def theme_chart(chart: Any, theme: Theme, label_font_size: int = 12) -> Any:
    """Restyle an Altair chart to the theme: palette, axis chrome, typography.

    Leaves width/height/padding untouched — callers own sizing.
    """
    grid = _DARK_GRID if theme.appearance == Appearance.DARK else _LIGHT_GRID
    return (
        chart.properties(background="transparent")
        .configure_view(stroke=None)
        .configure_axis(
            grid=True,
            gridColor=grid,
            gridWidth=1,
            domain=False,
            ticks=False,
            labelColor=theme.text_muted,
            labelFont=theme.font_stack,
            labelFontSize=label_font_size,
            labelFontWeight=500,
            labelPadding=8,
            titleColor=theme.text_secondary,
            titleFont=theme.font_stack,
            titleFontSize=label_font_size,
            titleFontWeight=500,
            titlePadding=16,
        )
        .configure_legend(
            labelColor=theme.text_muted,
            labelFont=theme.font_stack,
            titleColor=theme.text_secondary,
            titleFont=theme.font_stack,
        )
        .configure_mark(color=theme.chart_palette[0])
        .configure_bar(cornerRadiusEnd=4)
        .configure_line(strokeWidth=2.5)
        # translucent fill + a full-opacity boundary line so a SOLO area chart
        # stays crisp; a bare opacity=0.15 (the old value) read as empty when
        # no line layer sat on top
        .configure_area(fillOpacity=0.35, line={"strokeWidth": 2.5})
        .configure_range(category=list(theme.chart_palette))
    )
