"""HTML backend and shared HTML assembly for report rendering."""

from __future__ import annotations

import io
import json
from dataclasses import dataclass
from html import escape
from pathlib import Path
from string import Template

from trilogy.core.enums import ChartType
from trilogy.rendering.theme import DEFAULT_THEME, REPORT_LAYOUT, Layout, Theme
from trilogy.report.backends.base import ReportBackend
from trilogy.report.charts import style_chart
from trilogy.report.document import (
    Chart,
    ErrorBox,
    Prose,
    RenderedElement,
    RenderedRow,
    Table,
)
from trilogy.report.tables import table_to_html

_VEGA_SCRIPTS = (
    '<script src="https://cdn.jsdelivr.net/npm/vega@5"></script>'
    '<script src="https://cdn.jsdelivr.net/npm/vega-lite@6"></script>'
    '<script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>'
)

# CSS as a string.Template so the literal { } of CSS need no escaping.
_CSS = Template("""
*, *::before, *::after { box-sizing: border-box; }

body {
  margin: 0;
  padding: 0;
  background: $page_background;
  color: $text_secondary;
  font-family: $font_stack;
  font-size: ${body_size}px;
  font-weight: $body_weight;
  line-height: $body_line_height;
  -webkit-font-smoothing: antialiased;
  text-rendering: optimizeLegibility;
}

main {
  max-width: ${content_width}px;
  margin: ${page_margin}px auto;
  padding: ${content_padding}px;
  background: $card_background;
  border: 1px solid $border;
  border-radius: ${card_radius}px;
  box-shadow: $card_shadow;
}

h1, h2, h3 { color: $text_primary; }
h1 {
  font-size: ${title_size}px;
  font-weight: $title_weight;
  letter-spacing: $title_tracking;
  line-height: $title_line_height;
  margin: 0 0 ${heading_gap}px;
}
h2 {
  font-size: ${section_size}px;
  font-weight: $section_weight;
  letter-spacing: $section_tracking;
  line-height: $section_line_height;
  margin: ${section_gap}px 0 ${heading_gap}px;
}
h3 {
  font-size: ${subsection_size}px;
  font-weight: $subsection_weight;
  letter-spacing: $subsection_tracking;
  line-height: $subsection_line_height;
  margin: ${section_gap_half}px 0 ${block_gap}px;
}

p { margin: 0 0 ${block_gap}px; }
strong { color: $text_primary; font-weight: 600; }
a { color: $accent_primary; text-decoration: none; }

table.report-table {
  width: 100%;
  border-collapse: collapse;
  margin: ${block_gap}px 0;
  font-size: ${table_font}px;
  line-height: $table_line_height;
  font-variant-numeric: tabular-nums;
}
table.report-table th {
  text-align: left;
  font-weight: 600;
  color: $text_primary;
  padding: $table_padding;
}
table.report-table td {
  padding: $table_padding;
  color: $text_primary;
  border-bottom: 1px solid rgba(0,0,0,0.04);
}
table.report-table tr:nth-child(even) td { background: rgba(0,0,0,0.012); }
table.report-table th.num, table.report-table td.num { text-align: right; }

.report-chart { width: 100%; margin: ${section_gap_half}px 0; }
.report-chart svg { max-width: 100%; height: auto; display: block;
  margin: 0 auto; }

.report-row {
  display: flex;
  gap: ${row_gap}px;
  align-items: flex-start;
  margin: ${section_gap_half}px 0;
}
.report-row-cell { flex: 1 1 0; min-width: 0; }
.report-row-cell > :first-child { margin-top: 0; }
.report-row-cell .report-chart { margin: 0; }

.report-error {
  margin: ${block_gap}px 0;
  padding: 14px 18px;
  background: #f7ecec;
  border: 1px solid rgba(140,42,42,0.18);
  border-radius: 10px;
  color: #8a2a2a;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: ${caption_size}px;
  white-space: pre-wrap;
}

code {
  background: $section_tint;
  padding: 2px 6px;
  border-radius: 5px;
  font-size: 0.9em;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
}
pre {
  background: $section_tint;
  padding: 18px 20px;
  border-radius: 12px;
  border: 1px solid $border;
  overflow: auto;
}
pre code { background: none; padding: 0; font-size: ${pre_font}px; }
""")


@dataclass
class _RenderState:
    """Mutable state threaded through element rendering."""

    chart_index: int = 0
    needs_vega: bool = False


def build_css(theme: Theme, layout: Layout) -> str:
    return _CSS.substitute(
        font_stack=theme.font_stack,
        page_background=theme.page_background,
        card_background=theme.card_background,
        section_tint=theme.section_tint,
        border=theme.border,
        text_primary=theme.text_primary,
        text_secondary=theme.text_secondary,
        accent_primary=theme.accent_primary,
        content_width=layout.content_width,
        content_padding=layout.content_padding,
        page_margin=layout.page_margin,
        card_radius=layout.card_radius,
        card_shadow=layout.card_shadow,
        title_size=layout.title_size,
        section_size=layout.section_size,
        subsection_size=layout.subsection_size,
        body_size=layout.body_size,
        caption_size=layout.caption_size,
        table_font=layout.body_size - 1,
        pre_font=layout.caption_size + 1,
        title_weight=layout.title_weight,
        section_weight=layout.section_weight,
        subsection_weight=layout.subsection_weight,
        body_weight=layout.body_weight,
        title_tracking=layout.title_tracking,
        title_line_height=layout.title_line_height,
        section_tracking=layout.section_tracking,
        section_line_height=layout.section_line_height,
        subsection_tracking=layout.subsection_tracking,
        subsection_line_height=layout.subsection_line_height,
        body_line_height=layout.body_line_height,
        section_gap=layout.section_gap,
        section_gap_half=layout.section_gap // 2,
        heading_gap=layout.heading_gap,
        block_gap=layout.block_gap,
        table_padding=layout.table_padding,
        table_line_height=layout.table_line_height,
        row_gap=layout.row_gap,
    )


def _prose_html(text: str) -> str:
    try:
        import markdown  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - import guard
        raise RuntimeError(
            "Report rendering requires the 'markdown' package; "
            "install pytrilogy[report]."
        ) from exc
    return markdown.markdown(text, extensions=["tables", "fenced_code"])


def _chart_interactive(
    chart: object,
    theme: Theme,
    layout: Layout,
    index: int,
    columns: int,
    chart_type: ChartType | None,
) -> str:
    styled = style_chart(
        chart, theme, layout, interactive=True, columns=columns, chart_type=chart_type
    )
    spec = json.dumps(styled.to_dict())
    return (
        f'<div class="report-chart" id="report-chart-{index}"></div>'
        f"<script>vegaEmbed('#report-chart-{index}', {spec}, "
        f"{{actions: false, renderer: 'svg'}});</script>"
    )


def _chart_static(
    chart: object,
    theme: Theme,
    layout: Layout,
    columns: int,
    chart_type: ChartType | None,
) -> str:
    styled = style_chart(
        chart, theme, layout, interactive=False, columns=columns, chart_type=chart_type
    )
    buffer = io.StringIO()
    styled.save(buffer, format="svg")
    return f'<div class="report-chart">{buffer.getvalue()}</div>'


def _webfont_links(theme: Theme) -> str:
    if not theme.webfont_url:
        return ""
    return (
        '<link rel="preconnect" href="https://fonts.googleapis.com">'
        '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
        f'<link rel="stylesheet" href="{theme.webfont_url}">'
    )


def _render_element(
    element: RenderedElement,
    theme: Theme,
    layout: Layout,
    interactive: bool,
    state: _RenderState,
    columns: int = 1,
) -> str:
    if isinstance(element, Prose):
        return _prose_html(element.markdown)
    if isinstance(element, Table):
        return table_to_html(element)
    if isinstance(element, Chart):
        if interactive:
            html = _chart_interactive(
                element.chart,
                theme,
                layout,
                state.chart_index,
                columns,
                element.chart_type,
            )
            state.chart_index += 1
            state.needs_vega = True
            return html
        return _chart_static(element.chart, theme, layout, columns, element.chart_type)
    if isinstance(element, ErrorBox):
        return f'<div class="report-error">{escape(element.message)}</div>'
    if isinstance(element, RenderedRow):
        count = max(len(element.elements), 1)
        cells = "".join(
            '<div class="report-row-cell">'
            + _render_element(child, theme, layout, interactive, state, count)
            + "</div>"
            for child in element.elements
        )
        return f'<div class="report-row">{cells}</div>'
    return ""


def build_html(
    elements: list[RenderedElement],
    theme: Theme = DEFAULT_THEME,
    layout: Layout = REPORT_LAYOUT,
    interactive: bool = True,
) -> str:
    """Assemble report elements into a complete, themed HTML document.

    With `interactive`, charts embed as live Vega-Lite; otherwise as static SVG.
    """
    state = _RenderState()
    body = "\n".join(
        _render_element(element, theme, layout, interactive, state)
        for element in elements
    )
    head = (
        '<meta charset="utf-8">'
        + _webfont_links(theme)
        + (_VEGA_SCRIPTS if state.needs_vega else "")
        + f"<style>{build_css(theme, layout)}</style>"
    )
    return (
        "<!DOCTYPE html>\n"
        f"<html><head>{head}</head>"
        f"<body><main>{body}</main></body></html>"
    )


class HtmlBackend(ReportBackend):
    extension = "html"

    def render(
        self,
        elements: list[RenderedElement],
        output_path: Path,
        theme: Theme = DEFAULT_THEME,
    ) -> None:
        output_path.write_text(
            build_html(elements, theme=theme, interactive=True), encoding="utf-8"
        )
