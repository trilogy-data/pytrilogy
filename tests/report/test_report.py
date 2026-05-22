import json

import pytest

from trilogy.core.enums import ChartType
from trilogy.rendering.theme import (
    DEFAULT_THEME,
    EDITORIAL_THEME,
    INTER_THEME,
    REPORT_LAYOUT,
    THEMES,
    get_theme,
)
from trilogy.report.backends import available_formats, get_backend
from trilogy.report.backends.html import build_css, build_html
from trilogy.report.charts import style_chart
from trilogy.report.document import (
    Chart,
    ErrorBox,
    Prose,
    RenderedRow,
    RowBlock,
    Table,
    TrilogyBlock,
    parse_markdown,
)
from trilogy.report.runner import run_document
from trilogy.report.tables import table_to_html, table_to_markdown

_NUMS_SETUP = (
    "```trilogy\n"
    "key n int;\n"
    "datasource nums (x: n) grain (n) "
    "query '''select 1 as x union all select 2 union all select 3''';\n"
    "```\n"
)

_CHART_BLOCK = (
    "```trilogy\n"
    "key c string;\n"
    "property c.v int;\n"
    "datasource d (cc: c, vv: v) grain (c) "
    "query '''select 'A' as cc, 1 as vv union all select 'B', 2''';\n"
    "chart layer bar ( x_axis <- c, y_axis <- v );\n"
    "```\n"
)

_HEADLINE_BLOCK = (
    "```trilogy\n"
    "key c string;\n"
    "property c.v int;\n"
    "datasource d (cc: c, vv: v) grain (c) "
    "query '''select 'A' as cc, 1 as vv union all select 'B', 2''';\n"
    "chart layer headline ( x_axis <- sum(v) as total );\n"
    "```\n"
)


def test_parse_markdown_splits_blocks():
    text = "# Title\n\nintro\n\n```trilogy\nselect 1;\n```\n\nmore\n\n```python\nx=1\n```\n"
    segments = parse_markdown(text)
    assert [type(s).__name__ for s in segments] == ["Prose", "TrilogyBlock", "Prose"]
    assert isinstance(segments[1], TrilogyBlock)
    assert segments[1].code == "select 1;"
    assert isinstance(segments[2], Prose)
    assert "```python" in segments[2].markdown


def test_parse_markdown_preserves_unterminated_fence():
    segments = parse_markdown("intro\n\n```trilogy\nselect 1;\n")
    assert all(isinstance(s, Prose) for s in segments)


def test_parse_markdown_row_container():
    text = (
        "intro\n\n"
        ":::row\n"
        "```trilogy\nselect 1;\n```\n"
        "```trilogy\nselect 2;\n```\n"
        ":::\n\n"
        "outro\n"
    )
    segments = parse_markdown(text)
    assert [type(s).__name__ for s in segments] == ["Prose", "RowBlock", "Prose"]
    row = segments[1]
    assert isinstance(row, RowBlock)
    assert [type(s).__name__ for s in row.segments] == ["TrilogyBlock", "TrilogyBlock"]


def test_run_document_row(tmp_path):
    text = (
        _NUMS_SETUP
        + "\n:::row\n"
        + "```trilogy\nselect n order by n asc;\n```\n"
        + "```trilogy\nselect n order by n desc;\n```\n"
        + ":::\n"
    )
    elements = run_document(parse_markdown(text), working_path=tmp_path)
    rows = [e for e in elements if isinstance(e, RenderedRow)]
    assert len(rows) == 1
    assert len(rows[0].elements) == 2
    assert all(isinstance(e, Table) for e in rows[0].elements)


def test_build_html_row_layout():
    pytest.importorskip("markdown")
    row = RenderedRow([Prose("left"), Prose("right")])
    html = build_html([row])
    assert html.count('<div class="report-row-cell">') == 2
    assert '<div class="report-row">' in html


def test_run_document_select_to_table(tmp_path):
    segments = parse_markdown(
        _NUMS_SETUP + "\n```trilogy\nselect n order by n asc;\n```\n"
    )
    elements = run_document(segments, working_path=tmp_path)
    tables = [e for e in elements if isinstance(e, Table)]
    assert len(tables) == 1
    assert len(tables[0].rows) == 3


def test_run_document_surfaces_errors(tmp_path):
    segments = parse_markdown("```trilogy\nselect undefined_concept;\n```\n")
    elements = run_document(segments, working_path=tmp_path)
    assert any(isinstance(e, ErrorBox) for e in elements)


def test_table_to_markdown():
    md = table_to_markdown(Table(columns=["a", "b"], rows=[[1, 2], [3, None]]))
    assert "| A | B |" in md
    assert "| 3 |  |" in md


def test_table_to_html_escapes():
    html = table_to_html(Table(columns=["x"], rows=[["<b>"]]))
    assert "&lt;b&gt;" in html


def test_table_headers_are_init_capped():
    table = Table(columns=["carrier_code", "flight_count"], rows=[["AA", 2]])
    html = table_to_html(table)
    assert "<th>Carrier Code</th>" in html
    assert '<th class="num">Flight Count</th>' in html
    assert "| Carrier Code | Flight Count |" in table_to_markdown(table)


def test_run_document_currency_table(tmp_path):
    text = (
        "```trilogy\n"
        "import std.currency;\n"
        "key region string;\n"
        "property region.revenue float::usd;\n"
        "datasource s (r: region, rev: revenue) grain (region) "
        "query '''select 'A' as r, 120000.0 as rev''';\n"
        "```\n"
        "```trilogy\nselect region, revenue;\n```\n"
    )
    elements = run_document(parse_markdown(text), working_path=tmp_path)
    table = next(e for e in elements if isinstance(e, Table))
    html = table_to_html(table)
    assert '<td class="num">$120,000</td>' in html


def test_headline_currency_formatting(tmp_path):
    pytest.importorskip("altair")
    text = (
        "```trilogy\n"
        "import std.currency;\n"
        "key region string;\n"
        "property region.revenue float::usd;\n"
        "datasource s (r: region, rev: revenue) grain (region) "
        "query '''select 'A' as r, 120000.0 as rev "
        "union all select 'B', 80000.0''';\n"
        "chart layer headline ( x_axis <- sum(revenue) as total_revenue );\n"
        "```\n"
    )
    chart = next(
        e
        for e in run_document(parse_markdown(text), working_path=tmp_path)
        if isinstance(e, Chart)
    ).chart
    assert "$200,000" in json.dumps(chart.to_dict())


def test_build_html_renders_table_and_prose():
    pytest.importorskip("markdown")
    html = build_html([Prose("# Hi"), Table(["a"], [[1]])])
    assert "<h1>Hi</h1>" in html
    assert "report-table" in html


def test_build_html_static_chart(tmp_path):
    pytest.importorskip("vl_convert")
    elements = run_document(parse_markdown(_CHART_BLOCK), working_path=tmp_path)
    charts = [e for e in elements if isinstance(e, Chart)]
    assert len(charts) == 1
    html = build_html(elements, interactive=False)
    assert "<svg" in html


def test_html_backend_writes_file(tmp_path):
    pytest.importorskip("markdown")
    out = tmp_path / "report.html"
    get_backend("html").render([Prose("hello world")], out)
    assert out.exists()
    assert "hello world" in out.read_text(encoding="utf-8")


def test_available_formats():
    assert set(available_formats()) == {"html", "png"}


def test_get_backend_unknown():
    with pytest.raises(ValueError, match="Unknown report format"):
        get_backend("docx")


def test_theme_constants():
    assert "Inter" in INTER_THEME.font_stack
    assert "Instrument Sans" in EDITORIAL_THEME.font_stack
    assert set(THEMES) == {"inter", "editorial"}
    assert DEFAULT_THEME is INTER_THEME
    assert get_theme("editorial") is EDITORIAL_THEME


def test_get_theme_unknown():
    with pytest.raises(ValueError, match="Unknown theme"):
        get_theme("brutalist")


def test_build_css_uses_theme_and_avoids_pure_white():
    css = build_css(INTER_THEME, REPORT_LAYOUT)
    assert INTER_THEME.page_background in css
    assert "Inter" in css
    assert "#ffffff" not in css.lower()
    assert "#fff;" not in css.lower()


def test_build_html_embeds_webfont():
    pytest.importorskip("markdown")
    html = build_html([Prose("# Hi")], theme=INTER_THEME)
    assert "fonts.googleapis.com" in html
    assert INTER_THEME.page_background in html


def test_run_document_chart_type(tmp_path):
    pytest.importorskip("altair")
    bar = run_document(parse_markdown(_CHART_BLOCK), working_path=tmp_path)
    headline = run_document(parse_markdown(_HEADLINE_BLOCK), working_path=tmp_path)
    bar_chart = next(e for e in bar if isinstance(e, Chart))
    headline_chart = next(e for e in headline if isinstance(e, Chart))
    assert bar_chart.chart_type == ChartType.BAR
    assert headline_chart.chart_type == ChartType.HEADLINE


def test_style_chart_headline_height(tmp_path):
    pytest.importorskip("altair")
    chart = next(
        e
        for e in run_document(parse_markdown(_HEADLINE_BLOCK), working_path=tmp_path)
        if isinstance(e, Chart)
    ).chart
    spec = style_chart(
        chart,
        INTER_THEME,
        REPORT_LAYOUT,
        interactive=False,
        chart_type=ChartType.HEADLINE,
    ).to_dict()
    assert spec["height"] == REPORT_LAYOUT.headline_height


def test_style_chart_applies_theme(tmp_path):
    pytest.importorskip("altair")
    chart = next(
        e
        for e in run_document(parse_markdown(_CHART_BLOCK), working_path=tmp_path)
        if isinstance(e, Chart)
    ).chart
    config = style_chart(
        chart, INTER_THEME, REPORT_LAYOUT, interactive=False, chart_type=ChartType.BAR
    ).to_dict()["config"]
    assert config["mark"]["color"] == INTER_THEME.chart_palette[0]
    assert config["bar"]["cornerRadiusEnd"] == 4
    assert config["axis"]["domain"] is False
    assert config["axis"]["ticks"] is False


def test_style_chart_full_width(tmp_path):
    pytest.importorskip("vl_convert")
    elements = run_document(parse_markdown(_CHART_BLOCK), working_path=tmp_path)
    chart = next(e for e in elements if isinstance(e, Chart)).chart
    spec = style_chart(chart, INTER_THEME, REPORT_LAYOUT, interactive=False).to_dict()
    assert spec["width"] == REPORT_LAYOUT.chart_width
    container = style_chart(chart, INTER_THEME, REPORT_LAYOUT, interactive=True)
    assert container.to_dict()["width"] == "container"
