from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import ChartStatement, CopyStatement
from trilogy.parser import parse
from trilogy.parsing.render import Renderer

_SETUP = """
key x string;
property x.y int;
property x.color_val string;
datasource d (
  col_x: x,
  col_y: y,
  col_c: color_val,
) grain (x) query '''select 'A' as col_x, 10 as col_y, 'north' as col_c''';
"""


def _render(body: str) -> str:
    env = Environment()
    _, statements = parse(_SETUP + body, env)
    return Renderer(environment=env).to_string(statements[-1])


def _roundtrip(body: str) -> str:
    once = _render(body)
    twice = _render(once)
    assert once == twice
    return once


def test_render_single_layer():
    assert _roundtrip("chart layer bar ( x_axis <- x, y_axis <- y );") == """chart
    layer bar (
        x_axis <- x,
        y_axis <- y
    )
;"""


def test_render_alias_binding_order_limit():
    assert (
        _roundtrip(
            "chart layer bar ( x_axis <- x, y_axis <- y, color <- y * 2 as scaled )"
            " order by y desc limit 5;"
        )
        == """chart
    layer bar (
        x_axis <- x,
        y_axis <- y,
        color <- y * 2 as scaled
    )
    order by
        y desc
    limit 5
;"""
    )


def test_render_explicit_from_select():
    rendered = _roundtrip(
        "chart layer bar ( x_axis <- x, y_axis <- y ) from select x, y where y > 2;"
    )
    assert ") from where" in rendered
    assert "y > 2" in rendered


def test_render_layers_placements_and_settings():
    assert (
        _roundtrip(
            "chart layer bar ( x_axis <- x, y_axis <- y )"
            " layer line ( x_axis <- x, y_axis <- y )"
            " place hline at 5 as threshold"
            " place vline at 'a'"
            " set scale_y: log"
            " set hide_legend"
            " set show_title;"
        )
        == """chart
    layer bar (
        x_axis <- x,
        y_axis <- y
    )
    layer line (
        x_axis <- x,
        y_axis <- y
    )
    place hline at 5 as threshold
    place vline at 'a'
    set scale_y: log
    set hide_legend
    set show_title
;"""
    )


def test_render_copy_into_chart_with_options():
    rendered = _roundtrip(
        "copy into png 'out.png' (width=100, title='hi') from chart"
        " layer bar ( x_axis <- x, y_axis <- y );"
    )
    assert rendered.startswith(
        "copy into png 'out.png' (width=100, title='hi') from chart"
    )


def test_render_preserves_statement_types():
    env = Environment()
    _, statements = parse(
        _SETUP
        + "chart layer bar ( x_axis <- x, y_axis <- y );"
        + "copy into png 'o.png' from chart layer line ( x_axis <- x, y_axis <- y );",
        env,
    )
    rendered = Renderer(environment=env).render_statement_string(statements)
    env2 = Environment()
    _, reparsed = parse(rendered, env2)
    assert isinstance(reparsed[-2], ChartStatement)
    assert isinstance(reparsed[-1], CopyStatement)
