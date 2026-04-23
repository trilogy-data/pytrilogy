import pytest

from trilogy.core.enums import ChartPlaceKind, ChartType
from trilogy.core.models.author import ConceptRef
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import ChartStatement
from trilogy.parser import parse
from trilogy.parsing.v2.model import HydrationError


def _parse(text: str) -> ChartStatement:
    env = Environment()
    _, statements = parse(text, env)
    chart = [s for s in statements if isinstance(s, ChartStatement)]
    assert len(chart) == 1
    return chart[0]


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


def test_single_layer_bar():
    chart = _parse(_SETUP + """
    chart
      layer bar ( x_axis <- x, y_axis <- y );
    """)
    assert len(chart.layers) == 1
    layer = chart.layers[0]
    assert layer.layer_type == ChartType.BAR
    assert {b.role for b in layer.bindings} == {"x_axis", "y_axis"}
    assert all(isinstance(b.expr, ConceptRef) for b in layer.bindings)


def test_line_with_color():
    chart = _parse(_SETUP + """
    chart
      layer line ( x_axis <- x, y_axis <- y, color <- color_val );
    """)
    assert chart.layers[0].layer_type == ChartType.LINE
    assert {b.role for b in chart.layers[0].bindings} >= {"color"}


def test_duplicate_y_axis_raises():
    with pytest.raises(HydrationError, match="may only be assigned once"):
        _parse(_SETUP + """
        chart
          layer line ( x_axis <- x, y_axis <- y, y_axis <- y );
        """)


def test_multi_layer_composition():
    chart = _parse(_SETUP + """
    chart
      layer bar ( x_axis <- x, y_axis <- y )
      layer line ( x_axis <- x, y_axis <- y );
    """)
    assert [layer.layer_type for layer in chart.layers] == [
        ChartType.BAR,
        ChartType.LINE,
    ]


def test_place_hline_and_vline():
    chart = _parse(_SETUP + """
    chart
      layer bar ( x_axis <- x, y_axis <- y )
      place hline at 5 as threshold
      place vline at 'a' as marker;
    """)
    kinds = [(p.kind, p.value, p.label) for p in chart.placements]
    assert (ChartPlaceKind.HLINE, 5, "threshold") in kinds
    assert (ChartPlaceKind.VLINE, "a", "marker") in kinds


def test_scalar_settings():
    chart = _parse(_SETUP + """
    chart
      layer line ( x_axis <- x, y_axis <- y )
      set scale_y: log
      set hide_legend;
    """)
    assert chart.scale_y == "log"
    assert chart.hide_legend is True


def test_unknown_role_raises_helpful_error():
    with pytest.raises(HydrationError, match="Unknown chart role"):
        _parse(_SETUP + """
        chart
          layer bar ( xaxis <- x, y_axis <- y );
        """)


def test_alias_for_computed_expression():
    chart = _parse(_SETUP + """
    chart
      layer bar ( x_axis <- x, y_axis <- y, color <- y * 2 as scaled_y );
    """)
    color = [b for b in chart.layers[0].bindings if b.role == "color"][0]
    assert color.alias == "scaled_y"


def test_computed_expression_without_alias_raises():
    with pytest.raises(HydrationError, match="computed expression"):
        _parse(_SETUP + """
        chart
          layer bar ( x_axis <- x, y_axis <- y * 2 );
        """)


def test_chart_types_roundtrip():
    for ct in ["bar", "line", "point", "area", "barh"]:
        chart = _parse(_SETUP + f"""
        chart
          layer {ct} ( x_axis <- x, y_axis <- y );
        """)
        assert chart.layers[0].layer_type == ChartType(ct)


def test_explicit_from_select():
    chart = _parse(_SETUP + """
    chart
      layer bar ( x_axis <- x, y_axis <- y ) from select x, y;
    """)
    layer = chart.layers[0]
    assert len(layer.select.selection) == 2
    assert {b.role for b in layer.bindings} == {"x_axis", "y_axis"}


def test_explicit_from_select_rejects_alias():
    with pytest.raises(HydrationError, match="direct concept"):
        _parse(_SETUP + """
        chart
          layer bar ( x_axis <- x, y_axis <- y * 2 as scaled_y ) from select x, y;
        """)


def test_layer_order_by_and_limit():
    chart = _parse(_SETUP + """
    chart
      layer bar ( x_axis <- x, y_axis <- y ) order by y desc limit 5;
    """)
    layer = chart.layers[0]
    assert layer.select is not None
    assert layer.select.limit == 5
    assert layer.select.order_by is not None
    assert len(layer.select.order_by.items) == 1


def test_layer_limit_without_order():
    chart = _parse(_SETUP + """
    chart
      layer bar ( x_axis <- x, y_axis <- y ) limit 3;
    """)
    assert chart.layers[0].select.limit == 3
    assert chart.layers[0].select.order_by is None


def test_chart_with_only_placements_raises():
    with pytest.raises(HydrationError, match="at least one layer"):
        _parse(_SETUP + """
        chart
          place hline at 5 as threshold;
        """)


def test_duplicate_copy_option_raises():
    from trilogy.parser import parse

    env = Environment()
    with pytest.raises(HydrationError, match="Duplicate copy option"):
        parse(
            _SETUP + """
        copy into png 'out.png' (width=100, width=200) from chart
          layer bar ( x_axis <- x, y_axis <- y );
        """,
            env,
        )
