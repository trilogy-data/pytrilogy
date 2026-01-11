from trilogy.core.enums import ChartType
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import ChartStatement
from trilogy.parser import parse


def test_basic_chart_parsing():
    text = """
    chart bar
      set x_axis: category
      set y_axis: value
    from select 'abc' as category, 3 as value;
    """
    env = Environment()
    _, statements = parse(text, env)
    assert len(statements) == 1
    assert isinstance(statements[0], ChartStatement)
    assert statements[0].config.chart_type == ChartType.BAR
    assert statements[0].config.x_fields == ["category"]
    assert statements[0].config.y_fields == ["value"]


def test_chart_parsing_multiple_y_fields():
    text = """
    chart line
      set x_axis: date
      set y_axis: sales, revenue
    from select '2024-01-01' as date, 100 as sales, 200 as revenue;
    """
    env = Environment()
    _, statements = parse(text, env)
    assert len(statements) == 1
    assert isinstance(statements[0], ChartStatement)
    assert statements[0].config.chart_type == ChartType.LINE
    assert statements[0].config.x_fields == ["date"]
    assert statements[0].config.y_fields == ["sales", "revenue"]


def test_chart_parsing_with_color():
    text = """
    chart bar
      set x_axis: category
      set y_axis: value
      set color: region
    from select 'abc' as category, 3 as value, 'north' as region;
    """
    env = Environment()
    _, statements = parse(text, env)
    assert len(statements) == 1
    assert isinstance(statements[0], ChartStatement)
    assert statements[0].config.color_field == "region"


def test_chart_parsing_with_bool_settings():
    text = """
    chart bar
      set x_axis: category
      set y_axis: value
      set hide_legend
    from select 'abc' as category, 3 as value;
    """
    env = Environment()
    _, statements = parse(text, env)
    assert len(statements) == 1
    assert isinstance(statements[0], ChartStatement)
    assert statements[0].config.hide_legend is True


def test_chart_parsing_with_scale():
    text = """
    chart line
      set x_axis: x
      set y_axis: y
      set scale_y: log
    from select 1 as x, 100 as y;
    """
    env = Environment()
    _, statements = parse(text, env)
    assert len(statements) == 1
    assert isinstance(statements[0], ChartStatement)
    assert statements[0].config.scale_y == "log"


def test_chart_types():
    for chart_type in ["bar", "line", "point", "area", "barh"]:
        text = f"""
        chart {chart_type}
          set x_axis: x
          set y_axis: y
        from select 1 as x, 2 as y;
        """
        env = Environment()
        _, statements = parse(text, env)
        assert len(statements) == 1
        assert isinstance(statements[0], ChartStatement)
        assert statements[0].config.chart_type == ChartType(chart_type)
