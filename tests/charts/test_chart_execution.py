import pytest

from trilogy import Executor
from trilogy.core.enums import ChartType
from trilogy.dialect.enums import Dialects
from trilogy.dialect.results import ChartResult


def _executor() -> Executor:
    return Executor(dialect=Dialects.DUCK_DB, engine=Dialects.DUCK_DB.default_engine())


_SETUP = """
key category string;
property category.value int;

datasource chart_data (
    cat: category,
    val: value
)
grain (category)
query '''
select 'A' as cat, 10 as val
union all select 'B', 20
union all select 'C', 30
''';
"""


def test_execute_chart_statement():
    results = list(_executor().execute_text(_SETUP + """
            chart
              layer bar ( x_axis <- category, y_axis <- value );
            """))
    # SETUP has 3 declaration statements + chart; execute_text only yields
    # result-bearing statements (chart).
    chart_results = [r for r in results if isinstance(r, ChartResult)]
    assert len(chart_results) == 1
    result = chart_results[0]
    layer = result.statement.layers[0]
    assert layer.layer_type == ChartType.BAR
    assert layer.x_fields == ["category"]
    assert layer.y_fields == ["value"]
    assert len(result.data[0]) == 3


def test_execute_line_chart():
    results = list(_executor().execute_text(_SETUP + """
            chart
              layer line ( x_axis <- category, y_axis <- value );
            """))
    chart_results = [r for r in results if isinstance(r, ChartResult)]
    assert chart_results[0].statement.layers[0].layer_type == ChartType.LINE


def test_execute_chart_with_color():
    results = list(_executor().execute_text("""
            key category string;
            property category.value int;
            property category.region string;

            datasource chart_data (
                cat: category,
                val: value,
                reg: region
            )
            grain (category)
            query '''
            select 'A' as cat, 10 as val, 'north' as reg
            union all select 'B', 20, 'south'
            ''';

            chart
              layer bar (
                x_axis <- category,
                y_axis <- value,
                color <- region
              );
            """))
    chart_results = [r for r in results if isinstance(r, ChartResult)]
    assert chart_results[0].statement.layers[0].color_field is not None


def test_execute_multi_layer_chart():
    results = list(_executor().execute_text(_SETUP + """
            chart
              layer bar ( x_axis <- category, y_axis <- value )
              layer line ( x_axis <- category, y_axis <- value );
            """))
    chart_results = [r for r in results if isinstance(r, ChartResult)]
    result = chart_results[0]
    assert [layer.layer_type for layer in result.statement.layers] == [
        ChartType.BAR,
        ChartType.LINE,
    ]
    assert len(result.data) == 2


def test_execute_chart_with_computed_alias():
    results = list(_executor().execute_text(_SETUP + """
            chart
              layer bar (
                x_axis <- category,
                y_axis <- value,
                color <- value * 2 as scaled_value
              );
            """))
    chart_results = [r for r in results if isinstance(r, ChartResult)]
    layer = chart_results[0].statement.layers[0]
    assert layer.color_field == "scaled_value"


def test_chart_result_protocol():
    from trilogy.core.statements.execute import ProcessedChartStatement

    statement = ProcessedChartStatement(layers=[])
    result = ChartResult(
        chart="mock_chart", data=[[{"x": 1, "y": 2}]], statement=statement
    )

    assert result.keys() == ["chart"]

    fetched = result.fetchone()
    assert fetched["chart"] == "mock_chart"

    fetched_all = result.fetchall()
    assert len(fetched_all) == 1
    assert fetched_all[0]["chart"] == "mock_chart"

    fetched_many = result.fetchmany(10)
    assert len(fetched_many) == 1

    for row in result:
        assert row["chart"] == "mock_chart"


def test_bad_alias_in_layer_raises():
    with pytest.raises(Exception, match="Unknown chart role"):
        list(_executor().execute_text(_SETUP + """
                chart
                  layer bar ( x_axis <- category, bogus <- value );
                """))


def test_copy_chart_format_mismatch_raises():
    with pytest.raises(Exception, match="chart format"):
        list(_executor().execute_text(_SETUP + """
                copy into csv 'out.csv' from chart
                  layer bar ( x_axis <- category, y_axis <- value );
                """))


def test_copy_select_format_mismatch_raises():
    with pytest.raises(Exception, match="chart source"):
        list(_executor().execute_text("""
                copy into png 'out.png' from
                  select "A" as label, 10 as value;
                """))


def test_copy_chart_processes_to_chart_copy_statement(tmp_path):
    from trilogy.core.statements.execute import ProcessedChartCopyStatement

    exec_ = _executor()
    parsed = list(exec_.parse_text_generator(_SETUP + f"""
            copy into png '{(tmp_path / "chart.png").as_posix()}' from chart
              layer bar ( x_axis <- category, y_axis <- value );
            """))
    stmts = [s for s in parsed if isinstance(s, ProcessedChartCopyStatement)]
    assert len(stmts) == 1
    stmt = stmts[0]
    assert stmt.target_type.value == "png"
    assert len(stmt.chart.layers) == 1


def test_copy_chart_with_size_options_parses(tmp_path):
    from trilogy.core.statements.execute import ProcessedChartCopyStatement

    exec_ = _executor()
    parsed = list(exec_.parse_text_generator(_SETUP + f"""
            copy into png '{(tmp_path / "chart.png").as_posix()}'
              (width=800, height=600, scale=2)
              from chart layer bar ( x_axis <- category, y_axis <- value );
            """))
    stmts = [s for s in parsed if isinstance(s, ProcessedChartCopyStatement)]
    assert len(stmts) == 1
    assert stmts[0].options == {"width": 800, "height": 600, "scale": 2}


def test_execute_chart_with_aggregate_binding_order_limit():
    results = list(_executor().execute_text("""
            key carrier_code string;
            property carrier_code.name string;
            key id int;

            datasource flights (
                id: id,
                c: carrier_code,
                cn: name
            )
            grain (id)
            query '''
            select 1 as id, 'AA' as c, 'American' as cn
            union all select 2, 'AA', 'American'
            union all select 3, 'DL', 'Delta'
            union all select 4, 'UA', 'United'
            ''';

            chart
              layer barh (
                y_axis <- name,
                x_axis <- count(id) as flight_count
              ) order by flight_count desc limit 2;
            """))
    chart_results = [r for r in results if isinstance(r, ChartResult)]
    assert len(chart_results[0].data[0]) == 2
    assert chart_results[0].data[0][0]["flight_count"] == 2


def test_copy_chart_rejects_unknown_option(tmp_path):
    exec_ = _executor()
    with pytest.raises(Exception, match="Unknown copy option"):
        list(exec_.execute_text(_SETUP + f"""
                copy into png '{(tmp_path / "chart.png").as_posix()}'
                  (dpi=300) from chart
                  layer bar ( x_axis <- category, y_axis <- value );
                """))


def test_copy_chart_accepts_scale_and_ppi_options(tmp_path):
    from trilogy.executor import _chart_copy_options

    size, save = _chart_copy_options(
        {"width": 400, "height": 300, "scale": 2, "ppi": 150}
    )
    assert size == {"width": 400, "height": 300}
    assert save == {"scale_factor": 2, "ppi": 150}


def test_chart_copy_options_empty():
    from trilogy.executor import _chart_copy_options

    assert _chart_copy_options({}) == ({}, {})


def test_copy_chart_writes_png(tmp_path):
    pytest.importorskip("vl_convert")
    out = tmp_path / "chart.png"
    list(_executor().execute_text(_SETUP + f"""
            copy into png '{out.as_posix()}'
              (width=400, height=300, scale=2)
              from chart
              layer bar ( x_axis <- category, y_axis <- value );
            """))
    assert out.exists()
    assert out.stat().st_size > 0


def test_copy_chart_requires_altair(tmp_path, monkeypatch):
    import trilogy.rendering.altair_renderer as ar

    monkeypatch.setattr(ar, "ALTAIR_AVAILABLE", False)
    exec_ = _executor()
    with pytest.raises(RuntimeError, match="requires altair"):
        list(exec_.execute_text(_SETUP + f"""
                copy into png '{(tmp_path / "chart.png").as_posix()}' from chart
                  layer bar ( x_axis <- category, y_axis <- value );
                """))


def test_execute_chart_when_altair_unavailable(monkeypatch):
    import trilogy.rendering.altair_renderer as ar

    monkeypatch.setattr(ar, "ALTAIR_AVAILABLE", False)
    results = list(_executor().execute_text(_SETUP + """
            chart
              layer bar ( x_axis <- category, y_axis <- value );
            """))
    chart_results = [r for r in results if isinstance(r, ChartResult)]
    assert chart_results[0].chart is None
