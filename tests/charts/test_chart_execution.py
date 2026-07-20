import pytest

from trilogy import Executor
from trilogy.core.enums import ChartType
from trilogy.dialect.enums import Dialects
from trilogy.dialect.results import ChartResult


def _executor() -> Executor:
    return Executor(dialect=Dialects.DUCK_DB, engine=Dialects.DUCK_DB.default_engine())


def _hooked_executor(hook):
    return Executor(
        dialect=Dialects.DUCK_DB,
        engine=Dialects.DUCK_DB.default_engine(),
        hooks=[hook],
    )


class _RecordingHook:
    def __init__(self):
        self.selects = []

    def process_select_info(self, select):
        self.selects.append(select)

    def process_multiselect_info(self, select):
        pass

    def process_persist_info(self, persist):
        pass

    def process_rowset_info(self, rowset):
        pass

    def process_root_datasource(self, datasource):
        pass

    def process_root_cte(self, cte):
        pass

    def process_root_strategy_node(self, node):
        pass


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

    size, save, theme, background = _chart_copy_options(
        {"width": 400, "height": 300, "scale": 2, "ppi": 150, "theme": "inter-dark"}
    )
    assert size == {"width": 400, "height": 300}
    assert save == {"scale_factor": 2, "ppi": 150}
    assert theme == "inter-dark"
    assert background is None


def test_chart_copy_options_empty():
    from trilogy.executor import _chart_copy_options

    assert _chart_copy_options({}) == ({}, {}, None, None)


def test_chart_statement_invokes_select_hook():
    hook = _RecordingHook()
    list(_hooked_executor(hook).execute_text(_SETUP + """
            chart
              layer bar ( x_axis <- category, y_axis <- value );
            """))
    assert len(hook.selects) >= 1


def test_copy_chart_invokes_select_hook_per_layer(tmp_path):
    hook = _RecordingHook()
    list(_hooked_executor(hook).parse_text_generator(_SETUP + f"""
            copy into png '{(tmp_path / "chart.png").as_posix()}' from chart
              layer bar ( x_axis <- category, y_axis <- value )
              layer line ( x_axis <- category, y_axis <- value );
            """))
    assert len(hook.selects) == 2


def test_copy_select_invokes_select_hook(tmp_path):
    hook = _RecordingHook()
    list(_hooked_executor(hook).parse_text_generator(_SETUP + f"""
            copy into csv '{(tmp_path / "out.csv").as_posix()}' from
              select category, value;
            """))
    assert len(hook.selects) >= 1


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


_DATED_SETUP = """
key id int;
property id.create_time datetime;
property id.score int;
property id.domain string;

datasource events (
    id: id,
    create_time: create_time,
    score: score,
    domain: domain
)
grain (id)
query '''
select 1 as id, cast('2024-01-01 05:00:00' as timestamp) as create_time, 10 as score, 'zzz.com' as domain
union all select 2, cast('2024-01-01 07:00:00' as timestamp), 30, 'aaa.com'
union all select 3, cast('2024-01-02 05:00:00' as timestamp), 20, 'mmm.com'
''';
"""


def test_chart_axis_bound_to_dotted_property_concept():
    # `create_time.hour` binds by the concept's safe address, which must match
    # the query's rendered column alias (create_time_hour, not
    # local_create_time_hour) for Altair to resolve the field.
    results = list(_executor().execute_text(_DATED_SETUP + """
            chart
              layer bar ( x_axis <- create_time.hour, y_axis <- score );
            """))
    chart_results = [r for r in results if isinstance(r, ChartResult)]
    result = chart_results[0]
    layer = result.statement.layers[0]
    assert layer.x_fields == ["create_time_hour"]
    assert set(result.data[0][0].keys()) == {"create_time_hour", "score"}
    spec = result.chart.to_dict()
    assert spec["encoding"]["x"]["field"] == "create_time_hour"
    # a bar's category axis is banded (ordinal) so bars fill their step
    assert spec["encoding"]["x"]["type"] == "ordinal"
    # without an ORDER BY, categories sort ascending for deterministic output
    assert spec["encoding"]["x"]["sort"] == "ascending"


def test_barh_category_axis_preserves_query_order():
    results = list(_executor().execute_text(_DATED_SETUP + """
            chart
              layer barh ( x_axis <- score, y_axis <- domain )
            from
            select domain, score
            order by score desc;
            """))
    chart_results = [r for r in results if isinstance(r, ChartResult)]
    result = chart_results[0]
    assert [row["domain"] for row in result.data[0]] == [
        "aaa.com",
        "mmm.com",
        "zzz.com",
    ]
    spec = result.chart.to_dict()
    # sort: null keeps the query's ORDER BY as the bar order instead of
    # Vega's alphabetical default; the value axis is left untouched
    assert spec["encoding"]["y"]["sort"] is None
    assert "sort" not in spec["encoding"]["x"]


def test_bar_temporal_category_axis_banded_with_time_labels():
    results = list(_executor().execute_text(_DATED_SETUP + """
            chart
              layer bar ( x_axis <- create_time.date, y_axis <- score );
            """))
    chart_results = [r for r in results if isinstance(r, ChartResult)]
    spec = chart_results[0].chart.to_dict()
    enc = spec["encoding"]["x"]
    assert enc["type"] == "ordinal"
    assert "timeFormat" in enc["axis"]["labelExpr"]
    # daily granularity -> month + day labels
    assert "%b %d" in enc["axis"]["labelExpr"]


def test_line_chart_keeps_temporal_axis():
    results = list(_executor().execute_text(_DATED_SETUP + """
            chart
              layer line ( x_axis <- create_time, y_axis <- score );
            """))
    chart_results = [r for r in results if isinstance(r, ChartResult)]
    spec = chart_results[0].chart.to_dict()
    assert spec["encoding"]["x"]["type"] == "temporal"
    assert "sort" not in spec["encoding"]["x"]


def test_date_columns_serialize():
    # DuckDB DATE comes back as datetime.date (object dtype); the renderer
    # must coerce it so the chart spec is JSON-serializable
    import json

    results = list(_executor().execute_text(_DATED_SETUP + """
            chart
              layer bar ( x_axis <- create_time.date, y_axis <- score );
            """))
    chart_results = [r for r in results if isinstance(r, ChartResult)]
    json.dumps(chart_results[0].chart.to_dict())


def test_scale_setting_applies_to_value_axis():
    results = list(_executor().execute_text(_SETUP + """
            chart
              set scale_y: log
              layer bar ( x_axis <- category, y_axis <- value );
            """))
    chart_results = [r for r in results if isinstance(r, ChartResult)]
    spec = chart_results[0].chart.to_dict()
    assert spec["encoding"]["y"]["scale"] == {"type": "log"}
    # log is meaningless on the banded category axis; it stays unscaled
    assert "scale" not in spec["encoding"]["x"]


def test_scale_setting_applies_in_copy_chart(tmp_path):
    target = (tmp_path / "chart.svg").as_posix()
    list(_executor().execute_text(_SETUP + f"""
            copy into svg '{target}' from chart
              set scale_y: log
              layer point ( x_axis <- value, y_axis <- value * 2 as scaled );
            """))
    svg = (tmp_path / "chart.svg").read_text(encoding="utf-8")
    assert len(svg) > 0


def test_multi_layer_shared_alias_binding():
    # layering area + line over the same aliased axis concept must resolve in
    # every declaration shape (regression: reported as Undefined concept on
    # the alias)
    shapes = [
        """chart
          layer area ( x_axis <- create_time.hour::int as hr, y_axis <- sum(score) as n )
          layer line ( x_axis <- hr, y_axis <- n );
        """,
        """chart
          layer area ( x_axis <- hr, y_axis <- n )
          from select create_time.hour::int as hr, sum(score) as n
          layer line ( x_axis <- hr, y_axis <- n );
        """,
        """chart
          layer area ( x_axis <- hr, y_axis <- n )
          from select create_time.hour::int as hr, sum(score) as n
          layer line ( x_axis <- hr, y_axis <- n )
          from select create_time.hour::int as hr, sum(score) as n;
        """,
    ]
    for shape in shapes:
        results = list(_executor().execute_text(_DATED_SETUP + shape))
        chart_results = [r for r in results if isinstance(r, ChartResult)]
        assert chart_results[0].chart is not None, shape
        assert len(chart_results[0].data) == 2, shape


_HEX_SETUP = """
import std.color;
key category string;
property category.value int;
property category.color_code string::hex;

datasource chart_data (
    cat: category,
    val: value,
    hex: color_code
)
grain (category)
query '''
select 'B' as cat, 20 as val, '#00FF00' as hex
union all select 'A', 10, '#FF0000'
union all select 'C', 30, cast(null as varchar)
''';
"""


def test_hex_trait_column_drives_color_scale():
    results = list(_executor().execute_text(_HEX_SETUP + """
            chart
              layer bar ( x_axis <- category, y_axis <- value, color <- category )
              from select category, value, color_code;
            """))
    chart_results = [r for r in results if isinstance(r, ChartResult)]
    spec = chart_results[0].chart.to_dict()
    scale = spec["encoding"]["color"]["scale"]
    assert scale["domain"] == ["A", "B", "C"]
    assert scale["range"] == ["#FF0000", "#00FF00", "#999999"]


def test_color_without_hex_column_keeps_default_scale():
    results = list(_executor().execute_text(_HEX_SETUP + """
            chart
              layer bar ( x_axis <- category, y_axis <- value, color <- category )
              from select category, value;
            """))
    chart_results = [r for r in results if isinstance(r, ChartResult)]
    spec = chart_results[0].chart.to_dict()
    assert "scale" not in spec["encoding"]["color"]


def test_hex_field_bound_directly_as_color():
    results = list(_executor().execute_text(_HEX_SETUP + """
            chart
              layer point ( x_axis <- category, y_axis <- value, color <- color_code );
            """))
    chart_results = [r for r in results if isinstance(r, ChartResult)]
    spec = chart_results[0].chart.to_dict()
    scale = spec["encoding"]["color"]["scale"]
    assert scale["domain"] == scale["range"] == ["#00FF00", "#FF0000"]


def test_same_concept_bound_to_multiple_roles():
    results = list(_executor().execute_text(_SETUP + """
            chart
              layer bar ( x_axis <- category, y_axis <- value, color <- category );
            """))
    chart_results = [r for r in results if isinstance(r, ChartResult)]
    layer = chart_results[0].statement.layers[0]
    assert layer.x_fields == ["category"]
    assert layer.color_field == "category"
    assert len(chart_results[0].data[0]) == 3
