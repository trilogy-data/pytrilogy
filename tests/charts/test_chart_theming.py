import pytest

pytest.importorskip("altair")

from trilogy import Executor
from trilogy.dialect.enums import Dialects
from trilogy.dialect.results import ChartResult
from trilogy.rendering.chart_theme import theme_chart
from trilogy.rendering.theme import (
    DEFAULT_THEME,
    INTER_DARK_THEME,
    INTER_THEME,
    Appearance,
    Theme,
    default_theme,
    get_theme,
    register_theme,
)

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
''';
"""


def _executor(**kwargs) -> Executor:
    return Executor(
        dialect=Dialects.DUCK_DB, engine=Dialects.DUCK_DB.default_engine(), **kwargs
    )


def _chart(executor: Executor):
    results = list(
        executor.execute_text(
            _SETUP + "chart layer bar ( x_axis <- category, y_axis <- value );"
        )
    )
    return next(r for r in results if isinstance(r, ChartResult)).chart


def test_dark_themes_registered_with_counterparts():
    for light, dark in (("inter", "inter-dark"), ("editorial", "editorial-dark")):
        light_theme = get_theme(light)
        dark_theme = get_theme(dark)
        assert light_theme.appearance == Appearance.LIGHT
        assert dark_theme.appearance == Appearance.DARK
        assert light_theme.counterpart == dark
        assert dark_theme.counterpart == light


def test_default_theme_by_appearance():
    assert default_theme() is DEFAULT_THEME
    assert default_theme(Appearance.DARK) is INTER_DARK_THEME


def test_register_theme():
    from dataclasses import replace

    custom = replace(INTER_THEME, name="custom-test", counterpart=None)
    register_theme(custom)
    try:
        assert get_theme("custom-test") is custom
    finally:
        from trilogy.rendering.theme import THEMES

        THEMES.pop("custom-test")


def test_theme_chart_applies_palette_and_chrome():
    chart = _chart(_executor())
    spec = theme_chart(chart, INTER_THEME).to_dict()
    assert spec["background"] == "transparent"
    assert spec["config"]["range"]["category"] == list(INTER_THEME.chart_palette)
    assert spec["config"]["axis"]["gridColor"] == "rgba(0,0,0,0.05)"

    dark_spec = theme_chart(chart, INTER_DARK_THEME).to_dict()
    assert dark_spec["config"]["range"]["category"] == list(
        INTER_DARK_THEME.chart_palette
    )
    assert dark_spec["config"]["axis"]["gridColor"] == "rgba(255,255,255,0.08)"
    assert dark_spec["config"]["axis"]["labelColor"] == INTER_DARK_THEME.text_muted


def test_theme_chart_leaves_sizing_alone():
    chart = _chart(_executor())
    spec = theme_chart(chart, INTER_THEME).to_dict()
    assert "width" not in spec
    assert "height" not in spec


def test_copy_chart_is_themed(tmp_path):
    target = (tmp_path / "chart.svg").as_posix()
    list(_executor().execute_text(_SETUP + f"""copy into svg '{target}' from chart
              layer bar ( x_axis <- category, y_axis <- value );"""))
    svg = (tmp_path / "chart.svg").read_text(encoding="utf-8")
    assert DEFAULT_THEME.chart_palette[0].lower() in svg.lower()


def test_copy_chart_theme_and_background_options(tmp_path):
    target = (tmp_path / "chart.svg").as_posix()
    list(
        _executor().execute_text(
            _SETUP
            + f"""copy into svg '{target}' (theme='inter-dark', background='#161514') from chart
              layer bar ( x_axis <- category, y_axis <- value );"""
        )
    )
    svg = (tmp_path / "chart.svg").read_text(encoding="utf-8")
    assert INTER_DARK_THEME.chart_palette[0].lower() in svg.lower()
    assert "#161514" in svg


def test_copy_chart_uses_executor_default_theme(tmp_path):
    target = (tmp_path / "chart.svg").as_posix()
    list(
        _executor(chart_theme="inter-dark").execute_text(
            _SETUP + f"""copy into svg '{target}' from chart
              layer bar ( x_axis <- category, y_axis <- value );"""
        )
    )
    svg = (tmp_path / "chart.svg").read_text(encoding="utf-8")
    assert INTER_DARK_THEME.chart_palette[0].lower() in svg.lower()


def test_copy_chart_unknown_theme_raises(tmp_path):
    target = (tmp_path / "chart.svg").as_posix()
    with pytest.raises(Exception, match="Unknown theme"):
        list(
            _executor().execute_text(
                _SETUP + f"""copy into svg '{target}' (theme='bogus') from chart
                  layer bar ( x_axis <- category, y_axis <- value );"""
            )
        )


def test_bar_category_axis_is_banded():
    # the category axis encodes as ordinal so bars fill their band at any
    # density, instead of Vega's fixed 5px slivers on continuous scales
    chart = _chart(_executor())
    spec = chart.to_dict()
    assert spec["mark"] == {"type": "bar"}
    assert spec["encoding"]["x"]["type"] == "nominal"


def test_style_chart_composes_theme_and_layout():
    from trilogy.rendering.theme import REPORT_LAYOUT
    from trilogy.report.charts import style_chart

    chart = _chart(_executor())
    spec = style_chart(chart, INTER_THEME, REPORT_LAYOUT, interactive=False).to_dict()
    assert spec["background"] == "transparent"
    assert spec["config"]["range"]["category"] == list(INTER_THEME.chart_palette)
    width, height = REPORT_LAYOUT.chart_box(1)
    assert spec["width"] == width
    assert spec["height"] == height


def test_theme_type_annotation():
    assert isinstance(get_theme("inter"), Theme)


def test_headline_text_uses_theme_colors():
    setup = _SETUP + "chart layer headline ( x_axis <- sum(value) as total );"
    results = list(_executor(chart_theme="inter-dark").execute_text(setup))
    chart = next(r for r in results if isinstance(r, ChartResult)).chart
    spec = chart.to_dict()
    colors = {layer["mark"]["color"] for layer in spec["layer"]}
    assert colors == {
        INTER_DARK_THEME.text_primary,
        INTER_DARK_THEME.text_muted,
    }

    light = list(_executor().execute_text(setup))
    light_spec = next(r for r in light if isinstance(r, ChartResult)).chart.to_dict()
    light_colors = {layer["mark"]["color"] for layer in light_spec["layer"]}
    assert light_colors == {INTER_THEME.text_primary, INTER_THEME.text_muted}


def test_run_document_pins_executor_chart_theme(tmp_path):
    from trilogy.report.runner import run_document

    executor = _executor(chart_theme="inter")
    run_document([], working_path=tmp_path, executor=executor, chart_theme="inter-dark")
    assert executor.chart_theme == "inter-dark"


def test_solo_area_is_visible():
    # a bare opacity=0.15 area (the old theme value) read as empty with no
    # line layer on top; the theme now pairs a translucent fill with a
    # full-opacity boundary line
    results = list(
        _executor().execute_text(
            _SETUP + "chart layer area ( x_axis <- category, y_axis <- value );"
        )
    )
    chart = next(r for r in results if isinstance(r, ChartResult)).chart
    spec = theme_chart(chart, INTER_THEME).to_dict()
    assert spec["config"]["area"] == {
        "fillOpacity": 0.35,
        "line": {"strokeWidth": 2.5},
    }
