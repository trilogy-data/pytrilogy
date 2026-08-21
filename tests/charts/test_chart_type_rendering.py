"""Chart types beyond the x/y marks: donut, heatmap and boxplot, plus the
refusal that names what IS supported. These build off the real executor because
the builders read declared datatypes off the layer's query, not pandas dtypes."""

import pytest

pytest.importorskip("altair")

from trilogy import Executor
from trilogy.dialect.enums import Dialects
from trilogy.dialect.results import ChartResult

_SETUP = """
key id int;
property id.category string;
property id.region string;
property id.value int;
property id.note string;

datasource chart_data (
    id: id,
    cat: category,
    reg: region,
    val: value,
    note: note
)
grain (id)
query '''
select 1 as id, 'A' as cat, 'east' as reg, 10 as val, 'low' as note
union all select 2, 'A', 'west', 20, 'mid'
union all select 3, 'B', 'east', 30, 'high'
union all select 4, 'B', 'west', 40, 'top'
''';
"""


def _chart(text: str):
    ex = Executor(dialect=Dialects.DUCK_DB, engine=Dialects.DUCK_DB.default_engine())
    results = [r for r in ex.execute_text(_SETUP + text) if isinstance(r, ChartResult)]
    return results[0].chart


def test_donut_sizes_slices_by_the_numeric_binding():
    spec = _chart(
        "chart layer donut ( x_axis <- category, y_axis <- sum(value) as total );"
    ).to_dict()
    assert spec["mark"]["type"] == "arc"
    assert spec["mark"]["innerRadius"] > 0
    assert spec["encoding"]["theta"]["field"] == "total"
    assert spec["encoding"]["color"]["field"] == "category"
    assert spec["encoding"]["order"]["sort"] == "descending"


def test_donut_accepts_the_color_shaped_spelling():
    spec = _chart(
        "chart layer donut ( x_axis <- sum(value) as total, color <- category );"
    ).to_dict()
    assert spec["encoding"]["theta"]["field"] == "total"
    assert spec["encoding"]["color"]["field"] == "category"


def test_donut_without_a_numeric_binding_raises():
    with pytest.raises(ValueError, match="numeric x_axis or y_axis"):
        _chart("chart layer donut ( x_axis <- category, y_axis <- note );")


def test_donut_without_a_category_raises():
    with pytest.raises(ValueError, match="name the slices"):
        _chart("chart layer donut ( y_axis <- sum(value) as total );")


def test_donut_rejects_annotation():
    with pytest.raises(ValueError, match="'annotation' role is not supported"):
        _chart(
            "chart layer donut ( x_axis <- category, y_axis <- sum(value) as total,"
            " annotation <- note );"
        )


def test_heatmap_bands_both_axes_and_colors_by_the_measure():
    spec = _chart(
        "chart layer heatmap ( x_axis <- region, y_axis <- category,"
        " color <- sum(value) as total );"
    ).to_dict()
    assert spec["mark"]["type"] == "rect"
    assert spec["encoding"]["x"]["type"] == "nominal"
    assert spec["encoding"]["y"]["type"] == "nominal"
    assert spec["encoding"]["color"]["field"] == "total"


def test_heatmap_bands_a_numeric_axis_as_ordinal():
    spec = _chart(
        "chart layer heatmap ( x_axis <- id, y_axis <- category,"
        " color <- sum(value) as total );"
    ).to_dict()
    assert spec["encoding"]["x"]["type"] == "ordinal"


def test_heatmap_without_color_raises():
    with pytest.raises(ValueError, match="needs a `color` binding"):
        _chart("chart layer heatmap ( x_axis <- region, y_axis <- category );")


def test_heatmap_annotation_labels_cells():
    spec = _chart(
        "chart layer heatmap ( x_axis <- region, y_axis <- category,"
        " color <- sum(value) as total, annotation <- count(id) as rows );"
    ).to_dict()
    assert [layer["mark"]["type"] for layer in spec["layer"]] == ["rect", "text"]


def test_boxplot_summarizes_the_value_axis_per_category():
    spec = _chart(
        "chart layer boxplot ( x_axis <- category, y_axis <- value )"
        " from select --id, category, value;"
    ).to_dict()
    assert spec["mark"]["type"] == "boxplot"
    assert spec["encoding"]["x"]["type"] == "nominal"
    assert spec["encoding"]["y"]["type"] == "quantitative"


def test_boxplot_rejects_annotation():
    with pytest.raises(ValueError, match="'annotation' role is not supported"):
        _chart(
            "chart layer boxplot ( x_axis <- category, y_axis <- value,"
            " annotation <- note );"
        )


def test_unimplemented_type_names_the_supported_set():
    with pytest.raises(NotImplementedError) as err:
        _chart(
            "chart layer treemap ( x_axis <- category, y_axis <- sum(value) as total );"
        )
    message = str(err.value)
    assert "treemap" in message
    for supported in ("bar", "barh", "line", "point", "area", "headline"):
        assert supported in message


def test_donut_hex_trait_colors_the_slices():
    spec = _chart(
        "import std.color;\n"
        "chart layer donut ( x_axis <- total, color <- category )"
        " from select category, sum(value) as total,"
        " case when category = 'A' then '#ff0000' else '#00ff00' end::hex as shade;"
    ).to_dict()
    assert spec["encoding"]["color"]["scale"] == {
        "domain": ["A", "B"],
        "range": ["#ff0000", "#00ff00"],
    }
