"""Every parsed chart role/setting must reach the rendered Vega spec — no
silent drops (the scale_x/scale_y class of bug). Covers group, trellis,
annotation, show_title, placement labels, and the loud geo refusal."""

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


def test_group_role_offsets_bars():
    spec = _chart(
        "chart layer bar ( x_axis <- category, y_axis <- sum(value) as total,"
        " group <- region );"
    ).to_dict()
    assert spec["encoding"]["xOffset"]["field"] == "region"


def test_group_role_offsets_barh_vertically():
    spec = _chart(
        "chart layer barh ( x_axis <- sum(value) as total, y_axis <- category,"
        " group <- region );"
    ).to_dict()
    assert spec["encoding"]["yOffset"]["field"] == "region"


def test_group_role_is_detail_on_lines():
    spec = _chart(
        "chart layer line ( x_axis <- category, y_axis <- sum(value) as total,"
        " group <- region );"
    ).to_dict()
    assert spec["encoding"]["detail"]["field"] == "region"


def test_trellis_roles_facet():
    spec = _chart(
        "chart layer bar ( x_axis <- category, y_axis <- sum(value) as total,"
        " x_trellis <- region );"
    ).to_dict()
    assert spec["encoding"]["column"]["field"] == "region"

    spec = _chart(
        "chart layer bar ( x_axis <- category, y_axis <- sum(value) as total,"
        " y_trellis <- region );"
    ).to_dict()
    assert spec["encoding"]["row"]["field"] == "region"


def test_trellis_rejects_layered_forms():
    with pytest.raises(Exception, match="Trellis"):
        _chart(
            "chart layer bar ( x_axis <- category, y_axis <- sum(value) as total,"
            " x_trellis <- region ) place hline at 5;"
        )


def test_annotation_role_overlays_text():
    spec = _chart(
        "chart layer bar ( x_axis <- category, y_axis <- sum(value) as total,"
        " annotation <- note );"
    ).to_dict()
    assert "layer" in spec
    marks = [layer["mark"]["type"] for layer in spec["layer"]]
    assert marks == ["bar", "text"]
    assert spec["layer"][1]["encoding"]["text"]["field"] == "note"


def test_geo_role_raises():
    with pytest.raises(NotImplementedError, match="geo"):
        _chart(
            "chart layer point ( x_axis <- category, y_axis <- sum(value) as total,"
            " geo <- region );"
        )


def test_show_title_sets_title():
    spec = _chart(
        "chart set show_title"
        " layer bar ( x_axis <- category, y_axis <- sum(value) as total );"
    ).to_dict()
    assert spec["title"] == "Total"


def test_no_title_by_default():
    spec = _chart(
        "chart layer bar ( x_axis <- category, y_axis <- sum(value) as total );"
    ).to_dict()
    assert "title" not in spec


def test_placement_label_renders_text():
    spec = _chart(
        "chart layer bar ( x_axis <- category, y_axis <- sum(value) as total )"
        " place hline at 25 as target;"
    ).to_dict()
    texts = [
        layer
        for layer in spec["layer"]
        if isinstance(layer.get("mark"), dict) and layer["mark"]["type"] == "text"
    ]
    assert len(texts) == 1
    assert texts[0]["encoding"]["text"]["value"] == "target"


def test_placement_without_label_has_no_text():
    spec = _chart(
        "chart layer bar ( x_axis <- category, y_axis <- sum(value) as total )"
        " place hline at 25;"
    ).to_dict()
    marks = [
        layer["mark"]["type"] if isinstance(layer["mark"], dict) else layer["mark"]
        for layer in spec["layer"]
    ]
    assert "text" not in marks
