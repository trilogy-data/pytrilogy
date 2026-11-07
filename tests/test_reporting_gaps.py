import json
from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.core import NumericType
from trilogy.dialect.results import ChartResult
from trilogy.parsing.render import Renderer
from trilogy.rendering.rich_types import is_numeric

PREAMBLE = """
key id int;
property id.ip string;
property id.route string;
property id.status int;

datasource reqs (id, ip, route, status)
grain (id)
query '''
select 1 id, 'a' ip, '/x' route, 200 status union all
select 2, 'a', '/y', 404 union all
select 3, 'b', '/x', 200 union all
select 4, 'b', '/x', 500
''';
"""


@pytest.mark.parametrize("binding", ["as", "->"])
def test_alias_rebound_by_later_select(binding: str):
    executor = Dialects.DUCK_DB.default_executor()
    results = executor.execute_text(
        PREAMBLE
        + "with per_route as select route, count(id) -> route_count;\n"
        + f"select per_route.route, per_route.route_count {binding} requests"
        " order by per_route.route asc;\n"
        + "select ip, count(id) -> requests order by ip asc;"
    )
    assert results[-1].fetchall() == [("a", 2), ("b", 2)]


def test_chart_alias_rebound_by_later_select():
    executor = Dialects.DUCK_DB.default_executor()
    results = executor.execute_text(
        PREAMBLE
        + "with per_route as select route, count(id) -> route_count;\n"
        + "chart layer bar (x_axis <- per_route.route,"
        " y_axis <- per_route.route_count as requests);\n"
        + "select ip, count(id) -> requests order by ip asc;"
    )
    assert results[-1].fetchall() == [("a", 2), ("b", 2)]


def _chart_spec(executor, text: str) -> dict:
    results = executor.execute_text(PREAMBLE + text)
    chart = [r for r in results if isinstance(r, ChartResult)][-1]
    return chart.chart.to_dict()


def test_one_point_line_draws_its_point():
    pytest.importorskip("altair")
    spec = _chart_spec(
        Dialects.DUCK_DB.default_executor(),
        "chart layer line (x_axis <- status, y_axis <- n)"
        " from select status, count(id) -> n where status = 200;",
    )
    assert spec["mark"] == {"type": "line", "point": True}


def test_multi_point_line_keeps_plain_mark():
    pytest.importorskip("altair")
    spec = _chart_spec(
        Dialects.DUCK_DB.default_executor(),
        "chart layer line (x_axis <- status, y_axis <- n)"
        " from select status, count(id) -> n;",
    )
    assert spec["mark"] == {"type": "line", "point": False}


def test_line_with_a_lone_series_point():
    pytest.importorskip("altair")
    spec = _chart_spec(
        Dialects.DUCK_DB.default_executor(),
        "chart layer line (x_axis <- status, y_axis <- n, color <- ip)"
        " from select ip, status, count(id) -> n where ip = 'b' or status = 200;",
    )
    assert spec["mark"]["point"] is True


def test_empty_chart_renders(tmp_path: Path):
    pytest.importorskip("vl_convert")
    target = tmp_path / "empty.png"
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(
        PREAMBLE + f"copy into png '{target}' from chart"
        " layer line (x_axis <- route, y_axis <- n, color <- ip)"
        " from select route, ip, count(id) -> n where status = 999;"
    )
    assert target.exists()


def test_empty_frame_is_typed():
    pytest.importorskip("altair")
    spec = _chart_spec(
        Dialects.DUCK_DB.default_executor(),
        "chart layer bar (x_axis <- route, y_axis <- n)"
        " from select route, count(id) -> n where status = 999;",
    )
    assert spec["encoding"]["x"]["type"] == "nominal"
    assert spec["encoding"]["y"]["type"] == "quantitative"


def test_numeric_with_precision_is_numeric():
    assert is_numeric(NumericType(precision=12, scale=2))


def test_binding_title():
    pytest.importorskip("altair")
    spec = _chart_spec(
        Dialects.DUCK_DB.default_executor(),
        "chart layer bar (x_axis <- route title 'Path',"
        " y_axis <- n as request_count title 'Requests (n)')"
        " from select route, count(id) -> n;",
    )
    assert spec["encoding"]["x"]["title"] == "Path"
    assert spec["encoding"]["y"]["title"] == "Requests (n)"


def test_binding_title_round_trip():
    executor = Dialects.DUCK_DB.default_executor()
    _, parsed = executor.environment.parse(
        PREAMBLE + "chart layer bar (x_axis <- route as r title 'Path', y_axis <- id);"
    )
    rendered = Renderer().to_string(parsed[-1])
    assert "x_axis <- route as r title 'Path'" in rendered


def _sqlite_copy(tmp_path: Path, fmt: str, where: str = "") -> Path:
    target = tmp_path / f"out.{fmt}"
    executor = Dialects.SQLITE.default_executor()
    executor.execute_text(
        PREAMBLE + f"copy into {fmt} '{target}' from select ip, count(id) -> n"
        f" {where} order by ip asc;"
    )
    return target


def test_client_side_copy_csv(tmp_path: Path):
    assert _sqlite_copy(tmp_path, "csv").read_text() == "ip,n\na,2\nb,2\n"


def test_client_side_copy_empty_csv_keeps_header(tmp_path: Path):
    target = _sqlite_copy(tmp_path, "csv", "where status = 999")
    assert target.read_text() == "ip,n\n"


def test_client_side_copy_json(tmp_path: Path):
    target = _sqlite_copy(tmp_path, "json")
    assert json.loads(target.read_text()) == [
        {"ip": "a", "n": 2},
        {"ip": "b", "n": 2},
    ]


def test_client_side_copy_parquet(tmp_path: Path):
    import pyarrow.parquet as pq

    target = _sqlite_copy(tmp_path, "parquet")
    assert pq.read_table(target).to_pylist() == [
        {"ip": "a", "n": 2},
        {"ip": "b", "n": 2},
    ]


def test_client_side_copy_rejects_remote(tmp_path: Path):
    executor = Dialects.SQLITE.default_executor()
    with pytest.raises(NotImplementedError, match="remote target"):
        executor.execute_text(
            PREAMBLE + "copy into csv 's3://bucket/out.csv' from select ip;"
        )
