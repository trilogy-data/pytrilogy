import datetime
import json
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

from trilogy import Dialects, Environment
from trilogy.core.enums import ChartType, IOType
from trilogy.core.models.core import DataType, NumericType
from trilogy.core.models.datasource import Address
from trilogy.core.statements.execute import (
    ProcessedChartLayer,
    ProcessedChartStatement,
    ProcessedCopyStatement,
)
from trilogy.dialect.results import ChartResult
from trilogy.execution.client_copy import write_rows
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


EMPTY_ROWSET = "rowset bad <- select ip, count(id) -> bad_n having bad_n > 99;\n"


@pytest.mark.parametrize(
    "select, expected",
    [
        ("count(id) -> total, array_agg(bad.ip) -> bad_ips", [(4, None)]),
        ("count(id) -> total, count(bad.ip) -> n", [(4, 0)]),
        ("count(bad.ip) -> n", []),
    ],
)
def test_grand_totals_union_over_an_empty_rowset(select: str, expected: list):
    executor = Dialects.DUCK_DB.default_executor()
    results = executor.execute_text(PREAMBLE + EMPTY_ROWSET + f"select {select};")
    assert results[-1].fetchall() == expected


def test_call_over_empty_rowset(tmp_path: Path):
    out_file = tmp_path / "args.txt"
    script = tmp_path / "echo_args.py"
    script.write_text(
        "import sys, pathlib\n"
        f"pathlib.Path(r'{out_file}').write_text(' '.join(sys.argv[1:]))\n",
        newline="\n",
    )
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    executor.execute_text(
        PREAMBLE
        + EMPTY_ROWSET
        + "call `./echo_args.py` from select count(id) -> total,"
        " count(bad.ip) -> bad_count, array_agg(bad.ip) -> bad_ips;"
    )
    assert out_file.read_text() == "--total 4 --bad_count 0"


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


def _sqlite_copy_statement(tmp_path: Path, fmt: str = "csv"):
    executor = Dialects.SQLITE.default_executor()
    statement = executor.parse_text(
        PREAMBLE + f"copy into {fmt} '{tmp_path / ('out.' + fmt)}' from select ip;"
    )[-1]
    assert isinstance(statement, ProcessedCopyStatement)
    return executor, statement


def test_client_side_copy_runs_verify_on_the_staged_file(tmp_path: Path):
    executor, statement = _sqlite_copy_statement(tmp_path)
    seen: list[str] = []
    executor._execute_copy(statement, verify=seen.append)
    assert len(seen) == 1
    assert (tmp_path / "out.csv").read_text() == "ip\na\nb\n"


def test_client_side_copy_rejects_append(tmp_path: Path):
    executor, statement = _sqlite_copy_statement(tmp_path)
    statement.append_to = Address(location=str(tmp_path / "out.csv"))
    with pytest.raises(NotImplementedError, match="Appending"):
        executor._execute_copy(statement, verify=None)


def test_write_rows_serializes_rich_values(tmp_path: Path):
    target = tmp_path / "out.json"
    row = [
        Decimal("1.5"),
        datetime.date(2026, 1, 2),
        datetime.time(3, 4),
        b"\x01",
        uuid.UUID(int=1),
    ]
    write_rows(str(target), IOType.JSON, ["d", "day", "t", "b", "u"], [row])
    assert json.loads(target.read_text()) == [
        {
            "d": 1.5,
            "day": "2026-01-02",
            "t": "03:04:00",
            "b": "01",
            "u": str(uuid.UUID(int=1)),
        }
    ]


def test_write_rows_csv_lowercases_booleans(tmp_path: Path):
    target = tmp_path / "out.csv"
    write_rows(str(target), IOType.CSV, ["flag", "n"], [[True, 1], [False, None]])
    assert target.read_text() == "flag,n\ntrue,1\nfalse,\n"


def test_write_rows_rejects_image_types(tmp_path: Path):
    with pytest.raises(NotImplementedError):
        write_rows(str(tmp_path / "out.png"), IOType.PNG, [], [])


@pytest.mark.parametrize(
    "datatype, dtype",
    [
        (DataType.DATE, "datetime64[ns]"),
        (DataType.BOOL, "bool"),
        (DataType.STRING, "string"),
        (DataType.INTEGER, "float64"),
    ],
)
def test_empty_frame_dtypes(datatype: DataType, dtype: str):
    pytest.importorskip("altair")
    from trilogy.rendering.altair_renderer import AltairRenderer

    assert AltairRenderer()._empty_dtype(datatype) == dtype


def test_untyped_field_without_data_is_nominal():
    pytest.importorskip("altair")
    from trilogy.rendering.altair_renderer import AltairRenderer

    layer = ProcessedChartLayer(
        layer_type=ChartType.BAR, x_fields=["x"], y_fields=["y"]
    )
    spec = AltairRenderer().render(ProcessedChartStatement(layers=[layer]), [[]])
    encoding = spec.to_dict()["encoding"]
    assert encoding["x"]["type"] == "nominal"
    assert encoding["y"]["type"] == "nominal"
