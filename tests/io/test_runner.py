import json
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from trilogy.io import emit, run, source
from trilogy.io.errors import ERROR_PREFIX, SCRIPT_ERROR_EXIT_CODE
from trilogy.io.runner import parse_args, stamp

ROWS = [{"i": i, "state": "CA" if i % 2 else "NY"} for i in range(10)]


def rows():
    return ROWS


def pushes(limit=None):
    return ROWS[: limit or len(ROWS)]


def boom():
    raise ValueError("script logic failed")


def flaky():
    raise ConnectionError("transient")


@pytest.fixture
def out(tmp_path: Path) -> Path:
    return tmp_path / "out.bin"


def arrow_at(path: Path) -> pa.Table:
    with pa.ipc.open_stream(path.read_bytes()) as reader:
        return reader.read_all()


def test_parse_args_builds_a_request():
    invocation = parse_args(
        [
            "--limit",
            "5",
            "--columns",
            "i, state",
            "--filter",
            "state = CA",
            "--since",
            "2026-01-01",
            "--partition",
            "day=2026-01-01",
        ]
    )
    assert invocation.request.limit == 5
    assert invocation.request.columns == ("i", "state")
    assert invocation.request.filters[0].column == "state"
    assert invocation.request.since == "2026-01-01"
    assert invocation.request.partition == {"day": "2026-01-01"}


def test_parse_args_builds_an_ordering():
    request = parse_args(["--order-by", "state:desc, i"]).request
    assert [(s.column, s.descending) for s in request.order_by] == [
        ("state", True),
        ("i", False),
    ]


def test_a_malformed_partition_is_reported_as_a_contract_error(capsys):
    """The rust twin raises `ContractError` here; the reported type must match."""
    assert run(rows, argv=["--partition", "day"]) == SCRIPT_ERROR_EXIT_CODE
    line = next(
        line
        for line in capsys.readouterr().err.splitlines()
        if line.startswith(ERROR_PREFIX)
    )
    detail = json.loads(line[len(ERROR_PREFIX) :])
    assert detail["type"] == "ContractError"
    assert "--partition expects KEY=VALUE" in detail["message"]


def test_writes_arrow_to_stdout_by_default(capsysbinary):
    assert run(rows, argv=["--limit", "2"]) == 0
    with pa.ipc.open_stream(capsysbinary.readouterr().out) as reader:
        assert reader.read_all().num_rows == 2


def test_writes_arrow_to_a_target(out: Path):
    assert run(rows, argv=["--output", str(out)]) == 0
    assert arrow_at(out).num_rows == 10


def test_flags_shape_the_output(out: Path):
    run(rows, argv=["--output", str(out), "--limit", "2", "--columns", "i"])
    table = arrow_at(out)
    assert table.num_rows == 2
    assert table.column_names == ["i"]


def test_formats(out: Path, tmp_path: Path):
    run(rows, argv=["--output", str(out), "--format", "parquet"])
    assert pq.read_table(out).num_rows == 10

    csv_path = tmp_path / "o.csv"
    run(rows, argv=["--output", str(csv_path), "--format", "csv"])
    assert csv_path.read_text().splitlines()[0] == '"i","state"'

    json_path = tmp_path / "o.json"
    run(rows, argv=["--output", str(json_path), "--format", "json", "--limit", "1"])
    assert json.loads(json_path.read_text()) == {"i": 0, "state": "NY"}


def test_metadata_rides_on_the_schema(out: Path):
    run(pushes, argv=["--output", str(out)])
    metadata = arrow_at(out).schema.metadata
    assert metadata[b"trilogy.contract"] == b"1"
    assert metadata[b"trilogy.pushdown"] == b"limit"


def test_watermark_metadata(out: Path):
    run(rows, argv=["--output", str(out)], watermark="2026-08-06")
    assert arrow_at(out).schema.metadata[b"trilogy.watermark"] == b"2026-08-06"


def test_stamp_preserves_existing_schema_metadata():
    reader = pa.Table.from_pylist(ROWS).replace_schema_metadata({"a": "b"}).to_reader()
    stamped = stamp(reader, {"contract": "1"})
    assert stamped.schema.metadata[b"a"] == b"b"
    assert stamped.schema.metadata[b"trilogy.contract"] == b"1"


def test_stamp_with_nothing_to_say_is_a_noop():
    reader = pa.Table.from_pylist(ROWS).to_reader()
    assert stamp(reader, {"watermark": None}) is reader


def test_describe(capsys):
    assert run(pushes, argv=["--describe"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["contract"] == 1
    assert payload["pushdown"] == ["limit"]
    assert payload["schema"][0] == {"name": "i", "type": "bigint", "nullable": True}
    assert payload["datasource"].startswith("datasource ")


def test_errors_exit_65_with_a_machine_readable_line(capsys):
    assert run(boom, argv=[]) == SCRIPT_ERROR_EXIT_CODE
    err = capsys.readouterr().err
    line = next(line for line in err.splitlines() if line.startswith(ERROR_PREFIX))
    detail = json.loads(line[len(ERROR_PREFIX) :])
    assert detail["type"] == "ValueError"
    assert detail["retryable"] is False
    assert "Traceback" in err


def test_transient_errors_are_marked_retryable(capsys):
    run(flaky, argv=[])
    line = next(
        line
        for line in capsys.readouterr().err.splitlines()
        if line.startswith(ERROR_PREFIX)
    )
    assert json.loads(line[len(ERROR_PREFIX) :])["retryable"] is True


def test_bad_flags_do_not_report_as_a_script_error(capsys):
    assert run(rows, argv=["--limit", "not-a-number"]) == 2
    assert ERROR_PREFIX not in capsys.readouterr().err


def test_source_decorator_keeps_the_function_callable(out: Path):
    decorated = source(rows)
    assert decorated() == ROWS
    with pytest.raises(SystemExit) as exit_info:
        decorated.cli(["--output", str(out)])
    assert exit_info.value.code == 0
    assert arrow_at(out).num_rows == 10


def test_source_decorator_takes_arguments(out: Path):
    schema = pa.schema([("i", pa.int64())])

    @source(schema=schema, watermark="2026-08-06")
    def empty():
        return []

    with pytest.raises(SystemExit) as exit_info:
        empty.cli(["--output", str(out)])
    assert exit_info.value.code == 0
    table = arrow_at(out)
    assert table.num_rows == 0
    assert table.column_names == ["i"]
    assert table.schema.metadata[b"trilogy.watermark"] == b"2026-08-06"


def test_emit_still_works_and_now_honors_flags(out: Path):
    emit(rows, argv=["--output", str(out), "--limit", "4"])
    assert arrow_at(out).num_rows == 4


def test_emit_lets_errors_propagate():
    with pytest.raises(ValueError):
        emit(boom, argv=[])


SCRIPT = """
import sys
sys.path.insert(0, {root!r})
from trilogy.io import run

def rows(limit=None):
    return [{{"i": i}} for i in range(limit or 3)]

if __name__ == "__main__":
    raise SystemExit(run(rows))
"""


def test_end_to_end_as_a_subprocess(tmp_path: Path):
    script = tmp_path / "src.py"
    script.write_text(SCRIPT.format(root=str(Path(__file__).parents[2])))
    result = subprocess.run(
        [sys.executable, str(script), "--limit", "2"], capture_output=True, check=False
    )
    assert result.returncode == 0, result.stderr
    with pa.ipc.open_stream(result.stdout) as reader:
        assert reader.read_all().num_rows == 2
