"""Run outputs: ``::trilogy-output`` markers on a called program's stdout become
``output`` report records and a summary section (``trilogy.execution.outputs``)."""

import json
from contextlib import contextmanager
from pathlib import Path

import pytest
from click.testing import CliRunner

from tests.scripts.test_json_output import events_of, parse_events
from trilogy import Dialects, Environment
from trilogy.execution.outputs import (
    RunOutput,
    collected_outputs,
    parse_output_line,
    record_outputs,
    reset_outputs,
    scan_outputs,
)
from trilogy.execution.report import ReportSink, set_report_sink
from trilogy.scripts import display_core
from trilogy.scripts.display import show_run_outputs
from trilogy.scripts.trilogy import cli

PR_URL = "https://github.com/o/r/pull/45?tab=files&x=1"


@pytest.fixture(autouse=True)
def _clean_outputs():
    reset_outputs()
    yield
    reset_outputs()
    from trilogy.scripts import display_core

    display_core.set_output_format("rich")


#: CI runs the CLI suites twice, with and without ``rich`` installed, and the
#: two renderings are different code paths. Tests that check what a run printed
#: pin the mode explicitly and assert the rendering that mode produces, so one
#: run covers both.
rich_modes = pytest.mark.parametrize("rich", [True, False], ids=["rich", "plain"])


def assert_output_listed(
    printed: str, name: str, kind: str, value: str, rich: bool
) -> None:
    if rich:
        assert "Outputs" in printed
        assert name in printed and kind in printed
    else:
        assert f"Output {name} ({kind}): {value}" in printed


@contextmanager
def rich_mode(enabled: bool):
    if enabled and not display_core.RICH_AVAILABLE:
        pytest.skip("rich not installed")
    with display_core.set_rich_mode(enabled):
        yield


def test_parse_link_defaults_kind_and_keeps_value_verbatim():
    out = parse_output_line(f"::trilogy-output name=fix_pr value={PR_URL}")
    assert out == RunOutput("fix_pr", PR_URL, "link")


def test_parse_text_default_and_explicit_kind():
    assert parse_output_line("::trilogy-output name=note value=hello world") == (
        RunOutput("note", "hello world", "text")
    )
    assert parse_output_line(
        f"::trilogy-output kind=text name=raw value={PR_URL}"
    ) == RunOutput("raw", PR_URL, "text")


def test_parse_json_kind():
    out = parse_output_line('::trilogy-output name=stats kind=json value={"rows": 3}')
    assert out == RunOutput("stats", {"rows": 3}, "json")
    degraded = parse_output_line("::trilogy-output name=stats kind=json value={oops")
    assert degraded == RunOutput("stats", "{oops", "text")


@pytest.mark.parametrize(
    "line",
    [
        "plain stdout line",
        "::trilogy-output name=no_value",
        "::trilogy-output value=no name",
        "::trilogy-output name=1bad value=x",
        "::trilogy-output name=ok color=red value=x",
        "::trilogy-output name=ok kind=blob value=x",
    ],
)
def test_parse_rejects(line: str):
    assert parse_output_line(line) is None


def test_scan_outputs_keeps_order_and_source():
    text = (
        "starting\n"
        "::trilogy-output name=a value=1\n"
        "noise\n"
        "  ::trilogy-output name=b value=2  \n"
    )
    assert scan_outputs(text, source="./x.py") == [
        RunOutput("a", "1", "text", "./x.py"),
        RunOutput("b", "2", "text", "./x.py"),
    ]


def test_record_outputs_emits_report_records(tmp_path: Path):
    report = tmp_path / "report.jsonl"
    set_report_sink(ReportSink(report, "run-1", "run"))
    try:
        record_outputs([RunOutput("fix_pr", PR_URL, "link", "./repair.py")])
    finally:
        set_report_sink(None)
    records = [json.loads(line) for line in report.read_text().splitlines()]
    assert len(records) == 1
    assert records[0]["type"] == "output"
    assert records[0]["run_id"] == "run-1"
    assert {k: records[0][k] for k in ("name", "kind", "value", "source")} == {
        "name": "fix_pr",
        "kind": "link",
        "value": PR_URL,
        "source": "./repair.py",
    }
    assert collected_outputs() == [RunOutput("fix_pr", PR_URL, "link", "./repair.py")]


def _emit_script(tmp_path: Path, name: str, exit_code: int = 0) -> None:
    (tmp_path / name).write_text(
        "import sys\n"
        "print('working...')\n"
        f"print('::trilogy-output name=fix_pr value={PR_URL}')\n"
        f"sys.exit({exit_code})\n",
        newline="\n",
    )


def test_call_execution_records_outputs(tmp_path: Path):
    _emit_script(tmp_path, "emit.py")
    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    exec.execute_text("call `./emit.py`;")
    assert collected_outputs() == [RunOutput("fix_pr", PR_URL, "link", "./emit.py")]


def test_call_failure_keeps_outputs_it_declared(tmp_path: Path):
    _emit_script(tmp_path, "boom.py", exit_code=2)
    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    with pytest.raises(RuntimeError, match="exit 2"):
        exec.execute_text("call `./boom.py`;")
    assert collected_outputs() == [RunOutput("fix_pr", PR_URL, "link", "./boom.py")]


def _call_workspace(tmp_path: Path) -> Path:
    _emit_script(tmp_path, "emit.py")
    script = tmp_path / "job.preql"
    script.write_text("call `./emit.py`;\n", encoding="utf-8")
    return script


@rich_modes
def test_cli_run_reports_and_prints_outputs(tmp_path: Path, rich: bool):
    script = _call_workspace(tmp_path)
    report = tmp_path / "report.jsonl"
    with rich_mode(rich):
        result = CliRunner().invoke(
            cli, ["run", str(script), "duck_db", "--report-file", str(report)]
        )
    assert result.exit_code == 0, result.output
    records = [json.loads(line) for line in report.read_text().splitlines()]
    outputs = [r for r in records if r["type"] == "output"]
    assert [(o["name"], o["kind"], o["value"]) for o in outputs] == [
        ("fix_pr", "link", PR_URL)
    ]
    assert records[-1]["type"] == "summary"
    assert_output_listed(result.output, "fix_pr", "link", PR_URL, rich)


def test_cli_run_json_mode_emits_outputs_event(tmp_path: Path):
    script = _call_workspace(tmp_path)
    result = CliRunner().invoke(
        cli, ["run", str(script), "duck_db", "--format", "json"]
    )
    assert result.exit_code == 0, result.output
    events = events_of(parse_events(result.output), "outputs")
    assert len(events) == 1
    assert events[0]["outputs"] == [
        {"name": "fix_pr", "kind": "link", "value": PR_URL, "source": "./emit.py"}
    ]


@rich_modes
def test_show_run_outputs_renders_in_both_modes(rich: bool, capsys):
    with rich_mode(rich):
        show_run_outputs([RunOutput("fix_pr", PR_URL, "link", "./emit.py")])
    assert_output_listed(capsys.readouterr().out, "fix_pr", "link", PR_URL, rich)


@rich_modes
def test_show_run_outputs_says_nothing_when_there_are_none(rich: bool, capsys):
    with rich_mode(rich):
        show_run_outputs([])
    assert capsys.readouterr().out == ""


@rich_modes
def test_cli_run_directory_prints_outputs(tmp_path: Path, rich: bool):
    _call_workspace(tmp_path)
    with rich_mode(rich):
        result = CliRunner().invoke(cli, ["run", str(tmp_path), "duck_db"])
    assert result.exit_code == 0, result.output
    assert_output_listed(result.output, "fix_pr", "link", PR_URL, rich)
