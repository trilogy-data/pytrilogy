"""Tests for the JSONL execution report (--report-file / --run-id).

The report file is the machine-facing execution contract
(``trilogy/execution/report.py``): strict JSONL — one JSON object per line —
with a fixed envelope (``ts``/``type``/``schema_version``/``run_id``/``seq``),
a ``run_start`` opener and a guaranteed terminal ``summary`` record. These
tests pin that contract for orchestrator consumers.
"""

import json
from pathlib import Path

import duckdb
import pytest
from click.testing import CliRunner

from tests.scripts.test_json_output import events_of, parse_events
from trilogy.scripts.trilogy import cli

ENVELOPE_KEYS = {"ts", "type", "schema_version", "run_id", "seq"}


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture(autouse=True)
def _reset_output_format():
    """Each ``--format json`` invocation flips the process-global output format;
    restore ``rich`` after every test so we don't leak JSON mode into unrelated
    tests that assume the default rendering."""
    from trilogy.scripts import display_core

    yield
    display_core.set_output_format("rich")


def read_report(path: Path) -> list[dict]:
    """Read a JSONL report: every line must independently json.loads-parse."""
    assert path.exists(), f"report file {path} was never written"
    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        assert line.strip(), "report contains a blank line"
        records.append(json.loads(line))
    return records


def records_of(records: list[dict], record_type: str) -> list[dict]:
    return [r for r in records if r.get("type") == record_type]


def _file_name(record: dict) -> str:
    """Basename of a record's file attribution, robust to Windows backslashes."""
    return Path(record["file"]).name


# ---------------------------------------------------------------------------
# Workspace builders
# ---------------------------------------------------------------------------

RUN_A = """key uid int;

datasource users (
    uid
)
grain (uid)
query '''
select 1 as uid
union all
select 2 as uid
''';

select uid;
"""

RUN_B = """import a;

select count(uid) -> user_count;
"""

FAIL_A = """key fid int;

datasource broken (
    fid
)
grain (fid)
query '''
select not_a_real_column as fid
''';

select fid;
"""

SKIP_B = """import fail_a;

select fid;
"""

OK_C = """select 2 -> standalone;
"""


def _run_workspace(tmp_path: Path) -> None:
    (tmp_path / "a.preql").write_text(RUN_A, encoding="utf-8")
    (tmp_path / "b.preql").write_text(RUN_B, encoding="utf-8")


def _failing_workspace(tmp_path: Path) -> None:
    (tmp_path / "fail_a.preql").write_text(FAIL_A, encoding="utf-8")
    (tmp_path / "skip_b.preql").write_text(SKIP_B, encoding="utf-8")
    (tmp_path / "ok_c.preql").write_text(OK_C, encoding="utf-8")


SOURCE_PREQL = """key ev_id int;
property ev_id.ev_ts datetime;

root datasource src_events (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
file `{src}`;
"""

TARGET_PREQL = """import source;

datasource target_events (
    ev_id: ev_id,
    ev_ts: ev_ts
)
grain (ev_id)
file `{target}`
incremental by ev_ts;
"""


def _refresh_workspace(tmp_path: Path) -> None:
    """Directory workspace with file-backed (parquet) datasources: a root
    source plus a derived incremental persist — the managed-asset shape the
    refresh directory tests use. Parquet targets survive across CLI
    invocations, so a second refresh can observe the first one's write."""
    src = (tmp_path / "src_events.parquet").as_posix()
    target = (tmp_path / "target_events.parquet").as_posix()
    con = duckdb.connect()
    con.execute(f"""COPY (
            SELECT 1 AS ev_id, TIMESTAMP '2024-01-10 12:00:00' AS ev_ts
            UNION ALL
            SELECT 2, TIMESTAMP '2024-01-15 12:00:00'
        ) TO '{src}' (FORMAT PARQUET)""")
    con.close()
    (tmp_path / "source.preql").write_text(
        SOURCE_PREQL.format(src=src), encoding="utf-8"
    )
    (tmp_path / "base.preql").write_text(
        TARGET_PREQL.format(target=target), encoding="utf-8"
    )


REFRESH_SINGLE = """key item_id int;
property item_id.updated_at datetime;

root datasource source_items (
    item_id: item_id,
    updated_at: updated_at
)
grain (item_id)
query '''
SELECT 1 as item_id, TIMESTAMP '2024-01-10 12:00:00' as updated_at
''';

datasource target_items (
    item_id: item_id,
    updated_at: updated_at
)
grain (item_id)
address target_items_table
incremental by updated_at;
"""


# ---------------------------------------------------------------------------
# Contract shape
# ---------------------------------------------------------------------------


def test_report_is_true_jsonl(runner, tmp_path):
    report = tmp_path / "report.jsonl"
    result = runner.invoke(
        cli,
        ["run", "select 1 -> num;", "duck_db", "--report-file", str(report)],
    )
    assert result.exit_code == 0, result.output

    # read_report json.loads's each line independently — true JSONL.
    records = read_report(report)
    assert records, "report is empty"
    for record in records:
        assert ENVELOPE_KEYS <= set(record), record
        assert record["schema_version"] == 1
    assert records[0]["type"] == "run_start"
    assert records[-1]["type"] == "summary"
    # seq is a monotonic per-process ordering key.
    assert [r["seq"] for r in records] == sorted(r["seq"] for r in records)
    # One invocation, one correlation id.
    assert len({r["run_id"] for r in records}) == 1


def test_run_directory_per_file_and_statement_records(runner, tmp_path):
    _run_workspace(tmp_path)
    report = tmp_path / "report.jsonl"
    result = runner.invoke(
        cli, ["run", str(tmp_path), "duckdb", "--report-file", str(report)]
    )
    assert result.exit_code == 0, result.output

    records = read_report(report)
    starts = records_of(records, "file_start")
    ends = records_of(records, "file_end")
    assert {_file_name(r) for r in starts} == {"a.preql", "b.preql"}
    assert {_file_name(r) for r in ends} == {"a.preql", "b.preql"}
    for end in ends:
        assert end["success"] is True
        assert isinstance(end["duration_s"], (int, float))
        assert end["node_kind"] == "script"

    statements = records_of(records, "statement_end")
    assert statements, "directory mode must emit statement_end records"
    # Directory-mode statement records always carry file attribution.
    assert all("file" in s for s in statements)
    assert {_file_name(s) for s in statements} == {"a.preql", "b.preql"}
    for s in statements:
        assert s["success"] is True

    summary = records_of(records, "summary")[-1]
    assert summary["success"] is True
    assert summary["total"] == 2
    assert summary["succeeded"] == 2
    assert summary["failed"] == 0


def test_failure_and_skip(runner, tmp_path):
    _failing_workspace(tmp_path)
    report = tmp_path / "report.jsonl"
    result = runner.invoke(
        cli, ["run", str(tmp_path), "duckdb", "--report-file", str(report)]
    )
    assert result.exit_code == 1, result.output

    records = read_report(report)
    ends = {_file_name(r): r for r in records_of(records, "file_end")}

    failed = ends["fail_a.preql"]
    assert failed["success"] is False
    assert failed.get("skipped") is not True
    assert failed["error_type"]

    skipped = ends["skip_b.preql"]
    assert skipped["success"] is False
    assert skipped["skipped"] is True

    assert ends["ok_c.preql"]["success"] is True

    summary = records_of(records, "summary")[-1]
    assert summary["success"] is False
    assert summary["exit_code"] == 1
    assert summary["total"] == 3
    assert summary["succeeded"] == 1
    # failed counts real failures only; dependency-skips count as skipped.
    assert summary["failed"] == 1
    assert summary["skipped"] == 1
    assert summary["partial_failure"] is True


def test_run_id_precedence(runner, tmp_path, monkeypatch):
    query = "select 1 -> num;"

    # Flag beats env.
    monkeypatch.setenv("TRILOGY_RUN_ID", "from-env")
    report = tmp_path / "flag.jsonl"
    result = runner.invoke(
        cli,
        [
            "run",
            query,
            "duck_db",
            "--report-file",
            str(report),
            "--run-id",
            "from-flag",
        ],
    )
    assert result.exit_code == 0, result.output
    assert {r["run_id"] for r in read_report(report)} == {"from-flag"}

    # Env beats generated.
    report = tmp_path / "env.jsonl"
    result = runner.invoke(cli, ["run", query, "duck_db", "--report-file", str(report)])
    assert result.exit_code == 0, result.output
    assert {r["run_id"] for r in read_report(report)} == {"from-env"}

    # Neither: a generated uuid4 hex (32 hex chars).
    monkeypatch.delenv("TRILOGY_RUN_ID")
    report = tmp_path / "generated.jsonl"
    result = runner.invoke(cli, ["run", query, "duck_db", "--report-file", str(report)])
    assert result.exit_code == 0, result.output
    run_ids = {r["run_id"] for r in read_report(report)}
    assert len(run_ids) == 1
    generated = run_ids.pop()
    assert len(generated) == 32
    int(generated, 16)  # raises if not hex


def test_report_with_json_display_mode(runner, tmp_path):
    """--format json owns stdout; the report file is an independent channel."""
    report = tmp_path / "report.jsonl"
    result = runner.invoke(
        cli,
        [
            "--format",
            "json",
            "run",
            "select 1 -> num;",
            "duck_db",
            "--report-file",
            str(report),
        ],
    )
    assert result.exit_code == 0, result.output

    # stdout still parses as the pretty-printed event stream.
    events = parse_events(result.output)
    assert events_of(events, "result")[0]["rows"] == [[1]]

    # The report file is unaffected: strict JSONL with the same guarantees.
    records = read_report(report)
    assert records[0]["type"] == "run_start"
    assert records[-1]["type"] == "summary"
    assert records[-1]["success"] is True


def test_refresh_report_records(runner, tmp_path):
    _refresh_workspace(tmp_path)

    first = tmp_path / "first.jsonl"
    result = runner.invoke(
        cli, ["refresh", str(tmp_path), "duckdb", "--report-file", str(first)]
    )
    assert result.exit_code == 0, result.output

    records = read_report(first)
    plans = records_of(records, "refresh_plan")
    assert plans, "refresh must emit a refresh_plan record"
    plan = plans[0]
    assert plan["stale_count"] >= 1
    addresses = {a["address"] for a in plan["assets"]}
    assert any(a.endswith("target_events.parquet") for a in addresses)
    for asset in plan["assets"]:
        assert asset["datasource_id"]
        assert asset["reason"]
        assert asset["kind"]

    refreshes = records_of(records, "asset_refresh")
    assert any(r["datasource_id"] == "target_events" for r in refreshes)
    summary = records_of(records, "summary")[-1]
    assert summary["success"] is True
    assert summary.get("refreshed_assets", 0) >= 1

    # Second refresh: everything is up to date — a successful no-op (exit 2).
    second = tmp_path / "second.jsonl"
    result = runner.invoke(
        cli, ["refresh", str(tmp_path), "duckdb", "--report-file", str(second)]
    )
    assert result.exit_code == 2, result.output

    records = read_report(second)
    plan = records_of(records, "refresh_plan")[0]
    assert plan["stale_count"] == 0
    assert "assets" not in plan  # None fields are dropped from the envelope
    summary = records_of(records, "summary")[-1]
    assert summary["success"] is True
    assert summary["exit_code"] == 2
    assert not records_of(records, "asset_refresh")


VALIDATION_FAIL = """select not_a_declared_concept;
"""


def _validation_workspace(tmp_path: Path) -> None:
    (tmp_path / "a.preql").write_text(RUN_A, encoding="utf-8")
    (tmp_path / "bad.preql").write_text(VALIDATION_FAIL, encoding="utf-8")


@pytest.mark.parametrize("command", ["unit", "integration"])
def test_validation_report_records(runner, tmp_path, command):
    """`unit`/`integration` are the platform's validation commands: each file's
    outcome must reach the report as a file_end, with a terminal summary."""
    _validation_workspace(tmp_path)
    report = tmp_path / "report.jsonl"
    args = [command, str(tmp_path)]
    if command == "integration":
        args.append("duckdb")
    result = runner.invoke(
        cli, [*args, "--report-file", str(report), "--run-id", "rid"]
    )
    assert result.exit_code == 1, result.output

    records = read_report(report)
    _assert_vocabulary(records)
    assert {r["run_id"] for r in records} == {"rid"}
    assert records[0]["type"] == "run_start"
    assert records[0]["command"] == command

    ends = {_file_name(r): r for r in records_of(records, "file_end")}
    assert ends["a.preql"]["success"] is True
    assert ends["bad.preql"]["success"] is False
    assert ends["bad.preql"]["error"]

    summary = records[-1]
    assert summary["type"] == "summary"
    assert summary["success"] is False
    assert summary["total"] == 2
    assert summary["succeeded"] == 1
    assert summary["failed"] == 1


@pytest.mark.parametrize("command", ["unit", "integration"])
def test_validation_report_env_fallback_and_environment_flag(
    runner, tmp_path, command, monkeypatch
):
    """TRILOGY_REPORT_FILE activates the sink with no flag, and --environment
    is accepted (the cloud worker passes it unconditionally)."""
    (tmp_path / "a.preql").write_text(RUN_A, encoding="utf-8")
    report = tmp_path / "env.jsonl"
    monkeypatch.setenv("TRILOGY_REPORT_FILE", str(report))
    args = [command, str(tmp_path)]
    if command == "integration":
        args.append("duckdb")
    result = runner.invoke(cli, [*args, "--environment", "branch1"])
    assert result.exit_code == 0, result.output

    records = read_report(report)
    _assert_vocabulary(records)
    assert records[0]["type"] == "run_start"
    assert records[-1]["type"] == "summary"
    assert records[-1]["success"] is True


@pytest.mark.parametrize("command", ["unit", "integration"])
def test_validation_summary_fallback_on_missing_input(runner, tmp_path, command):
    """Validation dies before the file loop more than any other command; the
    fallback summary is what tells the consumer the run failed."""
    report = tmp_path / "report.jsonl"
    args = [command, str(tmp_path / "missing")]
    if command == "integration":
        args.append("duckdb")
    result = runner.invoke(cli, [*args, "--report-file", str(report)])
    assert result.exit_code != 0

    records = read_report(report)
    assert records[0]["type"] == "run_start"
    assert records[-1]["type"] == "summary"
    assert records[-1]["success"] is False
    assert isinstance(records[-1]["exit_code"], int)


def test_report_file_parent_directory_is_created(runner, tmp_path):
    """The orchestrator's path (`.trilogy/exec.jsonl`) names a directory it
    expects the run to create."""
    report = tmp_path / ".trilogy" / "nested" / "exec.jsonl"
    result = runner.invoke(
        cli, ["run", "select 1 -> num;", "duck_db", "--report-file", str(report)]
    )
    assert result.exit_code == 0, result.output
    assert read_report(report)[-1]["type"] == "summary"


def test_summary_fallback_on_config_error(runner, tmp_path):
    """A pathlike input that doesn't exist dies before the file loop (exit 2);
    the report must still end with a terminal failure summary (fallback path)."""
    report = tmp_path / "report.jsonl"
    missing = tmp_path / "missing.preql"
    result = runner.invoke(
        cli, ["run", str(missing), "duckdb", "--report-file", str(report)]
    )
    assert result.exit_code == 2, result.output

    records = read_report(report)
    assert records[0]["type"] == "run_start"
    summary = records[-1]
    assert summary["type"] == "summary"
    assert summary["success"] is False
    assert summary["exit_code"] == 2


# ---------------------------------------------------------------------------
# Forward-compat vocabulary pins
# ---------------------------------------------------------------------------

# Required fields per record type (beyond the envelope). Consumers may rely on
# these; new fields/types can be added freely, but these must not disappear
# without a schema_version bump.
REQUIRED_FIELDS: dict[str, set[str]] = {
    "run_start": {"command", "trilogy_version", "target"},
    "file_start": {"node_kind"},
    "file_end": {"success", "node_kind"},
    "statement_end": {"index", "total", "statement_type", "duration_s", "success"},
    "refresh_plan": {"stale_count", "forced_count", "all_assets"},
    "asset_refresh": {"datasource_id", "reason"},
    "asset_refresh_query": {"datasource_id", "sql_bytes"},
    "plan_graph": {"nodes", "edges"},
    "state_snapshot": set(),
    "error": {"error_type"},
    "summary": {"success"},
}


def _assert_vocabulary(records: list[dict]) -> None:
    for record in records:
        assert ENVELOPE_KEYS <= set(record), record
        required = REQUIRED_FIELDS.get(record["type"])
        assert required is not None, f"unpinned record type: {record['type']}"
        assert required <= set(record), record
        if record["type"] in ("file_start", "file_end"):
            # Attribution: file for scripts, address for managed refresh nodes.
            assert "file" in record or "address" in record, record
        if record["type"] == "summary" and record["success"] is False:
            assert isinstance(record["exit_code"], int), record


def test_record_vocabulary_pins(runner, tmp_path):
    reports: list[Path] = []

    # Success run (inline / single-file path).
    r1 = tmp_path / "r1.jsonl"
    result = runner.invoke(
        cli, ["run", "select 1 -> num;", "duck_db", "--report-file", str(r1)]
    )
    assert result.exit_code == 0, result.output
    reports.append(r1)

    # Failing run (statement_end failure + failure summary).
    r2 = tmp_path / "r2.jsonl"
    result = runner.invoke(
        cli, ["run", "select notacolumn;", "duck_db", "--report-file", str(r2)]
    )
    assert result.exit_code != 0
    reports.append(r2)

    # Single-file refresh, dry-run: refresh_plan + asset_refresh +
    # asset_refresh_query.
    script = tmp_path / "refreshable.preql"
    script.write_text(REFRESH_SINGLE, encoding="utf-8")
    r3 = tmp_path / "r3.jsonl"
    result = runner.invoke(
        cli,
        ["refresh", str(script), "duckdb", "--dry-run", "--report-file", str(r3)],
    )
    assert result.exit_code == 0, result.output
    reports.append(r3)

    # Plan: plan_graph.
    plan_target = tmp_path / "planme.preql"
    plan_target.write_text("select 1 -> x;\n", encoding="utf-8")
    r4 = tmp_path / "r4.jsonl"
    result = runner.invoke(cli, ["plan", str(plan_target), "--report-file", str(r4)])
    assert result.exit_code == 0, result.output
    reports.append(r4)

    # State: state_snapshot.
    r5 = tmp_path / "r5.jsonl"
    snap = tmp_path / "snap.json"
    result = runner.invoke(
        cli,
        [
            "state",
            str(script),
            "duckdb",
            "--output",
            str(snap),
            "--report-file",
            str(r5),
        ],
    )
    assert result.exit_code == 0, result.output
    reports.append(r5)

    seen: set[str] = set()
    for report in reports:
        records = read_report(report)
        _assert_vocabulary(records)
        seen.update(r["type"] for r in records)

    # The scenarios above must have exercised the core vocabulary.
    assert {
        "run_start",
        "file_start",
        "file_end",
        "statement_end",
        "refresh_plan",
        "asset_refresh",
        "asset_refresh_query",
        "plan_graph",
        "state_snapshot",
        "summary",
    } <= seen


def test_run_dry_run_is_stamped_on_the_report(tmp_path: Path):
    """A report consumer must be able to tell an invocation that wrote nothing
    on purpose from one that did the work — both report success."""
    script = tmp_path / "q.preql"
    script.write_text("select 1 -> one;")
    report = tmp_path / "report.jsonl"

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["run", str(script), "duckdb", "--dry-run", "--report-file", str(report)],
    )
    assert result.exit_code == 0, result.output
    start = json.loads(report.read_text().splitlines()[0])
    assert start["type"] == "run_start"
    assert start["dry_run"] is True


def test_run_without_dry_run_omits_the_flag(tmp_path: Path):
    script = tmp_path / "q.preql"
    script.write_text("select 1 -> one;")
    report = tmp_path / "report.jsonl"

    runner = CliRunner()
    runner.invoke(cli, ["run", str(script), "duckdb", "--report-file", str(report)])
    start = json.loads(report.read_text().splitlines()[0])
    assert "dry_run" not in start


def test_run_dry_run_json_mode_emits_events_not_raw_sql(tmp_path: Path):
    """The SQL a dry run prints would corrupt the NDJSON stream if written to
    stdout, so JSON mode carries it as one event per statement instead."""
    script = tmp_path / "q.preql"
    script.write_text("select 1 -> one;")

    runner = CliRunner()
    result = runner.invoke(
        cli, ["--format", "json", "run", str(script), "duckdb", "--dry-run"]
    )
    assert result.exit_code == 0, result.output

    # Raises on any non-JSON text, which is the regression being pinned.
    events = parse_events(result.output)
    compiled = events_of(events, "compiled_query")
    assert len(compiled) == 1
    assert "SELECT" in compiled[0]["sql"].upper()

    summary = events_of(events, "summary")[-1]
    assert summary["dry_run"] is True
    assert summary["statements"] == 1
