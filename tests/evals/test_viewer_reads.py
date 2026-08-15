"""Unit tests for the viewer's lazy per-run reads and the cross-run grid.

These are what the Debug page is built on: the question index (report.json
only), one question's trajectory (one log), and the matrix that encodes a whole
results dir as one status character per cell. Synthetic run dirs keep them
fast and independent of whatever is sitting in evals/*/results."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "evals"))

from common.spec import BenchmarkSpec
from viewer import matrix, runs
from viewer.suites import Suite

_LOG = [
    {"type": "session_start", "command": "answer q", "model": "m", "provider": "p"},
    {
        "type": "llm_response",
        "text": "thinking",
        "tool_calls": [{"name": "trilogy", "arguments": {"args": ["file", "write"]}}],
        "usage": {"prompt_tokens": 100, "completion_tokens": 10, "total_tokens": 110},
    },
    {"type": "tool_result", "name": "trilogy", "result": "exit_code: 0"},
]


def write_log(run_dir: Path, key: str) -> None:
    (run_dir / f"agent_log.{key}.jsonl").write_text(
        "\n".join(json.dumps(e) for e in _LOG), encoding="utf-8"
    )


def make_spec(tmp_path: Path) -> BenchmarkSpec:
    return BenchmarkSpec(
        name="Fake",
        short_name="fake",
        duckdb_extension="",
        generator_sql="",
        db_filename="fake.duckdb",
        eval_dir=tmp_path,
        prompts_file=tmp_path / "query_prompts.json",
    )


def make_run(root: Path, name: str, report: dict | None, logs=(), repeat=False) -> Path:
    run_dir = root / name
    run_dir.mkdir(parents=True)
    if report is not None:
        filename = "repeat_report.json" if repeat else "report.json"
        (run_dir / filename).write_text(json.dumps(report), encoding="utf-8")
    for key in logs:
        write_log(run_dir, key)
    return run_dir


def eval_report(rows: list[tuple[int, str, str]], metrics=True) -> dict:
    """rows: (id, status, source)."""
    return {
        "meta": {
            "category": "enriched",
            "model": "m",
            "provider": "p",
            "scale_factor": 1,
        },
        "summary": {"pass_count": sum(1 for r in rows if r[1] == "pass")},
        "queries": [
            {"id": i, "status": s, "detail": "", "source": src} for i, s, src in rows
        ],
        "per_query": [{"id": i, "duration_seconds": 12.5} for i, _, _ in rows],
        "per_query_metrics": (
            [{"id": i, "iterations": 7, "prompt_tokens": 1234} for i, _, _ in rows]
            if metrics
            else []
        ),
    }


# ---------------------------------------------------------------- run index


def test_index_reads_report_without_parsing_logs(tmp_path):
    spec = make_spec(tmp_path)
    run = make_run(
        tmp_path / "results",
        "20260101-000000_enriched",
        eval_report([(1, "pass", "this_run"), (2, "fail", "this_run")]),
        logs=["q01", "q02"],
    )
    index = runs.run_index(run, spec)
    assert index["category"] == "enriched"
    assert index["summary"] == {"passed": 1, "total": 2, "prompt_tokens": 2468}
    first = index["questions"][0]
    assert (first["key"], first["status"], first["iterations"]) == ("q01", "pass", 7)
    assert first["has_log"] and first["stamp"]
    assert index["replayable"] is False  # no workspace/ dir


def test_index_marks_spliced_questions_that_have_no_log_here(tmp_path):
    spec = make_spec(tmp_path)
    run = make_run(
        tmp_path / "results",
        "20260101-000000_enriched",
        eval_report([(1, "pass", "this_run"), (2, "pass", "20251231-000000_enriched")]),
        logs=["q01"],
    )
    spliced = runs.run_index(run, spec)["questions"][1]
    assert spliced["source"] == "20251231-000000_enriched"
    assert spliced["has_log"] is False and spliced["key"] is None


def test_index_of_a_live_run_derives_metrics_from_the_logs(tmp_path):
    spec = make_spec(tmp_path)
    run = make_run(
        tmp_path / "results", "20260101-000000_enriched", None, logs=["q01", "q02"]
    )
    index = runs.run_index(run, spec)
    assert [q["status"] for q in index["questions"]] == ["running", "running"]
    assert index["questions"][0]["prompt_tokens"] == 100  # parsed back out of the log


def test_index_of_a_repeat_run_is_keyed_by_rep(tmp_path):
    spec = make_spec(tmp_path)
    report = {
        "meta": {"query_id": 96, "mode": "ingest", "model": "m"},
        "runs": [
            {"rep": 0, "status": "pass", "iterations": 6, "prompt_tokens": 88},
            {"rep": 1, "status": "fail", "iterations": 9, "prompt_tokens": 99},
        ],
    }
    run = make_run(
        tmp_path / "results",
        "repeat_q96",
        report,
        logs=["q96.r0", "q96.r1"],
        repeat=True,
    )
    index = runs.run_index(run, spec)
    assert index["kind"] == "repeat"
    assert [(q["rep"], q["status"], q["qid"]) for q in index["questions"]] == [
        (0, "pass", 96),
        (1, "fail", 96),
    ]


# ---------------------------------------------------------------- trajectory


def test_trajectory_parses_one_log(tmp_path):
    run = make_run(
        tmp_path / "results", "r", eval_report([(1, "pass", "this_run")]), logs=["q01"]
    )
    traj = runs.trajectory(run, "q01")
    assert traj["meta"]["model"] == "m"
    assert [e["role"] for e in traj["timeline"]] == ["assistant", "tool"]
    assert traj["derived"]["prompt_tokens"] == 100


@pytest.mark.parametrize("key", ["../../secrets", "q99", "", "q01/../../x", "a b"])
def test_trajectory_refuses_keys_that_do_not_name_a_log_here(tmp_path, key):
    run = make_run(
        tmp_path / "results", "r", eval_report([(1, "pass", "this_run")]), logs=["q01"]
    )
    assert runs.trajectory(run, key) is None


# ---------------------------------------------------------------- matrix


def build_matrix(tmp_path) -> dict:
    spec = make_spec(tmp_path)
    return matrix.build(Suite(key="fake", spec=spec))


def test_matrix_encodes_one_character_per_cell(tmp_path):
    results = tmp_path / "results"
    make_run(
        results,
        "20260101-000000_enriched",
        eval_report([(1, "pass", "this_run"), (3, "error", "this_run")]),
        logs=["q01", "q03"],
    )
    make_run(
        results,
        "20260102-000000_ingest",
        eval_report([(1, "fail", "this_run"), (2, "pass", "20260101-000000_enriched")]),
        logs=["q01"],
    )
    payload = build_matrix(tmp_path)
    assert payload["questions"] == [1, 2, 3]
    newest, older = payload["runs"]
    assert newest["name"] == "20260102-000000_ingest"  # chronological, newest first
    assert newest["cells"] == "fP."  # uppercase P = spliced, . = never ran here
    assert older["cells"] == "p.e"
    assert payload["legend"]["p"] == "pass"
    assert (newest["passed"], newest["total"]) == (1, 2)


def test_matrix_rebuild_is_stable_and_cached(tmp_path):
    make_run(
        tmp_path / "results",
        "20260101-000000_enriched",
        eval_report([(1, "pass", "this_run")]),
        logs=["q01"],
    )
    first = build_matrix(tmp_path)
    assert build_matrix(tmp_path) == first  # cached rows must not be mutated in place


def test_matrix_covers_live_and_repeat_runs(tmp_path):
    results = tmp_path / "results"
    make_run(results, "20260103-000000_live", None, logs=["q02"])
    make_run(
        results,
        "20260102-000000_repeat_q01",
        {
            "meta": {"query_id": 1, "mode": "ingest"},
            "runs": [{"rep": 0, "status": "pass"}, {"rep": 1, "status": "fail"}],
        },
        logs=["q01.r0"],
        repeat=True,
    )
    rows = {r["name"]: r for r in build_matrix(tmp_path)["runs"]}
    assert rows["20260103-000000_live"]["cells"] == ".r"  # in flight
    # A 10x run that split 1 pass / 1 fail reports the spread, not a verdict.
    assert rows["20260102-000000_repeat_q01"]["cells"] == "w."
    assert rows["20260102-000000_repeat_q01"]["reps"] == {"passed": 1, "total": 2}


def test_matrix_cache_answers_ready_when_the_build_is_quick(tmp_path):
    make_run(
        tmp_path / "results",
        "20260101-000000_enriched",
        eval_report([(1, "pass", "this_run")]),
        logs=["q01"],
    )
    payload = matrix.MatrixCache().get(
        Suite(key="fake", spec=make_spec(tmp_path)), wait=5
    )
    assert payload["ready"] and payload["runs"][0]["cells"] == "p"
