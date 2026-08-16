"""Unit tests for the history db as the durable copy of eval results.

Run dirs get reclaimed; these rows don't. So: a run dir syncs into the archive
(once, unless its report moves), a cleaned-up run keeps its row in the grid and
its per-question detail in the drilldown, and the agent's final query survives
the files it was written in."""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "evals"))

from common import archive
from common.spec import BenchmarkSpec
from viewer import matrix
from viewer.suites import Suite


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "history.db"
    monkeypatch.setattr(archive, "default_db_path", lambda: path)
    return path


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


def make_run(
    root: Path, name: str, report: dict, *, queries: dict | None = None, repeat=False
) -> Path:
    run_dir = root / name
    (run_dir / "workspace").mkdir(parents=True)
    (run_dir / ("repeat_report.json" if repeat else "report.json")).write_text(
        json.dumps(report), encoding="utf-8"
    )
    (
        run_dir / f"agent_log.q{report.get('queries', [{'id': 1}])[0]['id']:02d}.jsonl"
    ).write_text("", encoding="utf-8")
    for qid, text in (queries or {}).items():
        (run_dir / "workspace" / f"query{qid:02d}.preql").write_text(
            text, encoding="utf-8"
        )
    return run_dir


def report(rows, spliced=False) -> dict:
    out = {
        "meta": {
            "category": "enriched",
            "model": "m",
            "provider": "p",
            "scale_factor": 1,
            "timestamp": "20260101-000000",
        },
        "queries": [{"id": i, "status": s, "detail": ""} for i, s in rows],
        "per_query": [{"id": i, "duration_seconds": 3.0} for i, _ in rows],
        "per_query_metrics": [
            {"id": i, "iterations": 4, "prompt_tokens": 500, "total_tokens": 600}
            for i, _ in rows
        ],
    }
    if spliced:
        out["spliced_from"] = {"run_dir": "older", "fresh_ids": [1], "spliced_ids": [2]}
    return out


def test_archiving_keeps_the_agents_final_query(db, tmp_path):
    run = make_run(
        tmp_path / "results",
        "20260101-000000_enriched",
        report([(1, "pass"), (2, "fail")]),
        queries={1: "select 1 as x;", 2: "select 2 as y;"},
    )
    conn = archive.connect()
    assert archive.archive_run(conn, run, "fake", stamp="s1") == 2
    assert archive.archived_query(conn, "fake", run.name, 1) == "select 1 as x;"
    assert archive.run_stamps(conn, "fake") == {run.name: "s1"}
    conn.close()


def test_curated_runs_are_kept_but_flagged(db, tmp_path):
    """A spliced rerun used to be dropped on the floor; it belongs in the grid,
    just not in a pass-rate trend."""
    run = make_run(
        tmp_path / "results",
        "20260101-000000_enriched",
        report([(1, "pass")], spliced=True),
    )
    conn = archive.connect()
    assert archive.archive_run(conn, run, "fake", stamp="s1") == 1
    curated = conn.execute(
        "SELECT curated FROM run_meta WHERE run_name = ?", (run.name,)
    ).fetchone()
    assert curated[0] == 1
    assert (
        conn.execute("SELECT COUNT(*) FROM questions WHERE curated = 1").fetchone()[0]
        == 1
    )
    conn.close()


def test_archived_index_survives_the_run_dir(db, tmp_path):
    results = tmp_path / "results"
    run = make_run(
        results,
        "20260101-000000_enriched",
        report([(1, "pass"), (2, "error")]),
        queries={2: "select oops;"},
    )
    conn = archive.connect()
    archive.archive_run(conn, run, "fake", stamp="s1")
    conn.close()
    shutil.rmtree(run)  # the cleanup sweep

    conn = archive.connect()
    index = archive.archived_index(conn, "fake", "20260101-000000_enriched")
    conn.close()
    assert index["archived"] and index["summary"] == {
        "passed": 1,
        "total": 2,
        "prompt_tokens": 1000,
    }
    second = index["questions"][1]
    assert (second["key"], second["qid"], second["status"]) == ("q02", 2, "error")
    assert second["has_log"] is False and second["has_query"] is True


def test_grid_spans_disk_and_archive(db, tmp_path):
    results = tmp_path / "results"
    suite = Suite(key="fake", spec=make_spec(tmp_path))
    gone = make_run(
        results, "20260101-000000_enriched", report([(1, "pass"), (2, "fail")])
    )
    make_run(results, "20260102-000000_ingest", report([(1, "fail"), (2, "pass")]))
    matrix._ROW_CACHE.clear()

    matrix.build(suite)  # syncs both into the archive
    shutil.rmtree(gone)
    matrix._ROW_CACHE.clear()
    rows = {r["name"]: r for r in matrix.build(suite)["runs"]}

    assert len(rows) == 2, "the deleted run must still have a row"
    assert rows["20260102-000000_ingest"]["on_disk"] is True
    archived = rows["20260101-000000_enriched"]
    assert archived["on_disk"] is False and archived["archived"] is True
    assert archived["cells"] == "pf" and archived["category"] == "enriched"


def test_sync_only_re_reads_a_run_whose_report_moved(db, tmp_path, monkeypatch):
    results = tmp_path / "results"
    run = make_run(results, "20260101-000000_enriched", report([(1, "pass")]))
    suite = Suite(key="fake", spec=make_spec(tmp_path))
    matrix._ROW_CACHE.clear()

    calls = []
    real = archive.archive_run
    monkeypatch.setattr(
        archive,
        "archive_run",
        lambda conn, d, s, stamp=None: (calls.append(d.name), real(conn, d, s, stamp))[
            1
        ],
    )
    matrix.build(suite)
    matrix._ROW_CACHE.clear()
    matrix.build(suite)
    assert calls == [run.name], "an unchanged run must not be re-read"

    (run / "report.json").write_text(
        json.dumps(report([(1, "fail")])), encoding="utf-8"
    )
    matrix._ROW_CACHE.clear()
    matrix.build(suite)
    assert calls == [run.name, run.name]
    conn = archive.connect()
    assert conn.execute("SELECT status FROM questions").fetchall() == [("fail",)]
    conn.close()


def test_repeat_runs_report_their_spread_from_the_archive(db, tmp_path):
    results = tmp_path / "results"
    run = make_run(
        results,
        "repeat_q07_20260101-000000_ingest",
        {
            "meta": {"query_id": 7, "mode": "ingest", "model": "m"},
            "runs": [
                {"rep": 0, "status": "pass"},
                {"rep": 1, "status": "fail"},
                {"rep": 2, "status": "pass"},
            ],
        },
        repeat=True,
    )
    conn = archive.connect()
    archive.archive_run(conn, run, "fake", stamp="s1")
    runs = archive.archived_runs(conn, "fake")
    conn.close()
    assert len(runs) == 1
    assert (runs[0]["passed"], runs[0]["total"]) == (2, 3)
    assert runs[0]["cells"] == {7: "partial"}


# The shipped v1 `questions` table: no curated/final_query/kind, no run_meta.
# Real databases in the wild look exactly like this.
_V1_SCHEMA = """
CREATE TABLE questions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_name TEXT NOT NULL, suite TEXT NOT NULL, variant TEXT NOT NULL,
    question INTEGER NOT NULL, rep INTEGER NOT NULL DEFAULT 0,
    status TEXT, passed INTEGER, total_tokens INTEGER, prompt_tokens INTEGER,
    completion_tokens INTEGER, total_turns INTEGER, final_query_size INTEGER,
    tool_errors INTEGER, ref_rows INTEGER, cand_rows INTEGER,
    duration_seconds REAL, detail TEXT, model TEXT, provider TEXT,
    scale_factor REAL, run_timestamp TEXT, archived_at TEXT NOT NULL,
    UNIQUE (run_name, variant, question, rep)
);
CREATE TABLE tool_use (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_name TEXT NOT NULL, suite TEXT NOT NULL, variant TEXT NOT NULL,
    question INTEGER NOT NULL, rep INTEGER NOT NULL DEFAULT 0,
    tool TEXT NOT NULL, calls INTEGER NOT NULL, archived_at TEXT NOT NULL
);
"""


def test_schema_migrates_a_v1_db(db, tmp_path):
    """A db written before final_query/curated/run_meta existed must still open,
    and its history must still read back."""
    import sqlite3

    old = sqlite3.connect(db)
    old.executescript(_V1_SCHEMA)
    old.execute(
        "INSERT INTO questions (run_name, suite, variant, question, status, passed,"
        " prompt_tokens, model, archived_at)"
        " VALUES ('old_run','fake','enriched',1,'pass',1,700,'m','2026-01-01T00:00:00')"
    )
    old.commit()
    old.close()

    conn = archive.connect()
    columns = {row[1] for row in conn.execute("PRAGMA table_info(questions)")}
    assert {"curated", "final_query", "kind"} <= columns
    runs = archive.archived_runs(conn, "fake")
    conn.close()
    assert runs[0]["name"] == "old_run" and runs[0]["cells"] == {1: "pass"}
