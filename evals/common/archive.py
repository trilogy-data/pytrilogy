"""Longitudinal archive of eval-run summary stats in a (gitignored) sqlite db.

The cleanup script archives runs into here as compact per-question summaries
*before* it deletes the (large) raw logs, so historical pass-rate / token /
turn trends survive the disk reclaim. Two tables:

``questions`` — one row per (run, variant, question, rep): status, tokens,
turns, final-query size, plus a little denormalised run context.
``tool_use``  — one row per (run, variant, question, rep, tool): call counts,
so tool-mix drift is queryable over time.

Archival is idempotent: re-archiving a run replaces its existing rows.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

# evals/ root — the db sits here (outside results/, which is wiped), gitignored.
_EVAL_ROOT = Path(__file__).resolve().parents[1]
DB_FILENAME = "eval_history.db"


def default_db_path() -> Path:
    return _EVAL_ROOT / DB_FILENAME


_SCHEMA = """
CREATE TABLE IF NOT EXISTS questions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_name TEXT NOT NULL,
    suite TEXT NOT NULL,
    variant TEXT NOT NULL,
    question INTEGER NOT NULL,
    rep INTEGER NOT NULL DEFAULT 0,
    status TEXT,
    passed INTEGER,
    total_tokens INTEGER,
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    total_turns INTEGER,
    final_query_size INTEGER,
    tool_errors INTEGER,
    ref_rows INTEGER,
    cand_rows INTEGER,
    duration_seconds REAL,
    detail TEXT,
    model TEXT,
    provider TEXT,
    scale_factor REAL,
    run_timestamp TEXT,
    archived_at TEXT NOT NULL,
    UNIQUE (run_name, variant, question, rep)
);
CREATE TABLE IF NOT EXISTS tool_use (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_name TEXT NOT NULL,
    suite TEXT NOT NULL,
    variant TEXT NOT NULL,
    question INTEGER NOT NULL,
    rep INTEGER NOT NULL DEFAULT 0,
    tool TEXT NOT NULL,
    calls INTEGER NOT NULL,
    archived_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_questions_run ON questions (run_name);
CREATE INDEX IF NOT EXISTS idx_questions_suite_variant ON questions (suite, variant);
CREATE INDEX IF NOT EXISTS idx_tool_use_run ON tool_use (run_name);
"""


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or default_db_path())
    conn.executescript(_SCHEMA)
    return conn


def _tool_rows(calls_by_name: dict, subcommands: dict) -> dict[str, int]:
    """Flatten the tool tally: trilogy is exploded into ``trilogy:<sub>`` rows
    from ``trilogy_subcommands`` (which sum to the ``trilogy`` count), other
    tools pass through by name — so no call is double-counted."""
    tools: dict[str, int] = {}
    for name, count in (calls_by_name or {}).items():
        if name == "trilogy":
            continue
        tools[name] = tools.get(name, 0) + int(count)
    for sub, count in (subcommands or {}).items():
        tools[f"trilogy:{sub}"] = tools.get(f"trilogy:{sub}", 0) + int(count)
    return tools


def _summaries_full(report: dict) -> list[dict]:
    """Per-question summaries from a full run_eval ``report.json``."""
    meta = report.get("meta", {})
    by_id: dict[int, dict] = {}
    for q in report.get("queries", []):
        by_id[q["id"]] = {
            "question": q["id"],
            "rep": 0,
            "status": q.get("status"),
            "final_query_size": q.get("generated_sql_len"),
            "ref_rows": q.get("ref_rows"),
            "cand_rows": q.get("cand_rows"),
            "detail": q.get("detail"),
            "tools": {},
        }
    for p in report.get("per_query", []):
        by_id.setdefault(p["id"], {"question": p["id"], "rep": 0, "tools": {}})[
            "duration_seconds"
        ] = p.get("duration_seconds")
    for m in report.get("per_query_metrics", []):
        row = by_id.setdefault(m["id"], {"question": m["id"], "rep": 0, "tools": {}})
        row["total_turns"] = m.get("iterations")
        row["total_tokens"] = m.get("total_tokens")
        row["prompt_tokens"] = m.get("prompt_tokens")
        row["completion_tokens"] = m.get("completion_tokens")
        row["tool_errors"] = m.get("tool_errors")
        row["tools"] = _tool_rows(
            m.get("tool_calls_by_name", {}), m.get("trilogy_subcommands", {})
        )
    ctx = {
        "model": meta.get("model"),
        "provider": meta.get("provider"),
        "scale_factor": meta.get("scale_factor"),
        "run_timestamp": meta.get("timestamp"),
    }
    return [{**ctx, **row} for row in by_id.values()]


def _summaries_repeat(report: dict) -> list[dict]:
    """Per-rep summaries from a ``repeat_report.json`` (one question, N reps)."""
    meta = report.get("meta", {})
    qid = meta.get("query_id")
    ctx = {
        "model": meta.get("model"),
        "provider": meta.get("provider"),
        "scale_factor": meta.get("scale_factor"),
        "run_timestamp": None,
    }
    out = []
    for r in report.get("runs", []):
        out.append(
            {
                **ctx,
                "question": qid,
                "rep": r.get("rep", 0),
                "status": r.get("status"),
                "total_turns": r.get("iterations"),
                "total_tokens": r.get("total_tokens"),
                "prompt_tokens": r.get("prompt_tokens"),
                "duration_seconds": r.get("duration_seconds"),
                "detail": r.get("detail"),
                # repeat_report only tallies explore calls, not the full mix.
                "tools": (
                    {"trilogy:explore": r["explore_calls"]}
                    if r.get("explore_calls")
                    else {}
                ),
            }
        )
    return out


def read_run(run_dir: Path) -> tuple[str | None, list[dict]]:
    """(variant, per-question summaries) for a run dir, or (None, []) if it
    holds no archivable report."""
    full = run_dir / "report.json"
    repeat = run_dir / "repeat_report.json"
    if full.exists():
        report = json.loads(full.read_text(encoding="utf-8"))
        variant = report.get("meta", {}).get("category") or report.get("meta", {}).get(
            "mode"
        )
        return variant, _summaries_full(report)
    if repeat.exists():
        report = json.loads(repeat.read_text(encoding="utf-8"))
        variant = report.get("meta", {}).get("mode") or report.get("meta", {}).get(
            "category"
        )
        return variant, _summaries_repeat(report)
    return None, []


def archive_run(conn: sqlite3.Connection, run_dir: Path, suite: str) -> int:
    """Archive one run dir's summary stats. Replaces any prior rows for the run
    (idempotent). Returns the number of question rows written (0 = not archivable)."""
    variant, summaries = read_run(run_dir)
    if not summaries:
        return 0
    variant = variant or "unknown"
    run_name = run_dir.name
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    with conn:
        conn.execute("DELETE FROM questions WHERE run_name = ?", (run_name,))
        conn.execute("DELETE FROM tool_use WHERE run_name = ?", (run_name,))
        for s in summaries:
            if s.get("question") is None:
                continue
            status = s.get("status")
            conn.execute(
                """INSERT INTO questions
                   (run_name, suite, variant, question, rep, status, passed,
                    total_tokens, prompt_tokens, completion_tokens, total_turns,
                    final_query_size, tool_errors, ref_rows, cand_rows,
                    duration_seconds, detail, model, provider, scale_factor,
                    run_timestamp, archived_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    run_name,
                    suite,
                    variant,
                    s["question"],
                    s.get("rep", 0),
                    status,
                    1 if status == "pass" else 0,
                    s.get("total_tokens"),
                    s.get("prompt_tokens"),
                    s.get("completion_tokens"),
                    s.get("total_turns"),
                    s.get("final_query_size"),
                    s.get("tool_errors"),
                    s.get("ref_rows"),
                    s.get("cand_rows"),
                    s.get("duration_seconds"),
                    s.get("detail"),
                    s.get("model"),
                    s.get("provider"),
                    s.get("scale_factor"),
                    s.get("run_timestamp"),
                    now,
                ),
            )
            for tool, calls in s.get("tools", {}).items():
                conn.execute(
                    """INSERT INTO tool_use
                       (run_name, suite, variant, question, rep, tool, calls, archived_at)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (
                        run_name,
                        suite,
                        variant,
                        s["question"],
                        s.get("rep", 0),
                        tool,
                        calls,
                        now,
                    ),
                )
    return sum(1 for s in summaries if s.get("question") is not None)
