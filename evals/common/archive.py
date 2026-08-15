"""Longitudinal archive of eval-run results in a (gitignored) sqlite db.

This is the durable copy. Run dirs are enormous (hundreds of GB of agent logs)
and get reclaimed; these rows are a few KB per run and outlive them, so the
viewer's history and trends survive a cleanup sweep. Three tables:

``questions`` — one row per (run, variant, question, rep): status, tokens,
turns, the agent's final query text, plus a little denormalised run context.
``tool_use``  — one row per (run, variant, question, rep, tool): call counts,
so tool-mix drift is queryable over time.
``run_meta``  - one row per run: its headline, and the ``stamp`` of the report
it was read from, so a run is only re-read when it actually changes.

Archival is idempotent: re-archiving a run replaces its existing rows.

Curated runs (a ``--query-ids`` rerun that spliced older results in, or a run
with offline replays) are archived with ``curated = 1``: they belong in the
question-by-run grid, but they'd inflate a pass-rate trend toward 100%, so the
summary excludes them.

Why sqlite and not duckdb: the eval writes here at the end of every run while a
viewer process holds the same file open, and duckdb takes a single-writer lock
on its file. Volume is thousands of rows, not analytics scale. DuckDB can read
this file directly when you do want to slice it:
``INSTALL sqlite; ATTACH 'evals/eval_history.db' AS h (TYPE sqlite);``
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
CREATE TABLE IF NOT EXISTS run_meta (
    run_name TEXT PRIMARY KEY,
    suite TEXT NOT NULL,
    variant TEXT,
    kind TEXT,
    model TEXT,
    provider TEXT,
    scale_factor REAL,
    run_timestamp TEXT,
    curated INTEGER NOT NULL DEFAULT 0,
    questions INTEGER,
    passed INTEGER,
    prompt_tokens INTEGER,
    stamp TEXT,
    archived_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_questions_run ON questions (run_name);
CREATE INDEX IF NOT EXISTS idx_questions_suite_variant ON questions (suite, variant);
CREATE INDEX IF NOT EXISTS idx_tool_use_run ON tool_use (run_name);
CREATE INDEX IF NOT EXISTS idx_run_meta_suite ON run_meta (suite);
"""

# Columns added after the first version shipped; sqlite has no
# ADD COLUMN IF NOT EXISTS, so they are applied against the live table list.
_ADDED_COLUMNS = {
    "questions": {
        "curated": "INTEGER NOT NULL DEFAULT 0",
        "final_query": "TEXT",
        "kind": "TEXT",
    }
}


def _migrate(conn: sqlite3.Connection) -> None:
    for table, columns in _ADDED_COLUMNS.items():
        have = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for name, decl in columns.items():
            if name not in have:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    # A finishing eval run and a viewer sync can reach for the file at the same
    # moment; wait for the other writer instead of raising "database is locked".
    conn = sqlite3.connect(db_path or default_db_path(), timeout=30)
    conn.executescript(_SCHEMA)
    with conn:
        _migrate(conn)
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


_MAX_QUERY_CHARS = 32_000  # a whole agent query is a few KB; this only stops runaways


def _candidate_text(run_dir: Path, question: int, rep: int) -> str | None:
    """The query the agent finally wrote, so the compare panel still works once
    the run dir is gone. Looked up where the scorer staged it, then in the
    per-worker copies."""
    workspace = run_dir / "workspace"
    for base in (workspace, workspace / f"_worker_{rep}"):
        for stem in (f"query{question:02d}", f"query{question}"):
            for ext in ("preql", "sql"):
                path = base / f"{stem}.{ext}"
                if path.is_file():
                    try:
                        return path.read_text(encoding="utf-8")[:_MAX_QUERY_CHARS]
                    except OSError:
                        return None
    return None


def read_run(run_dir: Path) -> tuple[str | None, list[dict]]:
    """(variant, per-question summaries) for a run dir, or (None, []) if it
    holds no archivable report. Kept for callers that only want the rows;
    :func:`read_run_full` carries the run-level context too."""
    _, variant, summaries = read_run_full(run_dir)
    return variant, summaries


def read_run_full(run_dir: Path) -> tuple[dict, str | None, list[dict]]:
    """(run-level meta, variant, per-question summaries) for a run dir."""
    full = run_dir / "report.json"
    repeat = run_dir / "repeat_report.json"
    if full.exists():
        report = json.loads(full.read_text(encoding="utf-8"))
        meta = report.get("meta", {})
        # `spliced_from` grafts passes from a prior run (a --query-ids rerun);
        # a non-empty `replays` list means individual queries were re-run
        # offline as bugs got fixed. Both are real results worth keeping - they
        # just aren't honest single-run baselines, so they are flagged rather
        # than dropped, and the summary's trend leaves them out.
        curated = "spliced_from" in report or bool(report.get("replays"))
        summaries = _summaries_full(report)
        for row in summaries:
            row["final_query"] = _candidate_text(run_dir, row["question"], 0)
        run_meta = {
            "kind": "eval",
            "curated": curated,
            "model": meta.get("model"),
            "provider": meta.get("provider"),
            "scale_factor": meta.get("scale_factor"),
            "run_timestamp": meta.get("timestamp"),
        }
        return run_meta, meta.get("category") or meta.get("mode"), summaries
    if repeat.exists():
        report = json.loads(repeat.read_text(encoding="utf-8"))
        meta = report.get("meta", {})
        summaries = _summaries_repeat(report)
        for row in summaries:
            if row["question"] is not None:
                row["final_query"] = _candidate_text(
                    run_dir, row["question"], row.get("rep", 0)
                )
        run_meta = {
            "kind": "repeat",
            "curated": False,
            "model": meta.get("model"),
            "provider": meta.get("provider"),
            "scale_factor": meta.get("scale_factor"),
            "run_timestamp": None,
        }
        return run_meta, meta.get("mode") or meta.get("category"), summaries
    return {}, None, []


def publish_run(run_dir: Path, suite: str, db_path: Path | None = None) -> int:
    """Archive one finished run into the db without touching its raw logs.

    The finish-of-run hook (and the manual ingest CLI); the cleanup sweep uses
    ``archive_run`` directly since it already holds the connection open. Opens
    and closes its own connection so callers don't manage db state. Idempotent."""
    conn = connect(db_path)
    try:
        return archive_run(conn, run_dir, suite)
    finally:
        conn.close()


def archive_run(
    conn: sqlite3.Connection, run_dir: Path, suite: str, stamp: str | None = None
) -> int:
    """Archive one run dir's results. Replaces any prior rows for the run
    (idempotent). ``stamp`` records which version of the report these rows came
    from, so a later sync can tell whether the run has moved on. Returns the
    number of question rows written (0 = not archivable)."""
    run_meta, variant, summaries = read_run_full(run_dir)
    if not summaries:
        return 0
    variant = variant or "unknown"
    run_name = run_dir.name
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    curated = 1 if run_meta.get("curated") else 0
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
                    run_timestamp, archived_at, curated, final_query, kind)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
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
                    curated,
                    s.get("final_query"),
                    run_meta.get("kind"),
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
        scored = [s for s in summaries if s.get("question") is not None]
        conn.execute(
            """INSERT OR REPLACE INTO run_meta
               (run_name, suite, variant, kind, model, provider, scale_factor,
                run_timestamp, curated, questions, passed, prompt_tokens, stamp,
                archived_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                run_name,
                suite,
                variant,
                run_meta.get("kind"),
                run_meta.get("model"),
                run_meta.get("provider"),
                run_meta.get("scale_factor"),
                run_meta.get("run_timestamp"),
                curated,
                len(scored),
                sum(1 for s in scored if s.get("status") == "pass"),
                sum(s.get("prompt_tokens") or 0 for s in scored) or None,
                stamp,
                now,
            ),
        )
    return len(scored)


# --------------------------------------------------------------- reading back


def run_stamps(conn: sqlite3.Connection, suite: str) -> dict[str, str]:
    """``{run name: stamp}`` already archived for this suite - what a sync
    compares against to decide which run dirs it still has to read."""
    return {
        name: stamp or ""
        for name, stamp in conn.execute(
            "SELECT run_name, stamp FROM run_meta WHERE suite = ?", (suite,)
        )
    }


def archived_runs(conn: sqlite3.Connection, suite: str) -> list[dict]:
    """One row per archived run, with its per-question statuses - the grid's
    history for runs whose files are gone."""
    rows = {
        name: {
            "name": name,
            "variant": variant,
            "kind": kind or "eval",
            "model": model,
            "provider": provider,
            "scale_factor": sf,
            "run_timestamp": ts,
            "curated": bool(curated),
            "passed": passed or 0,
            "total": total or 0,
            "prompt_tokens": tokens,
            "archived_at": archived_at,
            "cells": {},
        }
        for name, variant, kind, model, provider, sf, ts, curated, total, passed, tokens, archived_at in conn.execute(
            """SELECT run_name, variant, kind, model, provider, scale_factor,
                      run_timestamp, curated, questions, passed, prompt_tokens,
                      archived_at
               FROM run_meta WHERE suite = ?""",
            (suite,),
        )
    }
    reps_by_run: dict[str, list[int]] = {}
    # Pre-run_meta archives (and repeat runs) still have their question rows;
    # derive the run row from those so old history stays visible. A repeat run
    # has many reps of one question: report the spread, like a live one does.
    for (
        name,
        variant,
        model,
        provider,
        ts,
        question,
        status,
        passed,
        reps,
    ) in conn.execute(
        """SELECT run_name, MAX(variant), MAX(model), MAX(provider),
                  MAX(run_timestamp), question, MAX(status), SUM(passed), COUNT(*)
           FROM questions WHERE suite = ? GROUP BY run_name, question""",
        (suite,),
    ):
        tally = reps_by_run.setdefault(name, [0, 0])
        tally[0] += passed or 0
        tally[1] += reps
        if reps > 1:
            status = "pass" if passed == reps else "fail" if not passed else "partial"
        row = rows.get(name)
        if row is None:
            row = rows[name] = {
                "name": name,
                "variant": variant,
                "kind": "eval",
                "model": model,
                "provider": provider,
                "scale_factor": None,
                "run_timestamp": ts,
                "curated": False,
                "passed": 0,
                "total": 0,
                "prompt_tokens": None,
                "archived_at": None,
                "cells": {},
            }
        row["cells"][question] = status
    for name, row in rows.items():
        if row["total"]:
            continue
        passed, reps = reps_by_run.get(name, (0, 0))
        if reps > len(row["cells"]):  # more rows than questions: a repeat run
            row["passed"], row["total"] = passed, reps
        else:
            row["total"] = len(row["cells"])
            row["passed"] = sum(1 for s in row["cells"].values() if s == "pass")
    return list(rows.values())


def archived_index(conn: sqlite3.Connection, suite: str, run_name: str) -> dict | None:
    """A run's per-question detail, read back from the archive. Same shape the
    viewer builds from a run dir, minus everything that needed the raw logs."""
    meta = conn.execute(
        """SELECT variant, kind, model, provider, scale_factor, curated, archived_at
           FROM run_meta WHERE suite = ? AND run_name = ?""",
        (suite, run_name),
    ).fetchone()
    rows = conn.execute(
        """SELECT question, rep, status, detail, total_turns, prompt_tokens,
                  duration_seconds, ref_rows, cand_rows, final_query, variant,
                  model, provider
           FROM questions WHERE suite = ? AND run_name = ?
           ORDER BY question, rep""",
        (suite, run_name),
    ).fetchall()
    if meta is None and not rows:
        return None
    first = rows[0] if rows else None
    variant, kind, model, provider, scale_factor, curated, archived_at = meta or (
        first[10] if first else None,
        "eval",
        first[11] if first else None,
        first[12] if first else None,
        None,
        0,
        None,
    )
    questions = [
        {
            # A synthetic key: there is no log to open, but the page still keys
            # selection and the archived query lookup off it.
            "key": f"q{question:02d}" + (f".r{rep:02d}" if rep else ""),
            "qid": question,
            "rep": rep or 0,
            "status": status or "?",
            "detail": detail or "",
            "iterations": turns,
            "prompt_tokens": prompt_tokens,
            "duration_seconds": duration,
            "ref_rows": ref_rows,
            "cand_rows": cand_rows,
            "source": "this_run",
            "has_log": False,
            "has_query": final_query is not None,
            "stamp": "archived",
        }
        for question, rep, status, detail, turns, prompt_tokens, duration, ref_rows, cand_rows, final_query, *_ in rows
    ]
    return {
        "name": run_name,
        "kind": kind or "eval",
        "category": variant,
        "provider": provider,
        "model": model,
        "scale_factor": scale_factor,
        "replayable": False,
        "archived": True,
        "curated": bool(curated),
        "archived_at": archived_at,
        "summary": {
            "passed": sum(1 for q in questions if q["status"] == "pass"),
            "total": len(questions),
            "prompt_tokens": sum(q["prompt_tokens"] or 0 for q in questions),
        },
        "questions": questions,
    }


def archived_query(
    conn: sqlite3.Connection, suite: str, run_name: str, question: int, rep: int = 0
) -> str | None:
    """The agent's final query for one question, as archived."""
    row = conn.execute(
        """SELECT final_query FROM questions
           WHERE suite = ? AND run_name = ? AND question = ? AND rep = ?""",
        (suite, run_name, question, rep),
    ).fetchone()
    return row[0] if row else None
