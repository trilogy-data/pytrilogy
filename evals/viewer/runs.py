"""One run, read lazily.

The page asks for a run's question index first (cheap: report.json, no log
parsing) and only then for the trajectory of the question you actually opened.
Opening a full TPC-DS run used to mean parsing 99 logs and transpiling 198
queries - 41s and 13.5MB - to show one of them.
"""

from __future__ import annotations

import json
from pathlib import Path

from common.spec import BenchmarkSpec

from . import logs, queries

# Report fields worth surfacing per question in the index.
_QUERY_FIELDS = ("status", "detail", "ref_rows", "cand_rows", "source")
_METRIC_FIELDS = (
    "iterations",
    "tool_calls",
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
)


def _read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _log_index(run_dir: Path) -> dict[str, Path]:
    return {logs.key_of(p): p for p in sorted(run_dir.glob("agent_log.*.jsonl"))}


def _question(key: str | None, path: Path | None, **fields) -> dict:
    entry = {
        "key": key,
        "qid": None,
        "rep": 0,
        "status": "?",
        "detail": "",
        "iterations": None,
        "prompt_tokens": None,
        "duration_seconds": None,
        "source": "this_run",
        "has_log": path is not None,
        "stamp": logs.stamp(path) if path is not None else "",
    }
    entry.update({k: v for k, v in fields.items() if v is not None})
    return entry


def _derived_metrics(path: Path | None) -> dict:
    """Metrics parsed back out of the log - for questions the report doesn't
    carry (a live run, or a report written before per_query_metrics existed)."""
    if path is None:
        return {}
    derived = logs.parse_log_cached(path)["derived"]
    return {k: derived[k] for k in _METRIC_FIELDS if k in derived}


def _eval_index(run_dir: Path, report: dict, log_files: dict[str, Path]) -> list[dict]:
    per_query = {r["id"]: r for r in report.get("per_query", [])}
    metrics = {m["id"]: m for m in report.get("per_query_metrics", [])}
    scored = report.get("queries", [])
    live = not scored  # no report.json (or an empty one): the run is still going
    out: list[dict] = []
    seen: set[str] = set()
    for row in scored:
        qid = row["id"]
        key = f"q{qid:02d}"
        path = log_files.get(key)
        seen.add(key)
        m = metrics.get(qid) or _derived_metrics(path)
        out.append(
            _question(
                key if path is not None else None,
                path,
                qid=qid,
                **{k: row.get(k) for k in _QUERY_FIELDS},
                **{k: m.get(k) for k in _METRIC_FIELDS},
                duration_seconds=(per_query.get(qid) or {}).get("duration_seconds"),
            )
        )
    for key, path in log_files.items():
        if key in seen:
            continue
        qid, rep = logs.ids_in_key(key)
        out.append(
            _question(
                key,
                path,
                qid=qid,
                rep=rep,
                status="running" if live else "unscored",
                **_derived_metrics(path),
            )
        )
    out.sort(key=lambda q: (q["qid"] if q["qid"] is not None else 1e9, q["rep"]))
    return out


def _repeat_index(report: dict, log_files: dict[str, Path]) -> list[dict]:
    """The 10x harness: one question, N reps, keyed by rep instead of id."""
    qid = report.get("meta", {}).get("query_id")
    by_rep = {r["rep"]: r for r in report.get("runs", [])}
    out: list[dict] = []
    for key, path in log_files.items():
        _, rep = logs.ids_in_key(key)
        row = by_rep.get(rep if rep is not None else -1, {})
        out.append(
            _question(
                key,
                path,
                qid=qid,
                rep=rep,
                **{k: row.get(k) for k in _QUERY_FIELDS if k != "status"},
                iterations=row.get("iterations"),
                prompt_tokens=row.get("prompt_tokens"),
                duration_seconds=row.get("duration_seconds"),
                status=row.get("status") or "running",
            )
        )
    out.sort(key=lambda q: q["rep"] or 0)
    return out


def run_index(run_dir: Path, spec: BenchmarkSpec) -> dict:
    """Everything the drilldown needs before you pick a question."""
    logs.touch_cache_dir(run_dir.parent)
    log_files = _log_index(run_dir)
    repeat = _read_json(run_dir / "repeat_report.json")
    report = repeat or _read_json(run_dir / "report.json")
    meta = report.get("meta", {})
    kind = "repeat" if repeat else "eval"
    questions = (
        _repeat_index(report, log_files)
        if repeat
        else _eval_index(run_dir, report, log_files)
    )
    passed = sum(1 for q in questions if q["status"] == "pass")
    # Replay re-runs one question against the run's own workspace and rewrites
    # its slice of report.json - neither exists for repeat-harness dirs.
    replayable = (run_dir / "report.json").exists() and (run_dir / "workspace").is_dir()
    return {
        "name": run_dir.name,
        "kind": kind,
        "category": meta.get("category") or meta.get("mode"),
        "provider": meta.get("provider"),
        "model": meta.get("model"),
        "scale_factor": meta.get("scale_factor"),
        "replayable": replayable,
        "summary": {
            "passed": passed,
            "total": len(questions),
            "prompt_tokens": sum(q["prompt_tokens"] or 0 for q in questions),
        },
        "questions": questions,
    }


def trajectory(run_dir: Path, key: str) -> dict | None:
    """One question's parsed timeline. None when the key names no log here."""
    path = logs.log_path(run_dir, key)
    if path is None:
        return None
    parsed = logs.parse_log_cached(path)
    return {
        "key": key,
        "stamp": logs.stamp(path),
        "meta": parsed["meta"],
        "timeline": parsed["timeline"],
        "derived": parsed["derived"],
    }


def archived_index(suite_key: str, run_name: str) -> dict | None:
    """A cleaned-up run's index, read back from the history db."""
    from common import archive

    if not archive.default_db_path().exists():
        return None
    conn = archive.connect()
    try:
        return archive.archived_index(conn, suite_key, run_name)
    finally:
        conn.close()


def archived_query_pair(
    spec: BenchmarkSpec, suite_key: str, run_name: str, key: str, category: str | None
) -> dict:
    """Canonical vs agent query for a run whose files are gone: the agent's side
    comes from the archive, the canonical side is still in the repo."""
    from common import archive

    qid, rep = logs.ids_in_key(key)
    if qid is None or not archive.default_db_path().exists():
        return {"candidate": None, "canonical": None}
    conn = archive.connect()
    try:
        text = archive.archived_query(conn, suite_key, run_name, qid, rep or 0)
    finally:
        conn.close()
    ext = queries.category_ext(category)
    params = queries.load_params(spec).get(qid)
    candidate = (
        {
            "name": f"query{qid:02d}.{ext} (archived)",
            "lang": ext,
            "src": text,
            # Never read: a unique key for the render cache, and the curated
            # model dir as the import root - which is what an enriched run's
            # query resolves against. An ingest run's raw/ model died with the
            # run dir, so those render as an error, honestly.
            "_path": Path("archived") / suite_key / run_name / key,
            "_base": queries.ref_dir(spec) or spec.eval_dir,
        }
        if text
        else None
    )
    return {
        "candidate": queries.augment_sql(candidate, params),
        "canonical": queries.augment_sql(
            queries.read_query(queries.ref_dir(spec), qid, ext), params
        ),
    }


def query_pair(
    run_dir: Path, spec: BenchmarkSpec, key: str, category: str | None
) -> dict:
    """Canonical vs agent query for one question (the expensive transpile)."""
    qid, rep = logs.ids_in_key(key)
    if qid is None:
        return {"candidate": None, "canonical": None}
    if category is None:
        report = _read_json(run_dir / "repeat_report.json") or _read_json(
            run_dir / "report.json"
        )
        category = report.get("meta", {}).get("category") or report.get("meta", {}).get(
            "mode"
        )
    return queries.query_pair(run_dir, spec, qid, rep or 0, category)
