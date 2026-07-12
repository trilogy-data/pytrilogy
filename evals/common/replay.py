"""Re-run ONE query of a finished eval run *in place*.

The full eval is expensive, so validating a fix to a single prompt (or to the
engine a single query trips over) shouldn't mean re-running the benchmark. This
re-runs that one query against the run's existing workspace and overwrites its
slice of the run: the agent log, the candidate query file, and its entries in
``report.json`` — then re-aggregates the ``agent`` block and ``summary`` so the
headline still describes the whole benchmark, exactly as ``--splice-from`` does
for a partial re-run.

A replay picks up edits to all three inputs a fix usually lands in: the question
(``query_prompts.json``, re-read here), the engine (the agent subprocess
re-imports trilogy), and the semantic model (re-seeded by ``refresh_model``).
Only the database is reused as-is.

Overwriting is the point (the viewer reloads the run in place), so the prior
trajectory for that query is not recoverable afterwards.
"""

from __future__ import annotations

import dataclasses
import json
import os
import shutil
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from . import agent_runner, analyze_run, prompts, scoring
from .categories import Category, get_category
from .main import PROVIDER_ENV, SCORE_TIMEOUT
from .report import agent_metric_fields, load_env, render_markdown
from .spec import BenchmarkSpec

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_AGENT_TIMEOUT = 900


class ReplayError(RuntimeError):
    """A precondition failed — this run/query can't be replayed as-is."""


def _resolve_category(report: dict) -> Category:
    meta = report.get("meta", {})
    key = meta.get("category")
    if not key:
        # Pre-category reports only recorded how the model was seeded.
        key = (
            "enriched"
            if str(meta.get("model_source", "")).startswith("enriched")
            else "ingest"
        )
    return get_category(key)


def _worker_dir(workspace: Path, db_filename: str) -> Path:
    """Reuse the run's own worker copy so the agent sees the same trilogy.toml
    (iteration budget, tool gating) the original run used — a rebuilt toml would
    silently re-derive those from defaults and make the replay non-comparable."""
    worker = workspace / "_worker_0"
    if (worker / "trilogy.toml").exists() and (worker / db_filename).exists():
        return worker
    if not (workspace / "trilogy.toml").exists():
        raise ReplayError(f"no trilogy.toml in {workspace} — workspace was cleaned")
    return agent_runner.prepare_worker_workspace(workspace, 0, db_filename)


def _clear_candidates(
    dirs: list[Path], spec: BenchmarkSpec, qid: int, ext: str
) -> None:
    """A stale candidate from the prior run would be scored as this replay's
    answer if the agent writes nothing."""
    for base in dirs:
        for name in (
            f"query{qid:02d}{ext}",
            f"query{qid}{ext}",
            prompts.candidate_filename(spec, qid, ext),
        ):
            (base / name).unlink(missing_ok=True)


def _enriched_dir(report: dict) -> Path | None:
    """The ``--enriched-model-dir`` the run was seeded from, if it named one.
    Otherwise None, and the enriched setup falls back to the spec default."""
    source = str(report.get("meta", {}).get("model_source", ""))
    prefix = "enriched:"
    return Path(source[len(prefix) :]) if source.startswith(prefix) else None


def _sync_worker_model(workspace: Path, worker: Path) -> None:
    """Push the refreshed model into the worker copy. The worker's database and
    trilogy.toml are deliberately left alone — only the model is re-seeded."""
    if worker == workspace:
        return
    raw = workspace / "raw"
    if raw.exists():
        worker_raw = worker / "raw"
        if worker_raw.exists():
            shutil.rmtree(worker_raw)
        shutil.copytree(raw, worker_raw)
    schema = workspace / "schema.md"
    if schema.exists():
        shutil.copy2(schema, worker / "schema.md")


def _refresh_model(
    workspace: Path,
    worker: Path,
    spec: BenchmarkSpec,
    category: Category,
    report: dict,
    log: Callable[[str], None],
) -> None:
    """Re-seed the run's model the same way the original run did, so an edit to
    the curated .preql model (or to `trilogy ingest` itself) is what re-runs.

    Per leg: enriched re-copies the curated dir, ingest re-runs `ingest --all`,
    sql_schema regenerates schema.md, sql_bare is a no-op."""
    log(f"refreshing '{category.key}' model")
    setup = category.setup(
        workspace,
        spec,
        db_path=workspace / spec.db_filename,
        enriched_dir=_enriched_dir(report),
    )
    if setup["exit_code"] != 0:
        raise ReplayError(
            f"model refresh failed (exit {setup['exit_code']}): "
            f"{(setup.get('stderr') or '')[:300]}"
        )
    _sync_worker_model(workspace, worker)
    detail = (setup.get("stdout") or "").strip().splitlines()
    if detail:
        log(f"  {detail[-1][:120]}")


def _metrics_rows(report: dict, run_dir: Path) -> list[dict]:
    """Per-query metrics for every query in the report. Reports written before
    the lossless-splice change lack ``per_query_metrics``; rebuild from the
    local logs so the re-aggregate below isn't silently short."""
    stored = {m["id"]: m for m in report.get("per_query_metrics", [])}
    rows: list[dict] = []
    for q in report.get("queries", []):
        qid = q["id"]
        if qid in stored:
            rows.append(stored[qid])
            continue
        log = run_dir / f"agent_log.q{qid:02d}.jsonl"
        parsed = (
            scoring.parse_agent_log(log) if log.exists() else scoring.AgentMetrics()
        )
        rows.append({"id": qid, **scoring.metrics_to_dict(parsed)})
    return rows


def _upsert(rows: list[dict], row: dict) -> list[dict]:
    kept = [r for r in rows if r.get("id") != row["id"]]
    return sorted([*kept, row], key=lambda r: r["id"])


def _reaggregate(report: dict) -> None:
    """Rebuild the whole-benchmark ``agent`` + ``summary`` blocks in place from
    the (now updated) per-query rows."""
    queries = report["queries"]
    per_query = report["per_query"]
    agg = scoring.aggregate_metrics(
        [scoring.metrics_from_dict(m) for m in report["per_query_metrics"]]
    )
    duration = sum(r.get("duration_seconds") or 0 for r in per_query)
    total = len(queries)
    statuses: Counter[str] = Counter(q["status"] for q in queries)
    report["agent"] = {
        **report["agent"],
        **agent_metric_fields(agg),
        "exit_code": 0 if all(r.get("exit_code", 0) == 0 for r in per_query) else 1,
        "timed_out": any(r.get("timed_out") for r in per_query),
        "duration_seconds": round(duration, 1),
        "avg_query_seconds": round(duration / total, 1) if total else 0.0,
    }
    report["summary"] = {
        "pass_count": statuses.get("pass", 0),
        "pass_rate": round(statuses.get("pass", 0) / total, 3) if total else 0.0,
        "status_breakdown": dict(statuses),
    }
    report["meta"] = {**report["meta"], "num_queries": total}


def _mark_fresh(report: dict, qid: int) -> None:
    """A replayed query is this run's own result, not a spliced-in prior one."""
    sf = report.get("spliced_from")
    if not sf or qid not in sf.get("spliced_ids", []):
        return
    sf["spliced_ids"] = [i for i in sf["spliced_ids"] if i != qid]
    sf["fresh_ids"] = sorted({*sf.get("fresh_ids", []), qid})


def _render_artifacts(run_dir: Path, spec: BenchmarkSpec, report: dict) -> None:
    (run_dir / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (run_dir / "report.md").write_text(render_markdown(spec, report), encoding="utf-8")


def _fork_dir(run_dir: Path) -> Path:
    for n in range(1, 100):
        cand = run_dir.parent / f"{run_dir.name}_rerun{n}"
        if not cand.exists():
            return cand
    raise ReplayError(f"too many rerun forks of {run_dir.name}")


def fork_run(run_dir: Path, log: Callable[[str], None] = print) -> Path:
    """Copy a finished run into a fresh sibling dir so a full re-run leaves the
    original untouched as a baseline. The spare per-worker DB copies (each a full
    database) are skipped — a replay only reuses ``_worker_0`` and the root DB."""
    if not (run_dir / "report.json").exists():
        raise ReplayError(f"{run_dir.name} has no report.json (not a full eval run)")
    dest = _fork_dir(run_dir)
    log(f"forking {run_dir.name} -> {dest.name}")
    shutil.copytree(
        run_dir, dest, ignore=shutil.ignore_patterns("_worker_[1-9]*", "viewer.html")
    )
    return dest


def replay_all(
    run_dir: Path,
    spec: BenchmarkSpec,
    *,
    timeout: int = DEFAULT_AGENT_TIMEOUT,
    env_file: Path | None = None,
    log: Callable[[str], None] = print,
    on_progress: Callable[[int, int, int], None] | None = None,
    cancelled: Callable[[], bool] | None = None,
) -> dict:
    """Replay every query in ``run_dir`` in place. The model is re-seeded once up
    front (an edit to it should re-run) and reused for the rest. ``cancelled`` is
    polled between queries so a stop request lands at a query boundary."""
    report = json.loads((run_dir / "report.json").read_text(encoding="utf-8"))
    qids = sorted(q["id"] for q in report.get("queries", []))
    if not qids:
        raise ReplayError(f"{run_dir.name} has no queries to replay")
    done = 0
    for i, qid in enumerate(qids):
        if cancelled and cancelled():
            log(f"cancelled after {done}/{len(qids)}")
            break
        if on_progress:
            on_progress(i + 1, len(qids), qid)
        replay_query(
            run_dir,
            spec,
            qid,
            timeout=timeout,
            env_file=env_file,
            refresh_model=(i == 0),
            log=log,
        )
        done += 1
    report = json.loads((run_dir / "report.json").read_text(encoding="utf-8"))
    return {
        "count": done,
        "total": len(qids),
        "pass_count": report["summary"]["pass_count"],
        "num_queries": report["meta"]["num_queries"],
    }


def replay_query(
    run_dir: Path,
    spec: BenchmarkSpec,
    qid: int,
    *,
    timeout: int = DEFAULT_AGENT_TIMEOUT,
    env_file: Path | None = None,
    refresh_model: bool = True,
    log: Callable[[str], None] = print,
) -> dict:
    """Re-run ``qid`` in ``run_dir`` and splice the result over the old one.

    ``refresh_model`` re-seeds the workspace model first, so a fix to the curated
    .preql model lands in the replay; pass False to re-run against the model
    exactly as the original run saw it.

    Returns a summary dict of what changed. Raises :class:`ReplayError` when the
    run dir lacks what a replay needs (report, workspace, database)."""
    report_path = run_dir / "report.json"
    if not report_path.exists():
        raise ReplayError(f"{run_dir.name} has no report.json (not a full eval run)")
    report = json.loads(report_path.read_text(encoding="utf-8"))

    workspace = run_dir / "workspace"
    workspace_db = workspace / spec.db_filename
    if not workspace_db.exists():
        raise ReplayError(
            f"no {spec.db_filename} in {workspace} — workspace was cleaned"
        )

    category = _resolve_category(report)
    meta = report["meta"]
    provider, model = meta["provider"], meta["model"]

    load_env(env_file or REPO_ROOT / ".env.secrets")
    api_env = PROVIDER_ENV.get(provider)
    if not api_env or not os.environ.get(api_env):
        raise ReplayError(f"{api_env or provider} API key is not set")

    # Re-read the prompt catalog so an edited question is what actually re-runs.
    entry = next((p for p in prompts.active_prompts(spec) if p["id"] == qid), None)
    if entry is None:
        raise ReplayError(f"q{qid:02d} is not an active prompt in {spec.prompts_file}")

    prev = next((q for q in report.get("queries", []) if q["id"] == qid), None)
    prev_status = prev["status"] if prev else None

    worker = _worker_dir(workspace, spec.db_filename)
    if refresh_model:
        _refresh_model(workspace, worker, spec, category, report, log)
    task = category.build_task(spec, entry)
    (run_dir / f"task.q{qid:02d}.txt").write_text(task, encoding="utf-8")
    _clear_candidates([workspace, worker], spec, qid, category.candidate_ext)
    (run_dir / f"crash.q{qid:02d}.txt").unlink(missing_ok=True)

    log_path = run_dir / f"agent_log.q{qid:02d}.jsonl"
    log(
        f"q{qid:02d}: running agent ({provider}/{model}, was {prev_status or 'absent'})"
    )
    result = agent_runner.run_agent(
        worker,
        log_path,
        provider,
        model,
        task,
        timeout,
        "quiet",
        toolset=category.harness,
    )
    if result.get("exit_code", 0) != 0 and result.get("output"):
        (run_dir / f"crash.q{qid:02d}.txt").write_text(
            result["output"], encoding="utf-8"
        )

    produced = worker / prompts.candidate_filename(spec, qid, category.candidate_ext)
    if produced.exists():
        destination = workspace / f"query{qid:02d}{category.candidate_ext}"
        shutil.copy2(produced, destination)
        produced.unlink()

    log(
        f"q{qid:02d}: agent exit={result['exit_code']} "
        f"in {result['duration']:.0f}s - scoring"
    )
    metrics = scoring.parse_agent_log(log_path)
    refs = (
        spec.references_dir
        if spec.references_dir and spec.references_dir.exists()
        else None
    )
    try:
        score = scoring.score_query_timed(
            workspace_db,
            workspace,
            qid,
            spec.duckdb_extension,
            SCORE_TIMEOUT,
            params=entry.get("params"),
            custom_refs_dir=refs,
        )
    except Exception as exc:
        score = scoring.QueryResult(
            id=qid, status="error", detail=f"replay-score: {type(exc).__name__}: {exc}"
        )
    timed_out = result.get("timed_out", False)
    exit_code = result.get("exit_code", 0)
    score = scoring.apply_timeout(score, timed_out)
    score = scoring.apply_exhausted(score, exit_code, timed_out)
    score = scoring.apply_crash(score, exit_code, timed_out)

    report["per_query_metrics"] = _upsert(
        _metrics_rows(report, run_dir), {"id": qid, **scoring.metrics_to_dict(metrics)}
    )
    report["queries"] = _upsert(
        report.get("queries", []), {**dataclasses.asdict(score), "source": "this_run"}
    )
    report["per_query"] = _upsert(
        report.get("per_query", []),
        {
            "id": qid,
            "exit_code": exit_code,
            "timed_out": timed_out,
            "duration_seconds": round(result["duration"], 1),
            "source": "this_run",
        },
    )
    _mark_fresh(report, qid)
    _reaggregate(report)
    report.setdefault("replays", []).append(
        {
            "id": qid,
            "timestamp": datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S"),
            "prev_status": prev_status,
            "status": score.status,
            "trilogy_version": meta.get("trilogy_version"),
        }
    )
    _render_artifacts(run_dir, spec, report)

    try:
        _, events = analyze_run.load_run_spliced(run_dir)
        analyze_run.render(
            report, events, spec.charts_dir / f"dashboard_{category.key}_v2.png"
        )
    except Exception as exc:
        log(f"  dashboard render skipped: {type(exc).__name__}: {exc}")

    # ASCII only: this goes to a console that may be cp1252 (Windows).
    log(
        f"q{qid:02d}: {prev_status or 'absent'} -> {score.status}"
        f"  |  pass {report['summary']['pass_count']}/{report['meta']['num_queries']}"
    )
    return {
        "id": qid,
        "prev_status": prev_status,
        "status": score.status,
        "detail": score.detail,
        "iterations": metrics.iterations,
        "prompt_tokens": metrics.prompt_tokens,
        "duration_seconds": round(result["duration"], 1),
        "pass_count": report["summary"]["pass_count"],
        "num_queries": report["meta"]["num_queries"],
    }
