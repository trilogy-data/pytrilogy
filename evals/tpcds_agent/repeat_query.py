#!/usr/bin/env python
"""Run ONE benchmark query through the agent N times for sample-size analysis.

The full eval (`run_eval.py`) runs each query once, so a single exhausted/flaky
result is noise. This harness fixes the query and the model and repeats the
agent run, then reports the distribution of outcomes (status, iterations,
prompt tokens, explore count, whether a query file was ever written). Use it to
A/B a prompt/harness change against a baseline:

    python repeat_query.py --query-id 1 --repeats 10          # baseline
    # ...make a change...
    python repeat_query.py --query-id 1 --repeats 10          # compare

DB build is cached by scale factor and the enriched model is seeded once, so
repeats only pay for the agent run + scoring.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import agent_runner, db, prompts, scoring  # noqa: E402
from common.main import DEFAULT_MODEL, DEFAULT_PROVIDER, PROVIDER_ENV  # noqa: E402
from common.report import load_env  # noqa: E402
from spec import SPEC  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]


def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Repeat one agent query N times")
    p.add_argument("--query-id", type=int, default=1)
    p.add_argument("--repeats", type=int, default=10)
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--provider", default=DEFAULT_PROVIDER)
    p.add_argument("--scale-factor", type=float, default=0.1)
    p.add_argument("--max-iterations", type=int, default=50)
    p.add_argument("--timeout", type=int, default=900)
    p.add_argument("--concurrency", type=int, default=3)
    p.add_argument(
        "--base",
        action="store_true",
        help="ingest-only model instead of the enriched hand-curated dir",
    )
    p.add_argument("--output-dir", type=Path, default=None)
    p.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env.secrets")
    return p


def _seed_model(workspace: Path, base: bool) -> None:
    if base:
        result = agent_runner.run_pre_ingest(workspace)
    else:
        result = agent_runner.install_enriched_model(
            workspace, SPEC.default_enriched_dir, SPEC.enriched_skip_prefixes
        )
    if result["exit_code"] != 0:
        raise SystemExit(f"model seed failed: {result['stderr'][:500]}")


def _summarize(records: list[dict]) -> dict:
    """Distribution of outcomes across repeats — the numbers we compare runs on."""

    def stats(key: str) -> dict:
        vals = [r[key] for r in records if r.get(key) is not None]
        if not vals:
            return {}
        return {
            "mean": round(statistics.mean(vals), 1),
            "median": round(statistics.median(vals), 1),
            "min": min(vals),
            "max": max(vals),
        }

    n = len(records)
    statuses = Counter(r["status"] for r in records)
    return {
        "repeats": n,
        "status_breakdown": dict(statuses),
        "pass_rate": round(statuses.get("pass", 0) / n, 3) if n else 0.0,
        "exhausted_rate": round(statuses.get("exhausted", 0) / n, 3) if n else 0.0,
        "wrote_query_rate": (
            round(sum(r["wrote_query"] for r in records) / n, 3) if n else 0.0
        ),
        "iterations": stats("iterations"),
        "prompt_tokens": stats("prompt_tokens"),
        "explore_calls": stats("explore_calls"),
        "duration_seconds": stats("duration_seconds"),
    }


def main() -> int:
    args = _build_argparser().parse_args()
    load_env(args.env_file)
    api_env = PROVIDER_ENV.get(args.provider)
    if not api_env or not _env_has(api_env):
        print(f"ERROR: {api_env} not set (looked in {args.env_file})", file=sys.stderr)
        return 2

    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    qid = args.query_id
    # Resolve to absolute: the agent subprocess runs with cwd=worker, so a
    # relative --output-dir would make the --log-file path resolve under the
    # worker dir (nonexistent nested parents) and crash the agent on startup.
    out = (
        args.output_dir
        or SPEC.results_dir / f"repeat_q{qid:02d}_{ts}{'_base' if args.base else ''}"
    ).resolve()
    workspace = out / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)

    print(f"[1/3] Building TPC-DS DuckDB (sf={args.scale_factor:g}) ...")
    cached = db.build_database(SPEC, args.scale_factor)
    workspace_db = db.copy_database(cached, workspace / SPEC.db_filename)
    agent_runner.write_trilogy_toml(
        workspace, SPEC, args.provider, args.model, args.max_iterations
    )
    print(f"[2/3] Seeding {'base (ingest)' if args.base else 'enriched'} model ...")
    _seed_model(workspace, args.base)

    entry = next(p for p in prompts.active_prompts(SPEC) if p["id"] == qid)
    task = prompts.build_single_query_task(SPEC, entry)
    (out / f"task.q{qid:02d}.txt").write_text(task, encoding="utf-8")

    # Materialise per-worker workspaces BEFORE opening the scoring engine —
    # the engine takes a read-write lock on workspace_db, which would block the
    # file copy underlying prepare_worker_workspace on Windows.
    concurrency = max(1, min(args.concurrency, args.repeats))
    workers = [
        agent_runner.prepare_worker_workspace(workspace, i, SPEC.db_filename)
        for i in range(concurrency)
    ]
    engine = scoring.make_scoring_engine(workspace_db, workspace, SPEC.duckdb_extension)
    refs = SPEC.references_dir if SPEC.references_dir.exists() else None
    pool: list[Path] = list(workers)
    pool_lock = threading.Lock()
    score_lock = threading.Lock()
    records: list[dict] = [None] * args.repeats  # type: ignore[list-item]

    def run_one(rep: int) -> None:
        with pool_lock:
            worker = pool.pop()
        try:
            produced = worker / f"query{qid:02d}.preql"
            produced.unlink(missing_ok=True)  # never score a prior repeat's file
            log_path = out / f"agent_log.q{qid:02d}.r{rep:02d}.jsonl"
            result = agent_runner.run_agent(
                worker, log_path, args.provider, args.model, task, args.timeout, "quiet"
            )
            metrics = scoring.parse_agent_log(log_path)
            wrote = produced.exists()
            with score_lock:
                try:
                    score = scoring.score_query(
                        engine,
                        worker,
                        qid,
                        SPEC.duckdb_extension,
                        params=entry.get("params"),
                        custom_refs_dir=refs,
                    )
                except Exception as exc:
                    score = scoring.QueryResult(
                        id=qid, status="error", detail=f"{type(exc).__name__}: {exc}"
                    )
            score = scoring.apply_timeout(score, result.get("timed_out", False))
            score = scoring.apply_exhausted(
                score, result.get("exit_code", 0), result.get("timed_out", False)
            )
            records[rep] = {
                "rep": rep,
                "status": score.status,
                "detail": score.detail,
                "wrote_query": wrote,
                "exit_code": result["exit_code"],
                "timed_out": result.get("timed_out", False),
                "iterations": metrics.iterations,
                "prompt_tokens": metrics.prompt_tokens,
                "total_tokens": metrics.total_tokens,
                "explore_calls": metrics.trilogy_subcommands.get("explore", 0),
                "repeated_calls": sum(metrics.repeated_calls_by_name.values()),
                "duration_seconds": round(result["duration"], 1),
            }
            print(
                f"  [r{rep:02d}] {score.status:9} iters={metrics.iterations:<3}"
                f" prompt_tok={metrics.prompt_tokens:<8} explore={records[rep]['explore_calls']:<3}"
                f" wrote={wrote} ({result['duration']:.0f}s)",
                flush=True,
            )
        finally:
            with pool_lock:
                pool.append(worker)

    print(f"[3/3] Running q{qid:02d} x{args.repeats} (concurrency={concurrency}) ...")
    if concurrency == 1:
        for rep in range(args.repeats):
            run_one(rep)
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            for fut in as_completed(ex.submit(run_one, r) for r in range(args.repeats)):
                fut.result()

    summary = _summarize([r for r in records if r is not None])
    report = {
        "meta": {
            "query_id": qid,
            "repeats": args.repeats,
            "model": args.model,
            "provider": args.provider,
            "scale_factor": args.scale_factor,
            "max_iterations": args.max_iterations,
            "mode": "base" if args.base else "enriched",
            "trilogy_version": _trilogy_version(),
        },
        "summary": summary,
        "runs": [r for r in records if r is not None],
    }
    (out / "repeat_report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    print("\n=== SUMMARY ===")
    print(json.dumps(summary, indent=2))
    print(f"\nArtifacts in {out}")
    return 0


def _env_has(name: str) -> bool:
    import os

    return bool(os.environ.get(name))


def _trilogy_version() -> str:
    import trilogy

    return getattr(trilogy, "__version__", "?")


if __name__ == "__main__":
    raise SystemExit(main())
