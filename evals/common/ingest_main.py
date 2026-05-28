"""Ingest-mode entry: agent does ``trilogy ingest --all`` + answers a small set
of queries in a single subprocess (no per-query fresh-context restart). Used
to measure how an undirected agent fares with the full workflow."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from . import agent_runner, db, prompts, scoring
from .main import (
    DEFAULT_MODEL,
    OPENROUTER_ROUTING,
    PROVIDER_ENV,
    REPO_ROOT,
    _force_utf8_stdio,
)
from .report import build_report, load_env, render_markdown
from .spec import BenchmarkSpec


def _build_argparser(spec: BenchmarkSpec) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=f"Ingest-mode {spec.name} agent eval",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--model", default=DEFAULT_MODEL, help="LLM model id")
    parser.add_argument("--provider", default="openrouter", help="LLM provider")
    parser.add_argument(
        "--scale-factor",
        type=float,
        default=0.1,
        help=f"{spec.name} scale factor",
    )
    parser.add_argument(
        "--num-queries",
        type=int,
        default=5,
        help="queries the agent must produce on top of `trilogy ingest --all`",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=80,
        help="agent tool-loop budget for the whole run (ingest + queries)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=1500,
        help="agent subprocess timeout for the whole run (seconds)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="run output dir (default: results_ingest/<ts>)",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=REPO_ROOT / ".env.secrets",
        help="env file providing the provider API key",
    )
    parser.add_argument(
        "--monitor",
        choices=["feed", "raw", "both", "quiet"],
        default="feed",
        help="live monitoring while the agent runs",
    )
    # Per-query mode supports --enriched-model-dir; ingest mode doesn't (the
    # whole point is to have the agent run ingest). Stub to satisfy
    # build_report's expectation.
    parser.set_defaults(enriched_model_dir=None)
    return parser


def run(spec: BenchmarkSpec) -> int:
    args = _build_argparser(spec).parse_args()
    _force_utf8_stdio()

    load_env(args.env_file)
    if args.provider == "openrouter":
        os.environ.setdefault("OPENROUTER_PROVIDER", json.dumps(OPENROUTER_ROUTING))
        os.environ.setdefault("OPENROUTER_SANITIZE_HTML_ESCAPES", "true")
    api_env = PROVIDER_ENV.get(args.provider, "OPENROUTER_API_KEY")
    if not os.environ.get(api_env):
        print(
            f"ERROR: {api_env} is not set (looked in env and {args.env_file}).",
            file=sys.stderr,
        )
        return 2

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_dir = args.output_dir or (spec.eval_dir / "results_ingest" / timestamp)
    workspace = run_dir / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "agent_log.jsonl"

    print(f"[1/4] Building {spec.name} DuckDB (sf={args.scale_factor:g}) ...")
    cached = db.build_database(spec, args.scale_factor)
    workspace_db = db.copy_database(cached, workspace / spec.db_filename)

    agent_runner.write_trilogy_toml(
        workspace, spec, args.provider, args.model, args.max_iterations
    )
    active = prompts.active_prompts(spec)[: args.num_queries]
    query_ids = [p["id"] for p in active]
    task = prompts.build_task(spec, args.num_queries)
    (run_dir / "task.txt").write_text(task, encoding="utf-8")

    print(
        f"[2/4] Running Trilogy agent ({args.provider}/{args.model}) — "
        f"ingest + {len(active)} queries in one shot ..."
    )
    agent_run = agent_runner.run_agent(
        workspace,
        log_path,
        args.provider,
        args.model,
        task,
        args.timeout,
        args.monitor,
    )
    (run_dir / "agent_output.txt").write_text(agent_run["output"], encoding="utf-8")

    print(f"[3/4] Scoring {len(query_ids)} queries against {spec.name} reference ...")
    metrics = scoring.parse_agent_log(log_path)
    try:
        query_results = scoring.score_queries(
            workspace_db, workspace, query_ids, spec.duckdb_extension
        )
    except Exception as exc:
        print(f"  scoring aborted: {type(exc).__name__}: {exc}", file=sys.stderr)
        query_results = [
            scoring.QueryResult(id=i, status="error", detail="scoring aborted")
            for i in query_ids
        ]

    report = build_report(spec, args, timestamp, agent_run, metrics, query_results)
    report["mode"] = "ingest+queries"
    (run_dir / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    markdown = render_markdown(spec, report)
    (run_dir / "report.md").write_text(markdown, encoding="utf-8")

    print(f"[4/4] Done. Artifacts in {run_dir}\n")
    print(markdown)
    return 0
