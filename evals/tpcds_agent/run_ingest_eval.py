"""Trilogy ingest + small-query eval.

A separate runner that exercises the full agent pipeline INCLUDING the
`trilogy ingest --all` step inside a single agent invocation. The query set is
small by default (5) so the conversation stays bounded.

This complements `run_eval.py` (per-query mode), which pre-ingests outside the
agent and tests one query at a time in fresh contexts. This eval is the
inverse — it covers "can the agent stand up a model from scratch *and* answer
some questions on it?".

Reuses ``run_agent``, ``build_report``, ``render_markdown`` from ``run_eval``,
and the shared ``db`` / ``prompts`` / ``scoring`` modules.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

EVAL_DIR = Path(__file__).resolve().parent
REPO_ROOT = EVAL_DIR.parents[1]

import db  # noqa: E402
import prompts  # noqa: E402
import scoring  # noqa: E402
from run_eval import (  # noqa: E402
    DEFAULT_MODEL,
    OPENROUTER_ROUTING,
    PROVIDER_ENV,
    _force_utf8_stdio,
    build_report,
    load_env,
    render_markdown,
    run_agent,
    write_trilogy_toml,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--model", default=DEFAULT_MODEL, help="LLM model id")
    parser.add_argument("--provider", default="openrouter", help="LLM provider")
    parser.add_argument(
        "--scale-factor", type=float, default=0.1, help="TPC-DS scale factor"
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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    _force_utf8_stdio()

    load_env(args.env_file)
    if args.provider == "openrouter":
        os.environ.setdefault("OPENROUTER_PROVIDER", json.dumps(OPENROUTER_ROUTING))
    api_env = PROVIDER_ENV.get(args.provider, "OPENROUTER_API_KEY")
    if not os.environ.get(api_env):
        print(
            f"ERROR: {api_env} is not set (looked in env and {args.env_file}).",
            file=sys.stderr,
        )
        return 2

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_dir = args.output_dir or (EVAL_DIR / "results_ingest" / timestamp)
    workspace = run_dir / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "agent_log.jsonl"

    print(f"[1/4] Building TPC-DS DuckDB (sf={args.scale_factor:g}) ...")
    cached = db.build_database(args.scale_factor, EVAL_DIR / ".cache")
    workspace_db = db.copy_database(cached, workspace / "tpcds.duckdb")

    write_trilogy_toml(workspace, args.provider, args.model, args.max_iterations)
    active = prompts.active_prompts()[: args.num_queries]
    query_ids = [p["id"] for p in active]
    task = prompts.build_task(args.num_queries)
    (run_dir / "task.txt").write_text(task, encoding="utf-8")

    print(
        f"[2/4] Running Trilogy agent ({args.provider}/{args.model}) — "
        f"ingest + {len(active)} queries in one shot ..."
    )
    agent_run = run_agent(
        workspace,
        log_path,
        args.provider,
        args.model,
        task,
        args.timeout,
        args.monitor,
    )
    (run_dir / "agent_output.txt").write_text(agent_run["output"], encoding="utf-8")

    print(f"[3/4] Scoring {len(query_ids)} queries against TPC-DS reference ...")
    metrics = scoring.parse_agent_log(log_path)
    try:
        query_results = scoring.score_queries(workspace_db, workspace, query_ids)
    except Exception as exc:
        print(f"  scoring aborted: {type(exc).__name__}: {exc}", file=sys.stderr)
        query_results = [
            scoring.QueryResult(id=i, status="error", detail="scoring aborted")
            for i in query_ids
        ]

    report = build_report(args, timestamp, agent_run, metrics, query_results)
    report["mode"] = "ingest+queries"
    (run_dir / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    markdown = render_markdown(report)
    (run_dir / "report.md").write_text(markdown, encoding="utf-8")

    print(f"[4/4] Done. Artifacts in {run_dir}\n")
    print(markdown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
