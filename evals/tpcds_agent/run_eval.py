#!/usr/bin/env python
"""On-demand eval: drive the Trilogy agent CLI through a TPC-DS modeling task
and score the result.

This is NOT part of the normal test suite -- it depends on a live LLM and is
run manually. See evals/README.md.

    python evals/tpcds_agent/run_eval.py [--model ...] [--scale-factor ...]
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import subprocess
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import db
import monitor
import prompts
import scoring

EVAL_DIR = Path(__file__).resolve().parent
REPO_ROOT = EVAL_DIR.parents[1]
DEFAULT_MODEL = "deepseek/deepseek-v4-flash"

PROVIDER_ENV = {
    "openrouter": "OPENROUTER_API_KEY",
    "anthropic": "ANTHROPIC_API_KEY",
    "openai": "OPENAI_API_KEY",
    "google": "GOOGLE_API_KEY",
}

# OpenRouter multiplexes a model across providers and one (AtlasCloud) hard-400s
# our tool requests. Pin routing so runs are reliable and reproducible. The
# trilogy OpenRouter provider reads this from the OPENROUTER_PROVIDER env var.
OPENROUTER_ROUTING = {
    "order": ["DeepInfra"],
    "ignore": ["AtlasCloud"],
    "allow_fallbacks": True,
}


def _force_utf8_stdio() -> None:
    """Survive redirection on Windows, where stdout defaults to cp1252 and
    chokes on non-ASCII in the feed and report."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass


def _trilogy_version() -> str:
    try:
        from trilogy import __version__

        return str(__version__)
    except Exception:
        return "unknown"


def write_trilogy_toml(
    workspace: Path, provider: str, model: str, max_iterations: int
) -> None:
    api_key_env = PROVIDER_ENV.get(provider, "OPENROUTER_API_KEY")
    workspace.joinpath("trilogy.toml").write_text(
        f"""\
[engine]
dialect = "duck_db"

[engine.config]
db_location = "tpcds.duckdb"

[agent]
provider = "{provider}"
model = "{model}"
api_key_env = "{api_key_env}"
max_iterations = {max_iterations}
# agent-info is ~26KB and carries the Trilogy language reference; the default
# 8KB limit middle-truncates the syntax rules away, so give it real headroom.
tool_output_limit = 32768
""",
        encoding="utf-8",
    )


def load_env(env_file: Path) -> None:
    """Populate os.environ from an .env file without overriding existing vars."""
    if not env_file.exists():
        return
    from trilogy.execution.config import load_env_file

    for key, value in (load_env_file(env_file) or {}).items():
        os.environ.setdefault(key, value)


HEARTBEAT_INTERVAL = 30.0
POLL_INTERVAL = 0.3


def _pump_output(stream, sink: list[str], echo: bool) -> None:
    """Drain the subprocess output stream into ``sink``, optionally echoing it
    to the console as it arrives."""
    for line in stream:
        sink.append(line)
        if echo:
            sys.stdout.write(line)
            sys.stdout.flush()
    stream.close()


def run_agent(
    workspace: Path,
    log_path: Path,
    provider: str,
    model: str,
    task: str,
    timeout: int,
    monitor_mode: str,
) -> dict:
    cmd = [
        sys.executable,
        "-m",
        "trilogy.scripts.trilogy",
        "agent",
        "--provider",
        provider,
        "--model",
        model,
        "--log-file",
        str(log_path),
        task,
    ]
    start = time.perf_counter()
    proc = subprocess.Popen(
        cmd,
        cwd=workspace,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    out_lines: list[str] = []
    pump = threading.Thread(
        target=_pump_output,
        args=(proc.stdout, out_lines, monitor_mode in ("raw", "both")),
        daemon=True,
    )
    pump.start()

    track = monitor_mode in ("feed", "both")
    state = monitor.ProgressState(start=start)
    processed = 0
    last_beat = start
    timed_out = False

    while proc.poll() is None:
        if track:
            processed = monitor.drain_feed(
                log_path, processed, state, emit=monitor_mode == "feed"
            )
        if monitor_mode == "both" and time.perf_counter() - last_beat >= (
            HEARTBEAT_INTERVAL
        ):
            print(monitor.heartbeat(state), flush=True)
            last_beat = time.perf_counter()
        if time.perf_counter() - start > timeout:
            timed_out = True
            proc.kill()
            break
        time.sleep(POLL_INTERVAL)

    proc.wait()
    pump.join(timeout=5)
    if track:
        monitor.drain_feed(log_path, processed, state, emit=monitor_mode == "feed")
        print(monitor.heartbeat(state), flush=True)

    output = "".join(out_lines)
    if timed_out:
        output += f"\n[eval] agent timed out after {timeout}s\n"
    return {
        "exit_code": -1 if timed_out else proc.returncode,
        "timed_out": timed_out,
        "duration": time.perf_counter() - start,
        "output": output,
    }


def build_report(
    args: argparse.Namespace,
    timestamp: str,
    agent_run: dict,
    metrics: scoring.AgentMetrics,
    query_results: list[scoring.QueryResult],
) -> dict:
    statuses: Counter[str] = Counter(q.status for q in query_results)
    pass_count = statuses.get("pass", 0)
    total = len(query_results)
    return {
        "meta": {
            "timestamp": timestamp,
            "model": args.model,
            "provider": args.provider,
            "scale_factor": args.scale_factor,
            "num_queries": total,
            "max_iterations": args.max_iterations,
            "trilogy_version": _trilogy_version(),
            "openrouter_routing": os.environ.get("OPENROUTER_PROVIDER"),
        },
        "agent": {
            "exit_code": agent_run["exit_code"],
            "timed_out": agent_run["timed_out"],
            "duration_seconds": round(agent_run["duration"], 1),
            "iterations": metrics.iterations,
            "tool_calls_total": metrics.tool_calls_total,
            "tool_calls_by_name": metrics.tool_calls_by_name,
            "trilogy_subcommands": metrics.trilogy_subcommands,
            "tool_results_total": metrics.tool_results_total,
            "tool_errors": metrics.tool_errors,
            "tool_success_rate": round(metrics.tool_success_rate, 3),
            "tokens": {
                "prompt": metrics.prompt_tokens,
                "completion": metrics.completion_tokens,
                "total": metrics.total_tokens,
            },
            "farewell": metrics.farewell,
        },
        "queries": [dataclasses.asdict(q) for q in query_results],
        "summary": {
            "pass_count": pass_count,
            "pass_rate": round(pass_count / total, 3) if total else 0.0,
            "status_breakdown": dict(statuses),
        },
    }


def render_markdown(report: dict) -> str:
    meta, agent, summary = report["meta"], report["agent"], report["summary"]
    out: list[str] = []
    out.append(f"# TPC-DS Agent Eval — {meta['timestamp']}")
    out.append("")
    out.append(f"- Model: `{meta['provider']}/{meta['model']}`")
    out.append(
        f"- Scale factor: {meta['scale_factor']:g}  |  "
        f"Queries: {meta['num_queries']}  |  Trilogy: {meta['trilogy_version']}"
    )
    out.append("")
    out.append("## Result")
    out.append("")
    out.append(
        f"- **Pass rate: {summary['pass_count']}/{meta['num_queries']}** "
        f"({summary['pass_rate'] * 100:.0f}%)"
    )
    out.append(f"- Status breakdown: {summary['status_breakdown']}")
    timed = "  (TIMED OUT)" if agent["timed_out"] else ""
    out.append(f"- Agent wall time: {agent['duration_seconds']}s{timed}")
    out.append(f"- Agent exit code: {agent['exit_code']}")
    out.append("")
    out.append("## Agent harness metrics")
    out.append("")
    out.append(f"- LLM iterations: {agent['iterations']}")
    out.append(
        f"- Tool calls: {agent['tool_calls_total']}  →  {agent['tool_calls_by_name']}"
    )
    out.append(f"- `trilogy` subcommands: {agent['trilogy_subcommands']}")
    ok = agent["tool_results_total"] - agent["tool_errors"]
    out.append(
        f"- Tool success rate: {agent['tool_success_rate'] * 100:.0f}% "
        f"({ok}/{agent['tool_results_total']} ok)"
    )
    tok = agent["tokens"]
    out.append(
        f"- Tokens: {tok['total']} total "
        f"({tok['prompt']} prompt + {tok['completion']} completion)"
    )
    out.append("")
    out.append("## Per-query")
    out.append("")
    out.append("| Query | Status | Ref rows | Cand rows | SQL len | Detail |")
    out.append("|------:|:-------|---------:|----------:|--------:|:-------|")
    for q in report["queries"]:
        out.append(
            f"| {q['id']:02d} | {q['status']} | {q['ref_rows']} | {q['cand_rows']} "
            f"| {q['generated_sql_len']} | {q['detail'][:80]} |"
        )
    out.append("")
    if agent["farewell"]:
        out.append("## Agent final message")
        out.append("")
        out.append("> " + agent["farewell"].replace("\n", "\n> "))
        out.append("")
    return "\n".join(out)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--model", default=DEFAULT_MODEL, help="LLM model id")
    parser.add_argument("--provider", default="openrouter", help="LLM provider")
    parser.add_argument(
        "--scale-factor", type=float, default=0.01, help="TPC-DS scale factor"
    )
    parser.add_argument(
        "--num-queries", type=int, default=10, help="number of queries to attempt"
    )
    parser.add_argument(
        "--max-iterations", type=int, default=120, help="agent tool-loop budget"
    )
    parser.add_argument(
        "--timeout", type=int, default=2400, help="agent subprocess timeout (seconds)"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="run output dir (default: results/<ts>)",
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
        help="live monitoring while the agent runs: feed=parsed progress feed, "
        "raw=agent stdout passthrough, both=raw + periodic tally heartbeat, "
        "quiet=phase markers only",
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
    run_dir = args.output_dir or (EVAL_DIR / "results" / timestamp)
    workspace = run_dir / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "agent_log.jsonl"

    print(f"[1/4] Building TPC-DS DuckDB (sf={args.scale_factor:g}) ...")
    cached = db.build_database(args.scale_factor, EVAL_DIR / ".cache")
    workspace_db = db.copy_database(cached, workspace / "tpcds.duckdb")

    write_trilogy_toml(workspace, args.provider, args.model, args.max_iterations)
    query_ids = prompts.selected_ids(args.num_queries)
    task = prompts.build_task(args.num_queries)
    (run_dir / "task.txt").write_text(task, encoding="utf-8")

    print(f"[2/4] Running Trilogy agent ({args.provider}/{args.model}) ...")
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
    (run_dir / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    markdown = render_markdown(report)
    (run_dir / "report.md").write_text(markdown, encoding="utf-8")

    print(f"[4/4] Done. Artifacts in {run_dir}\n")
    print(markdown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
