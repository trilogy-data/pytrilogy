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
import re
import shutil
import subprocess
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import analyze_run
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
# Narration messages compound quadratically through history replays in long
# unattended runs; the eval drops show_message entirely.
quiet = true
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


def prepare_worker_workspace(src: Path, worker_idx: int) -> Path:
    """Materialise a self-contained per-worker copy of the eval workspace.

    DuckDB takes an exclusive file lock on open; parallel agents pointing at the
    same database would serialise. Each worker gets its own copy of the duckdb,
    its own `raw/`, and its own `trilogy.toml`."""
    worker_dir = src / f"_worker_{worker_idx}"
    worker_dir.mkdir(exist_ok=True)
    shutil.copy2(src / "tpcds.duckdb", worker_dir / "tpcds.duckdb")
    shutil.copy2(src / "trilogy.toml", worker_dir / "trilogy.toml")
    worker_raw = worker_dir / "raw"
    if worker_raw.exists():
        shutil.rmtree(worker_raw)
    shutil.copytree(src / "raw", worker_raw)
    return worker_dir


_ADDRESS_DB_PREFIX = re.compile(r"^(address\s+)([A-Za-z_]\w*)\.", re.MULTILINE)


def install_enriched_model(workspace: Path, src_dir: Path) -> dict:
    """Skip ingest and seed ``workspace/raw/`` from a hand-curated model directory.

    Used by the enriched-eval variant to measure how much a richer semantic layer
    (concept descriptions, derived metrics, named import aliases) lifts the
    agent above what bare ``trilogy ingest --all`` produces. Only top-level
    ``*.preql`` files are copied — ``query*.preql`` and ``adhoc*.preql`` are
    skipped so we hand the agent a model, not the reference answers.

    The source models were authored against an in-memory DuckDB (``address
    memory.<table>``), but the eval workspace's ``tpcds.duckdb`` attaches as a
    different database name — so we strip the leading database qualifier on
    each ``address`` line as we copy. Tables resolve via DuckDB's default
    catalog search path, which is what we want for an arbitrary db file."""
    raw = workspace / "raw"
    if raw.exists():
        shutil.rmtree(raw)
    raw.mkdir(parents=True)
    start = time.perf_counter()
    copied: list[str] = []
    rewrites = 0
    for path in sorted(src_dir.glob("*.preql")):
        name = path.name
        if name.startswith("query") or name.startswith("adhoc"):
            continue
        text = path.read_text(encoding="utf-8")
        new_text, n = _ADDRESS_DB_PREFIX.subn(r"\1", text)
        rewrites += n
        (raw / name).write_text(new_text, encoding="utf-8")
        copied.append(name)
    return {
        "exit_code": 0,
        "duration": time.perf_counter() - start,
        "stdout": (
            f"copied {len(copied)} files from {src_dir} "
            f"(stripped db-qualifier on {rewrites} address lines): {copied}\n"
        ),
        "stderr": "",
    }


def run_pre_ingest(workspace: Path, timeout: int = 600) -> dict:
    """Run ``trilogy ingest --all`` once before any agent invocations.

    Per-query mode hands each agent a populated ``raw/``, so the agent doesn't
    burn iterations rebuilding the model (and we eliminate ingest variability
    as a confound)."""
    cmd = [
        sys.executable,
        "-m",
        "trilogy.scripts.trilogy",
        "ingest",
        "--all",
    ]
    start = time.perf_counter()
    proc = subprocess.run(
        cmd,
        cwd=workspace,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )
    return {
        "exit_code": proc.returncode,
        "duration": time.perf_counter() - start,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
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
            "model_source": (
                f"enriched:{args.enriched_model_dir}"
                if args.enriched_model_dir
                else "ingest"
            ),
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
            "repeated_calls_by_name": metrics.repeated_calls_by_name,
            "tool_output_stats": {
                tool: {
                    "count": s.count,
                    "truncated": s.truncated,
                    "truncation_rate": round(s.truncation_rate, 3),
                    "avg_chars": round(s.avg_chars, 1),
                    "max_chars": s.max_chars,
                }
                for tool, s in metrics.tool_output_stats.items()
            },
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
    stats = agent.get("tool_output_stats") or {}
    if stats:
        out.append("## Tool-output sizes")
        out.append("")
        out.append("| Tool | calls | avg chars | max chars | truncated | trunc-rate |")
        out.append("|---|---:|---:|---:|---:|---:|")
        for tool, s in sorted(stats.items(), key=lambda x: -x[1]["count"]):
            out.append(
                f"| `{tool}` | {s['count']} | {s['avg_chars']:.0f} | "
                f"{s['max_chars']} | {s['truncated']} | "
                f"{s['truncation_rate'] * 100:.0f}% |"
            )
        out.append("")
    repeats = agent.get("repeated_calls_by_name") or {}
    if repeats:
        out.append(
            f"- Repeated calls (same args seen earlier in same query): "
            f"{repeats} — total {sum(repeats.values())}"
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
        "--num-queries", type=int, default=20, help="number of queries to attempt"
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=50,
        help="agent tool-loop budget PER QUERY (each query is a fresh agent)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="agent subprocess timeout PER QUERY (seconds)",
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
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="number of agents to run in parallel; >1 forces monitor=quiet and "
        "uses a per-worker workspace (copy of the duckdb + raw/) so DuckDB's "
        "file lock doesn't serialise the workers",
    )
    parser.add_argument(
        "--enriched-model-dir",
        type=Path,
        default=None,
        help="if set, skip `trilogy ingest --all` and seed workspace/raw/ from "
        "this directory of hand-curated *.preql files (e.g. "
        "tests/modeling/tpc_ds_duckdb). Used to measure the delta a richer "
        "semantic layer provides over bare ingest.",
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
    print(f"[1/5] Building TPC-DS DuckDB (sf={args.scale_factor:g}) ...")
    cached = db.build_database(args.scale_factor, EVAL_DIR / ".cache")
    workspace_db = db.copy_database(cached, workspace / "tpcds.duckdb")

    write_trilogy_toml(workspace, args.provider, args.model, args.max_iterations)
    active = prompts.active_prompts()[: args.num_queries]
    query_ids = [p["id"] for p in active]

    if args.enriched_model_dir:
        print(
            f"[2/5] Seeding raw/ from enriched model dir {args.enriched_model_dir} ..."
        )
        ingest = install_enriched_model(workspace, args.enriched_model_dir)
    else:
        print("[2/5] Pre-ingesting model with `trilogy ingest --all` ...")
        ingest = run_pre_ingest(workspace)
    (run_dir / "ingest_output.txt").write_text(
        f"exit={ingest['exit_code']}  duration={ingest['duration']:.1f}s\n"
        f"--- stdout ---\n{ingest['stdout']}\n--- stderr ---\n{ingest['stderr']}\n",
        encoding="utf-8",
    )
    if ingest["exit_code"] != 0:
        print(
            f"  ingest failed (exit {ingest['exit_code']}); see ingest_output.txt",
            file=sys.stderr,
        )
        # Per-query tasks tell the agent `raw/` is already populated; without it
        # every query starts from a broken premise. Abort instead of grading 0/N.
        return 2

    concurrency = max(1, args.concurrency)
    monitor_mode = "quiet" if concurrency > 1 else args.monitor
    print(
        f"[3/5] Running agent per query ({len(active)} queries, fresh context each,"
        f" concurrency={concurrency}) — {args.provider}/{args.model} ..."
    )
    if concurrency > 1:
        worker_dirs = [
            prepare_worker_workspace(workspace, i) for i in range(concurrency)
        ]
    else:
        worker_dirs = [workspace]

    per_query_runs: list[dict] = [None] * len(active)  # type: ignore[list-item]
    per_query_metrics: list[scoring.AgentMetrics] = [
        scoring.AgentMetrics() for _ in active
    ]
    per_query_scores: list[scoring.QueryResult | None] = [None] * len(active)
    worker_pool: "list[Path]" = list(worker_dirs)
    pool_lock = threading.Lock()

    def acquire_worker() -> Path:
        with pool_lock:
            return worker_pool.pop()

    def release_worker(worker: Path) -> None:
        with pool_lock:
            worker_pool.append(worker)

    # Enriched runs are useful to compare directly against base runs; give
    # their committed artifacts a distinct suffix so they don't clobber the
    # base baseline in charts/.
    suffix = "_enriched" if args.enriched_model_dir else ""

    # --- live dashboard machinery ---------------------------------------
    # Build the scoring engine ONCE and reuse it per query. Reusing avoids
    # paying engine setup + `INSTALL tpcds; LOAD tpcds;` on every grade.
    try:
        scoring_engine = scoring.make_scoring_engine(workspace_db, workspace)
    except Exception as exc:
        scoring_engine = None
        print(
            f"  scoring engine init failed; live dashboard disabled: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
    scoring_lock = threading.Lock()
    render_lock = threading.Lock()
    last_render = [0.0]  # one-element list so the closure can mutate it
    RENDER_MIN_INTERVAL = 10.0  # seconds — debounce so matplotlib isn't hot

    def maybe_render_dashboard(force: bool = False) -> None:
        if scoring_engine is None:
            return
        with render_lock:
            now = time.perf_counter()
            if not force and now - last_render[0] < RENDER_MIN_INTERVAL:
                return
            last_render[0] = now
            graded = [s for s in per_query_scores if s is not None]
            done_runs = [r for r in per_query_runs if r is not None]
            agent_run_partial = {
                "exit_code": 0,
                "timed_out": any(r.get("timed_out") for r in done_runs),
                "duration": sum(r["duration"] for r in done_runs),
                "output": "",
            }
            metrics_partial = scoring.aggregate_metrics(per_query_metrics)
            try:
                partial_report = build_report(
                    args, timestamp, agent_run_partial, metrics_partial, graded
                )
                # In-progress queries land in build_report as a smaller total
                # than meta.num_queries — patch the headline so the dashboard
                # title reflects the live denominator.
                partial_report["meta"]["num_queries"] = len(active)
                _, events = analyze_run.load_run_events(run_dir)
                analyze_run.render(
                    partial_report,
                    events,
                    analyze_run.CHARTS_DIR / f"dashboard{suffix}.png",
                )
            except Exception as exc:
                # Never let a render failure kill the run — chart is best-effort.
                print(
                    f"  live dashboard render skipped: {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )

    def run_one(index: int, entry: dict) -> None:
        qid = entry["id"]
        worker = acquire_worker()
        try:
            log_path = run_dir / f"agent_log.q{qid:02d}.jsonl"
            task = prompts.build_single_query_task(entry)
            (run_dir / f"task.q{qid:02d}.txt").write_text(task, encoding="utf-8")
            print(f"  [q{qid:02d}] starting (worker {worker.name})", flush=True)
            result = run_agent(
                worker,
                log_path,
                args.provider,
                args.model,
                task,
                args.timeout,
                monitor_mode,
            )
            result["id"] = qid
            if worker != workspace:
                produced = worker / f"query{qid:02d}.preql"
                if produced.exists():
                    shutil.copy2(produced, workspace / f"query{qid:02d}.preql")
                    produced.unlink()
            per_query_runs[index] = result
            per_query_metrics[index] = scoring.parse_agent_log(log_path)
            # Inline-score so the live dashboard reflects pass/fail rather than
            # just "agent finished". Lock around the shared engine.
            if scoring_engine is not None:
                try:
                    with scoring_lock:
                        per_query_scores[index] = scoring.score_query(
                            scoring_engine, workspace, qid
                        )
                except Exception as exc:
                    per_query_scores[index] = scoring.QueryResult(
                        id=qid,
                        status="error",
                        detail=f"inline-score: {type(exc).__name__}: {exc}",
                    )
            status = per_query_scores[index].status if per_query_scores[index] else "?"
            print(
                f"  [q{qid:02d}] done in {result['duration']:.0f}s"
                f" (exit {result['exit_code']}, score={status})",
                flush=True,
            )
            maybe_render_dashboard()
        finally:
            release_worker(worker)

    if concurrency == 1:
        for i, entry in enumerate(active):
            run_one(i, entry)
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            futures = [ex.submit(run_one, i, e) for i, e in enumerate(active)]
            for fut in as_completed(futures):
                # Surface worker exceptions instead of swallowing them in futures.
                fut.result()

    metrics = scoring.aggregate_metrics(per_query_metrics)
    agent_run = {
        "exit_code": 0 if all(r["exit_code"] == 0 for r in per_query_runs) else 1,
        "timed_out": any(r.get("timed_out") for r in per_query_runs),
        "duration": sum(r["duration"] for r in per_query_runs),
        "output": "\n".join(
            f"=== q{r['id']:02d} (exit {r['exit_code']}, {r['duration']:.0f}s) ===\n"
            f"{r['output']}"
            for r in per_query_runs
        ),
    }
    (run_dir / "agent_output.txt").write_text(agent_run["output"], encoding="utf-8")

    print(f"[4/5] Finalising scores for {len(query_ids)} queries ...")
    # Queries were scored inline as agents finished; backfill any that never
    # got that far (e.g. crashed in run_one before the scoring step).
    if scoring_engine is not None:
        for i, entry in enumerate(active):
            if per_query_scores[i] is None:
                try:
                    with scoring_lock:
                        per_query_scores[i] = scoring.score_query(
                            scoring_engine, workspace, entry["id"]
                        )
                except Exception as exc:
                    per_query_scores[i] = scoring.QueryResult(
                        id=entry["id"],
                        status="error",
                        detail=f"backfill-score: {type(exc).__name__}: {exc}",
                    )
        query_results = [
            s if s is not None else scoring.QueryResult(
                id=entry["id"], status="error", detail="never scored"
            )
            for s, entry in zip(per_query_scores, active)
        ]
    else:
        # No live scoring was possible — fall back to the legacy batch path.
        try:
            query_results = scoring.score_queries(workspace_db, workspace, query_ids)
        except Exception as exc:
            print(f"  scoring aborted: {type(exc).__name__}: {exc}", file=sys.stderr)
            query_results = [
                scoring.QueryResult(id=i, status="error", detail="scoring aborted")
                for i in query_ids
            ]

    report = build_report(args, timestamp, agent_run, metrics, query_results)
    report["per_query"] = [
        {
            "id": r["id"],
            "exit_code": r["exit_code"],
            "timed_out": r.get("timed_out", False),
            "duration_seconds": round(r["duration"], 1),
        }
        for r in per_query_runs
    ]
    report["ingest"] = {
        "exit_code": ingest["exit_code"],
        "duration_seconds": round(ingest["duration"], 1),
    }
    (run_dir / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    markdown = render_markdown(report)
    (run_dir / "report.md").write_text(markdown, encoding="utf-8")

    # `suffix` is defined upstream alongside the live-render setup so the
    # in-flight chart and the final chart share the same filename.
    try:
        _, events = analyze_run.load_run(run_dir)
        dashboard = analyze_run.render(
            report,
            events,
            analyze_run.CHARTS_DIR / f"dashboard{suffix}.png",
        )
        failures = analyze_run.collect_failures(events)
        failures_md = analyze_run.write_failures_report(
            run_dir,
            report,
            failures,
            analyze_run.CHARTS_DIR / f"trilogy_failures{suffix}.md",
        )
        print(
            f"  wrote {dashboard}  ({len(failures)} trilogy failures -> {failures_md})"
        )
    except Exception as exc:  # matplotlib missing, etc. — don't fail the eval
        print(f"  analyze_run skipped: {type(exc).__name__}: {exc}", file=sys.stderr)

    print(f"[5/5] Done. Artifacts in {run_dir}\n")
    print(markdown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
