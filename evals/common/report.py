"""Build the JSON ``report.json`` payload and the markdown summary string.

Both shapes are benchmark-agnostic — the only benchmark touch is the title in
``render_markdown``, taken from the BenchmarkSpec."""

from __future__ import annotations

import dataclasses
import os
from collections import Counter
from pathlib import Path
from typing import Any

from . import scoring
from .spec import BenchmarkSpec


def _trilogy_version() -> str:
    try:
        from trilogy import __version__

        return str(__version__)
    except Exception:
        return "unknown"


def agent_metric_fields(metrics: scoring.AgentMetrics) -> dict:
    """The metric-derived portion of the report's ``agent`` block — everything
    computed from an ``AgentMetrics``. Split out so a spliced run can rebuild it
    from the re-aggregated full-benchmark metrics (the run-level duration/exit
    fields are merged separately)."""
    return {
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
        "reviewer_verdicts": metrics.reviewer_verdicts,
        "reviewer_kickbacks": metrics.reviewer_kickbacks,
        "tokens": {
            "prompt": metrics.prompt_tokens,
            "completion": metrics.completion_tokens,
            "total": metrics.total_tokens,
        },
        "farewell": metrics.farewell,
    }


def build_report(
    spec: BenchmarkSpec,
    args: Any,
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
            "benchmark": spec.name,
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
            # `duration_seconds`: sum of per-query agent subprocess durations.
            # Equal to wall time at concurrency=1; larger when parallelised.
            # Use this for "agent CPU time" or to derive avg-per-query.
            "duration_seconds": round(agent_run["duration"], 1),
            # `wall_duration_seconds`: true elapsed wall clock of the agent
            # phase — collapses parallel work so it actually reflects "how
            # long the run took". Falls back to duration when an older agent
            # dict (e.g. spliced from a prior run) lacks the field.
            "wall_duration_seconds": round(
                agent_run.get("wall_duration", agent_run["duration"]), 1
            ),
            # `avg_query_seconds`: per-query agent time averaged across the
            # queries that actually ran — concurrency-independent so two
            # runs at different parallelism levels are comparable.
            "avg_query_seconds": (
                round(agent_run["duration"] / len(query_results), 1)
                if query_results
                else 0.0
            ),
            **agent_metric_fields(metrics),
        },
        "queries": [dataclasses.asdict(q) for q in query_results],
        "summary": {
            "pass_count": pass_count,
            "pass_rate": round(pass_count / total, 3) if total else 0.0,
            "status_breakdown": dict(statuses),
        },
    }


def render_markdown(spec: BenchmarkSpec, report: dict) -> str:
    meta, agent, summary = report["meta"], report["agent"], report["summary"]
    out: list[str] = []
    out.append(f"# {spec.name} Agent Eval — {meta['timestamp']}")
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
    wall = agent.get("wall_duration_seconds", agent["duration_seconds"])
    avg = agent.get("avg_query_seconds")
    out.append(
        f"- Wall time: {wall}s{timed}  |  "
        f"Agent time: {agent['duration_seconds']}s"
        + (f"  |  Avg/query: {avg}s" if avg is not None else "")
    )
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
    verdicts = agent.get("reviewer_verdicts", 0)
    kickbacks = agent.get("reviewer_kickbacks", 0)
    if verdicts:
        rate = kickbacks / verdicts
        out.append(
            f"- Reviewer kickbacks: {kickbacks}/{verdicts} verdicts "
            f"({rate * 100:.0f}% NOT_DONE) — lower is better"
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


def load_env(env_file: Path) -> None:
    """Populate os.environ from an .env file without overriding existing vars."""
    if not env_file.exists():
        return
    from trilogy.execution.config import load_env_file

    for key, value in (load_env_file(env_file) or {}).items():
        os.environ.setdefault(key, value)
