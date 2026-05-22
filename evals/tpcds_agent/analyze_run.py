#!/usr/bin/env python
"""Post-run analysis for the tpcds_agent eval.

Reads a run's report.json + agent_log.jsonl and renders a dashboard PNG for
tracking harness and prompt improvements over time. The PNG is written under
charts/ and is meant to be committed.

    python evals/tpcds_agent/analyze_run.py [run_dir] [--out PATH]
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import scoring

EVAL_DIR = Path(__file__).resolve().parent
RESULTS_DIR = EVAL_DIR / "results"
CHARTS_DIR = EVAL_DIR / "charts"

STATUS_COLORS = {
    "pass": "#2e7d32",
    "fail": "#ef6c00",
    "error": "#c62828",
    "missing": "#9e9e9e",
}
STATUS_ORDER = ["pass", "fail", "error", "missing"]
OK_COLOR = "#2e7d32"
ERR_COLOR = "#c62828"
_RUN_FILE = re.compile(r"query(\d+)\.preql")


def latest_run_dir() -> Path:
    runs = sorted(p for p in RESULTS_DIR.glob("*/") if (p / "report.json").exists())
    if not runs:
        raise SystemExit("No completed runs found under results/.")
    return runs[-1]


def load_run(run_dir: Path) -> tuple[dict, list[dict]]:
    report = json.loads((run_dir / "report.json").read_text(encoding="utf-8"))
    events: list[dict] = []
    log = run_dir / "agent_log.jsonl"
    if log.exists():
        for line in log.read_text(encoding="utf-8").splitlines():
            if line.strip():
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return report, events


def tool_outcomes(events: list[dict]) -> dict[str, list[int]]:
    """tool name -> [ok_count, error_count], pairing each call with its result."""
    outcomes: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    pending: str | None = None
    for e in events:
        if e.get("type") == "tool_call":
            pending = str(e.get("name", "?"))
        elif e.get("type") == "tool_result" and pending is not None:
            name = str(e.get("name", pending))
            is_err = scoring._is_error_result(name, str(e.get("result") or ""))
            outcomes[name][1 if is_err else 0] += 1
            pending = None
    return outcomes


def query_run_attempts(events: list[dict]) -> Counter[int]:
    """query id -> count of `trilogy run query<id>.preql` invocations."""
    attempts: Counter[int] = Counter()
    for e in events:
        if e.get("type") == "tool_call" and e.get("name") == "trilogy":
            args = (e.get("arguments") or {}).get("args") or []
            if isinstance(args, list) and len(args) >= 2 and args[0] == "run":
                for a in args[1:]:
                    m = _RUN_FILE.search(str(a))
                    if m:
                        attempts[int(m.group(1))] += 1
                        break
    return attempts


def _plot_tool_calls(ax, outcomes: dict[str, list[int]]) -> None:
    tools = sorted(outcomes, key=lambda t: -sum(outcomes[t]))
    ok = [outcomes[t][0] for t in tools]
    err = [outcomes[t][1] for t in tools]
    ax.bar(tools, ok, color=OK_COLOR, label="ok")
    ax.bar(tools, err, bottom=ok, color=ERR_COLOR, label="error")
    for i in range(len(tools)):
        total = ok[i] + err[i]
        rate = ok[i] / total * 100 if total else 0
        ax.text(i, total, f"{rate:.0f}%", ha="center", va="bottom", fontsize=8)
    ax.set_title("Tool calls — success / error")
    ax.set_ylabel("calls")
    ax.tick_params(axis="x", rotation=25)
    ax.legend(fontsize=8)


def _plot_per_query(ax, queries: list[dict], attempts: Counter[int]) -> None:
    from matplotlib.lines import Line2D

    xs = list(range(len(queries)))
    ys = [attempts.get(q["id"], 0) for q in queries]
    colors = [STATUS_COLORS.get(q["status"], "#000000") for q in queries]
    ax.scatter(xs, ys, c=colors, s=220, edgecolors="black", zorder=3)
    for x, y in zip(xs, ys):
        ax.text(x, y, str(y), ha="center", va="center", fontsize=7, color="white")
    ax.set_xticks(xs)
    ax.set_xticklabels([f"q{q['id']}" for q in queries])
    ax.set_ylim(-0.6, max([*ys, 2]) + 1.2)
    ax.set_ylabel("`trilogy run` attempts")
    ax.set_title("Per-query — run attempts, colored by final status")
    ax.grid(axis="y", alpha=0.3)
    ax.legend(
        handles=[
            Line2D(
                [0],
                [0],
                marker="o",
                color="w",
                markerfacecolor=c,
                markersize=9,
                label=s,
            )
            for s, c in STATUS_COLORS.items()
        ],
        fontsize=8,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.13),
        ncol=4,
    )


def _plot_outcomes(ax, breakdown: dict[str, int]) -> None:
    left = 0
    for status in STATUS_ORDER:
        n = breakdown.get(status, 0)
        if n:
            ax.barh(
                0, n, left=left, color=STATUS_COLORS[status], label=f"{status} ({n})"
            )
            left += n
    ax.set_title("Query outcomes")
    ax.set_xlabel("queries")
    ax.set_yticks([])
    ax.legend(fontsize=8, ncol=2)


def _plot_metrics(ax, report: dict) -> None:
    meta, agent, summary = report["meta"], report["agent"], report["summary"]
    tok = agent["tokens"]
    rows = [
        (
            "pass rate",
            f"{summary['pass_count']}/{meta['num_queries']}"
            f"  ({summary['pass_rate'] * 100:.0f}%)",
        ),
        ("wall time", f"{agent['duration_seconds']:.0f}s"),
        ("LLM iterations", str(agent["iterations"])),
        ("tool calls", str(agent["tool_calls_total"])),
        ("tool success", f"{agent['tool_success_rate'] * 100:.0f}%"),
        (
            "tokens",
            f"{tok['total']:,} ({tok['prompt']:,} in / {tok['completion']:,} out)",
        ),
        ("agent exit code", str(agent["exit_code"])),
    ]
    ax.axis("off")
    ax.text(0.0, 1.0, "Run metrics", fontsize=11, fontweight="bold", va="top")
    body = "\n".join(f"{k:<16}{v}" for k, v in rows)
    ax.text(0.0, 0.84, body, fontsize=10, va="top", family="monospace")


def render(report: dict, events: list[dict], out_path: Path) -> Path:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    meta, summary = report["meta"], report["summary"]
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    fig.suptitle(
        f"tpcds_agent eval — {meta['timestamp']}  |  "
        f"{meta['provider']}/{meta['model']}  |  sf={meta['scale_factor']:g}  |  "
        f"pass {summary['pass_count']}/{meta['num_queries']}",
        fontsize=13,
        fontweight="bold",
    )
    _plot_tool_calls(axes[0][0], tool_outcomes(events))
    _plot_per_query(axes[0][1], report["queries"], query_run_attempts(events))
    _plot_outcomes(axes[1][0], summary["status_breakdown"])
    _plot_metrics(axes[1][1], report)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return out_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "run_dir", nargs="?", type=Path, default=None, help="run dir (default: latest)"
    )
    parser.add_argument(
        "--out", type=Path, default=CHARTS_DIR / "dashboard.png", help="output PNG"
    )
    args = parser.parse_args()
    run_dir = args.run_dir or latest_run_dir()
    report, events = load_run(run_dir)
    out = render(report, events, args.out)
    print(f"Wrote {out}  (run: {run_dir.name})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
