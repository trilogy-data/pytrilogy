#!/usr/bin/env python
"""Post-run analysis for an agent eval.

Reads a run's ``report.json`` + ``agent_log*.jsonl`` and writes two artifacts
under the benchmark's ``charts/`` (meant to be committed) for tracking harness
and prompt improvements over time:

  - dashboard.png        — tool calls, per-query DB queries executed, outcomes, metrics
  - trilogy_failures.md  — every failed `trilogy` call, bucketed by category

Invoked from per-benchmark shims; ``run_main`` accepts an explicit ``eval_dir``
so the same code serves every benchmark (TPC-DS, TPC-H, ...)."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from collections.abc import Sequence
from pathlib import Path

from . import scoring

STATUS_COLORS = {
    "pass": "#2e7d32",
    "fail": "#ef6c00",
    "error": "#c62828",
    "missing": "#9e9e9e",
    "timeout": "#6a1b9a",
    "exhausted": "#5e35b1",
    "crashed": "#4527a0",
}
# Lighter palette used for the second (enriched) series in side-by-side
# comparison plots — same hue, lower saturation, so the status semantics still
# read at a glance but the two runs visibly differ. Pair with a triangle
# marker (vs the base run's circle) for unambiguous separation.
STATUS_COLORS_ALT = {
    "pass": "#81c784",
    "fail": "#ffb74d",
    "error": "#e57373",
    "missing": "#cfcfcf",
    "timeout": "#ba68c8",
    "exhausted": "#b39ddb",
    "crashed": "#9575cd",
}
STATUS_ORDER = ["pass", "fail", "error", "missing", "timeout", "exhausted", "crashed"]
OK_COLOR = "#2e7d32"
ERR_COLOR = "#c62828"
TOKEN_COLOR = "#1f77b4"  # per-query token-usage background bars (2nd y-axis)
_RUN_FILE = re.compile(r"query(\d+)\.(?:preql|sql)")
# Per-query log file name -> query id (`agent_log.q07.jsonl`, also the repeat
# harness's `agent_log.q07.r03.jsonl`). The per-query eval gives each query its
# own agent + log, so a read action's query id is its log file, not its args.
_LOG_QUERY_ID = re.compile(r"agent_log\.q(\d+)")


def latest_run_dir(results_dir: Path) -> Path:
    runs = sorted(p for p in results_dir.glob("*/") if (p / "report.json").exists())
    if not runs:
        raise SystemExit(f"No completed runs found under {results_dir}.")
    return runs[-1]


def load_run_events(run_dir: Path) -> tuple[None, list[dict]]:
    """Load only the JSONL events from a run dir, without requiring
    ``report.json`` to exist yet. Used by the live-dashboard renderer that
    runs while the eval is still in flight."""
    events: list[dict] = []
    for log in sorted(run_dir.glob("agent_log*.jsonl")):
        m = _LOG_QUERY_ID.search(log.name)
        qid = int(m.group(1)) if m else None
        for line in log.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            # Tag with the source query id so read-action attribution doesn't
            # depend on the call args naming the query file (inline executes
            # don't). Legacy single-file logs have no qid and fall back to args.
            if qid is not None and isinstance(event, dict):
                event["_query_id"] = qid
            events.append(event)
    return None, events


def load_run(run_dir: Path) -> tuple[dict, list[dict]]:
    report = json.loads((run_dir / "report.json").read_text(encoding="utf-8"))
    _, events = load_run_events(run_dir)
    return report, events


def _tool_label(name: str, arguments: dict | None) -> str:
    """Bar/table bucket for a tool call. ``trilogy`` is split by subcommand
    (``trilogy <args[0]>``) — half the iteration budget on exhausted runs goes
    to ``trilogy explore``, so a single rolled-up ``trilogy`` bucket hides where
    the cost lives. Other tools keep their flat name."""
    if name == "trilogy":
        args = (arguments or {}).get("args") or []
        sub = (str(args[0]) if isinstance(args, list) and args else "").strip()
        # `file` splits one level deeper — read (pulls content into context) and
        # write (a tiny ack) have very different response-size profiles, so a
        # single `trilogy file` bucket would hide the distinction.
        if sub == "file" and isinstance(args, list) and len(args) > 1:
            return f"trilogy file {str(args[1]).strip()}"
        return f"trilogy {sub}" if sub else "trilogy"
    return str(name)


def tool_outcomes(events: list[dict]) -> dict[str, list[int]]:
    """tool label -> [ok_count, error_count], pairing each call with its
    result."""
    outcomes: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    pending: tuple[str, str] | None = None
    for e in events:
        if e.get("type") == "tool_call":
            name = str(e.get("name", "?"))
            pending = (name, _tool_label(name, e.get("arguments")))
        elif e.get("type") == "tool_result" and pending is not None:
            tool_name, label = pending
            is_err = scoring._is_error_result(tool_name, str(e.get("result") or ""))
            outcomes[label][1 if is_err else 0] += 1
            pending = None
    return outcomes


def tool_response_tokens(events: list[dict]) -> dict[str, list[float]]:
    """tool label -> list of per-call RESPONSE token costs: how many tokens each
    call's *result* added to the conversation. This is the actionable number —
    a turn's total tokens are dominated by accumulated prior context and hide
    which tool's output is actually heavy.

    Measured as the real growth in ``prompt_tokens`` from the calling turn to
    the next, minus that turn's own assistant message (its completion) — what's
    left is the tokens the tool result(s) injected. A multi-call turn splits the
    growth across its calls by result size. Computed per query, since each query
    is a fresh agent whose prompt resets. The final turn of a query has no
    successor to diff against, so its result is estimated from text length
    (~4 chars/token)."""
    by_query: dict[object, list[dict]] = defaultdict(list)
    for e in events:
        by_query[e.get("_query_id")].append(e)

    credits: dict[str, list[float]] = defaultdict(list)
    for evs in by_query.values():
        turns: list[dict] = []
        cur: dict | None = None
        pending: str | None = None
        for e in evs:
            t = e.get("type")
            if t == "llm_response":
                u = e.get("usage") or {}
                cur = {
                    "prompt": u.get("prompt_tokens") or 0,
                    "completion": u.get("completion_tokens") or 0,
                    "calls": [],  # (label, result_chars)
                }
                turns.append(cur)
                pending = None
            elif t == "tool_call" and cur is not None:
                pending = _tool_label(str(e.get("name", "?")), e.get("arguments"))
            elif t == "tool_result" and cur is not None and pending is not None:
                cur["calls"].append((pending, len(str(e.get("result") or ""))))
                pending = None
        for i, turn in enumerate(turns):
            if not turn["calls"]:
                continue
            nxt = turns[i + 1] if i + 1 < len(turns) else None
            if nxt is not None:
                added = max(0.0, nxt["prompt"] - turn["prompt"] - turn["completion"])
            else:
                added = sum(c for _, c in turn["calls"]) / 4.0
            total_chars = sum(c for _, c in turn["calls"]) or 1
            for label, chars in turn["calls"]:
                credits[label].append(added * chars / total_chars)
    return credits


def _is_db_read_call(name: str, args: dict) -> bool:
    """True for any tool call that executes against the database — a "read
    action". Counts the answer-file run AND inline/ad-hoc executes, across both
    toolsets: trilogy `run <file|inline>`, and the SQL baseline's `run_file` /
    `run_query`."""
    if name == "trilogy":
        raw = args.get("args") or []
        return isinstance(raw, list) and bool(raw) and raw[0] == "run"
    return name in ("run_file", "run_query")


def _run_targets(name: str, args: dict) -> list[str]:
    """Strings a read-action call points at, for legacy query-id matching."""
    if name == "trilogy":
        raw = args.get("args") or []
        return [str(a) for a in raw[1:]] if isinstance(raw, list) else []
    val = args.get("path") or args.get("sql")
    return [val] if isinstance(val, str) else []


def query_run_attempts(events: list[dict]) -> Counter[int]:
    """query id -> count of ALL database read actions the agent made for that
    query: the answer-file run plus every inline/ad-hoc execute, across both
    toolsets (trilogy `run <file|inline>`, SQL `run_file` / `run_query`).

    Attribution is by the source log file (`agent_log.qNN.jsonl`), which
    `load_run_events` tags onto each event — the per-query eval gives each query
    its own agent + log, so every read action there belongs to that query. For
    legacy merged logs with no per-query filename, falls back to matching
    `queryNN` in the run target; inline executes are then unattributable and
    skipped (as before)."""
    attempts: Counter[int] = Counter()
    for e in events:
        if e.get("type") != "tool_call":
            continue
        name = str(e.get("name", "?"))
        args = e.get("arguments") or {}
        if not isinstance(args, dict) or not _is_db_read_call(name, args):
            continue
        qid = e.get("_query_id")
        if qid is not None:
            attempts[int(qid)] += 1
            continue
        for a in _run_targets(name, args):
            m = _RUN_FILE.search(a)
            if m:
                attempts[int(m.group(1))] += 1
                break
    return attempts


# Ordered most-specific first: the first matching rule wins.
FAILURE_RULES: list[tuple[str, list[str]]] = [
    # CLI-level — the agent invoked the `trilogy` command itself wrongly.
    (
        "cli-misuse",
        [
            "looks like a flag",
            "try 'python -m",
            "usage: python -m",
            "no such option",
            "unexpected extra argument",
            "invalid value for",
        ],
    ),
    ("file-not-found", ["does not exist"]),
    # Semantic — the query parsed but could not be resolved or typed.
    ("enum-value", ["not a valid member of enum"]),
    ("undefined-concept", ["is undefined", "undefinedconcept"]),
    (
        "join-resolution",
        ["could not resolve connections", "no datasource", "unresolvable"],
    ),
    ("type-error", ["invalid argument type", "not compatible", "incompatible type"]),
    # Syntax — the query failed to parse.
    ("syntax-missing-alias", ["missing alias", "alias must be specified"]),
    (
        "syntax-parse",
        ["syntax [", "expected eoi", "parsing error", "-->", "unexpected token"],
    ),
]


def categorize_failure(result: str) -> str:
    low = " ".join(result.split()).lower()
    for category, needles in FAILURE_RULES:
        if any(n in low for n in needles):
            return category
    return "other"


_ERROR_ANCHORS = ("^---", "Location:", "-->")


def _error_snippet(result: str, limit: int = 1200) -> str:
    """The meaningful error text from a `trilogy` tool result.

    Preserves newlines so the parser's caret diagram (``  | ^---``) remains
    readable. When the message is longer than ``limit``, center-truncates
    around the caret (or `Location:` / `-->` marker) so the failing column
    stays in view.
    """
    text = result
    if "--- stderr ---" in text:
        text = text.split("--- stderr ---", 1)[1]
    text = text.strip()
    for marker in ("Unexpected error:", "Syntax error:", "Error:"):
        if marker in text:
            text = text.split(marker, 1)[1].lstrip()
            break
    if len(text) <= limit:
        return text
    idx = -1
    for anchor in _ERROR_ANCHORS:
        idx = text.find(anchor)
        if idx != -1:
            break
    half = (limit - 1) // 2
    if idx == -1:
        return text[:half] + "\n…\n" + text[-half:]
    start = max(0, idx - half)
    end = min(len(text), idx + half)
    prefix = "…\n" if start > 0 else ""
    suffix = "\n…" if end < len(text) else ""
    return prefix + text[start:end] + suffix


def _format_args(args: list[str], limit: int = 300) -> str:
    """Render a tool-call argv for the report, middle-truncating so the
    leading subcommand AND the trailing inline query both stay visible."""
    s = " ".join(args)
    if len(s) <= limit:
        return s
    half = (limit - 1) // 2
    return s[:half] + "…" + s[-half:]


def collect_failures(events: list[dict]) -> list[dict]:
    """Failed `trilogy` calls, each tagged with a category and error snippet."""
    failures: list[dict] = []
    pending: list | None = None
    for e in events:
        if e.get("type") == "tool_call" and e.get("name") == "trilogy":
            pending = (e.get("arguments") or {}).get("args") or []
        elif e.get("type") == "tool_result" and e.get("name") == "trilogy":
            if pending is not None:
                result = str(e.get("result") or "")
                head = result.splitlines()[0].strip() if result.strip() else ""
                if head != "exit_code: 0":
                    failures.append(
                        {
                            "args": [str(a) for a in pending],
                            "category": categorize_failure(result),
                            "error": _error_snippet(result),
                        }
                    )
                pending = None
    return failures


def write_failures_report(
    run_dir: Path, report: dict, failures: list[dict], out_path: Path
) -> Path:
    meta = report["meta"]
    trilogy_calls = report["agent"]["tool_calls_by_name"].get("trilogy", 0)
    by_cat = Counter(f["category"] for f in failures)
    n = len(failures)

    rate = f" ({n / trilogy_calls * 100:.0f}%)" if trilogy_calls else ""
    lines = [
        f"# Trilogy failure analysis — {meta['timestamp']}",
        "",
        f"- Run `{run_dir.name}` | `{meta['provider']}/{meta['model']}` "
        f"| sf={meta['scale_factor']:g}",
        f"- `trilogy` calls: {trilogy_calls} | failed: {n}{rate}",
        "",
        "## Categories",
        "",
        "| Category | Count | Share |",
        "|---|---:|---:|",
    ]
    for cat, count in by_cat.most_common():
        share = f"{count / n * 100:.0f}%" if n else "—"
        lines.append(f"| `{cat}` | {count} | {share} |")

    lines += ["", "## Detail", ""]
    for cat, _ in by_cat.most_common():
        lines.append(f"### `{cat}`")
        lines.append("")
        for f in failures:
            if f["category"] == cat:
                lines.append(f"- `trilogy {_format_args(f['args'])}`")
                lines.append("")
                lines.append("  ```text")
                for err_line in f["error"].splitlines() or [""]:
                    lines.append(f"  {err_line}".rstrip())
                lines.append("  ```")
        lines.append("")
    if not failures:
        lines += ["_No `trilogy` calls failed in this run._", ""]

    out_path.write_text("\n".join(lines), encoding="utf-8")
    return out_path


_TOOL_LABEL_MAX = 28


def _short_tool_label(name: str) -> str:
    """Truncate hugely-long tool names so a malformed tool_call (e.g. a model
    crammed an entire CLI invocation into the `name` field instead of using
    arguments) doesn't blow up the bar-chart layout. The Unknown-tool branch
    in the agent loop rejects these on the wire; the dashboard just needs to
    render them without wrapping a 500-char label across the whole figure.
    Wider cap fits ``trilogy <subcommand>`` labels (``trilogy database``,
    ``trilogy agent-info``) without truncation."""
    if len(name) <= _TOOL_LABEL_MAX:
        return name
    return name[: _TOOL_LABEL_MAX - 1] + "…"


def _plot_tool_calls(
    ax, outcomes: dict[str, list[int]], credits: dict[str, list[float]] | None = None
) -> None:
    """Per-tool call counts (ok/error stacked) sorted by total calls. When
    ``credits`` is given (per-call RESPONSE token costs), a faint-blue
    total-response-token bar sits beside each call bar on a 2nd y-axis."""
    tools = sorted(outcomes, key=lambda t: -sum(outcomes[t]))
    labels = [_short_tool_label(t) for t in tools]
    xs = list(range(len(tools)))
    ok = [outcomes[t][0] for t in tools]
    err = [outcomes[t][1] for t in tools]
    w = 0.4 if credits else 0.8
    xc = [x - w / 2 for x in xs] if credits else xs
    ax.bar(xc, ok, width=w, color=OK_COLOR, label="ok")
    ax.bar(xc, err, width=w, bottom=ok, color=ERR_COLOR, label="error")
    for i, x in enumerate(xc):
        total = ok[i] + err[i]
        rate = ok[i] / total * 100 if total else 0
        ax.text(x, total, f"{rate:.0f}%", ha="center", va="bottom", fontsize=8)
    ax.set_title(
        "Tool calls — success / error" + (" + response tokens" if credits else "")
    )
    ax.set_ylabel("calls")
    ax.set_xticks(xs)
    ax.set_xticklabels(labels)
    ax.tick_params(axis="x", rotation=25)
    ax.legend(fontsize=8, loc="upper right")
    if credits:
        ax_tok = ax.twinx()
        toks = [sum(credits.get(t, [])) for t in tools]
        ax_tok.bar([x + w / 2 for x in xs], toks, width=w, color=TOKEN_COLOR, alpha=0.4)
        ax_tok.set_ylabel("response tokens (faint)", color=TOKEN_COLOR)
        ax_tok.tick_params(axis="y", labelcolor=TOKEN_COLOR)
        ax_tok.ticklabel_format(axis="y", style="sci", scilimits=(0, 0))


def _plot_tool_token_table(
    ax, outcomes: dict[str, list[int]], credits: dict[str, list[float]]
) -> None:
    """Full-width per-tool table: calls, success rate, and the per-call RESPONSE
    token distribution (P5/P50/P95/AVG/TOTAL) — tokens each call's result adds
    to context, not the turn total. Rows sorted by total calls to match the bar
    order above."""
    ax.axis("off")
    tools = sorted(outcomes, key=lambda t: -sum(outcomes[t]))
    rows = []
    for t in tools:
        ok, err = outcomes[t]
        calls = ok + err
        rate = ok / calls if calls else 1.0
        vals = sorted(credits.get(t, []))
        avg = sum(vals) / len(vals) if vals else 0.0
        rows.append(
            [
                _short_tool_label(t),
                f"{calls:,}",
                f"{rate * 100:.0f}%",
                f"{_pct(vals, 5):,.0f}",
                f"{_pct(vals, 50):,.0f}",
                f"{_pct(vals, 95):,.0f}",
                f"{avg:,.0f}",
                f"{sum(vals):,.0f}",
            ]
        )
    ax.set_title(
        "Per-tool cost — response tokens each call adds to context "
        "(not the turn total)",
        fontsize=10,
    )
    tbl = ax.table(
        cellText=rows,
        colLabels=[
            "tool",
            "calls",
            "success",
            "resp P5",
            "resp P50",
            "resp P95",
            "resp AVG",
            "resp TOTAL",
        ],
        loc="center",
        cellLoc="right",
        bbox=[0.0, 0.0, 1.0, 0.92],
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8)
    tbl.scale(1, 1.3)
    for (r, c), cell in tbl.get_celld().items():
        if r == 0:
            cell.set_text_props(fontweight="bold")
        if c == 0:
            cell.set_text_props(ha="left")


def _token_background(
    ax, bars: list[tuple[Sequence[float], Sequence[float], float, str]]
):
    """Draw per-query token usage as transparent bars on a 2nd y-axis BEHIND the
    foreground scatter. ``bars`` is a list of (xs, tokens, width, color); one
    entry for a single run, two (offset) for a comparison. Returns the twin axis.

    The twin axis is created on top of ``ax`` by matplotlib, so we lift ``ax``'s
    z-order above it and hide ``ax``'s opaque patch — that puts the scatter in
    front of the shaded bars while keeping the token scale on the right."""
    ax_tok = ax.twinx()
    top = 1.0
    for xs, tokens, width, color in bars:
        ax_tok.bar(xs, tokens, width=width, color=color, alpha=0.16, zorder=0)
        top = max(top, *tokens) if tokens else top
    ax_tok.set_ylim(0, top * 1.18)
    ax_tok.set_ylabel("tokens (bars)", color=TOKEN_COLOR)
    ax_tok.tick_params(axis="y", labelcolor=TOKEN_COLOR)
    ax_tok.ticklabel_format(axis="y", style="sci", scilimits=(0, 0))
    ax.set_zorder(ax_tok.get_zorder() + 1)
    ax.patch.set_visible(False)
    return ax_tok


def _plot_per_query(
    ax, queries: list[dict], attempts: Counter[int], tokens_by_id: dict[int, float]
) -> None:
    from matplotlib.lines import Line2D

    xs = list(range(len(queries)))
    ys = [attempts.get(q["id"], 0) for q in queries]
    _token_background(
        ax, [(xs, [tokens_by_id.get(q["id"], 0) for q in queries], 0.8, TOKEN_COLOR)]
    )
    colors = [STATUS_COLORS.get(q["status"], "#000000") for q in queries]
    ax.scatter(xs, ys, c=colors, s=220, edgecolors="black", zorder=3)
    for x, y in zip(xs, ys):
        ax.text(x, y, str(y), ha="center", va="center", fontsize=7, color="white")
    ax.set_xticks(xs)
    ax.set_xticklabels([f"q{q['id']}" for q in queries])
    ax.set_ylim(-0.6, max([*ys, 2]) + 1.2)
    ax.set_ylabel("db queries executed")
    ax.set_title(
        "Per-query — DB queries executed (file + inline), colored by final status"
    )
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
        ncol=5,
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


def _pct(sorted_vals: list[float], p: float) -> float:
    """Linear-interpolated percentile (numpy's 'linear' method), no numpy dep."""
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    k = (len(sorted_vals) - 1) * (p / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    if lo == hi:
        return float(sorted_vals[lo])
    return sorted_vals[lo] * (hi - k) + sorted_vals[hi] * (k - lo)


def _metric_row(label: str, vals: list[float], total: float, fmt) -> list[str]:
    """One table row: P5/P50/P95/AVG over the per-query values, plus a TOTAL the
    caller supplies (a sum for counts, the overall ratio for a rate)."""
    s = sorted(float(v) for v in vals)
    avg = sum(s) / len(s) if s else 0.0
    return [label] + [
        fmt(c) for c in (_pct(s, 5), _pct(s, 50), _pct(s, 95), avg, total)
    ]


def _per_query_metric_vectors(report: dict, events: list[dict]) -> dict:
    """Per-query metric vectors for the percentile table, attributed by the
    ``_query_id`` tag on events (so it works live and post-hoc). Duration prefers
    the authoritative subprocess time in ``report['per_query']`` and falls back
    to the first→last event-timestamp span."""
    from datetime import datetime

    iters: Counter[int] = Counter()
    calls: Counter[int] = Counter()
    results: Counter[int] = Counter()
    errors: Counter[int] = Counter()
    toks: Counter[int] = Counter()
    first_ts: dict[int, str] = {}
    last_ts: dict[int, str] = {}
    for e in events:
        raw_qid = e.get("_query_id")
        if raw_qid is None:
            continue
        qid = int(raw_qid)
        ts = e.get("ts")
        if isinstance(ts, str):  # ISO-8601 UTC — lexical compare gives min/max
            if qid not in first_ts or ts < first_ts[qid]:
                first_ts[qid] = ts
            if qid not in last_ts or ts > last_ts[qid]:
                last_ts[qid] = ts
        etype = e.get("type")
        if etype == "llm_response":
            iters[qid] += 1
            toks[qid] += (e.get("usage") or {}).get("total_tokens") or 0
        elif etype == "tool_call":
            calls[qid] += 1
        elif etype == "tool_result":
            results[qid] += 1
            if scoring._is_error_result(
                str(e.get("name", "?")), str(e.get("result") or "")
            ):
                errors[qid] += 1

    durations: dict[int, float] = {}
    for pq in report.get("per_query") or []:
        d = pq.get("duration_seconds")
        if d is not None:
            durations[int(pq["id"])] = float(d)

    qids = sorted(set(iters) | set(calls) | set(results) | set(toks) | set(durations))
    for qid in qids:
        if qid not in durations and qid in first_ts and qid in last_ts:
            try:
                durations[qid] = (
                    datetime.fromisoformat(last_ts[qid])
                    - datetime.fromisoformat(first_ts[qid])
                ).total_seconds()
            except ValueError:
                durations[qid] = 0.0

    ok_total = sum(results[q] - errors[q] for q in qids)
    res_total = sum(results[q] for q in qids)
    return {
        "qids": qids,
        "duration": [durations.get(q, 0.0) for q in qids],
        "iterations": [float(iters[q]) for q in qids],
        "tool_calls": [float(calls[q]) for q in qids],
        "tokens": [float(toks[q]) for q in qids],
        # per-query success rate; a query with no tool results scores 1.0,
        # mirroring AgentMetrics.tool_success_rate.
        "success": [
            (results[q] - errors[q]) / results[q] if results[q] else 1.0 for q in qids
        ],
        "duration_total": sum(durations.get(q, 0.0) for q in qids),
        "iterations_total": float(sum(iters[q] for q in qids)),
        "tool_calls_total": float(sum(calls[q] for q in qids)),
        "tokens_total": float(sum(toks[q] for q in qids)),
        "success_total": (ok_total / res_total) if res_total else 1.0,
    }


def _plot_metrics(ax, report: dict, events: list[dict]) -> None:
    meta, agent, summary = report["meta"], report["agent"], report["summary"]
    ax.axis("off")

    v = _per_query_metric_vectors(report, events)
    wall = agent.get("wall_duration_seconds", agent["duration_seconds"])
    per_query = report.get("per_query") or []
    # `agent.exit_code` is a run-level rollup (0 iff every per-query subprocess
    # exited 0, else 1), so the bare code is opaque. Report the count of queries
    # whose agent exited non-zero instead — the Outcomes panel shows which.
    if per_query:
        nonzero = sum(1 for r in per_query if r.get("exit_code", 0) != 0)
        exits = f"{nonzero}/{len(per_query)} non-zero"
    else:
        exits = f"rollup {agent['exit_code']}"
    kickbacks = agent.get("reviewer_kickbacks", 0)
    verdicts = agent.get("reviewer_verdicts", 0)
    kb = f"     reviewer kickbacks: {kickbacks}/{verdicts}" if verdicts else ""
    header = (
        f"pass {summary['pass_count']}/{meta['num_queries']} "
        f"({summary['pass_rate'] * 100:.0f}%)     wall {wall:.0f}s     "
        f"agent exits: {exits}{kb}"
    )
    ax.text(0.0, 1.0, "Run metrics", fontsize=11, fontweight="bold", va="top")
    ax.text(0.0, 0.90, header, fontsize=9, va="top", family="monospace")
    ax.text(
        0.0,
        0.80,
        "per-query distribution; TOTAL = sum (overall ratio for success)",
        fontsize=8,
        va="top",
        style="italic",
        color="#555555",
    )

    def _int(x: float) -> str:
        return f"{x:,.0f}"

    def _pctf(x: float) -> str:
        return f"{x * 100:.0f}%"

    table_rows = [
        _metric_row("duration (s)", v["duration"], v["duration_total"], _int),
        _metric_row("llm iterations", v["iterations"], v["iterations_total"], _int),
        _metric_row("tool calls", v["tool_calls"], v["tool_calls_total"], _int),
        _metric_row("tool success", v["success"], v["success_total"], _pctf),
        _metric_row("tokens", v["tokens"], v["tokens_total"], _int),
    ]
    tbl = ax.table(
        cellText=table_rows,
        colLabels=["metric", "P5", "P50", "P95", "AVG", "TOTAL"],
        loc="center",
        cellLoc="right",
        bbox=[0.0, 0.0, 1.0, 0.72],
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.4)
    for (r, c), cell in tbl.get_celld().items():
        if r == 0:
            cell.set_text_props(fontweight="bold")
        if c == 0:
            cell.set_text_props(ha="left")


def _plot_per_query_overlay(
    ax,
    queries_a: list[dict],
    attempts_a: Counter[int],
    queries_b: list[dict],
    attempts_b: Counter[int],
    label_a: str,
    label_b: str,
    tokens_a: dict[int, float],
    tokens_b: dict[int, float],
) -> None:
    """Both runs' per-query results on one chart. Each query id gets two
    markers — circle for run A (base palette), triangle for run B (alt
    palette) — offset slightly along x so they don't overlap. Color encodes
    final status; marker shape + palette encodes which run. Transparent
    background bars (2nd y-axis) show each run's per-query token usage."""
    from matplotlib.lines import Line2D

    by_id_a = {q["id"]: q for q in queries_a}
    by_id_b = {q["id"]: q for q in queries_b}
    ids = sorted(set(by_id_a) | set(by_id_b))
    xs = list(range(len(ids)))
    max_y = 2

    _token_background(
        ax,
        [
            (
                [x - 0.18 for x in xs],
                [tokens_a.get(i, 0) for i in ids],
                0.34,
                TOKEN_COLOR,
            ),
            (
                [x + 0.18 for x in xs],
                [tokens_b.get(i, 0) for i in ids],
                0.34,
                "#9575cd",
            ),
        ],
    )

    for x, qid in zip(xs, ids):
        qa = by_id_a.get(qid)
        qb = by_id_b.get(qid)
        if qa is not None:
            ya = attempts_a.get(qid, 0)
            ax.scatter(
                x - 0.18,
                ya,
                c=STATUS_COLORS.get(qa["status"], "#000000"),
                s=180,
                marker="o",
                edgecolors="black",
                linewidths=0.8,
                zorder=3,
            )
            ax.text(
                x - 0.18,
                ya,
                str(ya),
                ha="center",
                va="center",
                fontsize=7,
                color="white",
            )
            max_y = max(max_y, ya)
        if qb is not None:
            yb = attempts_b.get(qid, 0)
            ax.scatter(
                x + 0.18,
                yb,
                c=STATUS_COLORS_ALT.get(qb["status"], "#000000"),
                s=180,
                marker="^",
                edgecolors="black",
                linewidths=0.8,
                zorder=3,
            )
            ax.text(
                x + 0.18,
                yb,
                str(yb),
                ha="center",
                va="center",
                fontsize=7,
                color="black",
            )
            max_y = max(max_y, yb)

    ax.set_xticks(xs)
    ax.set_xticklabels([f"q{i}" for i in ids])
    ax.set_ylim(-0.6, max_y + 1.4)
    ax.set_ylabel("db queries executed")
    ax.set_title(
        f"Per-query — DB queries executed (file + inline), colored by final status  "
        f"(○ {label_a}, △ {label_b})"
    )
    ax.grid(axis="y", alpha=0.3)

    status_handles = [
        Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            markerfacecolor=STATUS_COLORS[s],
            markeredgecolor="black",
            markersize=9,
            label=s,
        )
        for s in STATUS_ORDER
    ]
    run_handles = [
        Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            markerfacecolor="#bdbdbd",
            markeredgecolor="black",
            markersize=10,
            label=label_a,
        ),
        Line2D(
            [0],
            [0],
            marker="^",
            color="w",
            markerfacecolor="#bdbdbd",
            markeredgecolor="black",
            markersize=10,
            label=label_b,
        ),
    ]
    ax.legend(
        handles=status_handles + run_handles,
        fontsize=8,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.13),
        ncol=len(STATUS_ORDER) + 2,
    )


def render(report: dict, events: list[dict], out_path: Path) -> Path:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.gridspec import GridSpec

    meta, summary = report["meta"], report["summary"]
    fig = plt.figure(figsize=(14, 12))
    gs = GridSpec(
        3, 2, figure=fig, height_ratios=[1.15, 1.15, 0.95], hspace=0.5, wspace=0.22
    )
    fig.suptitle(
        f"tpcds_agent eval — {meta['timestamp']}  |  "
        f"{meta['provider']}/{meta['model']}  |  sf={meta['scale_factor']:g}  |  "
        f"pass {summary['pass_count']}/{meta['num_queries']}",
        fontsize=13,
        fontweight="bold",
    )
    outcomes = tool_outcomes(events)
    credits = tool_response_tokens(events)
    _plot_tool_calls(fig.add_subplot(gs[0, 0]), outcomes, credits)
    v = _per_query_metric_vectors(report, events)
    tokens_by_id = dict(zip(v["qids"], v["tokens"]))
    _plot_per_query(
        fig.add_subplot(gs[0, 1]),
        report["queries"],
        query_run_attempts(events),
        tokens_by_id,
    )
    _plot_outcomes(fig.add_subplot(gs[1, 0]), summary["status_breakdown"])
    _plot_metrics(fig.add_subplot(gs[1, 1]), report, events)
    _plot_tool_token_table(fig.add_subplot(gs[2, :]), outcomes, credits)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return out_path


def render_comparison(
    report_a: dict,
    events_a: list[dict],
    report_b: dict,
    events_b: list[dict],
    out_path: Path,
    label_a: str = "base",
    label_b: str = "enriched",
) -> Path:
    """Single PNG that overlays two runs for direct comparison. Per-query
    attempts share one chart (different marker + palette per run); tool
    calls, outcomes and metrics are split across an axis-pair so each run
    is read separately."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.gridspec import GridSpec

    fig = plt.figure(figsize=(16, 14))
    gs = GridSpec(
        4, 2, figure=fig, height_ratios=[2.0, 1.4, 0.7, 1.0], hspace=0.55, wspace=0.18
    )

    meta_a, meta_b = report_a["meta"], report_b["meta"]
    sum_a, sum_b = report_a["summary"], report_b["summary"]
    fig.suptitle(
        "comparison  |  "
        f"{label_a}: {meta_a['timestamp']} pass {sum_a['pass_count']}/{meta_a['num_queries']}"
        f"  vs  "
        f"{label_b}: {meta_b['timestamp']} pass {sum_b['pass_count']}/{meta_b['num_queries']}",
        fontsize=13,
        fontweight="bold",
    )

    va = _per_query_metric_vectors(report_a, events_a)
    vb = _per_query_metric_vectors(report_b, events_b)
    ax_per_query = fig.add_subplot(gs[0, :])
    _plot_per_query_overlay(
        ax_per_query,
        report_a["queries"],
        query_run_attempts(events_a),
        report_b["queries"],
        query_run_attempts(events_b),
        label_a,
        label_b,
        dict(zip(va["qids"], va["tokens"])),
        dict(zip(vb["qids"], vb["tokens"])),
    )

    ax_tools_a = fig.add_subplot(gs[1, 0])
    ax_tools_b = fig.add_subplot(gs[1, 1])
    _plot_tool_calls(
        ax_tools_a, tool_outcomes(events_a), tool_response_tokens(events_a)
    )
    ax_tools_a.set_title(f"Tool calls — {label_a}")
    _plot_tool_calls(
        ax_tools_b, tool_outcomes(events_b), tool_response_tokens(events_b)
    )
    ax_tools_b.set_title(f"Tool calls — {label_b}")

    ax_out_a = fig.add_subplot(gs[2, 0])
    ax_out_b = fig.add_subplot(gs[2, 1])
    _plot_outcomes(ax_out_a, sum_a["status_breakdown"])
    ax_out_a.set_title(f"Outcomes — {label_a}")
    _plot_outcomes(ax_out_b, sum_b["status_breakdown"])
    ax_out_b.set_title(f"Outcomes — {label_b}")

    ax_metrics_a = fig.add_subplot(gs[3, 0])
    ax_metrics_b = fig.add_subplot(gs[3, 1])
    _plot_metrics(ax_metrics_a, report_a, events_a)
    ax_metrics_a.text(
        0.0, 1.06, f"({label_a})", fontsize=9, va="bottom", style="italic"
    )
    _plot_metrics(ax_metrics_b, report_b, events_b)
    ax_metrics_b.text(
        0.0, 1.06, f"({label_b})", fontsize=9, va="bottom", style="italic"
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return out_path


def _statuses_by_id(report: dict) -> dict[int, str]:
    return {q["id"]: q["status"] for q in report.get("queries", [])}


def _funnel_rows(reports_by_key: dict[str, dict]) -> list[dict]:
    """One row per category in the given order: pass set, marginal new passes
    vs the previous (less-scaffolded) leg, and regressions (passed earlier but
    not here). Pass sets are cumulative-union for the 'newly unlocked' delta so
    the funnel reads as 'value added by this layer'."""
    rows: list[dict] = []
    seen_pass: set[int] = set()
    for key, report in reports_by_key.items():
        statuses = _statuses_by_id(report)
        passing = {qid for qid, st in statuses.items() if st == "pass"}
        marginal = passing - seen_pass
        regressions = seen_pass - passing
        rows.append(
            {
                "key": key,
                "label": report["meta"].get("category_label", key),
                "passing": passing,
                "pass_count": len(passing),
                "total": report["meta"].get("num_queries", len(statuses)),
                "marginal": sorted(marginal),
                "regressions": sorted(regressions),
                "tokens": report.get("agent", {}).get("tokens", {}).get("total", 0),
                "pass_rate": report.get("summary", {}).get("pass_rate", 0.0),
            }
        )
        seen_pass |= passing
    return rows


def render_funnel(reports_by_key: dict[str, dict], out_path: Path) -> Path:
    """Cross-category funnel + per-query pass/fail matrix. ``reports_by_key`` is
    ordered least- to most-scaffolded (sql_bare → enriched)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.gridspec import GridSpec
    from matplotlib.patches import Patch

    rows = _funnel_rows(reports_by_key)
    keys = list(reports_by_key)
    all_ids = sorted({q["id"] for r in reports_by_key.values() for q in r["queries"]})

    fig = plt.figure(figsize=(15, 10))
    gs = GridSpec(2, 2, figure=fig, height_ratios=[1.0, 1.6], hspace=0.35, wspace=0.25)
    bench = next(iter(reports_by_key.values()))["meta"].get("benchmark", "")
    fig.suptitle(f"{bench} eval — category funnel", fontsize=14, fontweight="bold")

    # Funnel bars (pass count per category, annotated with marginal lift).
    ax_funnel = fig.add_subplot(gs[0, 0])
    labels = [r["label"] for r in rows]
    counts = [r["pass_count"] for r in rows]
    ypos = list(range(len(rows)))[::-1]  # most-scaffolded on top
    ax_funnel.barh(ypos, counts, color="#4c78a8")
    for y, r in zip(ypos, rows):
        delta = f"  (+{len(r['marginal'])}" if r["marginal"] else "  (+0"
        delta += f", -{len(r['regressions'])})" if r["regressions"] else ")"
        ax_funnel.text(
            r["pass_count"] + 0.1,
            y,
            f"{r['pass_count']}{delta}",
            va="center",
            fontsize=9,
        )
    ax_funnel.set_yticks(ypos)
    ax_funnel.set_yticklabels(labels)
    ax_funnel.set_xlabel("queries passing")
    ax_funnel.set_title("Funnel: passes + (newly unlocked, -regressions)")
    if rows:
        ax_funnel.set_xlim(0, max(r["total"] for r in rows) + 2)

    # Metrics table.
    ax_tbl = fig.add_subplot(gs[0, 1])
    ax_tbl.axis("off")
    table_rows = [
        [
            r["label"],
            f"{r['pass_count']}/{r['total']}",
            f"{r['pass_rate']:.2f}",
            f"{r['tokens']:,}",
        ]
        for r in rows
    ]
    tbl = ax_tbl.table(
        cellText=table_rows,
        colLabels=["category", "pass", "rate", "tokens"],
        loc="center",
        cellLoc="center",
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.5)
    ax_tbl.set_title("Per-category metrics")

    # Per-query matrix (rows=queries, cols=categories).
    ax_mat = fig.add_subplot(gs[1, :])
    status_maps = {k: _statuses_by_id(reports_by_key[k]) for k in keys}
    grid = [
        [1 if status_maps[k].get(qid) == "pass" else 0 for k in keys] for qid in all_ids
    ]
    ax_mat.imshow(
        grid, aspect="auto", cmap="RdYlGn", vmin=0, vmax=1, interpolation="nearest"
    )
    ax_mat.set_xticks(range(len(keys)))
    ax_mat.set_xticklabels(
        [reports_by_key[k]["meta"].get("category_label", k) for k in keys]
    )
    ax_mat.set_yticks(range(len(all_ids)))
    ax_mat.set_yticklabels([f"q{qid:02d}" for qid in all_ids], fontsize=7)
    ax_mat.set_title("Per-query pass (green) / not-pass (red)")
    ax_mat.legend(
        handles=[
            Patch(color=plt.get_cmap("RdYlGn")(1.0), label="pass"),
            Patch(color=plt.get_cmap("RdYlGn")(0.0), label="not pass"),
        ],
        loc="upper right",
        bbox_to_anchor=(1.18, 1.0),
        fontsize=8,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return out_path


def write_funnel_report(reports_by_key: dict[str, dict], out_path: Path) -> Path:
    """Markdown twin of :func:`render_funnel`: funnel deltas, a per-category
    metrics table, and the full query×category pass/fail matrix."""
    rows = _funnel_rows(reports_by_key)
    keys = list(reports_by_key)
    status_maps = {k: _statuses_by_id(reports_by_key[k]) for k in keys}
    all_ids = sorted({q["id"] for r in reports_by_key.values() for q in r["queries"]})
    bench = next(iter(reports_by_key.values()))["meta"].get("benchmark", "")

    lines = [f"# {bench} category funnel", ""]
    lines.append("## Funnel (increasing scaffolding)")
    lines.append("")
    lines.append("| category | passing | newly unlocked | regressions |")
    lines.append("|---|---|---|---|")
    for r in rows:
        unlocked = ", ".join(f"q{q:02d}" for q in r["marginal"]) or "—"
        regr = ", ".join(f"q{q:02d}" for q in r["regressions"]) or "—"
        lines.append(
            f"| {r['label']} | {r['pass_count']}/{r['total']} | {unlocked} | {regr} |"
        )
    lines += [
        "",
        "## Metrics",
        "",
        "| category | pass rate | total tokens |",
        "|---|---|---|",
    ]
    for r in rows:
        lines.append(f"| {r['label']} | {r['pass_rate']:.2f} | {r['tokens']:,} |")

    labels = [reports_by_key[k]["meta"].get("category_label", k) for k in keys]
    lines += ["", "## Per-query matrix", "", "| query | " + " | ".join(labels) + " |"]
    lines.append("|---|" + "|".join("---" for _ in keys) + "|")
    for qid in all_ids:
        cells = []
        for k in keys:
            st = status_maps[k].get(qid, "—")
            cells.append("✅" if st == "pass" else (st if st == "—" else f"❌ {st}"))
        lines.append(f"| q{qid:02d} | " + " | ".join(cells) + " |")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out_path


def run_main(results_dir: Path, charts_dir: Path, argv: list[str] | None = None) -> int:
    """Per-benchmark shim entry: ``results_dir`` and ``charts_dir`` come from the
    BenchmarkSpec, so the same code serves every benchmark."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "run_dir", nargs="?", type=Path, default=None, help="run dir (default: latest)"
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=charts_dir / "dashboard.png",
        help="output PNG",
    )
    parser.add_argument(
        "--compare-with",
        type=Path,
        default=None,
        help=(
            "render a side-by-side comparison PNG against this other run dir."
            " Output goes to <charts_dir>/dashboard_compare.png by default"
            " (override with --compare-out). Single-run dashboard is still"
            " written if no other flags suppress it."
        ),
    )
    parser.add_argument(
        "--compare-out",
        type=Path,
        default=charts_dir / "dashboard_compare.png",
        help="output PNG for the comparison render (only used with --compare-with).",
    )
    parser.add_argument(
        "--label",
        type=str,
        default="base",
        help="label for the primary run in the comparison legend.",
    )
    parser.add_argument(
        "--compare-label",
        type=str,
        default="enriched",
        help="label for the --compare-with run in the comparison legend.",
    )
    args = parser.parse_args(argv)
    run_dir = args.run_dir or latest_run_dir(results_dir)
    report, events = load_run(run_dir)
    out = render(report, events, args.out)
    failures = collect_failures(events)
    md = write_failures_report(
        run_dir, report, failures, args.out.parent / "trilogy_failures.md"
    )
    print(f"Wrote {out}  (run: {run_dir.name})")
    print(f"Wrote {md}  ({len(failures)} trilogy failures)")

    if args.compare_with is not None:
        report_b, events_b = load_run(args.compare_with)
        compare_out = render_comparison(
            report,
            events,
            report_b,
            events_b,
            args.compare_out,
            label_a=args.label,
            label_b=args.compare_label,
        )
        print(
            f"Wrote {compare_out}  "
            f"({args.label}: {run_dir.name}, "
            f"{args.compare_label}: {args.compare_with.name})"
        )
    return 0
