"""Live progress rendering for an in-flight agent run.

The agent writes its JSONL trace incrementally, so the eval can tail that file
while the agent subprocess is still running and surface a readable progress
feed without waiting for the run to finish.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .scoring import _is_error_result

RESULT_INDENT = " " * 21


@dataclass
class ProgressState:
    start: float
    iterations: int = 0
    tool_calls: int = 0
    tool_errors: int = 0
    pending_ts: str = ""


def _clock(seconds: float) -> str:
    s = int(seconds)
    return f"{s // 60}m{s % 60:02d}s"


def _call_duration(start_ts: str, end_ts: str) -> str:
    try:
        delta = datetime.fromisoformat(end_ts) - datetime.fromisoformat(start_ts)
        return f"{delta.total_seconds():.1f}s"
    except ValueError:
        return "?"


def _compact(value: object, limit: int) -> str:
    """Single-line, length-capped rendering of an argument or result value."""
    text = value if isinstance(value, str) else json.dumps(value, default=str)
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _render_call(name: str, args: dict) -> str:
    """Tool call as name + arguments, e.g. `trilogy run query01.preql` or
    `todo action=add description=...`."""
    if name == "trilogy":
        cli = args.get("args") or []
        joined = " ".join(str(a) for a in cli) if isinstance(cli, list) else str(cli)
        line = f"trilogy {joined}".strip()
        if args.get("stdin"):
            line += f"  | stdin: {_compact(args['stdin'], 60)}"
        return _compact(line, 200)
    rendered = "  ".join(
        f"{k}={_compact(v, 90)}" for k, v in args.items() if v is not None
    )
    return f"{name} {rendered}".strip()


def _result_preview(name: str, result: str) -> str:
    """Most informative slice of a tool result -- for `trilogy`, the stderr (on
    failure) or stdout body; otherwise the first line plus a line count, so a
    multi-line result is never silently truncated mid-content."""
    if name == "trilogy" and "--- stdout ---" in result:
        after = result.split("--- stdout ---", 1)[1]
        stdout, _, stderr = after.partition("--- stderr ---")
        body = stderr.strip() or stdout.strip()
        if body:
            return _compact(body, 160)
        return _compact(result.splitlines()[0] if result else "", 160)
    lines = [ln for ln in result.splitlines() if ln.strip()]
    if not lines:
        return ""
    head = _compact(lines[0], 160)
    return head if len(lines) == 1 else f"{head}  (+{len(lines) - 1} lines)"


def format_event(event: dict, state: ProgressState) -> list[str]:
    """Update ``state`` from a JSONL event and return console lines to print
    (empty if the event is not worth surfacing)."""
    etype = event.get("type")
    if etype == "llm_response":
        state.iterations += 1
        return []
    if etype == "tool_call":
        state.tool_calls += 1
        state.pending_ts = str(event.get("ts", ""))
        call = _render_call(str(event.get("name", "?")), event.get("arguments") or {})
        elapsed = _clock(time.perf_counter() - state.start)
        return [f"[iter {state.iterations:>3} | {elapsed:>6}] -> {call}"]
    if etype == "tool_result":
        name = str(event.get("name", "?"))
        result = str(event.get("result") or "")
        err = _is_error_result(name, result)
        if err:
            state.tool_errors += 1
        duration = _call_duration(state.pending_ts, str(event.get("ts", "")))
        status = "ERR" if err else "ok"
        return [
            f"{RESULT_INDENT}`- {status:<3} {duration:>6}  {_result_preview(name, result)}"
        ]
    return []


def heartbeat(state: ProgressState) -> str:
    ok = state.tool_calls - state.tool_errors
    return (
        f"  [{_clock(time.perf_counter() - state.start)}] "
        f"{state.iterations} iterations | {state.tool_calls} tool calls "
        f"| {ok} ok / {state.tool_errors} err"
    )


def drain_feed(log_path: Path, processed: int, state: ProgressState, emit: bool) -> int:
    """Process JSONL lines past ``processed``; print them when ``emit``. Returns
    the new count of complete lines consumed."""
    if not log_path.exists():
        return processed
    text = log_path.read_text(encoding="utf-8", errors="replace")
    # A trailing partial line (no newline yet) is left for the next drain.
    complete = text.split("\n")[:-1]
    for line in complete[processed:]:
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        rendered = format_event(event, state)
        if emit:
            for entry in rendered:
                print(entry, flush=True)
    return len(complete)
