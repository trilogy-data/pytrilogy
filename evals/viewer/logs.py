"""Agent trajectory log parsing.

One ``agent_log.*.jsonl`` in, one renderable timeline out. Kept free of any
run/benchmark knowledge so both the lazy per-question API (``runs.py``) and the
baked static page (``collect.py``) can use it.
"""

from __future__ import annotations

import itertools
import json
import os
import re
from pathlib import Path

# Log stems are the question keys the page passes back to us ("q05", "q96.r3").
KEY_RE = re.compile(r"^[A-Za-z0-9._-]+$")
QID_RE = re.compile(r"\bq(\d+)\b")
REP_RE = re.compile(r"\br(\d+)\b")


def log_path(run_dir: Path, key: str) -> Path | None:
    """The log a question key names, or None if the key is bogus. Keys arrive
    from the browser, so anything path-like is refused outright."""
    if not KEY_RE.match(key or "") or ".." in key:
        return None
    path = run_dir / f"agent_log.{key}.jsonl"
    return path if path.is_file() else None


def key_of(path: Path) -> str:
    return path.name[len("agent_log.") : -len(".jsonl")]


def ids_in_key(key: str) -> tuple[int | None, int | None]:
    """(query id, repeat index) parsed out of a log stem."""
    qid = QID_RE.search(key)
    rep = REP_RE.search(key)
    return (int(qid.group(1)) if qid else None, int(rep.group(1)) if rep else None)


def stamp(path: Path) -> str:
    """Change token for a log: the page re-fetches a trajectory only when this
    moves, so a live run streams without re-sending finished questions."""
    try:
        st = path.stat()
    except OSError:
        return "0"
    return f"{st.st_mtime_ns}-{st.st_size}"


def _call_label(name: str, args: list[str]) -> str:
    if name == "trilogy" and args:
        head = [a for a in args if not a.startswith("--")][:3]
        return "trilogy " + " ".join(head)
    return name


def _extract_content(args: list[str]) -> str | None:
    """The written payload of a `trilogy file write` - passed as `-c`/`--content`."""
    for flag in ("--content", "-c"):
        if flag in args:
            i = args.index(flag)
            if i + 1 < len(args):
                return args[i + 1]
    return None


def _result_ok(output: str) -> bool:
    low = output.lower()
    if "exit_code: 0" in low:
        return True
    return not any(
        k in low for k in ("error", "traceback", "exit_code: 1", '"event": "error"')
    )


def _reviewer_input(e: dict) -> str:
    """The exact input the reviewer saw: system prompt + agent-only transcript
    (older logs predate these fields and render an empty input)."""
    parts = []
    if e.get("system_prompt"):
        parts.append("SYSTEM PROMPT:\n" + e["system_prompt"])
    if e.get("transcript"):
        parts.append("AGENT'S RECENT MESSAGES:\n" + e["transcript"])
    return "\n\n".join(parts)


def _read_events(path: Path) -> list[dict]:
    """Tolerant JSONL read - skip blank/half-written lines so a log that's still
    being appended to (live run) parses cleanly up to the last complete record."""
    events: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


def _attribute_tool_tokens(timeline: list[dict]) -> None:
    """Tool results carry no usage, so price them from the prompt growth they cause:
    the next turn's prompt minus this turn's prompt+completion is what the results
    in between added. Split proportionally by output size when a turn made several
    calls (exact when it made one); skip non-positive deltas (history compaction)."""
    turns = [i for i, e in enumerate(timeline) if e["role"] == "assistant"]
    for a, b in itertools.pairwise(turns):
        u, nxt = timeline[a]["usage"], timeline[b]["usage"]
        delta = nxt.get("prompt_tokens", 0) - (
            u.get("prompt_tokens", 0) + u.get("completion_tokens", 0)
        )
        tools = [e for e in timeline[a + 1 : b] if e["role"] == "tool"]
        if delta <= 0 or not tools:
            continue
        sizes = [max(len(t["output"]), 1) for t in tools]
        spent = 0
        for t, size in zip(tools[:-1], sizes[:-1]):
            t["tokens"] = round(delta * size / sum(sizes))
            spent += t["tokens"]
        tools[-1]["tokens"] = delta - spent
        for t in tools:
            t["exact"] = len(tools) == 1


def parse_log(path: Path) -> dict:
    events = _read_events(path)
    meta: dict = {"task": "", "model": "", "provider": ""}
    timeline: list[dict] = []
    prompt = completion = total = iterations = tool_calls = 0
    for e in events:
        t = e.get("type")
        if t == "session_start":
            meta["task"] = e.get("command", "")
            meta["model"] = e.get("model", "")
            meta["provider"] = e.get("provider", "")
        elif t == "llm_response":
            calls = []
            for c in e.get("tool_calls") or []:
                args = (c.get("arguments") or {}).get("args") or []
                calls.append(
                    {
                        "label": _call_label(c.get("name", ""), args),
                        "args": args,
                        "content": _extract_content(args),
                    }
                )
            u = e.get("usage") or {}
            prompt += u.get("prompt_tokens", 0)
            completion += u.get("completion_tokens", 0)
            total += u.get("total_tokens", 0)
            iterations += 1
            tool_calls += len(calls)
            timeline.append(
                {
                    "role": "assistant",
                    "text": e.get("text") or "",
                    "calls": calls,
                    "usage": u,
                }
            )
        elif t == "tool_result":
            out = e.get("result")
            if not isinstance(out, str):
                out = json.dumps(out, indent=1)
            timeline.append(
                {
                    "role": "tool",
                    "name": e.get("name", ""),
                    "ok": _result_ok(out),
                    "output": out,
                }
            )
        elif t == "reviewer_verdict":
            done = bool(e.get("is_done"))
            timeline.append(
                {
                    "role": "reviewer",
                    "verdict": "DONE" if done else "NOT_DONE",
                    "ok": done,
                    "note": e.get("note") or "",
                    "kickback": e.get("kickback_count", 0),
                    "input": _reviewer_input(e),
                }
            )
        elif t == "reviewer_bypassed":
            timeline.append(
                {
                    "role": "reviewer",
                    "verdict": "BYPASSED",
                    "ok": True,
                    "note": e.get("reason") or "force=true",
                    "kickback": 0,
                }
            )
    _attribute_tool_tokens(timeline)
    derived = {
        "iterations": iterations,
        "tool_calls": tool_calls,
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": total,
    }
    return {"meta": meta, "timeline": timeline, "derived": derived}


# Parsed-log cache. The page fetches one question at a time now, so this mostly
# absorbs re-opens and the live poll; keyed by (mtime, size) so a still-appending
# log re-parses. Cached dicts are shared - callers must treat them as read-only.
_LOG_CACHE: dict[str, tuple[tuple[float, int], dict]] = {}
_LOG_CACHE_DIRS: list[str] = []  # LRU of run dirs with cached logs
_LOG_CACHE_MAX_DIRS = 3


def parse_log_cached(path: Path) -> dict:
    try:
        st = path.stat()
    except OSError:
        return parse_log(path)
    key, token = str(path), (st.st_mtime, st.st_size)
    cached = _LOG_CACHE.get(key)
    if cached is None or cached[0] != token:
        _LOG_CACHE[key] = (token, parse_log(path))
    return _LOG_CACHE[key][1]


def touch_cache_dir(results_dir: Path) -> None:
    d = str(results_dir)
    if d in _LOG_CACHE_DIRS:
        _LOG_CACHE_DIRS.remove(d)
    _LOG_CACHE_DIRS.append(d)
    while len(_LOG_CACHE_DIRS) > _LOG_CACHE_MAX_DIRS:
        gone = _LOG_CACHE_DIRS.pop(0) + os.sep
        for k in [k for k in _LOG_CACHE if k.startswith(gone)]:
            del _LOG_CACHE[k]
