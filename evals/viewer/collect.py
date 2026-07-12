"""Parse agent trajectory logs and assemble a run's viewer payload.

Reads every ``agent_log.*.jsonl`` in a run dir (plus ``report.json`` /
``repeat_report.json`` for per-query status and metrics when present) and
returns the JSON-ready run list the page renders. Benchmark specifics —
canonical query dir, prompt params — come from the suite's ``BenchmarkSpec``.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from common.spec import BenchmarkSpec

_RUN_RE = re.compile(r"\.r(\d+)\.jsonl$")
_QID_RE = re.compile(r"\.q(\d+)\.jsonl$")
_QID_ANY = re.compile(r"\.q(\d+)\b")  # query id in either eval or repeat log names
# Which candidate/canonical extension each eval leg authors.
_CATEGORY_EXT = {
    "enriched": "preql",
    "ingest": "preql",
    "sql_bare": "sql",
    "sql_schema": "sql",
}


def _call_label(name: str, args: list[str]) -> str:
    if name == "trilogy" and args:
        head = [a for a in args if not a.startswith("--")][:3]
        return "trilogy " + " ".join(head)
    return name


def _extract_content(args: list[str]) -> str | None:
    """The written payload of a `trilogy file write` — passed as `-c`/`--content`."""
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
    if any(
        k in low for k in ("error", "traceback", "exit_code: 1", '"event": "error"')
    ):
        return False
    return True


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
    """Tolerant JSONL read — skip blank/half-written lines so a log that's still
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
    for a, b in zip(turns, turns[1:]):
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


def _load_eval_report(results_dir: Path) -> dict[int, dict]:
    """run_eval's report.json: merge per-query status + duration, keyed by query id."""
    rp = results_dir / "report.json"
    if not rp.exists():
        return {}
    data = json.loads(rp.read_text(encoding="utf-8"))
    by_id: dict[int, dict] = {q["id"]: dict(q) for q in data.get("queries", [])}
    for p in data.get("per_query", []):
        by_id.setdefault(p["id"], {}).update(
            {k: p[k] for k in ("duration_seconds", "timed_out") if k in p}
        )
    return by_id


def _ref_dir(spec: BenchmarkSpec) -> Path | None:
    """The hand-authored canonical query dir for this benchmark."""
    for cand in (spec.references_dir, spec.default_enriched_dir):
        if cand is not None and cand.is_dir():
            return cand
    return None


def _read_query(base: Path | None, qid: int, ext: str) -> dict | None:
    if base is None:
        return None
    for name in (f"query{qid:02d}.{ext}", f"query{qid}.{ext}"):
        p = base / name
        if p.exists():
            return {
                "name": name,
                "lang": ext,
                "src": p.read_text(encoding="utf-8"),
                "_path": p,
                "_base": base,
            }
    return None


# --- preql → SQL transpilation (best-effort; the viewer stays usable without it) ---
_TRANSPILE_ENGINE: object | None = None
_ENGINE_FAILED = False
_SQL_CACHE: dict[tuple, tuple] = {}  # (path, mtime, working_path) -> (sql, is_error)
_PARAMS_CACHE: dict[Path, dict[int, dict]] = {}  # prompts_file -> qid -> params


def _transpile_engine():
    """A DB-less in-memory DuckDB executor reused across renders — generate_sql is
    pure transpilation, so we never open the (large) workspace database."""
    global _TRANSPILE_ENGINE, _ENGINE_FAILED
    if _TRANSPILE_ENGINE is None and not _ENGINE_FAILED:
        try:
            from trilogy import Dialects
            from trilogy.core.models.environment import Environment
            from trilogy.dialect.config import DuckDBConfig

            _TRANSPILE_ENGINE = Dialects.DUCK_DB.default_executor(
                environment=Environment(), conf=DuckDBConfig()
            )
        except Exception:
            _ENGINE_FAILED = True
    return _TRANSPILE_ENGINE


def _load_params(spec: BenchmarkSpec) -> dict[int, dict]:
    pf = spec.prompts_file
    if pf not in _PARAMS_CACHE:
        params: dict[int, dict] = {}
        if pf.exists():
            data = json.loads(pf.read_text(encoding="utf-8"))
            qs = data.get("queries", []) if isinstance(data, dict) else data
            params = {q["id"]: q["params"] for q in qs if q.get("params")}
        _PARAMS_CACHE[pf] = params
    return _PARAMS_CACHE[pf]


def _render_sql(text: str, working_path: Path, params: dict | None) -> tuple[str, bool]:
    engine = _transpile_engine()
    if engine is None:
        return "", True
    from trilogy.core.models.environment import Environment

    try:
        engine.environment = Environment(working_path=working_path)
        if params:
            engine.environment.set_parameters(
                **{name: spec.get("value") for name, spec in params.items()}
            )
        statements = engine.generate_sql(text)
        return (statements[-1] if statements else "-- (no statement)"), False
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}", True


def _augment_sql(q: dict | None, params: dict | None) -> dict | None:
    """Attach rendered SQL (cached by file mtime) and strip internal path fields."""
    if q is None:
        return None
    base, path = q.pop("_base"), q.pop("_path")
    if q["lang"] != "preql":
        q["sql"], q["sqlError"] = q["src"], False  # SQL legs: candidate is already SQL
        return q
    try:
        mtime = path.stat().st_mtime
    except OSError:
        mtime = 0.0
    key = (str(path), mtime, str(base))
    if key not in _SQL_CACHE:
        _SQL_CACHE[key] = _render_sql(q["src"], base, params)
    q["sql"], q["sqlError"] = _SQL_CACHE[key]
    return q


def _run_category(results_dir: Path) -> str | None:
    """The eval leg (enriched/ingest/sql_bare/sql_schema) — picks the query language."""
    rp = results_dir / "repeat_report.json"
    if rp.exists():
        return json.loads(rp.read_text(encoding="utf-8")).get("meta", {}).get("mode")
    ep = results_dir / "report.json"
    if ep.exists():
        return (
            json.loads(ep.read_text(encoding="utf-8")).get("meta", {}).get("category")
        )
    return None


def collect(results_dir: Path, spec: BenchmarkSpec) -> list[dict]:
    report = {}
    repeat_qid = None
    rp = results_dir / "repeat_report.json"
    if rp.exists():
        data = json.loads(rp.read_text(encoding="utf-8"))
        report = {r["rep"]: r for r in data.get("runs", [])}
        repeat_qid = data.get("meta", {}).get("query_id")
    eval_report = _load_eval_report(results_dir)
    ext = _CATEGORY_EXT.get(_run_category(results_dir) or "", "preql")
    ref_dir = _ref_dir(spec)
    workspace = results_dir / "workspace"
    # Replay re-runs one query against the run's own workspace and rewrites its
    # slice of report.json — neither is present for repeat-harness dirs.
    replayable = (results_dir / "report.json").exists() and workspace.is_dir()
    runs: list[dict] = []
    for path in sorted(results_dir.glob("agent_log.*.jsonl")):
        m = _RUN_RE.search(path.name)
        rep = int(m.group(1)) if m else len(runs)
        qm = _QID_RE.search(path.name)
        reported = report.get(rep, {})
        if not reported and qm:
            reported = eval_report.get(int(qm.group(1)), {})
        parsed = parse_log(path)
        # Derived per-run metrics fill gaps; authoritative report values win.
        metrics = {**parsed["derived"], **reported}
        qany = _QID_ANY.search(path.name)
        qid = int(qany.group(1)) if qany else repeat_qid
        queries = {}
        if qid is not None:
            # Candidate at workspace root (single-leg runs), else the rep's worker dir.
            candidate = _read_query(workspace, qid, ext) or _read_query(
                workspace / f"_worker_{rep}", qid, ext
            )
            params = _load_params(spec).get(qid)
            queries = {
                "candidate": _augment_sql(candidate, params),
                "canonical": _augment_sql(_read_query(ref_dir, qid, ext), params),
            }
        runs.append(
            {
                "name": path.name.replace("agent_log.", "").replace(".jsonl", ""),
                "rep": rep,
                "qid": qid,
                # `qm` (not `qany`) — a repeat log's `.qNN.rNN.` name carries a
                # qid but has no report entry to splice back into.
                "replayable": replayable and qm is not None,
                "meta": parsed["meta"],
                "timeline": parsed["timeline"],
                "metrics": metrics,
                "queries": queries,
            }
        )
    return runs
