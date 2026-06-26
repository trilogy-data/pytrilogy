#!/usr/bin/env python
"""Generate a markdown scan of every `trilogy` error an agent hit per query in a
run, with the query that produced it and the agent's next thought — for spotting
framework bugs vs agent thrash.

    python evals/tpcds_agent/error_scan.py                 # latest run, all queries
    python evals/tpcds_agent/error_scan.py --query-ids 5,11
    python evals/tpcds_agent/error_scan.py --run results/20260626-125555 --out scan.md
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from spec import SPEC  # noqa: E402


def _latest_run(results_dir: Path) -> Path:
    runs = sorted(
        (
            p
            for p in results_dir.iterdir()
            if p.is_dir() and re.match(r"\d{8}-", p.name)
        ),
        key=lambda p: p.stat().st_mtime,
    )
    if not runs:
        raise SystemExit(f"no run dirs in {results_dir}")
    return runs[-1]


def _write_content(arguments: dict) -> str:
    """The content of a `file write` call — via stdin or a `--content <text>` arg."""
    if arguments.get("stdin"):
        return arguments["stdin"]
    args = arguments.get("args", [])
    if "--content" in args:
        i = args.index("--content")
        if i + 1 < len(args):
            return args[i + 1]
    return ""


def _error_message(result: str) -> str:
    """Pull the JSON `message` out of a failing tool_result string."""
    body = result
    if "--- stdout ---" in result:
        body = result.split("--- stdout ---", 1)[1].split("--- stderr", 1)[0].strip()
    dec, i = json.JSONDecoder(), 0
    while i < len(body):
        while i < len(body) and body[i] in " \t\r\n":
            i += 1
        if i >= len(body):
            break
        try:
            obj, i = dec.raw_decode(body, i)
        except json.JSONDecodeError:
            break
        if isinstance(obj, dict) and obj.get("message"):
            return obj["message"]
    return body[:200]


def _trigger(args: list[str]) -> str:
    if not args:
        return "?"
    if args[0] == "run":
        return "run " + " ".join(a for a in args[1:] if a != "--content")[:70]
    if len(args) > 1 and args[1] == "write":
        return "write " + (args[2] if len(args) > 2 else "")
    return " ".join(args[:4])[:80]


def _query_for(args: list[str], files: dict[str, str], last_write: str) -> str:
    """The query text active for an error: a written file's content (`run file`),
    the inline query (`run --import … <query>`), or the content being written."""
    if not args:
        return ""
    if args[0] == "run":
        if len(args) > 1 and not args[1].startswith("--"):
            return files.get(args[1], "")
        rest = [
            a
            for a in args[1:]
            if not a.startswith("--") and a not in ("duckdb", "tpcds")
        ]
        return rest[-1] if rest else " ".join(args[1:])
    if len(args) > 1 and args[1] == "write":
        return last_write
    return ""


def scan_query(jsonl: Path) -> list[dict]:
    """Errors for one query: trigger, error message, producing query, next thought."""
    items: list[dict] = []
    last_call: list[str] = []
    files: dict[str, str] = {}
    last_write = ""
    pending: list[dict] = []
    for line in jsonl.open(encoding="utf-8"):
        o = json.loads(line)
        t = o.get("type")
        if t == "tool_call" and o.get("name") == "trilogy":
            a = o.get("arguments", {})
            last_call = a.get("args", [])
            if len(last_call) >= 3 and last_call[1] == "write":
                last_write = _write_content(a)
                files[last_call[2]] = last_write
        elif t == "tool_result":
            r = str(o.get("result", ""))
            if "exit_code: 1" in r or "exit_code: 2" in r:
                rec = {
                    "trigger": _trigger(last_call),
                    "error": _error_message(r),
                    "query": _query_for(last_call, files, last_write),
                    "thought": None,
                }
                items.append(rec)
                pending.append(rec)
        elif t == "llm_response":
            text = (o.get("text") or "").strip()
            if text and pending:
                for rec in pending:
                    rec["thought"] = text
                pending = []
    return items


def build_report(
    run: Path, query_ids: list[int] | None, max_query: int, max_thought: int
) -> tuple[str, int]:
    logs = sorted(run.glob("agent_log.q*.jsonl"))
    if query_ids is not None:
        wanted = {f"agent_log.q{q:02d}.jsonl" for q in query_ids}
        logs = [p for p in logs if p.name in wanted]
    out = [
        f"# Execute-trilogy error scan — run {run.name}\n",
        "Each `trilogy` error per query, the query that produced it, and the "
        "agent's next thought.\n",
    ]
    total = 0
    for jsonl in logs:
        qid = re.search(r"q(\d+)", jsonl.name).group(1)
        items = scan_query(jsonl)
        total += len(items)
        out.append(f"\n## q{qid} — {len(items)} error(s)\n")
        for n, rec in enumerate(items, 1):
            out.append(f"### q{qid} error {n} — `{rec['trigger']}`")
            out.append(f"**Error:** {rec['error'][:400]}\n")
            q = (rec["query"] or "(no query captured)").strip()
            trunc = "" if len(q) <= max_query else "\n…[truncated]"
            out.append("**Query:**\n```trilogy\n" + q[:max_query] + trunc + "\n```\n")
            out.append(
                f"**Followup thought:** {(rec['thought'] or '(none)')[:max_thought]}\n"
            )
    return "\n".join(out), total


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, help="run dir (default: latest)")
    parser.add_argument("--query-ids", help="comma-separated, e.g. 5,11,14")
    parser.add_argument(
        "--out",
        type=Path,
        help="output md (default: evals/tpcds_agent/error_scan_<run>.md)",
    )
    parser.add_argument("--max-query-chars", type=int, default=2200)
    parser.add_argument("--max-thought-chars", type=int, default=700)
    args = parser.parse_args()

    run = args.run or _latest_run(SPEC.results_dir)
    if not run.is_absolute():
        run = Path.cwd() / run
    ids = [int(x) for x in args.query_ids.split(",")] if args.query_ids else None
    # Default to the (findable) eval dir, not buried inside the run dir.
    out_path = args.out or SPEC.eval_dir / f"error_scan_{run.name}.md"

    report, total = build_report(run, ids, args.max_query_chars, args.max_thought_chars)
    out_path.write_text(report, encoding="utf-8")
    print(f"{total} errors across {run.name} -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
