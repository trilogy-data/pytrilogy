#!/usr/bin/env python
"""Where an agent run's context goes.

Context tax = a tool result's size x the number of later LLM calls that
re-send it. A 10k result read on turn 2 of a 12-turn run costs ~10x a 10k
result on the last turn, so this ranks tool categories by what they actually
cost, not by how big they look. Also tallies non-zero tool exits by kind.

    python evals/common/context_tax.py evals/<suite>/results/<run_dir> [--status pass|fail]
"""

from __future__ import annotations

import argparse
import ast
import collections
import json
import re
from pathlib import Path


def category(name: str, args: dict) -> str:
    if name != "trilogy":
        return name
    a = args.get("args", [])
    if not a:
        return "trilogy ?"
    if a[0] == "agent-info":
        return "agent-info " + (a[1] if len(a) > 1 else "(index)")
    if a[0] == "file" and len(a) > 1:
        if a[1] == "write":
            return (
                "file write --run-and-delete"
                if "--run-and-delete" in a
                else "file write"
            )
        return "file " + a[1]
    if a[0] == "explore":
        narrowed = any(x in a for x in ("--ns", "--regex"))
        return "explore --ns/--regex" if narrowed else "explore"
    return a[0]


def error_kind(result: str) -> str:
    if "Binder Error" in result or "Catalog Error" in result:
        return "duckdb binder/catalog"
    m = re.search(r"Syntax \[(\d+)\]", result)
    if m:
        return f"syntax[{m.group(1)}]"
    m = re.search(r'"(?:error_type|message|error)":\s*"([^"]{0,70})', result)
    return m.group(1) if m else result[:70].replace("\n", " ")


def parse_args(raw: object) -> dict:
    if isinstance(raw, dict):
        return raw
    try:
        parsed = ast.literal_eval(raw) if isinstance(raw, str) else {}
    except (ValueError, SyntaxError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--status", default=None, help="only queries with this status")
    args = parser.parse_args()
    report = json.loads((args.run_dir / "report.json").read_text(encoding="utf-8"))
    tax: collections.Counter[str] = collections.Counter()
    size: collections.Counter[str] = collections.Counter()
    calls: collections.Counter[str] = collections.Counter()
    errors: collections.Counter[str] = collections.Counter()
    error_queries: dict[str, set[int]] = collections.defaultdict(set)
    for query in report["queries"]:
        if args.status and query["status"] != args.status:
            continue
        log = args.run_dir / f"agent_log.q{query['id']:02d}.jsonl"
        if not log.exists():
            continue
        events = [json.loads(line) for line in log.open(encoding="utf-8")]
        llm_at = [i for i, e in enumerate(events) if e["type"] == "llm_response"]
        pending: list[str] = []
        for i, event in enumerate(events):
            if event["type"] == "tool_call":
                pending.append(category(event["name"], parse_args(event["arguments"])))
            elif event["type"] == "tool_result":
                cat = pending.pop(0) if pending else "?"
                result = event.get("result", "")
                calls[cat] += 1
                size[cat] += len(result)
                tax[cat] += len(result) * sum(1 for j in llm_at if j > i)
                if "exit_code:" in result and not result.startswith("exit_code: 0"):
                    kind = error_kind(result)
                    errors[kind] += 1
                    error_queries[kind].add(query["id"])
    total = sum(tax.values()) or 1
    print(f"context tax {total / 4 / 1e6:.1f}M tok-equiv (chars/4)")
    print(f"{'category':30} {'calls':>6} {'avg chars':>9} {'Mtok':>7} {'share':>6}")
    for cat, t in tax.most_common():
        avg = size[cat] // calls[cat]
        print(f"{cat:30} {calls[cat]:6} {avg:9} {t / 4 / 1e6:7.2f} {t / total:6.1%}")
    print("\ntool errors:")
    for kind, n in errors.most_common(30):
        print(f"  {n:4} {kind:50} q{sorted(error_queries[kind])}")


if __name__ == "__main__":
    main()
