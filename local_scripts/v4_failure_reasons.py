"""Re-run the V4 sweep's failing nodes (minus clickhouse + timeout files) with
tracebacks, capturing a concise failure reason per node so each can be triaged
correctness-vs-structure.

Reads local_scripts/v4_sweep/results.json, writes
local_scripts/v4_sweep/reasons.json + reasons.md.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PY = REPO / ".venv" / "Scripts" / "python.exe"
OUT_DIR = REPO / "local_scripts" / "v4_sweep"

EXCLUDE_FILE_SUBSTR = ("test_clickhouse_server.py",)
TIMEOUT = 150


def failing_nodes_by_file() -> dict[str, list[str]]:
    data = json.loads((OUT_DIR / "results.json").read_text())
    by_file: dict[str, list[str]] = {}
    for r in data:
        if r["status"] != "fail":
            continue
        if any(s in r["file"] for s in EXCLUDE_FILE_SUBSTR):
            continue
        if r["failures"]:
            by_file[r["file"]] = r["failures"]
    return by_file


def run_nodes(nodes: list[str]) -> str:
    env = dict(os.environ, TRILOGY_V4_DISCOVERY="1")
    cmd = [
        str(PY),
        "-m",
        "pytest",
        *nodes,
        "--tb=line",
        "-q",
        "-p",
        "no:cacheprovider",
        "-o",
        "addopts=",
    ]
    try:
        r = subprocess.run(
            cmd,
            env=env,
            cwd=str(REPO),
            capture_output=True,
            text=True,
            timeout=TIMEOUT,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return "<<TIMEOUT>>"
    return r.stdout + r.stderr


# `--tb=line` emits one line per failure: "/abs/path:line: ExceptionType: msg".
_LINE_RE = re.compile(r"^.*[/\\]([\w_]+\.py):\d+:\s*(.*)$")


def parse_reasons(out: str, nodes: list[str]) -> dict[str, str]:
    reasons: dict[str, str] = {}
    for line in out.splitlines():
        m = _LINE_RE.match(line)
        if m and (" Error" in line or "Error:" in line or "assert" in line.lower()):
            reasons.setdefault(m.group(1) + "::" + str(len(reasons)), m.group(2)[:240])
    # The above keys by file; instead capture raw reason lines for manual match.
    raw = [
        line.strip()
        for line in out.splitlines()
        if _LINE_RE.match(line) and ("rror" in line or "assert" in line.lower())
    ]
    return {"_raw": "\n".join(raw)} if raw else {"_raw": ""}


def main() -> int:
    by_file = failing_nodes_by_file()
    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(run_nodes, nodes): f for f, nodes in by_file.items()}
        for fut in as_completed(futs):
            f = futs[fut]
            out = fut.result()
            results[f] = {
                "nodes": by_file[f],
                "reasons": parse_reasons(out, by_file[f])["_raw"],
                "raw_tail": "\n".join(
                    out.strip().splitlines()[-(len(by_file[f]) + 6) :]
                ),
            }
            print(f"done {f}")

    (OUT_DIR / "reasons.json").write_text(json.dumps(results, indent=2))

    lines = ["# V4 failure reasons (clickhouse + timeouts excluded)", ""]
    grouped = defaultdict(list)
    for f in sorted(results):
        grouped[f.split("/")[0]].append(f)
    for top in sorted(grouped):
        lines.append(f"## {top}")
        lines.append("")
        for f in grouped[top]:
            r = results[f]
            lines.append(f"### {f}  ({len(r['nodes'])} nodes)")
            lines.append("```")
            lines.append(r["reasons"] or r["raw_tail"])
            lines.append("```")
            lines.append("")
    (OUT_DIR / "reasons.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"\nWrote {OUT_DIR / 'reasons.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
