"""Re-classify every entry in tests/v4_known_failing.py under the CURRENT v4
planner, in isolation, with --runxfail. Buckets each as:

  XPASS    -- now passes (candidate for removal from the list)
  CRASH    -- raises an exception (genuine feature/coverage gap)
  ROWS     -- assertion on result rows / row count (genuine correctness gap)
  SHAPE    -- assertion on SQL text / CTE count / source name (cosmetic structure)
  TIMEOUT  -- exceeded the per-node budget
  OTHER    -- couldn't classify from the tb-line

Writes local_scripts/v4_classify.json + prints a summary table.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PY = REPO / ".venv" / "Scripts" / "python.exe"
OUT = REPO / "local_scripts" / "v4_classify.json"
TIMEOUT = 180

spec = importlib.util.spec_from_file_location(
    "vkf", REPO / "tests" / "v4_known_failing.py"
)
vkf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vkf)
NODES = list(vkf.V4_KNOWN_FAILING.keys())

# SQL tokens that mark an assertion as comparing generated SQL (SHAPE), matched
# case-insensitively with word boundaries so they survive the newline/truncation
# formatting of real query text in the traceback.
# NB: no `where`/`and` -- pytest's own assertion-rewrite explanation lines
# (" + where 534 = ...") use those words and would false-positive as SQL.
_SQL_TOKEN = re.compile(
    r"\b(select|with recursive|with|from|group by|order by|join|"
    r"case when|cte|datasource|union)\b|\.sql\b|generated",
    re.I,
)
# A genuine row assertion compares counts or row tuples/lists, not SQL text.
_ROWCOUNT = re.compile(
    r"assert\s+\d+\s*[=<>!]+\s*\d+"  # numeric compare: count vs count, len < N
    r"|\blen\s*\("
    r"|row\s+(count|mismatch)"
    r"|assert\s*[\(\[].*[=!]=\s*[\(\[]",  # tuple/list equality => row data
    re.I,
)
# Parallel classifier workers race the shared `zquery_timing_*.log` rename; the
# benchmark suites read-modify-write it per test. This is infra contention, not
# a planner crash -- such nodes are re-run serially in a second pass.
_TIMING_COLLISION = re.compile(
    r"(permissionerror|fileexistserror).*zquery_timing", re.I
)


def _run_pytest(node: str) -> str:
    env = dict(os.environ, TRILOGY_V4_DISCOVERY="1")
    cmd = [
        str(PY),
        "-m",
        "pytest",
        node,
        "--runxfail",
        "--tb=short",
        "-q",
        "-p",
        "no:cacheprovider",
        "-o",
        "addopts=",
    ]
    r = subprocess.run(
        cmd,
        env=env,
        cwd=str(REPO),
        capture_output=True,
        text=True,
        timeout=TIMEOUT,
    )
    return r.stdout + r.stderr


def classify_one(node: str) -> dict:
    try:
        out = _run_pytest(node)
    except subprocess.TimeoutExpired:
        return {"node": node, "bucket": "TIMEOUT", "detail": ""}
    # passed?
    if (
        re.search(r"\b\d+ passed\b", out)
        and " failed" not in out
        and " error" not in out
    ):
        return {"node": node, "bucket": "XPASS", "detail": ""}

    if _TIMING_COLLISION.search(out):
        # Flag for the serial retry pass instead of mislabeling as CRASH.
        return {
            "node": node,
            "bucket": "CRASH",
            "detail": "timing-log collision",
            "collision": True,
        }

    # `--tb=short` prints the failing assertion / exception operands on `E ` lines.
    # Those carry the real signal (SQL text vs row tuples) -- the file:line summary
    # alone is often a bare "AssertionError:".
    e_lines = [ln[1:].strip() for ln in out.splitlines() if re.match(r"^E\s", ln)]
    etext = "\n".join(e_lines)
    low = etext.lower()
    rl = out.lower()
    reason = next((ln for ln in e_lines if ln), "")

    if not etext:
        bucket = "CRASH" if ("error" in rl and "passed" not in rl) else "OTHER"
    elif "assertionerror" in low or low.startswith("assert"):
        # SQL-text asserts are SHAPE even when the line also shows a length number;
        # a row signal with NO SQL token is a real ROWS gap; anything else is
        # ambiguous (OTHER) rather than silently mislabeled.
        sql = bool(_SQL_TOKEN.search(etext))
        rows = bool(_ROWCOUNT.search(etext))
        bucket = "SHAPE" if sql else ("ROWS" if rows else "OTHER")
    elif "error" in low or "exception" in low:
        bucket = "CRASH"
    else:
        bucket = "OTHER"
    return {"node": node, "bucket": bucket, "detail": reason[:200]}


def main() -> int:
    by_node: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(classify_one, n): n for n in NODES}
        for fut in as_completed(futs):
            res = fut.result()
            by_node[res["node"]] = res
            print(f"[{res['bucket']:7}] {res['node']}")

    # Re-run shared-timing-log collisions serially now that the pool has drained.
    collided = [n for n, r in by_node.items() if r.get("collision")]
    if collided:
        print(f"\n=== serial retry for {len(collided)} timing-log collision(s) ===")
        for node in collided:
            res = classify_one(node)
            by_node[node] = res
            print(f"[{res['bucket']:7}] {node} (serial retry)")

    results = list(by_node.values())
    OUT.write_text(json.dumps(results, indent=2))

    from collections import Counter, defaultdict

    counts = Counter(r["bucket"] for r in results)
    print("\n=== SUMMARY ===")
    for b, c in counts.most_common():
        print(f"  {b:8} {c}")
    by_bucket = defaultdict(list)
    for r in results:
        by_bucket[r["bucket"]].append(r)
    for b in ("XPASS", "CRASH", "ROWS", "TIMEOUT", "OTHER"):
        if by_bucket[b]:
            print(f"\n--- {b} ---")
            for r in by_bucket[b]:
                print(f"  {r['node']}\n      {r['detail']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
