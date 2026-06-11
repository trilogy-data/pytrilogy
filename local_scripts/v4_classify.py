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

_LINE_RE = re.compile(r"^.*[/\\][\w_]+\.py:\d+:\s*(.*)$")


def classify_one(node: str) -> dict:
    env = dict(os.environ, TRILOGY_V4_DISCOVERY="1")
    cmd = [
        str(PY),
        "-m",
        "pytest",
        node,
        "--runxfail",
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
        )
    except subprocess.TimeoutExpired:
        return {"node": node, "bucket": "TIMEOUT", "detail": ""}
    out = r.stdout + r.stderr
    # passed?
    if (
        re.search(r"\b\d+ passed\b", out)
        and " failed" not in out
        and " error" not in out
    ):
        return {"node": node, "bucket": "XPASS", "detail": ""}

    reason = ""
    for line in out.splitlines():
        m = _LINE_RE.match(line)
        if m and ("rror" in line or "assert" in line.lower() or "Exception" in line):
            reason = m.group(1).strip()
            break
    low = reason.lower()
    rl = out.lower()
    if (
        "collection error" in rl
        or "errors" in rl
        and "error" in rl
        and "passed" not in rl
        and not reason
    ):
        bucket = "CRASH"
    elif reason.startswith("assert") or "assertionerror" in low:
        # distinguish row asserts from SQL-shape asserts by peeking at the diff
        if re.search(
            r"select |\bcte\b| from |group by|join |datasource|\.sql|generated", rl
        ):
            bucket = "SHAPE"
        else:
            bucket = "ROWS"
        # row-count style: "assert 5 == 100" or len mismatch
        if re.search(r"assert \d+ == \d+", low) or "len(" in low:
            bucket = "ROWS"
    elif "error" in low or "exception" in low:
        bucket = "CRASH"
    elif " failed" in out or " error" in out:
        bucket = "OTHER"
    else:
        bucket = "OTHER"
    return {"node": node, "bucket": bucket, "detail": reason[:200]}


def main() -> int:
    results = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(classify_one, n): n for n in NODES}
        for fut in as_completed(futs):
            res = fut.result()
            results.append(res)
            print(f"[{res['bucket']:7}] {res['node']}")
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
