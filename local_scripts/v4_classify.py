"""Re-classify every entry in tests/v4_known_failing.py under the CURRENT v4
planner, in isolation, with --runxfail. Buckets each as:

  XPASS    -- now passes (candidate for removal from the list)
  CRASH    -- raises an exception (genuine feature/coverage gap)
  ROWS     -- assertion on result rows / row count (genuine correctness gap)
  SIZE     -- length-ceiling assert (`assert len(query) < N`); rows fine, SQL too long
  SHAPE    -- assertion on SQL text / CTE count / source name (cosmetic structure)
  TIMEOUT  -- exceeded the per-node budget
  OTHER    -- couldn't classify from the tb-line

It ALSO cross-checks each empirical bucket against the test's manual label in
`v4_known_failing.py` (the label's allowed severity is derived from the reason TEXT)
and reports, asymmetrically:
  - ESCALATIONS -- observed bucket strictly MORE severe than the label allows, e.g. a
    CRASH filed under a cosmetic `_TPCDS_SIZE`/`_MODELING` reason. This is the drift
    that hid 4 genuine crashes (q10/q2.1/rowset RecursionError, filter_constant
    ValueError) until 2026-06-25. Exits NON-ZERO so it's loud.
  - downgrades -- observed LESS severe than the label's floor (e.g. a CRASH-labeled
    test that renders SIZE on this seed). Harmless/informational; does NOT fail.

Individual tests are deterministic in isolation, but running many in parallel here can
introduce transient cross-run variance (e.g. a borderline size test flips over/under its
ceiling under contention). So each test is run REPEATS times (default 3, env
V4_CLASSIFY_REPEATS) and the WORST outcome is kept, so a transient never masks a real
crash/escalation. Re-run any test flagged "varied" in isolation -- it will be stable.

Writes local_scripts/v4_classify.json + prints a summary table. NB: this only
re-checks tests already on the skip list; a regression in a test NOT listed is
invisible here -- a full v4 sweep is still the only parity gate.
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
    re.IGNORECASE,
)
# A genuine row assertion compares counts or row tuples/lists, not SQL text.
_ROWCOUNT = re.compile(
    r"assert\s+\d+\s*[=<>!]+\s*\d+"  # numeric compare: count vs count, len < N
    r"|\blen\s*\("
    r"|row\s+(count|mismatch)"
    r"|assert\s*[\(\[].*[=!]=\s*[\(\[]",  # tuple/list equality => row data
    re.IGNORECASE,
)
# A SQL-length-ceiling assert (`assert len(query) < N`) renders as a `<` numeric
# compare whose `+ where N = len(...)` wraps the whole query. Checked BEFORE _SQL_TOKEN
# (the embedded query trips every SQL token) so SIZE doesn't masquerade as SHAPE.
_SIZE = re.compile(r"assert\s+\d+\s*<\s*\d+")
# Parallel classifier workers race the shared `zquery_timing_*.log` rename; the
# benchmark suites read-modify-write it per test. This is infra contention, not
# a planner crash -- such nodes are re-run serially in a second pass.
_TIMING_COLLISION = re.compile(
    r"(permissionerror|fileexistserror).*zquery_timing", re.IGNORECASE
)


# Which empirical bucket(s) a manual reason label is allowed to map to. Derived from
# the reason TEXT (keywords) rather than constant identity, so it stays correct across
# label renames and whether or not a dedicated `_CRASH_*` constant exists. Anything
# outside the allowed set (and not XPASS) is a LABEL MISMATCH -- the drift that hid the
# 4 crashes (a RecursionError filed under `_TPCDS_SIZE`, a ValueError under `_MODELING`).
def _expected_buckets(reason: str) -> set[str] | None:
    r = reason.lower()
    if any(k in r for k in ("crash", "raises", "recursion", "error", "exception")):
        return {"CRASH", "TIMEOUT"}
    if "verbosity" in r or "length ceiling" in r or "more verbose" in r:
        return {"SIZE"}
    # `_MODELING` self-describes as "pending classification into result vs structure"
    # -- genuinely ambiguous, so allow any non-crash bucket but still flag a CRASH.
    if "pending per-test classification" in r or "row-count" in r:
        return {"SHAPE", "ROWS", "SIZE"}
    if "shape" in r or "inlining" in r or "cte" in r:
        return {"SHAPE"}
    return None  # unknown label -> report bucket, don't assert a mismatch


# Worst-to-best severity. Used both to keep the worst of the N repeat runs and to
# compare an empirical bucket against its label's allowed set.
SEVERITY = {
    "CRASH": 6,
    "TIMEOUT": 5,
    "OTHER": 4,
    "ROWS": 3,
    "SIZE": 2,
    "SHAPE": 1,
    "XPASS": 0,
}


def _finalize(node: str, bucket: str, detail: str, **extra: object) -> dict:
    expected = _expected_buckets(vkf.V4_KNOWN_FAILING.get(node, ""))
    res = {
        "node": node,
        "bucket": bucket,
        "detail": detail,
        "expected": sorted(expected) if expected else [],
        # Asymmetric: an ESCALATION (observed bucket strictly MORE severe than the
        # label allows -- e.g. a CRASH under a `_TPCDS_SIZE` label) is the dangerous
        # hidden-blocker case and fails the run. A downgrade (less severe than the
        # label's floor -- e.g. a CRASH-labeled test rendering SIZE on this seed) is
        # harmless/intermittent and only reported.
        "escalation": False,
        "downgrade": False,
        **extra,
    }
    if expected and bucket != "XPASS":
        sev = SEVERITY.get(bucket, 4)
        res["escalation"] = sev > max(SEVERITY[e] for e in expected)
        res["downgrade"] = sev < min(SEVERITY[e] for e in expected)
    return res


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
        return _finalize(node, "TIMEOUT", "")
    # passed?
    if (
        re.search(r"\b\d+ passed\b", out)
        and " failed" not in out
        and " error" not in out
    ):
        return _finalize(node, "XPASS", "")

    if _TIMING_COLLISION.search(out):
        # Flag for the serial retry pass instead of mislabeling as CRASH.
        return _finalize(node, "CRASH", "timing-log collision", collision=True)

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
        # SIZE (length-ceiling) before SHAPE: the embedded query trips _SQL_TOKEN, so
        # a `assert N < N` numeric ceiling with a len() wrapper is verbosity, not a
        # structure diff. Then SQL-text asserts are SHAPE; a row signal with NO SQL
        # token is a real ROWS gap; anything else is OTHER, not silently mislabeled.
        size = bool(_SIZE.search(etext)) and "len(" in low
        sql = bool(_SQL_TOKEN.search(etext))
        rows = bool(_ROWCOUNT.search(etext))
        bucket = "SIZE" if size else ("SHAPE" if sql else ("ROWS" if rows else "OTHER"))
    elif "error" in low or "exception" in low:
        bucket = "CRASH"
    else:
        bucket = "OTHER"
    return _finalize(node, bucket, reason[:200])


# Tests are deterministic in isolation, but the parallel pool below can show transient
# cross-run variance (contention flips a borderline size test, etc.). Run each node
# REPEATS times and keep the WORST outcome so a transient never masks a real crash.
# Bump V4_CLASSIFY_REPEATS for a more thorough pass.
REPEATS = int(os.environ.get("V4_CLASSIFY_REPEATS", "3"))


def _worst(runs: list[dict]) -> dict:
    worst = max(runs, key=lambda r: SEVERITY.get(r["bucket"], 4))
    seen = {r["bucket"] for r in runs}
    if len(seen) > 1:
        worst = {**worst, "varied": sorted(seen)}
    return worst


def main() -> int:
    from collections import Counter, defaultdict

    runs: dict[str, list[dict]] = defaultdict(list)
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(classify_one, n): n for n in NODES for _ in range(REPEATS)}
        for fut in as_completed(futs):
            res = fut.result()
            runs[res["node"]].append(res)

    # Re-run shared-timing-log collisions serially now that the pool has drained.
    for node, rs in runs.items():
        for i, r in enumerate(rs):
            if r.get("collision"):
                rs[i] = classify_one(node)

    by_node = {n: _worst(rs) for n, rs in runs.items()}
    for n, r in sorted(by_node.items()):
        nd = " ~varied" + str(r["varied"]) if r.get("varied") else ""
        print(f"[{r['bucket']:7}] {n}{nd}")

    results = list(by_node.values())
    OUT.write_text(json.dumps(results, indent=2))

    counts = Counter(r["bucket"] for r in results)
    print(f"\n=== SUMMARY (worst of {REPEATS} run(s) per test) ===")
    for b, c in counts.most_common():
        print(f"  {b:8} {c}")

    varied = [r for r in results if r.get("varied")]
    if varied:
        print(
            "\n--- VARIED ACROSS PARALLEL RUNS (worst kept; re-run in isolation to "
            "confirm — tests are deterministic there) ---"
        )
        for r in varied:
            print(f"  {r['node']}  {r['varied']} -> {r['bucket']}")
    by_bucket = defaultdict(list)
    for r in results:
        by_bucket[r["bucket"]].append(r)
    for b in ("XPASS", "CRASH", "ROWS", "SIZE", "TIMEOUT", "OTHER"):
        if by_bucket[b]:
            print(f"\n--- {b} ---")
            for r in by_bucket[b]:
                print(f"  {r['node']}\n      {r['detail']}")

    escalations = [r for r in results if r.get("escalation")]
    downgrades = [r for r in results if r.get("downgrade")]
    if escalations:
        print(
            "\n=== LABEL ESCALATIONS (observed bucket MORE severe than the "
            "v4_known_failing.py reason allows -- a hidden correctness blocker) ==="
        )
        for r in escalations:
            print(
                f"  {r['node']}\n      got {r['bucket']}, label allows "
                f"{r['expected']}: {r['detail'][:120]}"
            )
        print(
            f"\n{len(escalations)} escalation(s) -- re-bucket these (e.g. a CRASH "
            "must not sit under a `_TPCDS_SIZE`/`_MODELING` reason)."
        )
    if downgrades:
        print(
            "\n--- label downgrades (observed LESS severe than the reason; harmless, "
            "often an over-conservative label -- verify before pruning) ---"
        )
        for r in downgrades:
            nd = f" {r['varied']}" if r.get("varied") else ""
            print(f"  {r['node']}  got {r['bucket']}, label allows {r['expected']}{nd}")
    # Only escalations fail the run; downgrades and XPASS are informational.
    return 1 if escalations else 0


if __name__ == "__main__":
    raise SystemExit(main())
