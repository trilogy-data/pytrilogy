"""Per-file V4 parity sweep.

Runs each test file as its own pytest subprocess with TRILOGY_V4_DISCOVERY=1
and an OS-level timeout, so a query that spins under the v4 planner only
forfeits its own file instead of hanging the whole run. Records pass / fail /
timeout / error per file plus the failing node ids.

    python local_scripts/v4_suite_sweep.py            # full scope
    python local_scripts/v4_suite_sweep.py --timeout 90 --workers 6

Writes local_scripts/v4_sweep/results.json and SUMMARY.md.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
TESTS = REPO / "tests"
PY = REPO / ".venv" / "Scripts" / "python.exe"
OUT_DIR = REPO / "local_scripts" / "v4_sweep"

IGNORE_PREFIXES = ("adventureworks/", "engine/bigquery/", "engine/snowflake/")


def collect_files() -> list[Path]:
    files = []
    for p in sorted(TESTS.rglob("test_*.py")):
        rel = p.relative_to(TESTS).as_posix()
        if rel.startswith(IGNORE_PREFIXES):
            continue
        files.append(p)
    return files


def _parse_failures(out: str) -> list[str]:
    nodes: list[str] = []
    for line in out.splitlines():
        for tag in ("FAILED ", "ERROR "):
            if line.startswith(tag):
                rest = line[len(tag) :]
                node = rest.split(" - ", 1)[0].strip()
                if node and "::" in node:
                    nodes.append(node)
                break
    return nodes


def run_file(p: Path, timeout: int) -> dict:
    env = dict(os.environ, TRILOGY_V4_DISCOVERY="1")
    cmd = [
        str(PY),
        "-m",
        "pytest",
        str(p),
        "--tb=no",
        "-q",
        "-rfE",
        "-p",
        "no:cacheprovider",
        "-m",
        "not adventureworks_execution",
    ]
    rel = p.relative_to(TESTS).as_posix()
    try:
        r = subprocess.run(
            cmd,
            env=env,
            cwd=str(REPO),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"file": rel, "status": "timeout", "failures": []}
    out = r.stdout + r.stderr
    failures = _parse_failures(out)
    if failures:
        status = "fail"
    elif r.returncode == 0:
        status = "pass"
    elif r.returncode == 5:
        status = "no_tests"
    else:
        status = "error"
    return {
        "file": rel,
        "status": status,
        "failures": failures,
        "rc": r.returncode,
        "tail": "\n".join(out.strip().splitlines()[-3:]),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--timeout", type=int, default=120)
    ap.add_argument("--workers", type=int, default=6)
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    files = collect_files()
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(run_file, p, args.timeout): p for p in files}
        for fut in as_completed(futs):
            res = fut.result()
            results.append(res)
            mark = {
                "pass": ".",
                "fail": "F",
                "timeout": "T",
                "error": "E",
                "no_tests": "-",
            }.get(res["status"], "?")
            n = len(res["failures"])
            print(f"{mark} {res['file']}" + (f" ({n} failed)" if n else ""))

    results.sort(key=lambda r: r["file"])
    (OUT_DIR / "results.json").write_text(json.dumps(results, indent=2))

    buckets: dict[str, list[dict]] = {}
    for r in results:
        buckets.setdefault(r["status"], []).append(r)

    lines = ["# V4 suite sweep", ""]
    for st in ("fail", "timeout", "error", "pass", "no_tests"):
        lines.append(f"- {st}: {len(buckets.get(st, []))}")
    total_failures = sum(len(r["failures"]) for r in results)
    lines.append(f"- total failing node ids: {total_failures}")
    lines.append("")
    for st in ("timeout", "error", "fail"):
        rows = buckets.get(st, [])
        if not rows:
            continue
        lines.append(f"## {st} ({len(rows)} files)")
        lines.append("")
        for r in rows:
            lines.append(f"### {r['file']}  ({len(r['failures'])} failures)")
            if r["status"] in ("error", "timeout") and r.get("tail"):
                lines.append("```")
                lines.append(r["tail"])
                lines.append("```")
            for node in r["failures"]:
                lines.append(f"- `{node}`")
            lines.append("")
    (OUT_DIR / "SUMMARY.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"\nWrote {OUT_DIR / 'SUMMARY.md'} ({total_failures} failing nodes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
