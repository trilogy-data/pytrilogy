"""Aggregate v2 parser benchmark. Run as a module:

    python -m tests.profiling.benchmark

Parses a pinned corpus spanning small/medium/large trilogy files under the
v2 parser and writes one JSON line per invocation to ``history.jsonl`` so
that parse-time regressions can be tracked across commits. The corpus is
intentionally small, fast, and stable — pick files from actively-tested
modeling directories so they stay green.
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from trilogy.constants import CONFIG, ParserVersion
from trilogy.core.models.environment import Environment
from trilogy.parser import parse_text

HERE = Path(__file__).parent
REPO_ROOT = HERE.parent.parent
HISTORY_PATH = HERE / "history.jsonl"

# Pinned corpus: (label, path-relative-to-repo-root). Files chosen for a
# spread of sizes and statement counts, all from directories with live tests.
CORPUS: tuple[tuple[str, str], ...] = (
    ("tpch_nation", "tests/modeling/tpc_h/nation.preql"),
    ("tpch_lineitem", "tests/modeling/tpc_h/lineitem.preql"),
    ("tpch_adhoc01", "tests/modeling/tpc_h/adhoc01.preql"),
    ("tpcds_customer", "tests/modeling/tpc_ds_duckdb/customer.preql"),
    ("tpcds_store_sales", "tests/modeling/tpc_ds_duckdb/store_sales.preql"),
    ("tpcds_query01", "tests/modeling/tpc_ds_duckdb/query01.preql"),
    ("tpcds_query10", "tests/modeling/tpc_ds_duckdb/query10.preql"),
    ("tpcds_query56", "tests/modeling/tpc_ds_duckdb/query56.preql"),
    ("tpcds_adhoc01", "tests/modeling/tpc_ds_duckdb/adhoc01.preql"),
    ("tpcds_adhoc01_imports", "tests/modeling/tpc_ds_duckdb/adhoc01_imports.preql"),
    ("tpcds_query08_large", "tests/modeling/tpc_ds_duckdb/query08.preql"),
)


def _git(*args: str) -> str:
    try:
        out = subprocess.check_output(
            ["git", *args], cwd=REPO_ROOT, stderr=subprocess.DEVNULL
        )
        return out.decode().strip()
    except Exception:
        return ""


def _time_parse(path: Path) -> tuple[float, int]:
    text = path.read_text()
    env = Environment(working_path=path.parent)
    start = time.perf_counter_ns()
    _, parsed = parse_text(text, environment=env)
    elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000
    return elapsed_ms, len(parsed)


def _bench_file(label: str, path: Path, runs: int) -> dict[str, Any]:
    samples: list[float] = []
    stmt_count = 0
    _time_parse(path)  # warm-up; excluded
    for _ in range(runs):
        elapsed, stmts = _time_parse(path)
        samples.append(elapsed)
        stmt_count = stmts
    return {
        "label": label,
        "file": path.relative_to(REPO_ROOT).as_posix(),
        "bytes": path.stat().st_size,
        "statements": stmt_count,
        "best_ms": min(samples),
        "median_ms": statistics.median(samples),
        "mean_ms": statistics.fmean(samples),
        "stdev_ms": statistics.stdev(samples) if len(samples) > 1 else 0.0,
    }


def run(runs: int, output: Path, label: str | None) -> dict:
    if CONFIG.parser_version is not ParserVersion.V2:
        CONFIG.parser_version = ParserVersion.V2
    files: list[dict[str, Any]] = []
    for corpus_label, rel in CORPUS:
        path = REPO_ROOT / rel
        if not path.exists():
            raise SystemExit(f"corpus file missing: {rel}")
        files.append(_bench_file(corpus_label, path, runs))
    total_best = sum(f["best_ms"] for f in files)
    total_statements = sum(f["statements"] for f in files)
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "git_sha": _git("rev-parse", "--short", "HEAD"),
        "git_branch": _git("rev-parse", "--abbrev-ref", "HEAD"),
        "parser": ParserVersion.V2.value,
        "python": platform.python_version(),
        "platform": platform.platform(terse=True),
        "runs": runs,
        "label": label or "",
        "total_best_ms": round(total_best, 3),
        "total_statements": total_statements,
        "files": [
            {k: round(v, 3) if isinstance(v, float) else v for k, v in f.items()}
            for f in files
        ],
    }
    with output.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")
    return record


def _print_summary(record: dict) -> None:
    print(
        f"sha={record['git_sha']} branch={record['git_branch']} "
        f"total_best={record['total_best_ms']:.1f}ms "
        f"stmts={record['total_statements']} runs/file={record['runs']}"
    )
    for f in record["files"]:
        print(
            f"  {f['label']:<22} best={f['best_ms']:>8.2f}ms "
            f"median={f['median_ms']:>8.2f}ms  stmts={f['statements']:>3}  "
            f"({f['bytes']}B)"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="samples per file (best-of-N after one warm-up)",
    )
    parser.add_argument("--output", type=Path, default=HISTORY_PATH)
    parser.add_argument(
        "--label",
        default=None,
        help="optional free-form tag to store on the record",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="suppress per-file summary on stdout",
    )
    args = parser.parse_args(argv)
    record = run(args.runs, args.output, args.label)
    if not args.quiet:
        _print_summary(record)
    return 0


if __name__ == "__main__":
    sys.exit(main())
