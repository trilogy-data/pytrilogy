#!/usr/bin/env python
"""Rebuild the cross-category funnel from a live multi-category run log, before any
leg has written its final ``report.json``.

Each leg prints ``[<cat>]   [qNN] done in Xs (exit N, score=<status>)`` as each
query is scored; we parse those lines into per-category status maps, synthesise
the minimal report dicts ``render_funnel`` needs, and re-render ``funnel_v2.png``
+ ``funnel.md`` on an interval. Stops once every leg has written its real
``report.json`` (the harness then renders the authoritative final funnel).

    python incremental_funnel.py <run_log> [--interval 45] [--once]
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import analyze_run
from common.categories import FUNNEL_ORDER, categories_for, funnel_order_for
from spec import SPEC

_CATEGORY_MAP = categories_for(SPEC)
_CATEGORY_PATTERN = "|".join(re.escape(key) for key in _CATEGORY_MAP)
_LINE = re.compile(
    rf"\[(?P<cat>{_CATEGORY_PATTERN})\]\s+"
    r"\[q(?P<qid>\d+)\] done in \d+s \(exit \d+, score=(?P<status>\w+)\)"
)


def _parse(log_text: str, categories: list[str]) -> dict[str, dict[int, str]]:
    """Latest score per (category, query id) — a requeried query keeps its last."""
    out: dict[str, dict[int, str]] = {key: {} for key in categories}
    for m in _LINE.finditer(log_text):
        if m["cat"] in out:
            out[m["cat"]][int(m["qid"])] = m["status"]
    return out


def _synth_report(cat: str, statuses: dict[int, str], total: int) -> dict:
    passes = sum(1 for s in statuses.values() if s == "pass")
    return {
        "meta": {
            "category": cat,
            "category_label": _CATEGORY_MAP[cat].label,
            "num_queries": total,
            "benchmark": "TPC-DS",
        },
        "queries": [{"id": q, "status": s} for q, s in sorted(statuses.items())],
        "summary": {"pass_rate": passes / len(statuses) if statuses else 0.0},
        "agent": {"tokens": {"total": 0}},
    }


def _leg_dirs(run_ts: str, categories: list[str]) -> dict[str, Path]:
    return {key: SPEC.results_dir / f"{run_ts}_{key}" for key in categories}


def build_once(log_path: Path, total: int, categories: list[str]) -> tuple[str, str]:
    parsed = _parse(log_path.read_text(encoding="utf-8", errors="replace"), categories)
    ordered = {key: _synth_report(key, parsed[key], total) for key in categories}
    analyze_run.render_funnel(ordered, SPEC.charts_dir / "funnel_v2.png")
    analyze_run.write_funnel_report(ordered, SPEC.charts_dir / "funnel.md")
    counts = ", ".join(
        f"{k}={sum(1 for s in parsed[k].values() if s=='pass')}/{len(parsed[k])}"
        for k in categories
    )
    # done when every leg has landed its authoritative report.json
    run_ts = log_path.stem.split("_")[0] if "_" in log_path.stem else None
    return counts, run_ts or ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("run_log")
    ap.add_argument("--interval", type=float, default=45.0)
    ap.add_argument("--num-queries", type=int, default=99)
    ap.add_argument(
        "--categories",
        default=",".join(FUNNEL_ORDER),
        help="comma-separated category keys present in the run log",
    )
    ap.add_argument(
        "--run-ts",
        default=None,
        help="Run timestamp (e.g. 20260706-135542) to detect leg completion.",
    )
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()
    log_path = Path(args.run_log)
    requested = [key.strip() for key in args.categories.split(",") if key.strip()]
    unknown = set(requested) - set(_CATEGORY_MAP)
    if unknown:
        ap.error(f"unknown categories: {sorted(unknown)}")
    categories = [key for key in funnel_order_for(SPEC) if key in requested]

    while True:
        counts, _ = build_once(log_path, args.num_queries, categories)
        print(f"[funnel] rebuilt funnel_v2.png — {counts}", flush=True)
        if args.once:
            return 0
        if args.run_ts:
            legs = _leg_dirs(args.run_ts, categories)
            if all((d / "report.json").exists() for d in legs.values()):
                print("[funnel] all legs finished; handing off to final render.")
                return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
