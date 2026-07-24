#!/usr/bin/env python
"""Rebuild the cross-category funnel from a LIVE 4-category run log, before any
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
from common.categories import CATEGORIES, FUNNEL_ORDER
from spec import SPEC

_LINE = re.compile(
    r"\[(?P<cat>sql_bare|sql_schema|ingest|enriched)\]\s+"
    r"\[q(?P<qid>\d+)\] done in \d+s \(exit \d+, score=(?P<status>\w+)\)"
)


def _parse(log_text: str) -> dict[str, dict[int, str]]:
    """Latest score per (category, query id) — a requeried query keeps its last."""
    out: dict[str, dict[int, str]] = {k: {} for k in FUNNEL_ORDER}
    for m in _LINE.finditer(log_text):
        out[m["cat"]][int(m["qid"])] = m["status"]
    return out


def _synth_report(cat: str, statuses: dict[int, str], total: int) -> dict:
    passes = sum(1 for s in statuses.values() if s == "pass")
    return {
        "meta": {
            "category": cat,
            "category_label": getattr(CATEGORIES[cat], "label", cat),
            "num_queries": total,
            "benchmark": "TPC-DS",
        },
        "queries": [{"id": q, "status": s} for q, s in sorted(statuses.items())],
        "summary": {"pass_rate": passes / len(statuses) if statuses else 0.0},
        "agent": {"tokens": {"total": 0}},
    }


def _leg_dirs(run_ts: str) -> dict[str, Path]:
    return {k: SPEC.results_dir / f"{run_ts}_{k}" for k in FUNNEL_ORDER}


def build_once(log_path: Path, total: int) -> tuple[bool, str]:
    parsed = _parse(log_path.read_text(encoding="utf-8", errors="replace"))
    ordered = {k: _synth_report(k, parsed[k], total) for k in FUNNEL_ORDER}
    analyze_run.render_funnel(ordered, SPEC.charts_dir / "funnel_v2.png")
    analyze_run.write_funnel_report(ordered, SPEC.charts_dir / "funnel.md")
    counts = ", ".join(
        f"{k}={sum(1 for s in parsed[k].values() if s=='pass')}/{len(parsed[k])}"
        for k in FUNNEL_ORDER
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
        "--run-ts",
        default=None,
        help="Run timestamp (e.g. 20260706-135542) to detect leg completion.",
    )
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()
    log_path = Path(args.run_log)

    while True:
        counts, _ = build_once(log_path, args.num_queries)
        print(f"[funnel] rebuilt funnel_v2.png — {counts}", flush=True)
        if args.once:
            return 0
        if args.run_ts:
            legs = _leg_dirs(args.run_ts)
            if all((d / "report.json").exists() for d in legs.values()):
                print("[funnel] all legs finished; handing off to final render.")
                return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
