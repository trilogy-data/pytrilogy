"""Re-splice an existing --query-ids run through the current (fixed) splice code
so its full-benchmark `agent` aggregate metrics (tokens/iterations/tool stats)
are rebuilt over all queries, then re-render report.md + the dashboard chart.

No agents are re-run — this only re-aggregates already-captured per-query metrics
(re-parsing agent_log JSONL where a prior report predates per_query_metrics).

Usage: python evals/tpcds_agent/regen_spliced_report.py <run_dir> [<run_dir> ...]
"""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import analyze_run, scoring  # noqa: E402
from common import main as eval_main  # noqa: E402
from common.report import render_markdown  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from spec import SPEC  # noqa: E402


def regen(run_dir: Path) -> None:
    report = json.loads((run_dir / "report.json").read_text(encoding="utf-8"))
    sf = report.get("spliced_from")
    if not sf:
        print(f"  {run_dir.name}: not a spliced report — skipping")
        return
    prior_dir = Path(sf["run_dir"])
    if not (prior_dir / "report.json").exists():
        print(f"  {run_dir.name}: prior {prior_dir} gone — skipping")
        return
    prior_report = json.loads((prior_dir / "report.json").read_text(encoding="utf-8"))
    fresh_ids = set(sf["fresh_ids"])

    # Reconstruct the "fresh" report (only the freshly-run ids), re-parsing this
    # run's own logs for their metrics, then splice the rest via the fixed code.
    fresh_report = copy.deepcopy(report)
    fresh_report["queries"] = [q for q in report["queries"] if q["id"] in fresh_ids]
    fresh_report["per_query"] = [r for r in report["per_query"] if r["id"] in fresh_ids]
    fresh_report["per_query_metrics"] = [
        {
            "id": qid,
            **scoring.metrics_to_dict(
                scoring.parse_agent_log(run_dir / f"agent_log.q{qid:02d}.jsonl")
                if (run_dir / f"agent_log.q{qid:02d}.jsonl").exists()
                else scoring.AgentMetrics()
            ),
        }
        for qid in sorted(fresh_ids)
    ]
    fresh_report.pop("spliced_from", None)

    merged = eval_main._splice_prior_results(
        fresh_report, prior_report, fresh_ids, prior_dir
    )

    (run_dir / "report.json").write_text(json.dumps(merged, indent=2), encoding="utf-8")
    (run_dir / "report.md").write_text(render_markdown(SPEC, merged), encoding="utf-8")
    try:
        _, events = analyze_run.load_run(run_dir)
        cat = merged.get("meta", {}).get("category")
        suffix = f"_{cat}" if cat else ""
        analyze_run.render(
            merged, events, SPEC.charts_dir / f"dashboard{suffix}_v2.png"
        )
    except Exception as exc:
        print(f"  {run_dir.name}: chart render skipped ({type(exc).__name__}: {exc})")

    a = merged["agent"]
    print(
        f"  {run_dir.name}: agent tokens {a['tokens']['total']:,} | "
        f"iters {a['iterations']} | pass {merged['summary']['pass_count']}/"
        f"{merged['meta']['num_queries']}"
    )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    for d in sys.argv[1:]:
        regen(Path(d))
