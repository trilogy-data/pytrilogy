"""Compare two plan traces of one statement, e.g. before and after a planner
change: steps only one trace has, field-level changes in the steps both have,
the SQL diff (traces from ``trace_query.py``) and planner time per phase.

    python local_scripts/plan_debugger/trace_query.py q.preql --out before.trace.json --no-html
    # ... change the planner ...
    python local_scripts/plan_debugger/trace_query.py q.preql --out after.trace.json --no-html
    python local_scripts/plan_debugger/trace_diff.py before.trace.json after.trace.json

Exits 1 when the plans differ, so it can gate a refactor that should not move
a plan.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from plan_trace_diff import diff_traces, format_diff


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("a", help="trace JSON")
    parser.add_argument("b", help="trace JSON")
    parser.add_argument(
        "--lines", type=int, default=20, help="differing fields shown per step"
    )
    args = parser.parse_args()
    traces = [json.loads(Path(p).read_text(encoding="utf-8")) for p in (args.a, args.b)]
    diff = diff_traces(*traces)
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    print(format_diff(diff, args.lines))
    sys.exit(0 if diff.same_plan else 1)


if __name__ == "__main__":
    main()
