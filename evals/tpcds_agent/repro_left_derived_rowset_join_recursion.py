"""Standalone repro: LEFT scoped join on a DERIVED-expression key between two
re-aggregated rowsets recurses forever (uncaught RecursionError, surfaced to a
CLI/agent as the opaque "Unexpected error: maximum recursion depth exceeded").

See bug_left_derived_rowset_join_recursion.md. No data needed — generate_sql
alone reproduces it. The recursion is deep; we raise the limit just so the
RecursionError is caught cleanly here instead of killing the interpreter.

Run: .venv/Scripts/python.exe evals/tpcds_agent/repro_left_derived_rowset_join_recursion.py
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment

SALES = """key sid int;
property sid.period int;
property sid.amt float;
datasource sales (sid: sid, p: period, a: amt) grain (sid) address sales_tbl;
"""

# Two rowsets over the same base, each re-aggregated, related by a scoped join.
BASE = """import sales as s;
rowset agg <- select s.period, sum(s.amt) as tot;
rowset fut <- select s.period, sum(s.amt) as tot;
"""

CASES = {
    # join_type, key_expression
    "T1_LEFT_derived   [BUG: recurses, should resolve or clean-error]": "select agg.period, sum(agg.tot) / sum(fut.tot) as r "
    "subset join fut.period = agg.period + 53;",
    "C1_INNER_derived  [ok: spike handles inner]": "select agg.period, sum(agg.tot) / sum(fut.tot) as r "
    "inner join agg.period + 53 = fut.period;",
    "C2_LEFT_equality  [ok: plain-equality key]": "select agg.period, sum(agg.tot) / sum(fut.tot) as r "
    "subset join fut.period = agg.period;",
}


def main() -> None:
    sys.setrecursionlimit(8000)
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        (d / "sales.preql").write_text(SALES)
        for name, tail in CASES.items():
            eng = Dialects.DUCK_DB.default_executor(
                environment=Environment(working_path=d)
            )
            try:
                eng.generate_sql(BASE + tail)
                print(f"  OK         {name}")
            except RecursionError:
                print(f"  RECURSION  {name}")
            except Exception as exc:
                print(f"  {type(exc).__name__:<10} {name}: {str(exc)[:70]}")


if __name__ == "__main__":
    main()
