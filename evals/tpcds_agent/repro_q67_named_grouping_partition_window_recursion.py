"""Standalone repro: a NAMED window concept whose `partition by` is a NAMED
concept whose lineage wraps `grouping()`, consumed through an alias, under a
`by rollup(...)` select, recurses forever building the select grain (uncaught
RecursionError → opaque "query could not be planned; this is a bug").

See bug_q67_named_grouping_partition_window_recursion.md (Bug #1). No data needed
— generate_sql alone reproduces it.

Run: .venv/Scripts/python.exe evals/tpcds_agent/repro_q67_named_grouping_partition_window_recursion.py
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment

SALES = """key sid int;
property sid.category string;
property sid.class string;
property sid.quantity int;
datasource sales (sid: sid, cat: category, cls: class, q: quantity)
    grain (sid) address sales_tbl;
"""

# Named window concept partitioned by a named grouping()-derived concept,
# consumed via an alias, under a rollup select.
BUG = """import sales as s;
auto part <- case when grouping(s.category)=1 then 'GT' else s.category end;
auto rnk  <- rank(s.class) over (partition by part order by sum(s.quantity) desc);
select s.category, s.class, rnk as r
by rollup (s.category, s.class);
"""

# Control: inline the grouping-case into the partition (dodges the conjunction).
OK_INLINE = """import sales as s;
auto rnk  <- rank(s.class) over (
    partition by case when grouping(s.category)=1 then 'GT' else s.category end
    order by sum(s.quantity) desc);
select s.category, s.class, rnk as r
by rollup (s.category, s.class);
"""

CASES = {
    "BUG_named_grouping_partition_aliased [should resolve or clean-error]": BUG,
    "OK_inline_grouping_partition        [ok]": OK_INLINE,
}


def main() -> None:
    sys.setrecursionlimit(6000)
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        (d / "sales.preql").write_text(SALES)
        for name, body in CASES.items():
            eng = Dialects.DUCK_DB.default_executor(
                environment=Environment(working_path=d)
            )
            try:
                eng.generate_sql(body)
                print(f"  OK         {name}")
            except RecursionError:
                print(f"  RECURSION  {name}")
            except Exception as exc:  # noqa: BLE001
                print(f"  {type(exc).__name__:<10} {name}: {str(exc)[:80]}")


if __name__ == "__main__":
    main()
