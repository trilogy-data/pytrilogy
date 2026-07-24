"""Standalone repro: Bug #2 from bug_q67. A `grouping()` over a rowset output
referenced in a DOWNSTREAM select with NO rollup clause renders a groupless
`SELECT grouping(...)` -> raw DuckDB "GROUPING statement cannot be used without
groups", instead of a clean trilogy validation error.

Run: .venv/Scripts/python.exe evals/tpcds_agent/repro_q67_grouping_no_rollup_binder_error.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment

SALES = """key sid int;
property sid.category string;
property sid.class string;
property sid.amt float;
datasource sales (sid: sid, cat: category, cls: class, a: amt)
    grain (sid) address sales_tbl;
"""

# rowset with a rollup, then grouping() over its output in a downstream
# select that has NO rollup clause.
NAMED = """import sales as s;
rowset rd <- select s.category, s.class, sum(s.amt) as summed
    by rollup (s.category, s.class);
auto g_cat <- grouping(rd.category);
select rd.category, rd.class, rd.summed, g_cat as gc limit 50;
"""

# same, inline grouping() directly in the downstream select.
INLINE = """import sales as s;
rowset rd <- select s.category, s.class, sum(s.amt) as summed
    by rollup (s.category, s.class);
select rd.category, rd.class, rd.summed, grouping(rd.category) as gc limit 50;
"""

CASES = {
    "NAMED_grouping_no_rollup  [want: clean trilogy error]": NAMED,
    "INLINE_grouping_no_rollup [want: clean trilogy error]": INLINE,
}


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        (d / "sales.preql").write_text(SALES)
        for name, body in CASES.items():
            eng = Dialects.DUCK_DB.default_executor(
                environment=Environment(working_path=d)
            )
            try:
                eng.generate_sql(body)
                print(f"  BUILT(no guard) {name}")
            except Exception as exc:
                print(f"  {type(exc).__name__:<24} {name}: {str(exc)[:70]}")


if __name__ == "__main__":
    main()
