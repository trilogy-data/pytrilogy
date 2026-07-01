"""Standalone repro: a global aggregate over a pure LITERAL (`count(1)`, `sum(1)`,
`count(1) by *`) with no real-column input and no grain key crashes with an
unguarded `IndexError: list index out of range` (surfaced to the CLI/agent as the
opaque "Unexpected error: list index out of range").

`count(1) by *` is the DOCUMENTED global-scalar idiom (agent-info: "Use `by *` to
aggregate to a global scalar"), so the docs steer authors straight into the crash.

Crash site: calculate_effective_parent_grain, discovery_utility.py:72 --
`return qds.datasources[0].grain` on an empty `datasources` list.

Run: .venv/Scripts/python.exe evals/tpcds_agent/repro_global_literal_aggregate_indexerror.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment

SALES = """key sid int;
property sid.item int;
property sid.amt float;
datasource sales (sid: sid, i: item, a: amt) grain (sid) address sales_tbl;
"""

CASES = {
    "count(1)            [BUG: IndexError]": "select count(1) as c;",
    "sum(1)              [BUG: IndexError]": "select sum(1) as c;",
    "count(1) by *       [BUG: documented global-scalar idiom]": "select count(1) by * as c;",
    "count(sid)          [ok: real-column input]": "select count(sid) as c;",
    "count(1) by item    [ok: has a grain key]": "select count(1) by item as c;",
}


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        (d / "sales.preql").write_text(SALES)
        for name, q in CASES.items():
            eng = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=d))
            try:
                eng.generate_sql("import sales as s; " + q.replace("item", "s.item").replace("sid", "s.sid"))
                print(f"  OK         {name}")
            except IndexError:
                print(f"  IndexError {name}")
            except Exception as exc:  # noqa: BLE001
                print(f"  {type(exc).__name__:<10} {name}: {str(exc)[:60]}")


if __name__ == "__main__":
    main()
