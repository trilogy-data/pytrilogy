"""Standalone repro for the derived-rowset-join re-aggregation disconnect bug.

See bug_derived_rowset_join_key_reaggregate_disconnect.md. No eval workspace or
DuckDB data needed — generate_sql alone reproduces the discovery failure.

Run: .venv/Scripts/python.exe evals/tpcds_agent/repro_derived_rowset_join.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment

ORDERS = """key oid int;
property oid.status int;
property oid.amt float;
property oid.grp int;
datasource orders (oid: oid, st: status, amt: amt, g: grp)
grain (oid) address orders_tbl;
"""

BASE = """import orders as orders;
with a as select orders.grp as grp, sum(orders.amt) as tot where orders.status = 1;
with b as select orders.grp as grp, sum(orders.amt) as tot where orders.status = 2;
"""

CASES = {
    "R1_reagg_anchor_derived  [BUG: should PASS]": "select a.grp, sum(a.tot) as at, sum(b.tot) as bt inner join a.grp + 1 = b.grp;",
    "R2_plain_projection      [pass: offset-contract]": "select a.grp, a.tot, b.tot inner join a.grp + 1 = b.grp;",
    "R3_derived_on_b_side      [pass]": "select a.grp, sum(a.tot) as at, sum(b.tot) as bt inner join a.grp = b.grp + 1;",
    "R4_keep_b_key_output      [pass]": "select a.grp, b.grp, sum(a.tot) as at, sum(b.tot) as bt inner join a.grp + 1 = b.grp;",
    "R5_only_b_reagg          [BUG: should PASS]": "select a.grp, a.tot, sum(b.tot) as bt inner join a.grp + 1 = b.grp;",
    "R6_plain_equality         [pass]": "select a.grp, sum(a.tot) as at, sum(b.tot) as bt inner join a.grp = b.grp;",
}


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        (d / "orders.preql").write_text(ORDERS)
        for name, tail in CASES.items():
            eng = Dialects.DUCK_DB.default_executor(
                environment=Environment(working_path=d)
            )
            try:
                eng.generate_sql(BASE + tail)
                print(f"  OK    {name}")
            except Exception as exc:
                print(f"  ERR   {name}: {type(exc).__name__}: {str(exc)[:90]}")


if __name__ == "__main__":
    main()
