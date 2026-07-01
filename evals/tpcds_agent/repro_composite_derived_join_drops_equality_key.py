"""Standalone repro: a cross-rowset scoped join that combines a PLAIN-EQUALITY key
with a DERIVED-EXPRESSION key silently DROPS the equality key from the emitted
join. SILENT wrong results (cross-key fan-out), no error.

    inner join a.store = b.store and a.period + 10 = b.period
    -> emitted ON:  a.period + 10 = b.period      (a.store = b.store is GONE)

Each `a` row then matches `b` rows at period+10 for EVERY store, not its own.

See bug_composite_derived_join_drops_equality_key.md. Run:
    .venv/Scripts/python.exe evals/tpcds_agent/repro_composite_derived_join_drops_equality_key.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment

SALES = """key sid int;
property sid.store int;
property sid.period int;
property sid.yr int;
property sid.amt float;
datasource sales (sid: sid, s: store, p: period, y: yr, a: amt) grain (sid) address sales_tbl;
"""

HEAD = """import sales as s;
rowset a <- where s.yr = 1 select s.store, s.period, sum(s.amt) as tot;
rowset b <- where s.yr = 2 select s.store, s.period, sum(s.amt) as tot;
select a.store, a.period, a.tot / b.tot as r
"""

CASES = {
    # derived key present -> BUG (store dropped). Correct answer = 2 rows (per-store 0.5).
    "equality+derived (store AND period+10)  [BUG: store dropped -> 4 rows]":
        "inner join a.store = b.store and a.period + 10 = b.period",
    # control: plain equality on the shifted values via a named yr2 offset would be
    # contrived; the two-equality control lives in the .md trigger matrix.
}


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        (d / "sales.preql").write_text(SALES)
        eng = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=d))
        eng.execute_raw_sql("create table sales_tbl (sid int, s int, p int, y int, a double)")
        # yr1: store1 p1=10, store2 p1=100 ;  yr2: store1 p11=20, store2 p11=200
        # Correct per-store ratios: store1 10/20=0.5, store2 100/200=0.5  -> 2 rows.
        eng.execute_raw_sql(
            "insert into sales_tbl values "
            "(1,1,1,1,10.0),(2,2,1,1,100.0),(3,1,11,2,20.0),(4,2,11,2,200.0)"
        )
        for name, join in CASES.items():
            sql = eng.generate_sql(HEAD + join + ";")[-1]
            store_in_join = any(
                "store" in ln.lower()
                for ln in sql.splitlines()
                if " join " in ln.lower() and " on " in ln.lower()
            )
            rows = sorted(tuple(r) for r in eng.execute_raw_sql(sql).fetchall())
            print(f"  store_in_join={store_in_join!s:5}  rows={len(rows)} (correct=2)  {name}")
            for r in rows:
                print(f"      store={r[0]} period={r[1]} ratio={r[2]}")
        print("  cross-store ratios (0.05, 5.0) = the fan-out; correct output is only 0.5 twice.")


if __name__ == "__main__":
    main()
