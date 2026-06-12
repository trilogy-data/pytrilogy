"""Repro: a multi-key query-scoped INNER join is NOT commutative in operand order.

`inner join A.k = B.k` and `inner join B.k = A.k` must be identical for an INNER
join. They are NOT, when BOTH of these hold:
  1. there are TWO scoped-join keys (here `item.id` AND `week_seq`), and
  2. the SELECT projects a *property* of one of the joined dimensions
     (`cs.item.desc`) rather than the bare join key (`cs.item.id`).
Flipping the operands then fans the result out ~18x (53712 -> 955347 rows).

Either condition alone is COMMUTATIVE (controls below). Found via TPC-DS q72:
the agent wrote `inv.item.id = cs.item.id` (fan-out) where the passing form
wrote `cs.item.id = inv.item.id`. The q72 reference sidesteps it by joining only
on item and filtering `week_seq` in WHERE.

Run:  python evals/tpcds_agent/repro_scoped_join_direction.py [WORKSPACE_DIR]
WORKSPACE_DIR must contain `raw/` (ingested model) + `tpcds.duckdb` — e.g. any
`results/<run>_enriched/workspace`. Regenerate with `trilogy ingest` if absent.

Could NOT reduce to a synthetic model (two facts + date dim + warehouse FK all
stay commutative), so the trigger appears tied to the RAW ingested model's
inferred grain/FKs — a likely starting point for the fix.
"""

import sys
from pathlib import Path

sys.path.insert(0, "evals")
from common import scoring  # noqa: E402

WS = Path(
    sys.argv[1]
    if len(sys.argv) > 1
    else "evals/tpcds_agent/results/20260612-133004_enriched/workspace"
)

_HEAD = """import raw.catalog_sales as cs;
import raw.inventory as inv;
where cs.sold_date.year = 1999 and inv.quantity_on_hand < cs.quantity
"""

# The two scoped-join keys; the repro flips the operand order of BOTH.
_TWO_KEY = "inner join cs.item.id = inv.item.id\ninner join cs.sold_date.week_seq = inv.date.week_seq"

# Trigger: project cs.item.desc (a property of the joined item dimension).
BUG = _HEAD + (
    "select cs.item.desc as item_desc, inv.warehouse.name as wh, "
    "cs.sold_date.week_seq as wk, count(cs.order_number) as t\n" + _TWO_KEY + "\norder by 1,2,3;"
)
# Control A: same two-key join, project the bare key cs.item.id -> commutative.
CTRL_KEY = BUG.replace("cs.item.desc as item_desc", "cs.item.id as item_id")
# Control B: project item.desc but a single join key -> commutative.
CTRL_ONE = _HEAD + (
    "select cs.item.desc as item_desc, inv.warehouse.name as wh, count(cs.order_number) as t\n"
    "inner join cs.item.id = inv.item.id\norder by 1,2;"
)


def _flip(body: str) -> str:
    return body.replace("cs.item.id = inv.item.id", "inv.item.id = cs.item.id").replace(
        "cs.sold_date.week_seq = inv.date.week_seq", "inv.date.week_seq = cs.sold_date.week_seq"
    )


def _check(eng, label: str, body: str) -> bool:
    def rows(x):
        return list(eng.execute_raw_sql(eng.generate_sql(x)[-1]).fetchall())

    a, b = rows(body), rows(_flip(body))
    same = sorted(map(str, a)) == sorted(map(str, b))
    print(f"{label:38} A={len(a):>7} B={len(b):>7} identical={same}")
    return same


def main() -> int:
    eng = scoring.make_scoring_engine(WS / "tpcds.duckdb", WS, "tpcds")
    bug_ok = _check(eng, "BUG: 2 keys + project item.desc", BUG)
    _check(eng, "control: 2 keys + project item.id", CTRL_KEY)
    _check(eng, "control: 1 key + project item.desc", CTRL_ONE)
    if not bug_ok:
        print("\nBUG REPRODUCED: INNER join result depends on join-key operand order.")
        return 1
    print("\n(no divergence — workspace model may differ)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
