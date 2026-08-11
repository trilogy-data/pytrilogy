"""Minimal repro: `count(<key>)` over-counts beside a filtered sibling count.

Leg A is the agent's q16 probe. Leg B drops the filtered count, which is the
only difference. Both counts share one aggregate bucket at input grain
`{order_number}`, but leg A's filter virtual is not determined by
`order_number`, so it lands in the normalization GROUP BY and the dedup that
made the plain COUNT distinct no longer holds.

Pre-fix leg A returned 254,337 orders for the 160,000 distinct catalog orders;
leg B returned 160,000. Run against `tests/modeling/tpc_ds_duckdb` (sf=1).
"""

from __future__ import annotations

from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = REPO_ROOT / "tests" / "modeling" / "tpc_ds_duckdb"

LEG_A = """
import catalog_sales as cs;
where 1=1
select
    count(cs.order_number) as all_orders,
    count(cs.order_number ? cs.is_returned = true) as returned_orders
;
"""

LEG_B = """
import catalog_sales as cs;
where 1=1
select count(cs.order_number) as all_orders;
"""

TRUTH = "SELECT count(distinct CS_ORDER_NUMBER) FROM memory.catalog_sales"


def main() -> None:
    engine = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=MODEL_DIR), conf=DuckDBConfig()
    )
    engine.execute_raw_sql(f"IMPORT DATABASE '{MODEL_DIR / 'memory'}';")

    truth = engine.execute_raw_sql(TRUTH).fetchall()[0][0]
    leg_a = engine.execute_text(LEG_A)[-1].fetchall()[0][0]
    leg_b = engine.execute_text(LEG_B)[-1].fetchall()[0][0]

    print(f"distinct orders (raw SQL): {truth}")
    print(f"leg A count(key) beside filtered count: {leg_a}")
    print(f"leg B count(key) alone:                 {leg_b}")

    if leg_b != truth:
        raise AssertionError("control leg is wrong; repro is invalid")
    if leg_a != truth:
        raise AssertionError(
            f"REPRODUCED: count(<key>) returned {leg_a} for {truth} distinct keys"
        )
    print("\nFIXED: count(<key>) is distinct in both legs")


if __name__ == "__main__":
    main()
