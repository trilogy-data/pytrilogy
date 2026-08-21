"""Seed the partial-bridge battery from `trilogy unit` mock data.

The tables are synthesized by `trilogy/dialect/mock.py` off the battery's own
model — no hand-written generator, no cache files. That only works because the
mocker respects the three things the model declares:

- `~` bindings cover a strict prefix of their key's domain, so never-ordered
  customers and never-sold products exist and LEFT is distinguishable from
  INNER (`assert_properties`);
- a column functionally determined by a key the same table binds is looked up
  rather than cycled, so `order_items.user_id` agrees with `orders.user_id`;
- the `sales_agg.preql` rollups are computed from the mocked fact instead of
  synthesized, so their metrics sum to it.

`assert_properties` is shared with `evals.thelook_agent.db_build`, whose
generator produces the same four invariants for the agent eval.
"""

from __future__ import annotations

from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

from evals.thelook_agent.db_build import assert_properties

if TYPE_CHECKING:
    from trilogy import Executor

working_path = Path(__file__).parent
# imports the whole model: the four base tables plus the precomputed rollups
ENTRYPOINT = "sales_agg.preql"


def _count(executor: Executor, sql: str) -> int:
    row = executor.execute_raw_sql(sql).fetchone()
    assert row is not None
    return int(row[0])


def seed(executor: Executor) -> None:
    from trilogy.core.models.environment import Environment

    executor.parse_text((working_path / ENTRYPOINT).read_text())
    targets = ", ".join(executor.environment.datasources.keys())
    executor.execute_text(f"mock datasources {targets};")
    # mocking repoints the datasources it wrote; queries parse their own
    # environment, so hand back a clean one
    executor.environment = Environment(working_path=working_path)
    assert_properties(partial(_count, executor))
