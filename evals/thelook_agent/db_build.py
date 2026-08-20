"""Build the deterministic DuckDB database for the thelook bridge eval.

The rows come from `trilogy unit`'s mock generator (`trilogy/dialect/mock.py`)
driven by `enriched_model` itself, so the fixture is a property of the model
rather than a second, hand-maintained description of it. See `docs/mock_data.md`
for what the generator guarantees; `assert_properties` below is the contract
this eval and `tests/modeling/thelook_duckdb` share, and is what pins the
never-ordered customers and never-sold products the `~` semantics exist to
describe.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from trilogy import Executor

EVAL_DIR = Path(__file__).resolve().parent
MODEL_DIR = EVAL_DIR / "enriched_model"
ENTRYPOINT = "order_items.preql"
# Cache name carries the generator: a database built by the old row generators
# would otherwise read as a hit forever.
CACHE_DB = EVAL_DIR / ".cache" / "thelook_mock.duckdb"
DB_FILENAME = "thelook.duckdb"
# Sizes the customer and product dimensions; orders and sale lines fan out
# above them (see FANOUT_FACTOR).
SCALE_FACTOR = 1_000


def _count(executor: Executor, sql: str) -> int:
    row = executor.execute_raw_sql(sql).fetchone()
    assert row is not None
    return int(row[0])


def assert_properties(count: Callable[[str], int]) -> None:
    """The four invariants both fixtures exist to provide.

    `count` runs one scalar query — each caller holds its own executor.
    """
    never_ordered = count(
        "SELECT count(*) FROM users u ANTI JOIN orders o ON o.user_id = u.id"
    )
    never_sold = count(
        "SELECT count(*) FROM products p ANTI JOIN order_items oi "
        "ON oi.product_id = p.id"
    )
    mismatched_users = count(
        "SELECT count(*) FROM order_items oi JOIN orders o USING (order_id) "
        "WHERE oi.user_id != o.user_id"
    )
    null_foreign_keys = count(
        "SELECT count(*) FROM order_items WHERE order_id IS NULL "
        "OR user_id IS NULL OR product_id IS NULL"
    )
    assert never_ordered > 0
    assert never_sold > 0
    assert mismatched_users == 0
    assert null_foreign_keys == 0


def build_database() -> Path:
    """Return the cached database, building it atomically on first use."""
    from trilogy import Dialects
    from trilogy.core.models.environment import Environment
    from trilogy.dialect.config import DuckDBConfig
    from trilogy.dialect.mock import mock_environment

    if CACHE_DB.exists():
        return CACHE_DB

    CACHE_DB.parent.mkdir(parents=True, exist_ok=True)
    temporary = CACHE_DB.with_suffix(".building")
    temporary.unlink(missing_ok=True)
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=MODEL_DIR),
        conf=DuckDBConfig(path=str(temporary)),
    )
    try:
        executor.parse_text((MODEL_DIR / ENTRYPOINT).read_text(encoding="utf-8"))
        mock_environment(executor.environment, executor, scale_factor=SCALE_FACTOR)
        assert_properties(partial(_count, executor))
        executor.execute_raw_sql("CHECKPOINT")
    finally:
        executor.close()
    temporary.with_name(temporary.name + ".wal").unlink(missing_ok=True)
    temporary.replace(CACHE_DB)
    return CACHE_DB
