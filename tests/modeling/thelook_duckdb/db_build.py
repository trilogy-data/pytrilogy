"""Deterministic thelook-style e-commerce data for the partial-bridge battery.

The row generators, counts and `SEED` come from `evals.thelook_agent.db_build`
so the battery and the agent eval are provably the same data — only the sink
differs (this seeds the test engine's in-memory DuckDB directly, plus the
precomputed aggregates `sales_agg.preql` binds).

Mock seeding (`trilogy unit`) is unsuitable here: mock pools cycle every key
into every table, so the extension rows the `~` semantics exist to preserve
(never-ordered users, never-sold products) would not exist and LEFT-vs-INNER
regressions would be invisible. See docs/handoff_trilogy_unit_partial_mock_gaps.md.
"""

from __future__ import annotations

import random
from functools import partial
from typing import TYPE_CHECKING

from evals.thelook_agent.db_build import (
    SEED,
    assert_properties,
    order_item_rows,
    order_rows,
    product_rows,
    user_rows,
)

if TYPE_CHECKING:
    from trilogy import Executor

_TABLES: dict[str, tuple[str, tuple[str, ...]]] = {
    "users": (
        "id INTEGER, state VARCHAR, age INTEGER, traffic_source VARCHAR",
        ("id", "state", "age", "traffic_source"),
    ),
    "products": (
        (
            "id INTEGER, brand VARCHAR, category VARCHAR, department VARCHAR, "
            "retail_price DECIMAL(12, 2), cost DECIMAL(12, 2)"
        ),
        ("id", "brand", "category", "department", "retail_price", "cost"),
    ),
    "orders": (
        "order_id INTEGER, user_id INTEGER, status VARCHAR, created_at TIMESTAMP",
        ("order_id", "user_id", "status", "created_at"),
    ),
    "order_items": (
        (
            "id INTEGER, order_id INTEGER, user_id INTEGER, product_id INTEGER, "
            "sale_price DECIMAL(12, 2), status VARCHAR"
        ),
        ("id", "order_id", "user_id", "product_id", "sale_price", "status"),
    ),
}


def _count(executor: Executor, sql: str) -> int:
    row = executor.execute_raw_sql(sql).fetchone()
    assert row is not None
    return int(row[0])


def seed(executor: Executor) -> None:
    from pyarrow import table as arrow_table

    rng = random.Random(SEED)
    users = user_rows(rng)
    products = product_rows(rng)
    orders = order_rows(rng)
    order_items = order_item_rows(rng, orders, products)
    data: dict[str, list[tuple]] = {
        "users": users,
        "products": products,
        "orders": orders,
        "order_items": list(order_items),
    }
    for name, (schema, headers) in _TABLES.items():
        columns = {h: list(vals) for h, vals in zip(headers, zip(*data[name]))}
        executor.execute_raw_sql(
            "register(:name, :tbl)", {"name": "seed_tbl", "tbl": arrow_table(columns)}
        )
        executor.execute_raw_sql(f"CREATE OR REPLACE TABLE {name} ({schema})")
        executor.execute_raw_sql(f"INSERT INTO {name} SELECT * FROM seed_tbl")
    # Precomputed aggregates (sales_agg.preql) derived from the seeded fact so
    # they are consistent by construction.
    executor.execute_raw_sql(
        "CREATE OR REPLACE TABLE daily_sales AS "
        "SELECT CAST(o.created_at AS DATE) AS order_date, "
        "sum(oi.sale_price) AS revenue, count(oi.id) AS sale_line_count "
        "FROM order_items oi JOIN orders o ON o.order_id = oi.order_id "
        "GROUP BY 1"
    )
    executor.execute_raw_sql(
        "CREATE OR REPLACE TABLE user_product_sales AS "
        "SELECT user_id, product_id, "
        "sum(sale_price) AS revenue, count(id) AS sale_line_count "
        "FROM order_items GROUP BY 1, 2"
    )
    assert_properties(partial(_count, executor))
