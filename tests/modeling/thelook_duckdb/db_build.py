"""Deterministic thelook-style e-commerce data for the partial-bridge battery.

Mirrors evals/thelook_agent/db_build.py (same seed, same row generators) but
seeds directly into the test engine's in-memory DuckDB. Mock seeding
(`trilogy unit`) is unsuitable here: mock pools cycle every key into every
table, so the extension rows the `~` semantics exist to preserve
(never-ordered users, never-sold products) would not exist and LEFT-vs-INNER
regressions would be invisible.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from trilogy import Executor

USER_COUNT = 2_000
PRODUCT_COUNT = 500
ORDER_COUNT = 5_000
ACTIVE_USER_COUNT = 1_800
SOLD_PRODUCT_COUNT = 450
SEED = 8675309


def _user_rows(rng: random.Random) -> list[tuple[int, str, int, str]]:
    states = ("CA", "NY", "TX", "FL", "WA", "IL", "MA", "CO", "GA", "NC")
    sources = ("Search", "Organic", "Email", "Facebook", "Display")
    return [
        (user_id, rng.choice(states), rng.randint(18, 78), rng.choice(sources))
        for user_id in range(1, USER_COUNT + 1)
    ]


def _product_rows(
    rng: random.Random,
) -> list[tuple[int, str, str, str, float, float]]:
    brands = tuple(f"Brand {index:02d}" for index in range(1, 26))
    categories = (
        "Accessories",
        "Active",
        "Denim",
        "Dresses",
        "Fashion Hoodies & Sweatshirts",
        "Intimates",
        "Outerwear & Coats",
        "Pants",
        "Shorts",
        "Tops & Tees",
    )
    rows: list[tuple[int, str, str, str, float, float]] = []
    for product_id in range(1, PRODUCT_COUNT + 1):
        retail_price = round(rng.uniform(12, 240), 2)
        cost = round(retail_price * rng.uniform(0.28, 0.7), 2)
        rows.append(
            (
                product_id,
                rng.choice(brands),
                rng.choice(categories),
                rng.choice(("Men", "Women")),
                retail_price,
                cost,
            )
        )
    return rows


def _order_rows(
    rng: random.Random,
) -> list[tuple[int, int, str, datetime]]:
    start = datetime(2024, 1, 1)
    statuses = ("Complete", "Complete", "Complete", "Shipped", "Processing")
    return [
        (
            order_id,
            rng.randint(1, ACTIVE_USER_COUNT),
            rng.choice(statuses),
            start
            + timedelta(
                days=rng.randint(0, 364),
                seconds=rng.randint(0, 86_399),
            ),
        )
        for order_id in range(1, ORDER_COUNT + 1)
    ]


def _order_item_rows(
    rng: random.Random,
    orders: list[tuple[int, int, str, datetime]],
    products: list[tuple[int, str, str, str, float, float]],
) -> list[tuple[int, int, int, int, float, str]]:
    retail_prices = {row[0]: row[4] for row in products}
    rows: list[tuple[int, int, int, int, float, str]] = []
    item_id = 1
    for order_id, user_id, _, _ in orders:
        for _ in range(rng.randint(2, 4)):
            product_id = rng.randint(1, SOLD_PRODUCT_COUNT)
            sale_price = round(retail_prices[product_id] * rng.uniform(0.65, 1), 2)
            rows.append(
                (
                    item_id,
                    order_id,
                    user_id,
                    product_id,
                    sale_price,
                    rng.choice(("Complete", "Complete", "Shipped")),
                )
            )
            item_id += 1
    return rows


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


def _assert_properties(executor: Executor) -> None:
    never_ordered = _count(
        executor,
        "SELECT count(*) FROM users u ANTI JOIN orders o ON o.user_id = u.id",
    )
    never_sold = _count(
        executor,
        "SELECT count(*) FROM products p ANTI JOIN order_items oi "
        "ON oi.product_id = p.id",
    )
    mismatched_users = _count(
        executor,
        "SELECT count(*) FROM order_items oi JOIN orders o USING (order_id) "
        "WHERE oi.user_id != o.user_id",
    )
    null_foreign_keys = _count(
        executor,
        "SELECT count(*) FROM order_items WHERE order_id IS NULL "
        "OR user_id IS NULL OR product_id IS NULL",
    )
    assert never_ordered > 0
    assert never_sold > 0
    assert mismatched_users == 0
    assert null_foreign_keys == 0


def seed(executor: Executor) -> None:
    from pyarrow import table as arrow_table

    rng = random.Random(SEED)
    users = _user_rows(rng)
    products = _product_rows(rng)
    orders = _order_rows(rng)
    order_items = _order_item_rows(rng, orders, products)
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
    _assert_properties(executor)
