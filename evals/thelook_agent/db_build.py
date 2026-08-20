"""Build the deterministic DuckDB database for the thelook bridge eval.

`assert_properties` is the contract this fixture shares with the partial-bridge
regression battery (`tests/modeling/thelook_duckdb/db_build.py` imports it, but
seeds itself from `trilogy unit` mock data rather than these generators). It is
what guarantees the never-ordered users and never-sold products that the `~`
semantics exist to describe — keep it passing when changing a generator or
`SEED`.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

EVAL_DIR = Path(__file__).resolve().parent
CACHE_DB = EVAL_DIR / ".cache" / "thelook.duckdb"
DB_FILENAME = "thelook.duckdb"

USER_COUNT = 2_000
PRODUCT_COUNT = 500
ORDER_COUNT = 5_000
ACTIVE_USER_COUNT = 1_800
SOLD_PRODUCT_COUNT = 450
SEED = 8675309


def user_rows(rng: random.Random) -> list[tuple[int, str, int, str]]:
    states = ("CA", "NY", "TX", "FL", "WA", "IL", "MA", "CO", "GA", "NC")
    sources = ("Search", "Organic", "Email", "Facebook", "Display")
    return [
        (user_id, rng.choice(states), rng.randint(18, 78), rng.choice(sources))
        for user_id in range(1, USER_COUNT + 1)
    ]


def product_rows(
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


def order_rows(
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


def order_item_rows(
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


def _count(connection: DuckDBPyConnection, sql: str) -> int:
    row = connection.execute(sql).fetchone()
    assert row is not None
    return int(row[0])


def assert_properties(count: Callable[[str], int]) -> None:
    """The four invariants both fixtures exist to provide.

    `count` runs one scalar query — the eval holds a raw connection, the test
    battery a trilogy Executor.
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
    import duckdb

    if CACHE_DB.exists():
        return CACHE_DB

    rng = random.Random(SEED)
    users = user_rows(rng)
    products = product_rows(rng)
    orders = order_rows(rng)
    order_items = order_item_rows(rng, orders, products)

    CACHE_DB.parent.mkdir(parents=True, exist_ok=True)
    temporary = CACHE_DB.with_suffix(".building")
    temporary.unlink(missing_ok=True)
    connection = duckdb.connect(str(temporary))
    try:
        connection.execute(
            "CREATE TABLE users (id INTEGER, state VARCHAR, age INTEGER, "
            "traffic_source VARCHAR)"
        )
        connection.execute(
            "CREATE TABLE products (id INTEGER, brand VARCHAR, category VARCHAR, "
            "department VARCHAR, retail_price DECIMAL(12, 2), cost DECIMAL(12, 2))"
        )
        connection.execute(
            "CREATE TABLE orders (order_id INTEGER, user_id INTEGER, status VARCHAR, "
            "created_at TIMESTAMP)"
        )
        connection.execute(
            "CREATE TABLE order_items (id INTEGER, order_id INTEGER, user_id INTEGER, "
            "product_id INTEGER, sale_price DECIMAL(12, 2), status VARCHAR)"
        )
        connection.executemany("INSERT INTO users VALUES (?, ?, ?, ?)", users)
        connection.executemany(
            "INSERT INTO products VALUES (?, ?, ?, ?, ?, ?)", products
        )
        connection.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", orders)
        connection.executemany(
            "INSERT INTO order_items VALUES (?, ?, ?, ?, ?, ?)", order_items
        )
        assert_properties(partial(_count, connection))
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    temporary.with_name(temporary.name + ".wal").unlink(missing_ok=True)
    temporary.replace(CACHE_DB)
    return CACHE_DB
