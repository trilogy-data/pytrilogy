"""Cheap model and data guards for the thelook partial-bridge eval.

NOTE: the eval originally measured agent recovery from
UnconstrainedPartialBridgeException. That guard is gone — unpinned spans now
generate (fact pairs plus per-`~`-side extension rows) — so the error-recovery
metric (error_recovery.py) is retired; these guards now pin the generating
behavior instead."""

from __future__ import annotations

from pathlib import Path

import db_build
import pytest

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig

EVAL_DIR = Path(__file__).resolve().parent
MODEL_DIR = EVAL_DIR / "enriched_model"
IMPORTS = "import order_items as order_item;\n"

TRIGGERS = (
    (
        "select order_item.user.state, order_item.product.brand, "
        "sum(order_item.sale_price);"
    ),
    "select order_item.user.traffic_source, order_item.product.category;",
    (
        "select order_item.user.state, order_item.product.department, "
        "sum(order_item.item_margin);"
    ),
    (
        "select order_item.user.age, order_item.product.category, "
        "avg(order_item.sale_price);"
    ),
    (
        "select order_item.user.traffic_source, order_item.product.department, "
        "sum(order_item.sale_price);"
    ),
)


def _scalar(engine: Executor, sql: str) -> int:
    row = engine.execute_raw_sql(sql).fetchone()
    assert row is not None
    return int(row[0])


@pytest.fixture(scope="module")
def engine() -> Executor:
    return Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=MODEL_DIR),
        conf=DuckDBConfig(path=str(db_build.build_database()), read_only=True),
    )


@pytest.mark.parametrize("query", TRIGGERS)
def test_naive_spanning_queries_generate(engine: Executor, query: str) -> None:
    sql = engine.generate_sql(IMPORTS + query)[-1]
    rows = engine.execute_raw_sql(sql).fetchall()
    assert rows
    # extension families never cross-pair into an all-NULL keyed row
    assert not [r for r in rows if r[0] is None and r[1] is None]


def test_suggested_pin_heals_spanning_query(engine: Executor) -> None:
    sql = engine.generate_sql(
        IMPORTS
        + "where order_item.user.id is not null "
        + "and order_item.product.id is not null "
        + TRIGGERS[0]
    )[-1]
    assert "FULL JOIN" not in sql
    assert engine.execute_raw_sql(sql).fetchall()


def test_database_contract_and_references(engine: Executor) -> None:
    checks = (
        "SELECT count(*) FROM users u ANTI JOIN orders o ON o.user_id = u.id",
        (
            "SELECT count(*) FROM products p ANTI JOIN order_items oi "
            "ON oi.product_id = p.id"
        ),
    )
    assert all(_scalar(engine, sql) > 0 for sql in checks)
    assert (
        _scalar(
            engine,
            "SELECT count(*) FROM order_items oi JOIN orders o USING (order_id) "
            "WHERE oi.user_id != o.user_id",
        )
        == 0
    )
    for reference in sorted((EVAL_DIR / "references").glob("query*.sql")):
        assert engine.execute_raw_sql(reference.read_text(encoding="utf-8")).fetchall()
