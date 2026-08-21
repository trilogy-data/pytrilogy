"""The 2026-08 field report's repro, verbatim: four source tables, `~` marks
on the fact FKs, and the full-key select over three cross-table aggregates
(`item_count * price` multiplies across the two facts that both bind
`~user_id`). Locks the union-of-branches contract end to end on mocked data:
every fact row exactly once, one extension row per unmatched member per `~`
side, extension families never cross-paired, and no null-safe join stitches
in the rendered SQL."""

from tests.modeling._row_compare import rows_match
from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig

MODEL = """
key order_id int;
key item_id int;
key user_id int;
key product_id int;

property order_id.item_count int;
property item_id.price float;
property user_id.state string;
property product_id.cost float;

auto revenue        <- item_count * price;
auto total_revenue  <- sum(revenue);
auto total_cost     <- sum(cost * item_count);
auto item_quantity  <- group(item_count) by item_id;
auto total_quantity <- sum(item_quantity);

root datasource orders_src (
    order_id: order_id,
    user_id: ~user_id,
    item_count: item_count,
    )
grain (order_id)
address orders;

root datasource items_src (
    item_id: item_id,
    order_id: order_id,
    product_id: ~product_id,
    user_id: ~user_id,
    price: price,
    )
grain (item_id)
address items;

root datasource users_src (
    user_id: user_id,
    state: state,
    )
grain (user_id)
address users;

root datasource products_src (
    product_id: product_id,
    cost: cost,
    )
grain (product_id)
address products;
"""

QUERY = """
select
    order_id,
    item_id,
    user_id,
    product_id,
    total_revenue,
    total_quantity,
    total_cost,
;
"""

TRUTH = """
WITH fact AS (
    SELECT i.order_id, i.item_id,
           coalesce(i.user_id, o.user_id) AS user_id,
           i.product_id,
           o.item_count * i.price AS total_revenue,
           o.item_count AS total_quantity,
           p.cost * o.item_count AS total_cost
    FROM items i
    JOIN orders o ON i.order_id = o.order_id
    LEFT JOIN products p ON i.product_id = p.product_id
)
SELECT * FROM fact
UNION ALL
SELECT NULL, NULL, u.user_id, NULL, NULL, NULL, NULL FROM users u
WHERE u.user_id NOT IN (SELECT user_id FROM items)
  AND u.user_id NOT IN (SELECT user_id FROM orders)
UNION ALL
SELECT NULL, NULL, NULL, p.product_id, NULL, NULL, NULL FROM products p
WHERE p.product_id NOT IN (SELECT product_id FROM items)
"""


def _sort_key(t):
    return tuple((v is None, str(v)) for v in t)


def test_field_report_select(tmp_path):
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=str(tmp_path)), conf=DuckDBConfig()
    )
    engine.parse_text(MODEL)
    targets = ", ".join(engine.environment.datasources.keys())
    engine.execute_text(f"mock datasources {targets};")

    def count(sql: str) -> int:
        row = engine.execute_raw_sql(sql).fetchone()
        assert row is not None
        return int(row[0])

    # without unmatched members on both `~` sides the span is just an INNER
    # star and the comparison proves nothing
    assert (
        count(
            "select count(*) from users u where u.user_id not in "
            "(select user_id from items) and u.user_id not in "
            "(select user_id from orders)"
        )
        > 0
    )
    assert (
        count(
            "select count(*) from products p where p.product_id not in "
            "(select product_id from items)"
        )
        > 0
    )

    engine.environment = Environment(working_path=str(tmp_path))
    engine.parse_text(MODEL)
    sql = engine.generate_sql(QUERY)[-1]
    assert "is not distinct from" not in sql, sql
    got = sorted(
        (tuple(r) for r in engine.execute_raw_sql(sql).fetchall()), key=_sort_key
    )
    want = sorted(
        (tuple(r) for r in engine.execute_raw_sql(TRUTH).fetchall()), key=_sort_key
    )
    assert len(got) == len(want), (len(got), len(want))
    for g, w in zip(got, want):
        assert rows_match(g, w), (g, w)
