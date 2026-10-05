"""A merge whose row identity is projected needs no GROUP BY.

`users LEFT items FULL products`, both dimension keys bound `~` on the fact:
the stream is one row per (user, item, product), and the key-hierarchy fold
(`user.id` under `id`) holds only where the item is present. Projecting all
three keys projects the identity; projecting a property of the FULL'd side in
place of its key does not (the unsold products collapse per category)."""

import pytest

from tests.helpers.rows import executor_for, sort_rows
from trilogy.executor import Executor

MODEL = """
key user_id int;
property user_id.state string;
key product_id int;
property product_id.category string;
key item_id int;
property item_id.price int;

root datasource users (user_id: user_id, state: state)
grain (user_id)
query '''select 1 as user_id, 'CA' as state union all select 2, 'NY' union all select 3, 'TX' ''';

root datasource products (product_id: product_id, category: category)
grain (product_id)
query '''select 10 as product_id, 'shoes' as category union all select 20, 'shoes'
union all select 30, 'hats' union all select 40, 'hats' ''';

root datasource items (item_id: item_id, user_id: ~user_id, product_id: ~product_id, price: price)
grain (item_id)
query '''select 1000 as item_id, 1 as user_id, 10 as product_id, 5 as price union all
select 1001, 1, 20, 7 union all select 1002, 2, 10, 3 ''';
"""

IDENTITY_ROWS = [
    (1, 1000, 10),
    (1, 1001, 20),
    (2, 1002, 10),
    (3, None, None),
    (None, None, 30),
    (None, None, 40),
]


@pytest.fixture(scope="module")
def executor() -> Executor:
    return executor_for(MODEL)


def _run(executor: Executor, query: str) -> tuple[str, list[tuple]]:
    sql = executor.generate_sql(query)[-1]
    rows = executor.execute_raw_sql(sql).fetchall()
    return sql, sort_rows(rows)


def test_projected_identity_is_not_grouped(executor: Executor):
    sql, rows = _run(executor, "select user_id, item_id, product_id;")
    assert "GROUP BY" not in sql, sql
    assert rows == sort_rows(IDENTITY_ROWS)


def test_projected_identity_beside_an_aggregate_is_not_regrouped(
    executor: Executor,
):
    sql, rows = _run(
        executor,
        "select user_id, item_id, product_id, sum(price) by user_id as user_total;",
    )
    assert sql.count("GROUP BY") == 1, sql
    assert rows == sort_rows(
        [
            (1, 1000, 10, 12),
            (1, 1001, 20, 12),
            (2, 1002, 10, 3),
            (3, None, None, None),
            (None, None, 30, None),
            (None, None, 40, None),
        ]
    )


def test_unprojected_full_side_key_is_grouped(executor: Executor):
    sql, rows = _run(executor, "select user_id, item_id, category;")
    assert "GROUP BY" in sql, sql
    assert rows == sort_rows(
        [
            (1, 1000, "shoes"),
            (1, 1001, "shoes"),
            (2, 1002, "shoes"),
            (3, None, None),
            (None, None, "hats"),
        ]
    )
