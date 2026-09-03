"""A conditioned projection folds into its single parent through
CollapseSingleParent's filtered-projection branch, but never into a parent
that computes a window: the predicate would then apply before the window
instead of over its output."""

from trilogy import Dialects, Environment

MODEL = """
key order_id int;
property order_id.order_value float;

datasource orders (
    order_id: order_id,
    order_value: order_value,
)
grain (order_id)
query '''
select 1 as order_id, 25.99 as order_value
union all
select 2 as order_id, 55.50 as order_value
union all
select 3 as order_id, 33.25 as order_value
union all
select 4 as order_id, 78.00 as order_value
''';

auto value_rank <- rank order_id order by order_value desc;
"""


def test_filter_over_window_arithmetic_keeps_window_scope():
    env = Environment()
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    query = """
    select order_id, value_rank
    where value_rank + 1 <= 3
    order by order_id asc;
    """
    sql = executor.generate_sql(query)[-1]
    rows = [tuple(r) for r in executor.execute_query(query).fetchall()]
    assert rows == [(2, 2), (4, 1)]
    assert "QUALIFY" in sql or sql.count("rank()") == 1


def test_plain_filter_folds_into_parent():
    env = Environment()
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    query = """
    select order_id, order_value
    where order_value > 30
    order by order_id asc;
    """
    sql = executor.generate_sql(query)[-1]
    rows = [tuple(r) for r in executor.execute_query(query).fetchall()]
    assert rows == [(2, 55.5), (3, 33.25), (4, 78.0)]
    assert "WITH" not in sql
