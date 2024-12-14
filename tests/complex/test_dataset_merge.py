from datetime import date

from trilogy import Dialects
from trilogy.core.enums import ComparisonOperator
from trilogy.core.models import Comparison, Environment
from trilogy.core.processing.node_generators.select_helpers.datasource_injection import (
    get_union_sources,
    simplify_conditions,
)


def test_source_merged_datasources():

    declarations = """
key order_id int;
property order_id.order_date date;
property order_id.store_id int;
property order_id.web_id int;

datasource web_orders (
    order_id: ~order_id,
    web_id: web_id,
    order_date:order_date
)
grain (order_id)
complete where order_date <= cast('2024-01-01' as date)
query '''
select 1 as order_id, 4 web_id, cast('2024-01-01' as date) as order_date
'''
where order_date <= '2024-01-01'::date

;

datasource store_orders (
    order_id: ~order_id,
    store_id: store_id,
    order_date : order_date
)
grain (order_id)
complete where order_date > cast('2024-01-01' as date)
query '''
select 2 as order_id, 3 store_id, cast('2024-10-01' as date) as order_date
'''
where order_date > '2024-01-01'::date

;
"""

    x = Dialects.DUCK_DB.default_executor()

    x.parse_text(declarations)

    env = x.environment

    unions = get_union_sources(env.datasources.values(), [env.concepts["order_id"]])
    assert unions, unions
    assert unions[0] == [env.datasources["web_orders"], env.datasources["store_orders"]]

    z = x.execute_query(
        """select order_id where order_date <='2024-01-01'::date;"""
    ).fetchall()

    assert len(z) == 1
    assert z[0].order_id == 1

    z = x.execute_query("""select order_id order by order_id asc;""").fetchall()

    assert len(z) == 2
    assert z[0].order_id == 1
    assert z[1].order_id == 2


def test_conditional_merge():
    env = Environment()
    env.parse(
        """
key x int;
key y date;
"""
    )
    left = Comparison(left=env.concepts["x"], right=2, operator=ComparisonOperator.GT)
    right = Comparison(left=env.concepts["x"], right=2, operator=ComparisonOperator.LTE)
    conditions = [left, right]

    assert simplify_conditions(conditions)

    left = Comparison(left=env.concepts["x"], right=3, operator=ComparisonOperator.GT)
    right = Comparison(left=env.concepts["x"], right=2, operator=ComparisonOperator.LTE)
    conditions = [left, right]

    assert simplify_conditions(conditions) is False

    left = Comparison(
        left=env.concepts["y"],
        right=date(year=2024, month=1, day=1),
        operator=ComparisonOperator.GT,
    )
    right = Comparison(
        left=env.concepts["y"],
        right=date(year=2024, month=1, day=1),
        operator=ComparisonOperator.LTE,
    )
    conditions = [left, right]

    assert simplify_conditions(conditions)

    left = Comparison(
        left=env.concepts["y"],
        right=date(year=2025, month=1, day=1),
        operator=ComparisonOperator.GT,
    )
    right = Comparison(
        left=env.concepts["y"],
        right=date(year=2024, month=1, day=1),
        operator=ComparisonOperator.LTE,
    )
    conditions = [left, right]

    assert simplify_conditions(conditions) is False
