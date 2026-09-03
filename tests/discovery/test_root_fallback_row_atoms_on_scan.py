"""A WHERE mixing plain row atoms with a cross-row gate (`sum(x) by k > n`)
sends ROOT sourcing to its feeder-merge fallback. The row atoms belong on the
sourced row scan; only the gate rides the feeder merge."""

from trilogy import Dialects, Environment
from trilogy.core.processing.nodes import StrategyNode
from trilogy.core.query_processor import get_query_node
from trilogy.core.statements.author import SelectStatement

MODEL = """
key order_id int;
key customer_id int;
key product_id int;
property order_id.order_value float;
property product_id.product_name string;

datasource orders (
    order_id: order_id,
    customer_id: customer_id,
    product_id: product_id,
    order_value: order_value,
)
grain (order_id)
query '''
select 1 as order_id, 101 as customer_id, 201 as product_id, 25.99 as order_value
union all
select 2 as order_id, 102 as customer_id, 202 as product_id, 55.50 as order_value
union all
select 3 as order_id, 101 as customer_id, 202 as product_id, 33.25 as order_value
union all
select 4 as order_id, 103 as customer_id, 201 as product_id, 78.00 as order_value
union all
select 5 as order_id, 105 as customer_id, 202 as product_id, 12.75 as order_value
''';

datasource products (
    product_id: product_id,
    name: product_name,
)
grain (product_id)
query '''
select 201 as product_id, 'Laptop' as name
union all
select 202 as product_id, 'Mouse' as name
''';
"""

QUERY = (
    "where product_name = 'Mouse' and sum(order_value) by customer_id > 50 "
    "select order_id, customer_id order by order_id asc;"
)


def _conditioned_nodes(node: StrategyNode) -> list[StrategyNode]:
    found: list[StrategyNode] = []
    stack = [node]
    seen: set[int] = set()
    while stack:
        current = stack.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if current.conditions is not None:
            found.append(current)
        stack.extend(current.parents)
    return found


def test_rows():
    env = Environment()
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    rows = [tuple(r) for r in executor.execute_query(QUERY).fetchall()]
    # customer 105's only order is a Mouse but its total is under the gate
    assert rows == [(2, 102), (3, 101)]


def test_row_atom_hosted_below_the_gate_merge():
    env = Environment()
    _, statements = env.parse(MODEL + QUERY)
    select = statements[-1]
    assert isinstance(select, SelectStatement)
    root = get_query_node(env, select.as_lineage(env))
    hosts = {str(node.conditions): node for node in _conditioned_nodes(root)}
    gate_host = next(node for text, node in hosts.items() if "> 50" in text)
    row_host = next(
        node for text, node in hosts.items() if "product_name = Mouse" in text
    )
    assert "product_name" not in str(gate_host.conditions)
    assert "> 50" not in str(row_host.conditions)
    assert row_host is not gate_host
    assert all(parent.conditions is None for parent in row_host.parents)
