from preql.core.models import Join, OrderItem
from typing import List

from preql.core.models import CTE
from preql.core.models import Join, OrderItem


def render_join(join: Join) -> str:
    # {% for key in join.joinkeys %}{{ key.inner }} = {{ key.outer}}{% endfor %}
    joinkeys = " AND ".join(
        [
            f'{join.left_cte.name}."{key.inner.name}" = {join.right_cte.name}."{key.outer.name}"'
            for key in join.joinkeys
        ]
    )
    return f"{join.jointype.value.upper()} JOIN {join.right_cte.name} on {joinkeys}"


def render_order_item(order_item: OrderItem, ctes: List[CTE]) -> str:
    output = [cte for cte in ctes if order_item.expr in cte.output_columns]
    for cte in ctes:
        for oc in cte.output_columns:
            print(oc.name)
            print(oc.grain)
    if not output:
        raise ValueError(f"No source found for concept {order_item.expr}")

    return f" {output[0].name}.{order_item.expr.name} {order_item.order.value}"
