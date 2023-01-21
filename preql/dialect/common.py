from preql.core.models import Join, OrderItem
from typing import List, Union, Optional

from preql.core.models import CTE
from preql.core.models import Join, OrderItem
from preql.core.models import Conditional, Expr, Comparison, Function, WindowItem,Concept, CTE, ProcessedQuery, CompiledCTE
from preql.dialect.base import BaseDialect
from preql.core.enums import FunctionType, WindowType, PurposeLineage

def render_join(join: Join, quote_character: str = '"') -> str:
    # {% for key in join.joinkeys %}{{ key.inner }} = {{ key.outer}}{% endfor %}
    joinkeys = " AND ".join(
        [
            f"{join.left_cte.name}.{quote_character}{key.concept.safe_address}{quote_character} = {join.right_cte.name}.{quote_character}{key.concept.safe_address}{quote_character}"
            for key in join.joinkeys
        ]
    )
    return f"{join.jointype.value.upper()} JOIN {join.right_cte.name} on {joinkeys}"


def render_order_item(order_item: OrderItem, ctes: List[CTE]) -> str:
    output = [
        cte
        for cte in ctes
        if order_item.expr.address in [a.address for a in cte.output_columns]
    ]
    if not output:
        raise ValueError(f"No source found for concept {order_item.expr}")

    return f" {output[0].name}.{order_item.expr.name} {order_item.order.value}"




def render_concept_sql(c: Concept, cte: CTE, alias: bool = True) -> str:
    """This should be consolidated with the render expr below."""

    # only recurse while it's in sources of the current cte
    if c.lineage and all([v.address in cte.source_map for v in c.lineage.arguments]):
        if isinstance(c.lineage, WindowItem):
            # args = [render_concept_sql(v, cte, alias=False) for v in c.lineage.arguments] +[c.lineage.sort_concepts]
            dimension = render_concept_sql(c.lineage.arguments[0], cte, alias=False)
            test = [x.expr.name for x in c.lineage.order_by]

            rval = f"{WINDOW_FUNCTION_MAP[WindowType.ROW_NUMBER](dimension, sort=','.join(test), order = 'desc')}"
        else:
            args = [
                render_concept_sql(v, cte, alias=False) for v in c.lineage.arguments
            ]
            if cte.group_to_grain:
                rval = f"{FUNCTION_MAP[c.lineage.operator](args)}"
            else:
                rval = f"{FUNCTION_GRAIN_MATCH_MAP[c.lineage.operator](args)}"
    # else if it's complex, just reference it from the source
    elif c.lineage:
        rval = f'{cte.source_map[c.address]}."{c.safe_address}"'
    else:
        rval = f'{cte.source_map.get(c.address, "this is a bug")}."{cte.get_alias(c)}"'
        # rval = f'{cte.source_map[c.address]}."{cte.get_alias(c)}"'

    if alias:
        return f'{rval} as "{c.safe_address}"'
    return rval


WINDOW_FUNCTION_MAP = {
    WindowType.ROW_NUMBER: lambda window, sort, order: f"row_number() over ( order by {sort} {order })"
}

FUNCTION_MAP = {
    FunctionType.COUNT: lambda x: f"count({x[0]})",
    FunctionType.SUM: lambda x: f"sum({x[0]})",
    FunctionType.LENGTH: lambda x: f"length({x[0]})",
    FunctionType.AVG: lambda x: f"avg({x[0]})",
    FunctionType.LIKE: lambda x: f" CASE WHEN {x[0]} like {x[1]} THEN 1 ELSE 0 END",
    FunctionType.NOT_LIKE: lambda x: f" CASE WHEN {x[0]} like {x[1]} THEN 0 ELSE 1 END",
}

FUNCTION_GRAIN_MATCH_MAP = {
    **FUNCTION_MAP,
    FunctionType.COUNT: lambda args: f"{args[0]}",
    FunctionType.SUM: lambda args: f"{args[0]}",
    FunctionType.AVG: lambda args: f"{args[0]}",
}


def render_expr(
        e: Union[Expr, Conditional, Concept, str, int, bool], cte: Optional[CTE] = None
) -> str:
    if isinstance(e, Comparison):
        return f"{render_expr(e.left, cte=cte)} {e.operator.value} {render_expr(e.right, cte=cte)}"
    elif isinstance(e, Conditional):
        return f"{render_expr(e.left, cte=cte)} {e.operator.value} {render_expr(e.right, cte=cte)}"
    elif isinstance(e, Function):
        if cte and cte.group_to_grain:
            return FUNCTION_MAP[e.operator](
                [render_expr(z, cte=cte) for z in e.arguments]
            )
        return FUNCTION_GRAIN_MATCH_MAP[e.operator](
            [render_expr(z, cte=cte) for z in e.arguments]
        )
    elif isinstance(e, Concept):
        if cte:
            # return f'{cte.source_map.get(e.address, "this is a bug")}."{cte.get_alias(e)}"'
            return f'{cte.source_map[e.address]}."{cte.get_alias(e)}"'
        return f'"{e.safe_address}"'
    elif isinstance(e, bool):
        return f"{1 if e else 0 }"
    elif isinstance(e, str):
        return f"'{e}'"
    return str(e)
