from typing import Optional, Union, List

from jinja2 import Template

from preql.core.enums import FunctionType
from preql.core.models import (
    Concept,
    CTE,
    ProcessedQuery,
    CompiledCTE,
    Conditional,
    Expr,
    Comparison,
    Function,
    Join,
    OrderItem,
)
from preql.dialect.base import BaseDialect

FUNCTION_MAP = {
    FunctionType.COUNT: lambda args: f"count({args[0]})",
    FunctionType.SUM: lambda args: f"sum({args[0]})",
    FunctionType.AVG: lambda args: f"avg({args[0]})",
    FunctionType.LENGTH: lambda args: f"length({args[0]})",
    FunctionType.LIKE: lambda args: f" CASE WHEN {args[0]} like {args[1]} THEN 1 ELSE 0 END",
    FunctionType.NOT_LIKE: lambda args: f" CASE WHEN {args[0]} like {args[1]} THEN 0 ELSE 1 END",
    FunctionType.CONCAT: lambda args: f"CONCAT({','.join([f''' '{a}' '''for a in args])})",
}

# if an aggregate function is called on a source that is at the same grain as the aggregate
# we may return a static value
FUNCTION_GRAIN_MATCH_MAP = {
    **FUNCTION_MAP,
    FunctionType.COUNT: lambda args: f"1",
    FunctionType.SUM: lambda args: f"{args[0]}",
    FunctionType.AVG: lambda args: f"{args[0]}",
}


TSQL_TEMPLATE = Template(
    """{%- if ctes %}
WITH {% for cte in ctes %}
{{cte.name}} as ({{cte.statement}}){% if not loop.last %},{% endif %}{% endfor %}{% endif %}
SELECT
{%- if limit %}
TOP {{ limit }}{% endif %}
{%- for select in select_columns %}
    {{ select }}{% if not loop.last %},{% endif %}{% endfor %}
FROM
    {{ base }}{% if joins %}
{% for join in joins %}
{{ join }}
{% endfor %}{% endif %}
{% if where %}WHERE
    {{ where }}
{% endif %}
{%- if group_by %}
GROUP BY {% for group in group_by %}
    {{group}}{% if not loop.last %},{% endif %}
{% endfor %}{% endif %}
{%- if order_by %}
ORDER BY {% for order in order_by %}
    {{ order }}{% if not loop.last %},{% endif %}
{% endfor %}{% endif %}
"""
)


def render_concept_sql(c: Concept, cte: CTE, alias: bool = True) -> str:
    """This should be consolidated with the render expr below."""
    if not c.lineage:
        rval = f'{cte.source_map[c.address]}."{cte.get_alias(c)}"'
    else:
        args = [render_concept_sql(v, cte, alias=False) for v in c.lineage.arguments]
        if cte.group_to_grain:
            rval = f"{FUNCTION_MAP[c.lineage.operator](args)}"
        else:
            rval = f"{FUNCTION_GRAIN_MATCH_MAP[c.lineage.operator](args)}"
    if alias:
        return f'{rval} as "{c.safe_address}"'
    return rval


def render_join(join: Join) -> str:
    # {% for key in join.joinkeys %}{{ key.inner }} = {{ key.outer}}{% endfor %}
    joinkeys = " AND ".join(
        [
            f'{join.left_cte.name}."{key.concept.safe_address}" =  {join.right_cte.name}."{key.concept.safe_address}"'
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

    return f" {output[0].name}.{order_item.expr.safe_address} {order_item.order.value}"


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
            return f'{cte.source_map[e.address]}."{cte.get_alias(e)}"'
        return f'"{e.safe_address}"'
    elif isinstance(e, bool):
        return f"{1 if e else 0 }"
    elif isinstance(e, str):
        return f"'{e}'"
    return str(e)


class SqlServerDialect(BaseDialect):
    def compile_statement(self, query: ProcessedQuery) -> str:
        select_columns = []
        output_concepts = []
        selected = set()
        output_addresses = [c.address for c in query.output_columns]
        for cte in query.ctes:
            for c in cte.output_columns:
                if (
                    c not in output_concepts
                    and c.address not in selected
                    and c.address in output_addresses
                ):
                    select_columns.append(f'{cte.name}."{c.safe_address}"')
                    output_concepts.append(c)
                    selected.add(c.address)
        where_assignment = {}
        if query.where_clause:
            found = False
            for cte in query.ctes:
                if set([x.name for x in query.where_clause.input]).issubset(
                    [z.name for z in cte.related_columns]
                ):
                    where_assignment[cte.name] = query.where_clause
                    found = True
            if not found:

                raise NotImplementedError(
                    "Cannot generate complex query with filtering on grain that does not match any source."
                )
        compiled_ctes: List[CompiledCTE] = []
        compiled_ctes += [
            CompiledCTE(
                name=cte.name,
                statement=TSQL_TEMPLATE.render(
                    select_columns=[
                        render_concept_sql(c, cte) for c in cte.output_columns
                    ],
                    joins=[render_join(join) for join in (cte.joins or [])],
                    base=f"{cte.base_name} as {cte.base_alias}",
                    grain=cte.grain,
                    where=render_expr(where_assignment[cte.name].conditional, cte)
                    if cte.name in where_assignment
                    else None,
                    group_by=[render_expr(c, cte) for c in cte.grain.components]
                    if cte.group_to_grain
                    else None,
                ),
            )
            for cte in query.ctes
        ]
        return TSQL_TEMPLATE.render(
            select_columns=select_columns,
            base=query.base.name,
            joins=[render_join(join) for join in query.joins],
            ctes=compiled_ctes,
            limit=query.limit,
            # this has been moved up to CTEs
            # where = render_expr(query.where_clause.conditional) if query.where_clause else None,
            order_by=[render_order_item(i, query.ctes) for i in query.order_by.items]
            if query.order_by
            else None,
        )
