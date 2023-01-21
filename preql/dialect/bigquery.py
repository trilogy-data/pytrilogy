from preql.core.models import Conditional, Expr, Comparison, Function
from typing import Optional, Union

from jinja2 import Template

from preql.core.models import Conditional, Expr, Comparison, Function, WindowItem,Concept, CTE, ProcessedQuery, CompiledCTE
from preql.dialect.base import BaseDialect
from preql.dialect.common import render_join, render_order_item
from preql.core.enums import FunctionType, WindowType, PurposeLineage

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

BQ_SQL_TEMPLATE = Template(
    """{%- if ctes %}
WITH {% for cte in ctes %}
{{cte.name}} as ({{cte.statement}}){% if not loop.last %},{% endif %}{% endfor %}{% endif %}
SELECT

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
{%- if limit %}
LIMIT {{ limit }}{% endif %}
"""
)

#
# def render_concept_sql(c: Concept, cte: CTE, alias: bool = True) -> str:
#     if not c.lineage:
#         rval = f"{cte.name}.{cte.source.get_alias(c)}"
#     else:
#         args = [render_concept_sql(v, cte, alias=False) for v in c.lineage.arguments]
#         rval = f"{FUNCTION_MAP[c.lineage.operator](args)}"
#     if alias:
#         return f"{rval} as {c.name}"
#     return rval
QUOTE_CHARACTER = "`"


def render_concept_sql(c: Concept, cte: CTE, alias: bool = True) -> str:
    """This should be consolidated with the render expr below."""
    # only recurse while it's in sources of the current cte
    # and don't recalculate if we already have it from a cte
    if (
        c.lineage and all([v.address in cte.source_map for v in c.lineage.arguments])
    ) and not cte.source_map.get(c.address, "").startswith("cte"):
        if isinstance(c.lineage, WindowItem):
            # args = [render_concept_sql(v, cte, alias=False) for v in c.lineage.arguments] +[c.lineage.sort_concepts]
            dimension = render_concept_sql(c.lineage.arguments[0], cte, alias=False)
            rendered_components = [
                render_concept_sql(x.expr, cte, alias=False) for x in c.lineage.order_by
            ]
            rval = f"{WINDOW_FUNCTION_MAP[WindowType.ROW_NUMBER](dimension, sort=','.join(rendered_components), order = 'desc')}"
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
        rval = f'{cte.source_map.get(c.address, "this is a bug")}.{QUOTE_CHARACTER}{c.safe_address}{QUOTE_CHARACTER}'
    else:
        rval = f'{cte.source_map.get(c.address, "this is a bug")}.{QUOTE_CHARACTER}{cte.get_alias(c)}{QUOTE_CHARACTER}'
        # rval = f'{cte.source_map[c.address]}."{cte.get_alias(c)}"'

    if alias:
        return f"{rval} as {QUOTE_CHARACTER}{c.safe_address}{QUOTE_CHARACTER}"
    return rval


def render_expr(
    e: Union[Expr, Conditional, Concept, str, int, bool], cte: Optional[CTE] = None
) -> str:
    if isinstance(e, Comparison):
        return f"{render_expr(e.left, cte=cte)} {e.operator.value} {render_expr(e.right, cte=cte)}"
    elif isinstance(e, Conditional):
        return f"{render_expr(e.left, cte=cte)} {e.operator.value} {render_expr(e.right, cte=cte)}"
    elif isinstance(e, Function):
        return FUNCTION_MAP[e.operator]([render_expr(z, cte=cte) for z in e.arguments])
    elif isinstance(e, Concept):
        if cte:
            return render_concept_sql(e, cte=cte, alias=False)
        return f"{e.safe_address}"
    elif isinstance(e, bool):
        return f"{1 if e else 0 }"
    elif isinstance(e, str):
        return f"'{e}'"
    return str(e)


class BigqueryDialect(BaseDialect):
    def compile_statement(self, query: ProcessedQuery) -> str:
        select_columns = []
        output_concepts = []
        cte_output_map = {}
        for cte in query.ctes:
            for c in cte.output_columns:
                if c not in output_concepts and c in query.output_columns:
                    select_columns.append(f"{cte.name}.{c.safe_address}")
                    cte_output_map[c.address] = cte
                    output_concepts.append(c)

        # where assignment
        where_assignment = {}
        output_where = False
        if query.where_clause:
            found = False
            filter = set([str(x) for x in query.where_clause.input])
            for cte in query.ctes:
                if filter.issubset(set([str(z) for z in cte.output_columns])):
                    # 2023-01-16 - removing related columns to look at output columns
                    # will need to backport pushing where columns into original output search
                    # if set([x.name for x in query.where_clause.input]).issubset(
                    #     [z.name for z in cte.related_columns]
                    # ):
                    where_assignment[cte.name] = query.where_clause
                    found = True
            # if all the where clause items are lineage derivation
            # check if they match at output grain
            if all([x.derivation in (PurposeLineage.WINDOW, PurposeLineage.AGGREGATE) for x in query.where_clause.input]):
                query_output = set([str(z) for z in query.output_columns])
                filter_at_output_grain = set([str(x.with_grain(query.grain)) for x in query.where_clause.input])
                if filter_at_output_grain.issubset(query_output):
                    output_where = True
                    found = True

            if not found:
                raise NotImplementedError(
                    "Cannot generate complex query with filtering on grain that does not match any source."
                )

        compiled_ctes = [
            CompiledCTE(
                name=cte.name,
                statement=BQ_SQL_TEMPLATE.render(
                    select_columns=[
                        render_concept_sql(c, cte) for c in cte.output_columns
                    ],
                    base=f"{cte.base_name} as {cte.base_alias}",
                    grain=cte.grain,
                    joins=[
                        render_join(join, QUOTE_CHARACTER) for join in (cte.joins or [])
                    ],
                    where=render_expr(where_assignment[cte.name].conditional, cte)
                    if cte.name in where_assignment
                    else None,
                    group_by=[
                        render_concept_sql(c, cte, alias=False)
                        for c in cte.grain.components
                    ]
                    if cte.group_to_grain
                    else None,
                ),
            )
            for cte in query.ctes
        ]
        return BQ_SQL_TEMPLATE.render(
            select_columns=select_columns,
            base=query.base.name,
            joins=[render_join(join, QUOTE_CHARACTER) for join in query.joins],
            ctes=compiled_ctes,
            limit=query.limit,
            # move up to CTEs
            where=render_expr(query.where_clause.conditional) #source_map=cte_output_map)
            if output_where
            else None,
            order_by=[render_order_item(i, query.ctes) for i in query.order_by.items]
            if query.order_by
            else None,
        )
