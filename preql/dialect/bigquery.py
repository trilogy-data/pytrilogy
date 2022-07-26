from jinja2 import Template
from preql.dialect.base import BaseDialect
from preql.core.models import Concept, Environment, Select, Datasource, SelectItem, ConceptTransform, CTE, Join, \
    JoinKey, ProcessedQuery, CompiledCTE, Conditional, Expr, Comparison, Function, OrderBy, OrderItem
from preql.core.enums import Purpose
from typing import Dict, List, Optional, Union
import networkx as nx
from jinja2 import Template
from preql.core.enums import FunctionType
from preql.core.models import Concept, CTE, ProcessedQuery, CompiledCTE
from preql.dialect.base import BaseDialect

FUNCTION_MAP = {
    FunctionType.COUNT: lambda x: f"count({x[0]})",
    FunctionType.SUM: lambda x:f"sum({x[0]})",
    FunctionType.LENGTH: lambda x:f"length({x[0]})",
    FunctionType.AVG: lambda x: f"avg({x[0]})",
    FunctionType.LIKE: lambda x: f" CASE WHEN {x[0]} like {x[1]} THEN 1 ELSE 0 END",
    FunctionType.NOT_LIKE: lambda x: f" CASE WHEN {x[0]} like {x[1]} THEN 0 ELSE 1 END",
}

BQ_SQL_TEMPLATE = Template('''{%- if ctes %}
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
''')


def render_concept_sql(c: Concept, cte: CTE, alias: bool = True) -> str:
    if not c.lineage:
        rval = f'{cte.name}.{cte.source.get_alias(c)}'
    else:
        args = [render_concept_sql(v, cte, alias=False) for v in c.lineage.arguments]
        rval = f'{FUNCTION_MAP[c.lineage.operator](args)}'
    if alias:
        return f'{rval} as {c.name}'
    return rval

def render_expr(e:Union[Expr, Conditional, Concept, str, int, bool], cte:Optional[CTE]=None)->str:
    if isinstance(e, Comparison):
        return  f'{render_expr(e.left, cte=cte)} {e.operator.value} {render_expr(e.right, cte=cte)}'
    elif isinstance(e, Conditional):
        return  f'{render_expr(e.left, cte=cte)} {e.operator.value} {render_expr(e.right, cte=cte)}'
    elif isinstance(e, Function):
        return FUNCTION_MAP[e.operator]([render_expr(z, cte=cte) for z in e.arguments])
    elif isinstance(e, Concept):
        if cte:
            return f'{cte.name}.{cte.source.get_alias(e)}'
        return f'{e.name}'
    elif isinstance(e, bool):
        return f'{1 if e else 0 }'
    elif isinstance(e, str):
        return f"'{e}'"
    return str(e)

def render_join(join:Join)->str:
    #{% for key in join.joinkeys %}{{ key.inner }} = {{ key.outer}}{% endfor %}
    joinkeys = ' AND '.join([f'{join.left_cte.name}.{key.inner.name} = {join.right_cte.name}.{key.outer.name}' for key in join.joinkeys])
    return f'{join.jointype.value.upper()} JOIN {join.right_cte.name} on {joinkeys}'

def render_order_item(order_item:OrderItem, ctes:List[CTE])->str:
    output = [cte for cte in ctes if order_item.expr in cte.output_columns]
    if not output:
        raise ValueError(f'No source found for concept {order_item.expr}')
    return f' {output[0].name}.{order_item.expr.name} {order_item.order.value}'


class BigqueryDialect(BaseDialect):

    def compile_statement(self, query: ProcessedQuery) -> str:
        select_columns = []
        output_concepts = []
        for cte in query.ctes:
            for c in cte.output_columns:
                if c not in output_concepts and c in query.output_columns:
                    select_columns.append(f'{cte.name}.{c.name}')
                    output_concepts.append(c)
        # where assignemnt
        where_assignment = {}

        if query.where_clause:
            found = False
            for cte in query.ctes:
                if set([x.name for x in query.where_clause.input]).issubset([z.name for z in cte.related_columns]):
                    where_assignment[cte.name] = query.where_clause
                    found = True
            if not found:
                for cte in query.ctes:
                    print(cte.source.grain)
                raise NotImplementedError('Cannot generate complex query with filtering on grain that does not match any source.')
        compiled_ctes = [CompiledCTE(name=cte.name, statement=BQ_SQL_TEMPLATE.render(
            select_columns=[render_concept_sql(c, cte) for c in cte.output_columns],
            base=f'{cte.source.address.location} as {cte.name}',
            grain=cte.grain,
            where = render_expr(where_assignment[cte.name].conditional, cte) if cte.name in where_assignment else None,
            group_by=[render_expr(c, cte) for c in cte.grain.components] if cte.group_to_grain else None
        )) for cte in query.ctes]
        return BQ_SQL_TEMPLATE.render(select_columns=select_columns, base=query.base.name, joins=[render_join(join) for join in query.joins],
                                    ctes=compiled_ctes, limit=query.limit,
                                    where = render_expr(query.where_clause.conditional) if query.where_clause else None,
                                    order_by=[render_order_item(i, query.ctes) for i in query.order_by.items] if query.order_by else None)

