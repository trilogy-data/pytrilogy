from preql.dialect.base import BaseDialect
from preql.core.models import Concept, Environment, Select, Datasource, SelectItem, ConceptTransform, CTE, Join, \
    JoinKey, ProcessedQuery, CompiledCTE, Conditional, Expr, Comparison
from preql.core.enums import Purpose
from typing import Dict, List, Optional, Union
import networkx as nx
from jinja2 import Template

from preql.core.enums import FunctionType

OPERATOR_MAP = {
    FunctionType.COUNT: "count",
    FunctionType.SUM: "sum",
    FunctionType.LENGTH: "length",
    FunctionType.AVG: "avg"
}



TSQL_TEMPLATE = Template('''{%- if ctes %}
WITH {% for cte in ctes %}
{{cte.name}} as ({{cte.statement}}){% if not loop.last %},{% endif %}{% endfor %}{% endif %}
SELECT
{%- if limit %}
TOP {{ limit }}{% endif %}
{%- for select in select_columns %}
    {{ select }},{% endfor %}
FROM
    {{ base }}{% if joins %}
{% for join in joins %}
{{join.jointype.value | upper }} JOIN {{ join.right_cte.name }} on {% for key in join.joinkeys %}{{ key.inner }} = {{ key.outer}}{% endfor %}
{% endfor %}{% endif %}
{% if where %}WHERE
    {{ where }}
{% endif %}
{%- if group_by %}
GROUP BY {% for group in group_by %}
    {{group}}{% if not loop.last %},{% endif %}
{% endfor %}{% endif %}
{%- if order_by %}
ORDER BY {% for order in order_by.items %}
    {{order.identifier}} {{order.order.value}}{% if not loop.last %},{% endif %}
{% endfor %}{% endif %}

''')



def render_concept_sql(c: Concept, cte: CTE, alias: bool = True) -> str:
    if not c.lineage:
        rval = f'{cte.name}.{cte.source.get_alias(c)}'
    else:
        args = ','.join([render_concept_sql(v, cte, alias=False) for v in c.lineage.arguments])
        rval = f'{OPERATOR_MAP[c.lineage.operator]}({args})'
    if alias:
        return f'{rval} as {c.name}'
    return rval

def render_expr(e:Union[Expr, Conditional])->str:
    if isinstance(e, Comparison):
        return  f'{render_expr(e.left)} {e.operator.value} {render_expr(e.right)}'
    elif isinstance(e, Conditional):
        return  f'{render_expr(e.left)} {e.operator.value} {render_expr(e.right)}'
    elif isinstance(e, Concept):
        return f'{e.name}'
    elif isinstance(e, str):
        return f"'{e}'"
    return str(e)



class SqlServerDialect(BaseDialect):

    def compile_statement(self, query: ProcessedQuery) -> str:
        select_columns = []
        output_concepts = []
        for cte in query.ctes:
            for c in cte.output_columns:
                if c not in output_concepts and c in query.output_columns:
                    select_columns.append(f'{cte.name}.{c.name}')
                    output_concepts.append(c)
        compiled_ctes = [CompiledCTE(name=cte.name, statement=TSQL_TEMPLATE.render(
            select_columns=[render_concept_sql(c, cte) for c in cte.output_columns],
            base=f'{cte.source.address.location} as {cte.source.identifier}',
            grain=cte.grain,
            group_by=[c.name for c in cte.grain] if cte.group_to_grain else None
        )) for cte in query.ctes]

        return TSQL_TEMPLATE.render(select_columns=select_columns, base=query.base.name, joins=query.joins,
                                      grain=query.joins,
                                      ctes=compiled_ctes, limit=query.limit,
                                        where = render_expr(query.where_clause.conditional) if query.where_clause.conditional else None,
                                      order_by=query.order_by)
