from preql.dialect.base import BaseDialect
from preql.core.models import Concept, Environment, Select, Datasource, SelectItem, ConceptTransform, CTE, Join, \
    JoinKey, ProcessedQuery, CompiledCTE
from preql.core.enums import Purpose
from typing import Dict, List, Optional
import networkx as nx
from jinja2 import Template

from preql.core.enums import FunctionType

OPERATOR_MAP = {
    FunctionType.COUNT: "count",
    FunctionType.SUM: "sum",
    FunctionType.LENGTH: "length",
    FunctionType.AVG: "avg"
}



BQ_SQL_TEMPLATE = Template('''{%- if ctes %}
WITH {% for cte in ctes %}
{{cte.name}} as ({{cte.statement}}){% if not loop.last %},{% endif %}{% endfor %}{% endif %}
SELECT
{%- for select in select_columns %}
    {{ select }},{% endfor %}
FROM
{{ base }}{% if joins %}
{% for join in joins %}
{{join.jointype.value | upper }} JOIN {{ join.right_cte.name }} on {% for key in join.joinkeys %}{{ key.inner }} = {{ key.outer}}{% endfor %}
{% endfor %}{% endif %}
{%- if group_by %}
GROUP BY {% for group in group_by %}
    {{group}}{% if not loop.last %},{% endif %}
{% endfor %}{% endif %}
{%- if order_by %}
ORDER BY {% for order in order_by.items %}
    {{order.identifier}} {{order.order.value}}{% if not loop.last %},{% endif %}
{% endfor %}{% endif %}
{%- if limit %}
LIMIT {{limit }}{% endif %}
''')


def concept_to_node(input: Concept) -> str:
    return f'c~{input.name}'


def datasource_to_node(input: Datasource) -> str:
    return f'ds~{input.identifier}'


def add_concept_node(g, concept):
    g.add_node(concept_to_node(concept), type=concept.purpose.value, concept=concept)


def generate_graph(environment: Environment, inputs: List[Concept], outputs: List[Concept]) -> nx.DiGraph:
    g = nx.DiGraph()
    for name, concept in environment.concepts.items():
        node_name = concept_to_node(concept)
        print('-----')
        print(concept.name)
        if concept not in inputs and concept not in outputs:
            print('skipping irrelevant concept')
            continue
        print('included in query')
        add_concept_node(g, concept)
        if concept.sources:
            for source in concept.sources:
                add_concept_node(g, source)
                g.add_edge(concept_to_node(source), node_name)
    for key, dataset in environment.datasources.items():
        if not any([t2 in dataset.concepts for t2 in inputs]):
            print(f'None of {inputs} found in dataset with {dataset.concepts}')
            continue
        node = datasource_to_node(dataset)
        g.add_node(node, type='datasource', datasource=dataset)
        for concept in dataset.concepts:
            if concept not in inputs:
                continue
            g.add_edge(node, concept_to_node(concept))
    return g


def select_base(datasets: List[Datasource]) -> Datasource:
    datasets.sort(key=lambda x: len(x.grain.components))
    return datasets[0]


def graph_to_dataset_graph(G):
    g = G.copy()

    while any(degree == 2 for _, degree in g.degree):
        g0 = g.copy()
        for node, degree in g.degree():
            if degree == 2:

                if g.is_directed():  # <-for directed graphs
                    a0, b0 = list(g0.in_edges(node))[0]
                    a1, b1 = list(g0.out_edges(node))[0]

                else:
                    edges = g0.edges(node)
                    edges = list(edges.__iter__())
                    a0, b0 = edges[0]
                    a1, b1 = edges[1]

                e0 = a0 if a0 != node else b0
                e1 = a1 if a1 != node else b1

                g0.remove_node(node)
                g0.add_edge(e0, e1)
        g = g0
    return g


def get_relevant_dataset_concepts(g, datasource: Datasource, grain: List[Concept], output_concepts: List[Concept]) -> \
List[Concept]:
    relevant = []
    # TODO: handle joins to get to appropriate grain
    relevant += grain
    for concept in output_concepts:
        try:
            path = nx.shortest_path(g, source=datasource_to_node(datasource), target=concept_to_node(concept))
            if len([p for p in path if g.nodes[p]['type'] == 'datasource']) == 1:
                if concept not in relevant:
                    relevant.append(concept)
        except nx.exception.NetworkXNoPath:
            continue
    return relevant


def render_concept_sql(c: Concept, cte: CTE, alias: bool = True) -> str:
    if not c.lineage:
        rval = f'{cte.name}.{cte.source.get_alias(c)}'
    else:
        args = ','.join([render_concept_sql(v, cte, alias=False) for v in c.lineage.arguments])
        rval = f'{OPERATOR_MAP[c.lineage.operator]}({args})'
    if alias:
        return f'{rval} as {c.name}'
    return rval


def dataset_to_grain(g: nx.Graph, datasources: List[Datasource], grain: List[Concept], output_concepts: List[Concept]):
    ''' for each subgraph that needs to get the target grain
    create a subquery'''
    output: Dict[str, Optional[str]] = {}

    for datasource in datasources:
        related = get_relevant_dataset_concepts(g, datasource, grain, output_concepts)
        print('----')
        print([r.name for r in related])
        group_by = None
        # related = list(g.neighbors(datasource_to_node(datasource)))
        if datasource.grain.components != grain:
            metrics = [render_concept_sql(n, datasource) for n in related if n.purpose in [Purpose.METRIC]]
            dimensions = [render_concept_sql(n, datasource) for n in related if
                          n.purpose in [Purpose.KEY, Purpose.PROPERTY]]
            select_columns = metrics + dimensions
            group_by = [v.name for v in grain]

        else:
            select_columns = [render_concept_sql(n, datasource) for n in related if
                              n.purpose in [Purpose.KEY, Purpose.PROPERTY]]

        sub_command = BQ_SQL_TEMPLATE.render(select_columns=select_columns, base=datasource.address.location, joins=[],
                                             group_by=group_by)
        output[datasource.identifier] = sub_command
    return output


def graph_to_query(environment: Environment, g: nx.Graph, statement: Select) -> str:
    # select base table
    # populate joins
    # populate columns
    datasets = [v for key, v in environment.datasources.items() if datasource_to_node(v) in g.nodes()]
    select_items: List[SelectItem] = statement.selection
    output_items = statement.output_components

    select_columns = []
    grain = [v for v in output_items if v.purpose == Purpose.KEY]
    for item in select_items:
        if isinstance(item.content, Concept):
            select_columns.append(item.content.name)
        elif isinstance(item.content, ConceptTransform):
            arguments = ','.join([v.name for v in item.content.function.arguments])
            rendered = f'{item.content.function.operator}({arguments}) as {item.content.output.name}'
            select_columns.append(rendered)
    base = select_base(datasets)
    joins = []
    # aggregate each table to the target granularity
    # then join
    inputs = dataset_to_grain(g, datasets, statement.grain, statement.output_components)
    select_columns = [v.name for v in output_items]

    # TODO: pick based upon which has the
    input_keys = list(inputs.keys())
    base = input_keys[0]

    joins = [Join(identifier=k, joinkeys=[JoinKey(inner=f'{base}.{g.name}', outer=f'{k}.{g.name}') for g in grain]) for
             k in input_keys[1:]]

    return BQ_SQL_TEMPLATE.render(select_columns=select_columns, base=base, joins=joins, grain=grain,
                                  ctes=[CTE(name=key, statement=value) for key, value in inputs.items()])


class BigqueryDialect(BaseDialect):

    def compile_sql(self, environment: Environment, statements) -> List[str]:
        output = []
        for statement in statements:
            if isinstance(statement, Select):
                graph = generate_graph(environment, statement.input_components, statement.output_components)
                output.append(graph_to_query(environment, graph, statement))
        return output

    def compile_statement(self, query: ProcessedQuery) -> str:
        select_columns = []
        output_concepts = []
        for cte in query.ctes:
            for c in cte.output_columns:
                if c not in output_concepts:
                    select_columns.append(f'{cte.name}.{c.name}')
                    output_concepts.append(c)
        compiled_ctes = [CompiledCTE(name=cte.name, statement=BQ_SQL_TEMPLATE.render(
            select_columns=[render_concept_sql(c, cte) for c in cte.output_columns],
            base=f'{cte.source.address.location} as {cte.source.identifier}',
            grain=cte.grain,
            group_by=[c.name for c in cte.grain] if cte.group_to_grain else None
            )) for cte in query.ctes]

        return BQ_SQL_TEMPLATE.render(select_columns=select_columns, base=query.base.name, joins=query.joins,
                                      grain=query.joins,
                                      ctes=compiled_ctes, limit=query.limit,
                                      order_by=query.order_by)
