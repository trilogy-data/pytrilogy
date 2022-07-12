from ttl.dialect.base import BaseDialect
from ttl.core.models import Concept, Environment, Select, Datasource, SelectItem, SelectTransform
from ttl.core.enums import Purpose
from typing import Dict, List
import networkx as nx
from jinja2 import Template

SQL_TEMPLATE = Template('''
SELECT
{%for select in select_columns %}
    {{ select }},{% endfor %}
from 
{{ base.identifier }}
{% for join in joins %}
LEFT OUTER JOIN {{ join.identifier }} on {% for key in join.keys %}{{ join.inner_key }} = {{ join.outer_key}}{% endfor %}
{% endfor %}
''')

def concept_to_node(input:Concept)->str:
    return f'c~{input.name}'

def datasource_to_node(input:Datasource)->str:
    return f'ds~{input.identifier}'

def generate_graph(environment:Environment, targets:List[Concept])->nx.Graph:
    g = nx.Graph()
    for name, concept in environment.concepts.items():
        if concept not in targets:
            continue
        g.add_node(concept_to_node(concept))
    for key, dataset in environment.datasources.items():
        if not any([t2 in dataset.concepts for t2 in targets]):
            print(f'None of {targets} found in dataset with {dataset.concepts}')
            continue
        node = datasource_to_node(dataset)
        g.add_node(node)
        for concept in dataset.concepts:
            if concept not in targets:
                continue
            g.add_edge(node, concept_to_node(concept))
    return g

def select_base(datasets:List[Datasource])->Datasource:
    datasets.sort(key= lambda x: len(x.grain.components))
    return datasets[0]

def graph_to_dataset_graph(G):
    g = G.copy()

    while any(degree==2 for _, degree in g.degree):
        g0 = g.copy()
        for node, degree in g.degree():
            if degree==2:

                if g.is_directed(): #<-for directed graphs
                    a0,b0 = list(g0.in_edges(node))[0]
                    a1,b1 = list(g0.out_edges(node))[0]

                else:
                    edges = g0.edges(node)
                    edges = list(edges.__iter__())
                    a0,b0 = edges[0]
                    a1,b1 = edges[1]

                e0 = a0 if a0!=node else b0
                e1 = a1 if a1!=node else b1

                g0.remove_node(node)
                g0.add_edge(e0, e1)
        g = g0
    return g

def generate_joins(g:nx.Graph, datasources:List[Datasource]):

    pass

def graph_to_query(environment:Environment, g:nx.Graph,  statement:Select)->str:
    # select base table
    # populate joins
    # populate columns
    datasets = [v for key, v in environment.datasources.items() if datasource_to_node(v) in g.nodes()]
    print(datasets)
    select_items:List[SelectItem] = statement.selection
    output_items = statement.output_components

    select_columns = []
    grain = [v for v in output_items if v.purpose == Purpose.KEY]
    for item in select_items:
        if isinstance(item.content, Concept):
            select_columns.append(item.content.name)
        elif isinstance(item.content, SelectTransform):
            arguments = ','.join([v.name for v in item.content.function.arguments])
            rendered = f'{item.content.function.operator}({arguments}) as {item.content.output.name}'
            select_columns.append(rendered)
    base = select_base(datasets)
    joins = []
    return SQL_TEMPLATE.render(select_columns = select_columns, base = base, joins = [], grain=grain )

class BigqueryDialect(BaseDialect):

    def compile_sql(self, environment:Environment, statements)->List[str]:
        output = []
        for statement in statements:
            if isinstance(statement, Select):
                graph = generate_graph(environment, statement.input_components)
                output.append(graph_to_query(environment, graph, statement))
        return output
