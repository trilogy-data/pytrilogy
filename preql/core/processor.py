from preql.core.models import Concept, Environment, Select, Datasource, SelectItem, ConceptTransform, CTE, Join, JoinKey, \
    ProcessedQuery
from preql.core.enums import Purpose, JoinType
from typing import Dict, List, Optional
import networkx as nx
from jinja2 import Template


def concept_to_node(input: Concept) -> str:
    return f'c~{input.name}'


def datasource_to_node(input: Datasource) -> str:
    return f'ds~{input.identifier}'


def add_concept_node(g, concept:Concept):
    g.add_node(concept_to_node(concept), type=concept.purpose.value, concept=concept)


def generate_graph(environment: Environment, inputs: List[Concept], outputs: List[Concept]) -> nx.DiGraph:
    g = nx.DiGraph()
    for name, concept in environment.concepts.items():

        if concept not in inputs and concept not in outputs:
            continue
        add_concept_node(g, concept)

        # if we have sources, recursively add them
        if concept.sources:
            node_name = concept_to_node(concept)
            for source in concept.sources:
                add_concept_node(g, source)
                g.add_edge(concept_to_node(source), node_name)

    for key, dataset in environment.datasources.items():
        if not any([t2 in dataset.concepts for t2 in inputs]):
            continue
        node = datasource_to_node(dataset)
        g.add_node(node, type='datasource', datasource=dataset)
        for concept in dataset.concepts:
            g.add_edge(node, concept_to_node(concept))
    return g


def select_base(datasets: List[Datasource]) -> Datasource:
    datasets.sort(key=lambda x: len(x.grain.components))
    return datasets[0]


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


def render_concept_sql(c: Concept, datasource: Datasource, alias: bool = True) -> str:
    if not c.lineage:
        rval = f'{datasource.get_alias(c)}'
    else:
        args = ','.join([render_concept_sql(v, datasource, alias=False) for v in c.lineage.arguments])
        rval = f'{c.lineage.operator}({args})'
    if alias:
        return rval + f' as {c.name}'
    return rval


def dataset_to_grain(g: nx.Graph, datasources: List[Datasource], grain: List[Concept],
                     output_concepts: List[Concept]) -> List[CTE]:
    ''' for each subgraph that needs to get the target grain
    create a subquery'''
    output = []
    found = []
    for datasource in datasources:
        related = get_relevant_dataset_concepts(g, datasource, grain, output_concepts)
        found += related
        new = CTE(source=datasource, output_columns=related, name=datasource.identifier, grain=grain,
                  # TODO: support CTEs with different grain/aggregation
                  base=datasource.grain.components == grain,
                  group_to_grain = datasource.grain.components != grain)
        output.append(new)
    found_set = set([c.name for c in found])
    all_set = set([c.name for c in output_concepts])
    if not found_set == all_set:

        raise ValueError(f'Not all concepts could be queried: missing {all_set.difference(found_set)}')
    return output


def graph_to_query(environment: Environment, g: nx.Graph, statement: Select) -> ProcessedQuery:
    # select base table
    # populate joins
    # populate columns
    datasets = [v for key, v in environment.datasources.items() if datasource_to_node(v) in g.nodes()]
    output_items = statement.output_components
    grain = [v for v in output_items if v.purpose == Purpose.KEY]
    inputs = dataset_to_grain(g, datasets, statement.grain, statement.output_components)

    base = [d for d in inputs if d.base == True][0]
    other = [d for d in inputs if d != base]

    joins = [Join(left_cte=base, right_cte=k, joinkeys=
    [JoinKey(inner=f'{base.name}.{g.name}', outer=f'{k.name}.{g.name}') for g in grain],
                  jointype=JoinType.INNER) for k in other

             ]

    return ProcessedQuery(ctes=inputs, joins=joins, grain=statement.grain, limit=statement.limit, order_by = statement.order_by)
