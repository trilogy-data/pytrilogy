from typing import List

import networkx as nx

from preql.core.enums import Purpose, JoinType
from preql.core.models import Concept, Environment, Select, Datasource, CTE, Join, \
    JoinKey, \
    ProcessedQuery, Grain


def concept_to_node(input: Concept) -> str:
    return f'c~{input.namespace}.{input.name}'


def datasource_to_node(input: Datasource) -> str:
    return f'ds~{input.identifier}'


def add_concept_node(g, concept: Concept):
    g.add_node(concept_to_node(concept), type=concept.purpose.value, concept=concept)


def generate_graph(environment: Environment, statement: Select) -> nx.DiGraph:
    g = nx.DiGraph()
    # statement.input_components, statement.output_components
    for name, concept in environment.concepts.items():
        if concept not in statement.all_components:
            continue
        add_concept_node(g, concept)

        # if we have sources, recursively add them
        if concept.sources:
            node_name = concept_to_node(concept)
            for source in concept.sources:
                add_concept_node(g, source)
                g.add_edge(concept_to_node(source), node_name)

    for key, dataset in environment.datasources.items():
        if not any([t2 in dataset.concepts for t2 in statement.input_components]):
            continue
        node = datasource_to_node(dataset)
        g.add_node(node, type='datasource', datasource=dataset)
        for concept in dataset.concepts:
            g.add_edge(node, concept_to_node(concept))
    # for z in g.nodes():
    #     print(z)
    # for ed in g.edges():
    #     print(ed)
    return g


def select_base(datasets: List[Datasource]) -> Datasource:
    datasets.sort(key=lambda x: len(x.grain.components))
    return datasets[0]


def get_relevant_dataset_concepts(g, datasource: Datasource, grain: Grain, output_concepts: List[Concept]) -> \
        List[Concept]:
    relevant: List[Concept] = []
    # TODO: handle joins to get to appropriate grain
    relevant += grain.components
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


def dataset_to_grain(g: nx.Graph, datasources: List[Datasource], grain: Grain, all_concepts: List[Concept],
                     output_concepts: List[Concept]) -> List[CTE]:
    ''' for each subgraph that needs to get the target grain
    create a subquery'''
    output = []
    found = []
    for datasource in datasources:
        found_set = set([c.name for c in found])
        all_set = set([c.name for c in all_concepts])
        if found_set == all_set:
            continue
        related = get_relevant_dataset_concepts(g, datasource, grain, all_concepts)
        found += related
        output_columns = [i for i in related if i.name in [z.name for z in output_concepts+grain.components]]
        new = CTE(source=datasource,
                  related_columns=related,
                  output_columns=output_columns,
                  name=datasource.identifier,
                  grain=grain,
                  # TODO: support CTEs with different grain/aggregation
                  base=datasource.grain == grain,
                  group_to_grain=(
                              datasource.grain != grain or any([c.purpose == Purpose.METRIC for c in output_columns])))
        output.append(new)
    found_set = set([c.name for c in found])
    all_set = set([c.name for c in all_concepts])
    if not found_set == all_set:
        raise ValueError(f'Not all target concepts {all_set} could be queried: missing {all_set.difference(found_set)}')
    return output


def graph_to_query(environment: Environment, g: nx.Graph, statement: Select) -> ProcessedQuery:
    datasets = [v for key, v in environment.datasources.items() if datasource_to_node(v) in g.nodes()]
    # for key, v in environment.datasources.items():
    #     print(key)
    #     print(v)
    grain = statement.grain
    inputs = dataset_to_grain(g, datasets, statement.grain, statement.all_components, statement.output_components)

    if statement.grain.components:
        possible_bases = [d for d in inputs if d.base == True]
    else:
        possible_bases = [d for d in inputs]
    if not possible_bases:
        raise ValueError(f'No valid base source found for desired grain {statement.grain}')
    base = possible_bases[0]
    other = [d for d in inputs if d != base]
    print('debug')
    print(len(other))
    for z in other:
        print(z.name)
    joins = [Join(left_cte=base, right_cte=k, joinkeys=
    [JoinKey(inner=g, outer=g) for g in grain.components],
                  jointype=JoinType.INNER) for k in other

             ]

    return ProcessedQuery(output_columns=statement.output_components, ctes=inputs, joins=joins, grain=statement.grain,
                          where_clause=statement.where_clause,
                          limit=statement.limit, order_by=statement.order_by)

# 3 cases
# grain of where clause matches grain of source
# move to source
# grain of where clause matches grain of output
# move to output
# grain of where clause does not match anyting
