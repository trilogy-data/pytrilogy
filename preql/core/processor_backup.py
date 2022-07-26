from typing import List

import networkx as nx

from preql.utility import string_to_hash
from preql.constants import logger
from preql.core.enums import Purpose, JoinType
from preql.core.models import Concept, Environment, Select, Datasource, CTE, Join, \
    JoinKey, \
    ProcessedQuery, Grain


def concept_to_node(input: Concept) -> str:
    return f'c~{input.namespace}.{input.name}'


def datasource_to_node(input: Datasource) -> str:
    return f'ds~{input.namespace}.{input.identifier}'


def add_concept_node(g, concept: Concept):
    g.add_node(concept_to_node(concept), type=concept.purpose.value, concept=concept)


class ReferenceGraph(nx.DiGraph):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bases = []


def generate_graph(environment: Environment, statement: Select) -> ReferenceGraph:
    g = ReferenceGraph()
    # statement.input_components, statement.output_components
    for name, concept in environment.concepts.items():
        add_concept_node(g, concept)

        # if we have sources, recursively add them
        if concept.sources:
            node_name = concept_to_node(concept)
            for source in concept.sources:
                add_concept_node(g, source)
                g.add_edge(concept_to_node(source), node_name)
    for key, dataset in environment.datasources.items():
        node = datasource_to_node(dataset)
        g.add_node(node, type='datasource', datasource=dataset)
        for concept in dataset.concepts:
            g.add_edge(node, concept_to_node(concept))
    return g


def select_base(datasets: List[Datasource]) -> Datasource:
    datasets.sort(key=lambda x: len(x.grain.components))
    return datasets[0]


def get_relevant_dataset_concepts(g, datasource: Datasource, grain: Grain, output_concepts: List[Concept]) -> \
        List[Concept]:
    relevant: List[Concept] = []
    # TODO: handle joins to get to appropriate grain
    for concept in output_concepts + grain.components:
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


def dataset_to_grain(g: ReferenceGraph, datasources: List[Datasource], grain: Grain, all_concepts: List[Concept],
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
        grain_additions = [item for item in grain.components if item in related]
        output_columns = [i for i in related if i.name in [z.name for z in output_concepts + grain_additions]]
        new = CTE(source=datasource,
                  related_columns=related,
                  output_columns=output_columns,
                  name=datasource.identifier,
                  grain=Grain(components=grain_additions),
                  # TODO: support CTEs with different grain/aggregation
                  base=Grain(components=grain_additions) == grain,
                  group_to_grain=(
                          (not (datasource.grain.issubset(grain) or datasource.grain == grain)) or any(
                      [c.purpose == Purpose.METRIC for c in output_columns])))
        output.append(new)
    found_set = set([c.name for c in found])
    all_set = set([c.name for c in all_concepts])
    if not found_set == all_set:
        raise ValueError(f'Not all target concepts {all_set} could be queried: missing {all_set.difference(found_set)}')
    return output


def graph_to_query(environment: Environment, g: ReferenceGraph, statement: Select) -> ProcessedQuery:
    datasets = [v for key, v in environment.datasources.items() if datasource_to_node(v) in g.nodes()]
    grain = statement.grain
    inputs = dataset_to_grain(g, datasets, statement.grain, statement.all_components, statement.output_components)

    if statement.grain.components:
        possible_bases = [d for d in inputs if d.base]
    else:
        possible_bases = [d for d in inputs]
    if not possible_bases:
        raise ValueError(f'No valid base source found for desired grain {statement.grain}')
    base = possible_bases[0]
    other = [d for d in inputs if d != base]
    joins = []
    for cte in other:
        joins.append(Join(left_cte=base, right_cte=cte, joinkeys=
        [JoinKey(inner=g, outer=g) for g in cte.grain.intersection(grain).components],
                          jointype=JoinType.INNER)

                     )

    return ProcessedQuery(output_columns=statement.output_components, ctes=inputs, joins=joins, grain=statement.grain,
                          where_clause=statement.where_clause,
                          limit=statement.limit, order_by=statement.order_by)


def get_datasource_by_concept_and_grain(concept, grain: Grain, environment: Environment, g: ReferenceGraph):
    for datasource in environment.datasources.values():
        if not datasource.grain == grain:
            continue
        try:
            path = nx.shortest_path(g, source=datasource_to_node(datasource), target=concept_to_node(concept))
        except nx.exception.NetworkXNoPath as e:
            continue
        if len([p for p in path if g.nodes[p]['type'] == 'datasource']) == 1:
            return datasource
    for datasource in environment.datasources.values():
        all_found = True
        for item in grain.components:
            try:
                path = nx.shortest_path(g, source=datasource_to_node(datasource), target=concept_to_node(item))
            except nx.exception.NetworkXNoPath as e:
                all_found = False
                continue
            if not len([p for p in path if g.nodes[p]['type'] == 'datasource']) == 1:
                all_found = False
        if all_found:
            return datasource
    neighbors = list(g.predecessors(concept_to_node(concept)))
    raise ValueError(f"No source for {concept_to_node(concept)} grain {grain} found, neighbors {neighbors}")


import uuid


def get_operation_node(op_type: str) -> str:
    return f'{op_type}_{uuid.uuid4()}'


def find_upstream(concept: Concept,  # input_grain:Grain,
                  output_grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: nx.DiGraph, virtual:bool = False):
    print(f'building sources for {concept.name}')
    cnode = concept_to_node(concept)
    query_graph.add_node(cnode, concept=concept, virtual= virtual)
    if concept.sources:
        print(f'transformation {concept.grain} to {output_grain}')
        operator = f'function-{cnode}'
        query_graph.add_node(operator)
        query_graph.add_edge(operator, cnode)
        for source in concept.sources:
            query_graph.add_edge(concept_to_node(source), operator)
    elif concept.grain == output_grain:
        print(f'select node {concept.grain} to {output_grain}')
        datasource = get_datasource_by_concept_and_grain(concept, output_grain, environment, g)
        print(f'can fetch from {datasource.identifier}')
        operator = f'select_{datasource.identifier}'
        query_graph.add_node(operator, grain=output_grain)
        query_graph.add_edge(operator, cnode)
        ds_label = datasource_to_node(datasource)
        query_graph.add_node(ds_label, ds=datasource)
        query_graph.add_edge(ds_label, operator)
    elif output_grain.issubset(concept.grain):
        print(f'group node {concept.grain} to {output_grain}')
        datasource = get_datasource_by_concept_and_grain(concept, concept.grain, environment, g)

        print(f'can group from {datasource.identifier}')
        operator = f'group_{datasource.identifier}'
        query_graph.add_node(operator, grain=output_grain)
        query_graph.add_edge(operator, cnode)
        ds_label = datasource_to_node(datasource)
        query_graph.add_node(ds_label, ds=datasource)
        query_graph.add_edge(ds_label, operator)
    elif concept.grain.isdisjoint(output_grain):
        print(f'group node {concept.grain} to {output_grain}')
        datasource = get_datasource_by_concept_and_grain(concept, concept.grain + output_grain, environment, g)
        print(f'can join from {datasource.identifier}')
        operator = f'select_group_{datasource.identifier}'
        query_graph.add_node(operator, grain=output_grain)
        query_graph.add_edge(operator, cnode)
        for item in output_grain.components:
            query_graph.add_node(concept_to_node(item), concept=item)
            query_graph.add_edge(operator, concept_to_node(item))
        ds_label = datasource_to_node(datasource)
        query_graph.add_node(ds_label, ds=datasource)
        query_graph.add_edge(ds_label, operator)
    else:
        raise ValueError('NO CONNECTION CAN BE MADE')

    for item in concept.sources:
        find_upstream(item, concept.grain, environment=environment, g=g, query_graph=query_graph,
                      virtual=True
                      )


def print_node(input: str, G, indentation: int = 1):
    if input.startswith('select-'):
        output = 'SELECT'
    else:
        output = input
    print('\t' * indentation + output)
    for node in G.predecessors(input):
        print_node(node, G, indentation + 1)


def walk_query_graph(input: str, G, index: int = 1, ) -> List[CTE]:
    from random import randint
    output = []

    if input.startswith('select_'):
        info = G.nodes[input]
        source = [node for node in G.predecessors(input) if node.startswith('ds~')][0]
        datasource: Datasource = G.nodes[source]['ds']
        if input.startswith('select_group'):
            group_to_grain = True
        else:
            group_to_grain = False
        outputs = [G.nodes[node]['concept'] for node in nx.descendants(G, source) if node.startswith('c~') and not G.nodes[node]['virtual']]
        output.append(
            CTE(
                name=f'{input}_{string_to_hash(input)}',
                source=datasource,
                # output columns are what are selected/grouped by
                output_columns=outputs,
                # related columns include all referenced columns, such as filtering
                related_columns=datasource.concepts,
                grain=info['grain'],
                base=True,
                group_to_grain=group_to_grain
            )
        )

    for node in G.successors(input):
        output += walk_query_graph(node, G, index)
    return output


def process_query(environment: Environment, statement: Select) -> ProcessedQuery:
    import matplotlib.pyplot as plt

    logger.debug(f'Statement grain is {statement.grain}')
    source_grains = set([str(ds.grain) for ds in environment.datasources.values()])
    logger.debug(f'Source grains are {source_grains}')
    # environment graph
    graph = generate_graph(environment, statement)

    # graph of query operations
    query_graph = nx.DiGraph()
    root = get_operation_node('output')
    query_graph.add_node(root)
    for concept in statement.output_components:
        cname = concept_to_node(concept)
        query_graph.add_node(cname, concept=concept, virtual = False)
        query_graph.add_edge(cname, root)
        find_upstream(concept, statement.grain, environment, graph, query_graph)
    nx.draw_spring(query_graph, with_labels=True)
    plt.show()
    print_node(root, query_graph)
    starts = [n for n in query_graph.nodes if query_graph.in_degree(n) == 0]
    ctes = []
    for node in starts:
        ctes += walk_query_graph(node, query_graph)

    joins = []
    base = ctes[0]
    for cte in ctes[1:]:
        joins.append(
            Join(
                left_cte=base,
                right_cte=cte,
                joinkeys = [JoinKey(c, c) for c in statement.grain.components],
                jointype = JoinType.INNER
            )
        )
    return ProcessedQuery(
        order_by=statement.order_by,
        grain=statement.grain,
        limit=statement.limit,
        where_clause=statement.where_clause,
        output_columns=statement.output_components,
        ctes=ctes,
        joins =joins

    )
