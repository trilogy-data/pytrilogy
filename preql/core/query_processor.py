from typing import List, Optional, Union

import networkx as nx

from preql.constants import logger
from preql.core.env_processor import generate_graph
from preql.core.graph_models import ReferenceGraph, concept_to_node, datasource_to_node
from preql.core.hooks import BaseProcessingHook
from preql.utility import unique
from preql.core.models import (
    Concept,
    Environment,
    Select,
    Datasource,
    CTE,
    Join,
    JoinKey,
    ProcessedQuery,
    Grain,
    QueryDatasource,
    JoinType,
    BaseJoin

)
from preql.utility import string_to_hash

from collections import defaultdict


def concept_to_inputs(concept:Concept)->List[Concept]:
    output = []
    if not concept.lineage:
        return [concept]
    for source in concept.sources:
        # with_grain = source.with_grain(concept.grain)
        output+= concept_to_inputs(source)
    return output


def get_datasource_from_direct_select(
        concept: Concept, grain: Grain, environment: Environment, g: ReferenceGraph
) -> QueryDatasource:
    all_concepts = concept_to_inputs(concept)

    for datasource in environment.datasources.values():
        if not datasource.grain == grain:
            continue
        all_found = True
        for req_concept in all_concepts:
            try:
                path = nx.shortest_path(
                    g,
                    source=datasource_to_node(datasource),
                    target=concept_to_node(req_concept),
                )
            except nx.exception.NetworkXNoPath as e:
                all_found = False
                break
            if len([p for p in path if g.nodes[p]["type"] == "datasource"]) != 1:
                all_found = False
                break
        if all_found:
            return QueryDatasource(output_concepts=[concept],
                                   input_concepts = all_concepts,
                                   source_map={concept.name: {datasource} for concept in all_concepts},
                                   datasources=[datasource],
                                   grain=grain, joins=[])
    raise ValueError(f'No direct select for {concept}')


def get_datasource_from_group_select(
        concept: Concept, grain: Grain, environment: Environment, g: ReferenceGraph
) -> QueryDatasource:
    all_concepts = concept_to_inputs(concept) + grain.components
    for datasource in environment.datasources.values():
        all_found = True
        for req_concept in all_concepts:
            try:
                path = nx.shortest_path(
                    g,
                    source=datasource_to_node(datasource),
                    target=concept_to_node(req_concept),
                )
            except nx.exception.NetworkXNoPath as e:
                all_found = False
                break
            if len([p for p in path if g.nodes[p]["type"] == "datasource"]) != 1:
                all_found = False
                break
        if all_found:
            return QueryDatasource(
                input_concepts = all_concepts,
                output_concepts=[concept],
                   source_map={concept.name: {datasource} for concept in all_concepts},
                datasources=[datasource],
                   grain=grain, joins=[])
    raise ValueError(f'No grouped select for {concept}')


from typing import Tuple


def parse_path_to_matches(input: List[str]) -> List[Tuple[str, str, List[str]]]:
    left_ds = None
    right_ds = None
    concept = None
    output = []
    while input:
        ds = None
        next = input.pop(0)
        if next.startswith('ds~'):
            ds = next
        elif next.startswith('c~'):
            concept = next
        if ds and not left_ds:
            left_ds = ds
            continue
        elif ds:
            right_ds = ds
            output.append((left_ds, right_ds, [concept]))
            left_ds = right_ds
            concept = None
    if left_ds and not right_ds:
        output.append([left_ds, None, [concept]])
    return output


def path_to_joins(input: List[str], g: ReferenceGraph) -> List[
    BaseJoin]:
    ''' Build joins and ensure any required CTEs are also created/tracked'''
    out = []
    zipped = parse_path_to_matches(input)
    for row in zipped:
        left_ds, right_ds, concepts = row
        concepts = [g.nodes[concept]['concept'] for concept in concepts]
        left_value = g.nodes[left_ds]['datasource']
        if not right_ds:
            continue
        right_value = g.nodes[right_ds]['datasource']
        out.append(BaseJoin(left_datasource=left_value, right_datasource=right_value, join_type=JoinType.LEFT_OUTER,
                            concepts=concepts))
    return out


def get_datasource_by_joins(
        concept: Concept, grain: Grain, environment: Environment, g: ReferenceGraph,  # query_graph: ReferenceGraph
) -> QueryDatasource:
    join_candidates = []
    all_requirements = concept_to_inputs(concept) + grain.components

    for datasource in environment.datasources.values():
        all_found = True
        paths = {}
        for item in all_requirements:
            try:
                path = nx.shortest_path(
                    g,
                    source=datasource_to_node(datasource),
                    target=concept_to_node(item),
                )
                paths[concept_to_node(item)] = path
                print(f'found path to {item}')
            except nx.exception.NetworkXNoPath as e:
                all_found = False
                print(f'not found path to {item}')
                continue
        if all_found:
            join_candidates.append({'paths': paths, 'datasource': datasource})
    join_candidates.sort(key=lambda x: sum([len(v) for v in x['paths'].values()]))
    if not join_candidates:
        raise ValueError(f'No joins to get to {concept}')
    shortest = join_candidates[0]
    source_map = defaultdict(set)
    join_paths = []
    parents = []
    all_datasets = set()
    all_concepts = set()
    for key, value in shortest['paths'].items():
        datasource_nodes = [v for v in value if v.startswith('ds~')]
        concept_nodes = [v for v in value if v.startswith('c~')]
        all_datasets = all_datasets.union(set(datasource_nodes))
        all_concepts = all_concepts.union(set(concept_nodes))
        root = datasource_nodes[-1]
        source_concept = g.nodes[value[-1]]['concept']
        parents.append(source_concept)

        new_joins = path_to_joins(value, g=g)

        join_paths += new_joins
        source_map[source_concept.address].add(g.nodes[root]['datasource'])
        # ensure we add in all keys required for joins as inputs
        # even if they are not selected out
        for join in new_joins:
            for jconcept in join.concepts:
                source_map[jconcept.address].add(join.left_datasource)
                source_map[jconcept.address].add(join.right_datasource)
                all_requirements.append(jconcept)
    all_requirements = unique(all_requirements, 'address')
    output = QueryDatasource(output_concepts=[concept] + grain.components,
                             input_concepts= all_requirements,
                             source_map=source_map,
                             grain=grain,
                             datasources = sorted([g.nodes[key]['datasource'] for key in all_datasets], key=lambda x: x.identifier),
                             joins=join_paths)
    return output


def get_datasource_by_concept_and_grain(
        concept, grain: Grain, environment: Environment, g: ReferenceGraph
) -> Union[Datasource, QueryDatasource]:
    '''Determine if it's possible to get a certain concept at a certain grain.
    '''
    try:
        return get_datasource_from_direct_select(concept, grain, environment, g)
    except ValueError as e:
        logger.error(e)
    try:
        return get_datasource_from_group_select(concept, grain, environment, g)
    except ValueError as e:
        logger.error(e)
    try:
        return get_datasource_by_joins(concept, grain, environment, g)
    except ValueError as e:
        logger.error(e)

    neighbors = list(g.predecessors(concept_to_node(concept)))
    raise ValueError(
        f"No source for {concept} found, neighbors {neighbors}"
    )

def base_join_to_join(base_join:BaseJoin, ctes:List[CTE])-> Join:
    left_cte = [cte for cte in ctes if cte.source.datasources[0]== base_join.left_datasource][0]
    right_cte = [cte for cte in ctes if cte.source.datasources[0] == base_join.right_datasource][0]

    return Join(left_cte=left_cte, right_cte = right_cte, joinkeys=[JoinKey(concept=concept) for concept in base_join.concepts], jointype = base_join.join_type)

def datasource_to_ctes(query_datasource: QueryDatasource) -> List[CTE]:
    int_id = string_to_hash(query_datasource.identifier)
    group_to_grain = False if sum([ds.grain for ds in query_datasource.datasources]) == query_datasource.grain else True
    output = []
    if len(query_datasource.datasources) > 1:
        print('SUB LOOP DEBUG')
        source_map = {}
        for datasource in query_datasource.datasources:
            print('AFTER')
            sub_select = {key: item for key, item in query_datasource.source_map.items() if datasource in item}
            concepts = [c for c in datasource.concepts if c.address in sub_select.keys()]
            concepts = unique(concepts, 'address')
            sub_datasource = QueryDatasource(
                output_concepts=concepts,
                input_concepts=concepts,
                source_map=sub_select,
                grain=datasource.grain,
                datasources = [datasource],
                joins=[]
            )
            sub_cte = datasource_to_ctes(sub_datasource)
            output += sub_cte
            for cte in sub_cte:
                for value in cte.output_columns:
                    source_map[value.address] = cte.name

            print(datasource.name)
            print([str(c) for c in concepts])
            print(source_map)
    else:
        source = query_datasource.datasources[0]
        source_map = {concept.address: source.identifier for concept in query_datasource.output_concepts}
    human_id = query_datasource.identifier.replace('<', '').replace('>', '').replace(',', '_')
    output.append(CTE(
        name=f"cte_{human_id}_{int_id}",
        source=query_datasource,
        # output columns are what are selected/grouped by
        output_columns=[c.with_grain(query_datasource.grain) for c in query_datasource.output_concepts],
        source_map=source_map,
        # related columns include all referenced columns, such as filtering
        # related_columns=datasource.concepts,
        joins = [base_join_to_join(join, output) for join in query_datasource.joins],
        related_columns=query_datasource.input_concepts,
        grain=query_datasource.grain,
        group_to_grain=group_to_grain, ))
    return output


def get_query_datasources(environment: Environment, statement: Select, graph: ReferenceGraph):
    concept_map = defaultdict(list)
    datasource_map = {}
    for concept in statement.output_components + statement.grain.components:
        datasource = get_datasource_by_concept_and_grain(
            concept, statement.grain, environment, graph
        )
        concept_map[datasource.identifier].append(concept)
        datasource_map[datasource.identifier] = datasource
    return concept_map, datasource_map


def process_query(environment: Environment, statement: Select,
                  hooks: Optional[List[BaseProcessingHook]] = None) -> ProcessedQuery:
    '''Turn the raw query input into an instantiated execution tree.'''
    graph = generate_graph(environment)

    concepts, datasources = get_query_datasources(environment=environment, graph=graph,
                                                  statement=statement)
    ctes = []
    joins = []
    for datasource in datasources.values():
        ctes += datasource_to_ctes(datasource)
    for cte in ctes:
        print(str(cte)[:100])
    base = [cte for cte in ctes if cte.grain == statement.grain][0]
    others = [cte for cte in ctes if cte != base]
    for cte in others:
        joinkeys = [
            JoinKey(c)
            for c in statement.grain.components
            if c in cte.output_columns
        ]
        if joinkeys:
            joins.append(
                Join(
                    left_cte=base,
                    right_cte=cte,
                    joinkeys=joinkeys,
                    jointype=JoinType.LEFT_OUTER,
                )
            )
    return ProcessedQuery(
        order_by=statement.order_by,
        grain=statement.grain,
        limit=statement.limit,
        where_clause=statement.where_clause,
        output_columns=statement.output_components,
        ctes=ctes,
        joins=joins,
    )
