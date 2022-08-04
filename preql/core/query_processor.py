import uuid
from typing import List, Optional, Union

import networkx as nx

from preql.constants import logger
from preql.core.enums import Purpose
from preql.core.hooks import BaseProcessingHook
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
    JoinedDataSource,
    JoinType,
    Address

)
from preql.utility import string_to_hash
from preql.core.graph_models import ReferenceGraph, concept_to_node, datasource_to_node


#
#
# def add_concept_node(g, concept: Concept):
#     g.add_node(concept_to_node(concept), type=concept.purpose.value, concept=concept)
#
#




def get_datasource_from_direct_select(
        concept: Concept, grain: Grain, environment: Environment, g: ReferenceGraph
) -> Datasource:
    for datasource in environment.datasources.values():
        if not datasource.grain == grain:
            continue
        try:
            path = nx.shortest_path(
                g,
                source=datasource_to_node(datasource),
                target=concept_to_node(concept),
            )
        except nx.exception.NetworkXNoPath as e:
            continue
        if len([p for p in path if g.nodes[p]["type"] == "datasource"]) == 1:
            return datasource
    raise ValueError(f'No direct select for {concept}')

def get_datasource_from_group_select(
        concept: Concept, grain: Grain, environment: Environment, g: ReferenceGraph
) -> Datasource:
    for datasource in environment.datasources.values():
        if not grain.issubset(datasource.grain):
            continue
        try:
            path = nx.shortest_path(
                g,
                source=datasource_to_node(datasource),
                target=concept_to_node(concept),
            )
        except nx.exception.NetworkXNoPath as e:
            continue
        if len([p for p in path if g.nodes[p]["type"] == "datasource"]) == 1:
            return datasource
    raise ValueError(f'No grouped select for {concept}')

from typing import Tuple

def parse_path_to_matches(input: List[str])->List[Tuple[str, str, List[str]]]:
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

def path_to_joins(input: List[str], g: ReferenceGraph, environment: Environment, query_graph: ReferenceGraph) ->List[
    Join]:
    ''' Build joins and ensure any required CTEs are also created/tracked'''
    out = []
    zipped = parse_path_to_matches(input)
    for row in zipped:
        left_ds, right_ds, concepts = row
        concepts = [g.nodes[concept]['concept'] for concept in concepts]
        left_value = g.nodes[left_ds]['datasource']
        for concept in concepts:
            build_select_upstream(concept.with_grain(left_value.grain), output_grain=left_value.grain, environment=environment, g=g,
                                  query_graph=query_graph, datasource=left_value)
        left_cte = node_to_cte(left_ds, query_graph)
        if not right_ds:
            continue
        right_value = g.nodes[right_ds]['datasource']
        for concept in concepts:
            build_select_upstream(concept.with_grain(right_value.grain), output_grain=right_value.grain, environment=environment, g=g,
                                  query_graph=query_graph, datasource=right_value)
        right_cte = node_to_cte(right_ds, query_graph)
        out.append(Join(left_cte=left_cte, right_cte=right_cte, jointype=JoinType.LEFT_OUTER,
                           joinkeys=[JoinKey(concept) for concept in concepts]))
    return out


def get_datasource_by_joins(
        concept: Concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph
) -> JoinedDataSource:
    join_candidates = []
    all_requirements = [concept] + grain.components

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
    source_map = {}
    join_paths = []
    ctes = []
    parents = []
    for key, value in shortest['paths'].items():
        record = [*value]
        datasource_nodes = [v for v in value if v.startswith('ds~')]
        root = datasource_nodes[-1]
        source_concept = g.nodes[value[-1]]['concept']
        parents.append(source_concept)

        new_joins = path_to_joins(value, environment=environment, g=g, query_graph=query_graph)

        join_paths += new_joins

        root_cte = node_to_cte(root, query_graph)
        # ctes = {**ctes, **{n:node_to_datasource(n, environment=environment) for n in datasource_nodes} }


        source_map[source_concept.address] = root_cte
        ctes.append(root_cte)
        # build_select_upstream(concept, output_grain=source_concept.grain, environment=environment, g=g,
        #                       query_graph=query_graph)

    # for key, value in datasource.source_map.items():
    #     source_concept = environment.concepts[key]

    output = JoinedDataSource(concepts=all_requirements,
                              source_map=source_map,
                              grain=grain,
                              address=Address(location=ctes[0].name),
                              joins=join_paths, )
    for parent in parents:
        query_graph.add_edge(parent, f'select_join_group_{output.identifier}')
    return output


def get_datasource_by_concept_and_grain(
        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph: ReferenceGraph
) -> Union[Datasource, JoinedDataSource]:
    try:
        return get_datasource_from_direct_select(concept, grain, environment, g)
    except ValueError as e:
        logger.error(e)
    try:
        return get_datasource_from_group_select(concept, grain, environment, g)
    except ValueError as e:
        logger.error(e)
    try:
        return get_datasource_by_joins(concept, grain, environment, g, query_graph)
    except ValueError as e:
        raise e
        logger.error(e)

    neighbors = list(g.predecessors(concept_to_node(concept)))
    raise ValueError(
        f"No source for {concept} found, neighbors {neighbors}"
    )


def get_operation_node(op_type: str) -> str:
    return f"{op_type}_{uuid.uuid4()}"


def build_derivation_upstream(concept: Concept, query_graph: ReferenceGraph, virtual: bool):
    for source in concept.sources:
        with_grain = source.with_grain(concept.grain)
        query_graph.add_node(
            with_grain, virtual=virtual
        )
        query_graph.add_edge(with_grain, concept)


"""    group_node = f'group_to_grain_{concept.grain}'
    query_graph.add_node(group_node)
    query_graph.add_edge(group_node, concept)
    for source in concept.sources:
        query_graph.add_node(
            source, virtual=virtual
        )
        query_graph.add_edge(source, group_node)
    for source in concept.grain.components:
        query_graph.add_node(
            source, virtual=virtual
        )
        query_graph.add_edge(source, group_node)"""


def build_select_upstream(concept: Concept, output_grain: Grain, environment, g, query_graph: ReferenceGraph,
                          datasource: Optional[Datasource] = None) -> str:
    # if we are providing a datasource
    # assume a direct selection is possible
    datasource = datasource or get_datasource_by_concept_and_grain(
        concept, output_grain, environment, g, query_graph
    )
    if concept.grain == datasource.grain:
        operator = f"select_{datasource.identifier}"
    elif concept.grain.issubset(datasource.grain):
        operator = f"select_group_{datasource.identifier}"
    else:
        raise ValueError(f"No way to get to node {concept} versus {datasource.grain}")

    query_graph.add_node(operator, grain=output_grain)
    query_graph.add_node(concept)
    query_graph.add_edge(operator, concept)
    query_graph.add_node(datasource)
    query_graph.add_edge(datasource, operator)
    for dsconcept in datasource.concepts:
        if dsconcept in output_grain.components:
            query_graph.add_node(dsconcept)
            query_graph.add_edge(operator, dsconcept)
    return operator


def build_component_upstream(concept: Concept, output_grain: Grain, environment, g, query_graph: ReferenceGraph):
    print(
        f"select node {concept.grain} to {concept.grain} as subset of {output_grain}"
    )
    datasource = get_datasource_by_concept_and_grain(
        concept, concept.grain, environment, g, query_graph
    )
    print(f"can fetch from {datasource.identifier}")
    if concept.purpose == Purpose.METRIC:
        operator = f"select_group_{datasource.identifier}"
    else:
        operator = f"select_{datasource.identifier}"
    query_graph.add_node(operator, grain=datasource.grain)
    query_graph.add_edge(operator, concept)
    query_graph.add_node(datasource)
    query_graph.add_edge(datasource, operator)
    for dsconcept in datasource.concepts:
        if dsconcept in output_grain.components:
            query_graph.add_node(dsconcept, )
            query_graph.add_edge(operator, dsconcept)


def build_superset_upstream(concept: Concept, output_grain: Grain, environment, g, query_graph: ReferenceGraph):
    datasource = get_datasource_by_concept_and_grain(
        concept, concept.grain, environment, g, query_graph
    )

    operator = f"group_{datasource.identifier}"
    query_graph.add_node(operator, grain=output_grain)
    query_graph.add_edge(operator, concept)
    ds_label = datasource_to_node(datasource)
    query_graph.add_node(ds_label, ds=datasource)
    query_graph.add_edge(ds_label, operator)
    for dsconcept in datasource.concepts:
        if dsconcept in output_grain.components:
            query_graph.add_node(dsconcept )
            query_graph.add_edge(operator, dsconcept)


def build_upstream_join_required(concept: Concept, output_grain: Grain, environment, g: ReferenceGraph, query_graph: ReferenceGraph):
    logger.debug(f"group node {concept.grain} to {output_grain}")
    datasource = get_datasource_by_concept_and_grain(
        concept, concept.grain + output_grain, environment, g, query_graph
    )
    if isinstance(datasource, JoinedDataSource):
        operator = f'select_join_group_{datasource.identifier}'

    else:
        operator = f"select_group_{datasource.identifier}"
    query_graph.add_node(operator, grain=output_grain)
    query_graph.add_edge(operator, concept.with_grain(output_grain))
    for item in output_grain.components:
        query_graph.add_node(item)
        query_graph.add_edge(operator, item)
    query_graph.add_node(datasource)
    query_graph.add_edge(datasource, operator)


def build_upstream(
        concept: Concept,  # input_grain:Grain,
        output_grain: Grain,
        environment: Environment,
        g: ReferenceGraph,
        query_graph: ReferenceGraph,
        virtual: bool = False,
):
    logger.info(
        f"building sources for {concept.name} {concept.grain} output grain {output_grain}"
    )
    query_graph.add_node(concept, virtual=virtual)
    if concept.sources:
        # the concept is derived from other measures; don't try to look for a direct source
        # TODO: look at materialized sources
        build_derivation_upstream(concept, query_graph, virtual)
    elif concept.grain == output_grain:
        # the concept is at the same grain as the output
        # we can select it from any available sources
        build_select_upstream(concept, output_grain, environment, g, query_graph)
    elif concept.grain.issubset(output_grain):
        # the concept is a component of the final grain
        # it can likely be joined directly without aggregation
        build_component_upstream(concept, output_grain, environment, g, query_graph)
    elif output_grain.issubset(concept.grain):
        # the concept grain is more granular than the final grain
        # the concept will have to be grouped/aggregated
        build_superset_upstream(concept, output_grain, environment, g, query_graph)
    elif concept.grain.isdisjoint(output_grain):
        # the concept has no relation to the output grain
        # find a way to join to get to the output grain
        build_upstream_join_required(concept, output_grain, environment, g, query_graph)
    else:
        # getting this concept to the output query
        # is impossible
        raise ValueError(
            f"There is no way to build this query. Concept {concept.name} grain {concept.grain} cannot be joined/aggreggated to target_grain {output_grain}"
        )

    for item in concept.sources:
        build_upstream(
            item,
            output_grain,
            environment=environment,
            g=g,
            query_graph=query_graph,
            virtual=True,
        )


def node_to_cte(input_node: str, G):
    if input_node.startswith('ds~'):
        input_node = [n for n in G.successors(input_node) if n.startswith('select_')][0]
    grain = G.nodes[input_node]['grain']
    _source = [node for node in G.predecessors(input_node) if node.startswith("ds~")][0]
    datasource: Datasource = G.nodes[_source]["ds"]
    outputs = [
        node
        for node in nx.descendants(G, input_node)
        if node.startswith("c~") and not G.nodes[node].get("virtual", False)
           # downstream is only not divided
           and not any([node.startswith('select_') for node in nx.shortest_path(G, input_node, node)[1:]])
    ]
    print(outputs)
    outputs = [
        G.nodes[node]["concept"]
        for node in nx.descendants(G, input_node)
        if node.startswith("c~") and not G.nodes[node].get("virtual", False)
           # downstream is only not divided
           and not any([node.startswith('select_') for node in nx.shortest_path(G, input_node, node)[1:]])
    ]
    grain = Grain(
        components=[
            component
            for component in outputs
            if component in grain.components
        ]
    )
    # use a unique identifier for each CTE/grain
    if input_node.startswith("select_group"):
        group_to_grain = True
        int_id = string_to_hash(datasource.identifier + "group")
    elif any([c.purpose == Purpose.METRIC for c in outputs]):
        group_to_grain = True
        int_id = string_to_hash(datasource.identifier)
    else:
        group_to_grain = False
        int_id = string_to_hash(datasource.identifier)

    return CTE(
        name=f"{input_node}_{int_id}",
        source=datasource,
        # output columns are what are selected/grouped by
        output_columns=outputs,
        # related columns include all referenced columns, such as filtering
        related_columns=datasource.concepts,
        grain=grain,
        base=True
        if all([component in outputs for component in grain.components])
        else False,
        group_to_grain=group_to_grain, )


def walk_query_graph(input: str, G, query_grain: Grain, seen, index: int = 1) -> List[CTE]:
    '''Traverse the graph and turn every select node into a CTE.
    '''
    output = []
    if input in seen:
        return output
    seen.add(input)
    for node in G.predecessors(input):
        output += walk_query_graph(node, G, query_grain, index=index, seen=seen)
    if input.startswith("select_"):
        _source = [node for node in G.predecessors(input) if node.startswith("ds~")][0]
        datasource: Datasource = G.nodes[_source]["ds"]
        outputs = [
            G.nodes[node]["concept"]
            for node in nx.descendants(G, input)
            if node.startswith("c~") and not G.nodes[node].get("virtual", False)
               # downstream is only not divided
               and not any([node.startswith('select_') for node in nx.shortest_path(G, input, node)[1:]])
        ]
        grain = Grain(
            components=[
                component
                for component in outputs
                if component in query_grain.components
            ]
        )
        # use a unique identifier for each CTE/grain
        if input.startswith("select_group"):
            group_to_grain = True
            int_id = string_to_hash(datasource.identifier + "group")
        elif any([c.purpose == Purpose.METRIC for c in outputs]):
            group_to_grain = True
            int_id = string_to_hash(datasource.identifier)
        else:
            group_to_grain = False
            int_id = string_to_hash(datasource.identifier)

        output.append(
            CTE(
                name=f"{input}_{int_id}",
                source=datasource,
                # output columns are what are selected/grouped by
                output_columns=outputs,
                # related columns include all referenced columns, such as filtering
                related_columns=datasource.concepts,
                grain=grain,
                base=True
                if all([component in outputs for component in query_grain.components])
                else False,
                group_to_grain=group_to_grain,
            )
        )

    for node in G.successors(input):
        output += walk_query_graph(node, G, query_grain, index=index, seen=seen)
    return output


def build_execution_graph(statement: Select, environment: Environment,
                          relation_graph: ReferenceGraph) -> ReferenceGraph:
    query_graph = ReferenceGraph()
    root = get_operation_node("output")
    query_graph.add_node(root)
    for concept in statement.output_components + statement.grain.components:
        query_graph.add_node(concept, virtual=False)
        query_graph.add_edge(concept, root)
        build_upstream(concept, statement.grain, environment, relation_graph, query_graph)
    return query_graph


def process_query(environment: Environment, statement: Select,
                  hooks: Optional[List[BaseProcessingHook]] = None) -> ProcessedQuery:
    '''Turn the raw query input into an instantiated execution tree.'''
    graph = generate_graph(environment)

    query_graph = build_execution_graph(statement, environment=environment, relation_graph=graph)

    # run lifecycle hooks
    # typically for logging
    hooks = hooks or []
    for hook in hooks:
        hook.query_graph_built(query_graph)
    starts = [
        n for n in query_graph.nodes if query_graph.in_degree(n) == 0
    ]  # type: ignore
    ctes = []
    seen = set()
    for node in starts:
        ctes += walk_query_graph(node, query_graph, statement.grain, seen=seen)
    joins = []
    for cte in ctes:
        print(cte.name)
        print(cte.grain)
    base = [cte for cte in ctes if cte.base][0]
    others = [cte for cte in ctes if cte != base]
    print(seen)
    print('----------')
    for value in ctes:
        print(value.name)
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
