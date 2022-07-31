import uuid
from typing import List, Optional, Union, Tuple

import networkx as nx
from preql.constants import logger

from preql.core.enums import Purpose, JoinType
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


def concept_to_node(input: Concept) -> str:
    # if input.purpose == Purpose.METRIC:
    #     return f"c~{input.namespace}.{input.name}@{input.grain}"
    return f"c~{input.namespace}.{input.name}@{input.grain}"


def datasource_to_node(input: Union[Datasource, JoinedDataSource]) -> str:
    if isinstance(input, JoinedDataSource):
        return 'ds~join~'+','.join([datasource_to_node(sub) for sub in input.datasources])
    return f"ds~{input.namespace}.{input.identifier}"

def node_to_datasource(input:str, environment:Environment)->Datasource:
    stripped = input.lstrip('ds~')
    namespace, title = stripped.split('.')
    if namespace == 'None':
        return environment.datasources[title]
    return environment.datasources[stripped]

def add_concept_node(g, concept: Concept):
    g.add_node(concept_to_node(concept), type=concept.purpose.value, concept=concept)


class ReferenceGraph(nx.DiGraph):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def add_node(self, node_for_adding, **attr):
        if isinstance(node_for_adding, Concept):
            node_name = concept_to_node(node_for_adding)
            attr['type'] = 'concept'
            attr['concept'] = node_for_adding
        elif isinstance(node_for_adding, Datasource):
            node_name = datasource_to_node(node_for_adding)
            attr['type'] = 'datasource'
            attr['ds'] = node_for_adding
        elif isinstance(node_for_adding, JoinedDataSource):
            node_name = datasource_to_node(node_for_adding)
            attr['type'] = 'joineddatasource'
            attr['ds'] = node_for_adding
        else:
            node_name = node_for_adding
        super().add_node(node_name, **attr)

    def add_edge(self, u_of_edge, v_of_edge, **attr):
        if isinstance(u_of_edge, Concept):
            u_of_edge = concept_to_node(u_of_edge)
        elif isinstance(u_of_edge, Datasource):
            u_of_edge = datasource_to_node(u_of_edge)
        elif isinstance(u_of_edge, JoinedDataSource):
            u_of_edge = datasource_to_node(u_of_edge)
        if isinstance(v_of_edge, Concept):
            v_of_edge = concept_to_node(v_of_edge)
        elif isinstance(v_of_edge, Datasource):
            v_of_edge = datasource_to_node(v_of_edge)
        elif isinstance(v_of_edge, JoinedDataSource):
            v_of_edge = datasource_to_node(v_of_edge)
        super().add_edge(u_of_edge, v_of_edge, **attr)


def generate_graph(environment: Environment, ) -> ReferenceGraph:
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
        g.add_node(node, type="datasource", datasource=dataset)
        for concept in dataset.concepts:
            g.add_edge(node, concept_to_node(concept))
            g.add_edge(concept_to_node(concept), node)
    return g


def get_datasource_from_direct_select(
        concept, grain: Grain, environment: Environment, g: ReferenceGraph
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
    raise ValueError


def path_to_joins(input:List[str], g:ReferenceGraph, environment:Environment, query_graph:ReferenceGraph)->List[Join]:
    left_value = None
    left_cte = None
    concept = None
    output = []
    while input:
        ds = None
        next = input.pop(0)
        if next.startswith('ds~'):
            ds =g.nodes[next]['datasource']
        elif next.startswith('c~'):
            concept = g.nodes[next]['concept']
        if ds and not left_value:
            left_value = ds
            build_select_upstream(concept, output_grain=concept.grain, environment=environment, g=g, query_graph=query_graph, datasource=left_value)
            left_cte = node_to_cte(next, query_graph)
            continue
        elif ds:
            right_value = ds
            build_select_upstream(concept, output_grain=concept.grain, environment=environment, g=g, query_graph=query_graph, datasource=right_value)
            right_cte = node_to_cte(next, query_graph)
            output.append(Join(left_cte=left_cte, right_cte = right_cte, jointype = JoinType.LEFT_OUTER,
                               joinkeys=[JoinKey(concept)]))
            left_value = None
            concept=None
    return output


def get_datasource_by_joins(
        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph:ReferenceGraph
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
            except nx.exception.NetworkXNoPath as e:
                all_found = False
                continue
        if all_found:
            join_candidates.append({'paths':paths, 'datasource': datasource})
    join_candidates.sort(key=lambda x: sum([len(v) for v in x['paths'].values()]))
    if not join_candidates:
        raise ValueError
    shortest = join_candidates[0]
    source_map = {}
    join_paths = []
    # ctes = {}
    parents = []
    for key, value in shortest['paths'].items():
        datasource_nodes = [v for v in value if v.startswith('ds~')]
        # ctes = {**ctes, **{n:node_to_datasource(n, environment=environment) for n in datasource_nodes} }
        source_concept = g.nodes[value[-1]]['concept']
        source_map[source_concept.address] = node_to_datasource(datasource_nodes[-1], environment=environment)
        build_select_upstream(concept, output_grain=source_concept.grain, environment=environment, g=g, query_graph=query_graph)
        join_paths+= path_to_joins(value, environment=environment, g=g, query_graph=query_graph)
        parents.append(source_concept)
    # for key, value in datasource.source_map.items():
    #     source_concept = environment.concepts[key]


    output = JoinedDataSource(concepts=all_requirements,
                            source_map=source_map,
                            grain=grain,
                            address = Address(location='test'),
                            joins = join_paths,)
    for parent in parents:
        query_graph.add_edge(parent, f'select_join_group_{output.identifier}')
    return output

def get_datasource_by_concept_and_grain(
        concept, grain: Grain, environment: Environment, g: ReferenceGraph, query_graph:ReferenceGraph
) -> Union[Datasource, JoinedDataSource]:
    try:
        return get_datasource_from_direct_select(concept, grain, environment, g)
    except ValueError:
        pass
    try:
        return get_datasource_by_joins(concept, grain, environment, g, query_graph)
    except ValueError:
        pass

    neighbors = list(g.predecessors(concept_to_node(concept)))
    raise ValueError(
        f"No source for {concept_to_node(concept)} grain {grain} found, neighbors {neighbors}"
    )



def get_operation_node(op_type: str) -> str:
    return f"{op_type}_{uuid.uuid4()}"

def build_derivation_upstream(concept:Concept, query_graph:ReferenceGraph, virtual:bool):
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
def build_select_upstream(concept:Concept, output_grain:Grain, environment, g, query_graph:ReferenceGraph, datasource:Optional[Datasource]=None)->str:
    datasource = datasource or get_datasource_by_concept_and_grain(
        concept, output_grain, environment, g, query_graph
    )
    if concept.grain == datasource.grain:
        operator = f"select_{datasource.identifier}"
    elif concept.grain.issubset(datasource.grain):
        operator = f"select_group_{datasource.identifier}"
    else:
        raise ValueError("No way to get to node")
    query_graph.add_node(operator, grain=output_grain)
    query_graph.add_edge(operator, concept)
    query_graph.add_node(datasource)
    query_graph.add_edge(datasource, operator)
    for dsconcept in datasource.concepts:
        if dsconcept in output_grain.components:
            query_graph.add_node(dsconcept, concept=dsconcept)
            query_graph.add_edge(operator, dsconcept)
    return operator

def build_component_upstream(concept:Concept, output_grain:Grain, environment, g, query_graph:ReferenceGraph):
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
            query_graph.add_node(dsconcept,)
            query_graph.add_edge(operator, dsconcept)

def build_superset_upstream(concept:Concept, output_grain:Grain, environment, g, query_graph:ReferenceGraph):
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
            query_graph.add_node(dsconcept,)
            query_graph.add_edge(operator, dsconcept)

def build_upstream_join_required(concept:Concept, output_grain:Grain, environment, g, query_graph:ReferenceGraph):
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

def node_to_cte(input_node:str, G):
    if input_node.startswith('ds~'):
        input_node = [n for n in G.successors(input_node) if n.startswith('select_')][0]
    grain = G.nodes[input_node]['grain']
    _source = [node for node in G.predecessors(input_node) if node.startswith("ds~")][0]
    datasource: Datasource = G.nodes[_source]["ds"]
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
        group_to_grain=group_to_grain,)


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
        cname = concept_to_node(concept)
        query_graph.add_node(cname, concept=concept, virtual=False)
        query_graph.add_edge(cname, root)
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
        joinkeys=[
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
