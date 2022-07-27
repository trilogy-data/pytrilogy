import uuid
from typing import List, Optional, Union

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
    JoinedDataSource
)
from preql.utility import string_to_hash


def concept_to_node(input: Concept) -> str:
    return f"c~{input.namespace}.{input.name}"


def datasource_to_node(input: Union[Datasource, JoinedDataSource]) -> str:
    if isinstance(input, JoinedDataSource):
        return ','.join([datasource_to_node(sub) for sub in input.datasources])
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


def get_datasource_by_joins(
        concept, grain: Grain, environment: Environment, g: ReferenceGraph
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
            join_candidates.append(paths)
    join_candidates.sort(key=lambda x: sum([len(v) for v in x.values()]))
    if not join_candidates:
        raise ValueError
    shortest = join_candidates[0]
    source_map = {}
    for key, value in shortest.items():
        datasource = [v for v in value if v.startswith('ds~')]
        source_map[g.nodes[value[-1]]['concept'].name] = node_to_datasource(datasource[-1], environment=environment)
    return JoinedDataSource(concepts=all_requirements, source_map=source_map,
                            grain=grain,
                            join_paths = shortest)


def get_datasource_by_concept_and_grain(
        concept, grain: Grain, environment: Environment, g: ReferenceGraph
) -> Union[Datasource, JoinedDataSource]:
    try:
        return get_datasource_from_direct_select(concept, grain, environment, g)
    except ValueError:
        pass
    try:
        return get_datasource_by_joins(concept, grain, environment, g)
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
        query_graph.add_node(
            source, virtual=virtual
        )
        query_graph.add_edge(source, concept)

def build_select_upstream(concept:Concept, output_grain:Grain, environment, g, query_graph:ReferenceGraph):
    datasource = get_datasource_by_concept_and_grain(
        concept, output_grain, environment, g
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

def build_component_upstream(concept:Concept, output_grain:Grain, environment, g, query_graph:ReferenceGraph):
    print(
        f"select node {concept.grain} to {concept.grain} as subset of {output_grain}"
    )
    datasource = get_datasource_by_concept_and_grain(
        concept, concept.grain, environment, g
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
        concept, concept.grain, environment, g
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
    print(f"group node {concept.grain} to {output_grain}")
    datasource = get_datasource_by_concept_and_grain(
        concept, concept.grain + output_grain, environment, g
    )
    if isinstance(datasource, JoinedDataSource):
        operator = f'select_join_group_{datasource.identifier}'
    else:
        operator = f"select_group_{datasource.identifier}"
    query_graph.add_node(operator, grain=output_grain)
    query_graph.add_edge(operator, concept)
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


def walk_query_graph(input: str, G, query_grain: Grain, index: int = 1) -> List[CTE]:
    '''Traverse the graph and turn every select node into a CTE.
    '''
    output = []

    if input.startswith("select_"):
        info = G.nodes[input]
        source = [node for node in G.predecessors(input) if node.startswith("ds~")][0]
        datasource: Datasource = G.nodes[source]["ds"]
        outputs = [
            G.nodes[node]["concept"]
            for node in nx.descendants(G, source)
            if node.startswith("c~") and not G.nodes[node].get("virtual", False)
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
        output += walk_query_graph(node, G, query_grain, index)
    return output


def build_execution_graph(statement: Select, environment: Environment,
                          relation_graph: nx.DiGraph) -> nx.DiGraph:
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
    # graph of query operations

    # run lifecycle hooks
    # typically for logging
    hooks = hooks or []
    for hook in hooks:
        hook.query_graph_built(query_graph)
    starts = [
        n for n in query_graph.nodes if query_graph.in_degree(n) == 0
    ]  # type: ignore
    ctes = []
    for node in starts:
        ctes += walk_query_graph(node, query_graph, statement.grain)
    joins = []
    base = [cte for cte in ctes if cte.base][0]
    others = [cte for cte in ctes if cte != base]
    for cte in others:
        joins.append(
            Join(
                left_cte=base,
                right_cte=cte,
                joinkeys=[
                    JoinKey(c, c)
                    for c in statement.grain.components
                    if c in cte.output_columns
                ],
                jointype=JoinType.INNER,
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
