import uuid
from typing import List

import networkx as nx

from preql.core.enums import Purpose, JoinType
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
)
from preql.utility import string_to_hash


def concept_to_node(input: Concept) -> str:
    return f"c~{input.namespace}.{input.name}"


def datasource_to_node(input: Datasource) -> str:
    return f"ds~{input.namespace}.{input.identifier}"


def add_concept_node(g, concept: Concept):
    g.add_node(concept_to_node(concept), type=concept.purpose.value, concept=concept)


class ReferenceGraph(nx.DiGraph):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bases = []


def generate_graph(environment: Environment,) -> ReferenceGraph:
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
    return g


def get_datasource_by_concept_and_grain(
    concept, grain: Grain, environment: Environment, g: ReferenceGraph
):
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
    for datasource in environment.datasources.values():
        all_found = True
        for item in grain.components:
            try:
                path = nx.shortest_path(
                    g,
                    source=datasource_to_node(datasource),
                    target=concept_to_node(item),
                )
            except nx.exception.NetworkXNoPath as e:
                all_found = False
                continue
            if not len([p for p in path if g.nodes[p]["type"] == "datasource"]) == 1:
                all_found = False
        if all_found:
            return datasource
    neighbors = list(g.predecessors(concept_to_node(concept)))
    raise ValueError(
        f"No source for {concept_to_node(concept)} grain {grain} found, neighbors {neighbors}"
    )


def get_operation_node(op_type: str) -> str:
    return f"{op_type}_{uuid.uuid4()}"


def find_upstream(
    concept: Concept,  # input_grain:Grain,
    output_grain: Grain,
    environment: Environment,
    g: ReferenceGraph,
    query_graph: nx.DiGraph,
    virtual: bool = False,
):
    print(
        f"building sources for {concept.name} {concept.grain} output grain {output_grain}"
    )
    cnode = concept_to_node(concept)
    query_graph.add_node(cnode, concept=concept, virtual=virtual)
    if concept.sources:
        for source in concept.sources:
            query_graph.add_node(
                concept_to_node(source), concept=source, virtual=virtual
            )
            query_graph.add_edge(concept_to_node(source), cnode)
    elif concept.grain == output_grain:
        print(f"select node {concept.grain} to {output_grain}")
        datasource = get_datasource_by_concept_and_grain(
            concept, output_grain, environment, g
        )
        print(f"can fetch from {datasource.identifier}")
        if concept.grain == datasource.grain:
            operator = f"select_{datasource.identifier}"
        elif concept.grain.issubset(datasource.grain):
            operator = f"select_group_{datasource.identifier}"
        else:
            raise ValueError("No way to get to node")
        query_graph.add_node(operator, grain=output_grain)
        query_graph.add_edge(operator, cnode)
        ds_label = datasource_to_node(datasource)
        query_graph.add_node(ds_label, ds=datasource)
        query_graph.add_edge(ds_label, operator)
        for dsconcept in datasource.concepts:
            if dsconcept in output_grain.components:
                dscnode = concept_to_node(dsconcept)
                query_graph.add_node(dscnode, concept=dsconcept)
                query_graph.add_edge(operator, dscnode)
    elif concept.grain.issubset(output_grain):
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
        query_graph.add_edge(operator, cnode)
        ds_label = datasource_to_node(datasource)
        query_graph.add_node(ds_label, ds=datasource)
        query_graph.add_edge(ds_label, operator)
        for dsconcept in datasource.concepts:
            if dsconcept in output_grain.components:
                dscnode = concept_to_node(dsconcept)
                query_graph.add_node(dscnode, concept=dsconcept)
                query_graph.add_edge(operator, dscnode)
    elif output_grain.issubset(concept.grain):
        print(f"group node {concept.grain} to {output_grain}")
        datasource = get_datasource_by_concept_and_grain(
            concept, concept.grain, environment, g
        )

        print(f"can group from {datasource.identifier}")
        operator = f"group_{datasource.identifier}"
        query_graph.add_node(operator, grain=output_grain)
        query_graph.add_edge(operator, cnode)
        ds_label = datasource_to_node(datasource)
        query_graph.add_node(ds_label, ds=datasource)
        query_graph.add_edge(ds_label, operator)
        for dsconcept in datasource.concepts:
            if dsconcept in output_grain.components:
                dscnode = concept_to_node(dsconcept)
                query_graph.add_node(dscnode, concept=dsconcept)
                query_graph.add_edge(operator, dscnode)
    elif concept.grain.isdisjoint(output_grain):
        print(f"group node {concept.grain} to {output_grain}")
        datasource = get_datasource_by_concept_and_grain(
            concept, concept.grain + output_grain, environment, g
        )
        print(f"can group from source with both {datasource.identifier}")
        operator = f"select_group_{datasource.identifier}"
        query_graph.add_node(operator, grain=output_grain)
        query_graph.add_edge(operator, cnode)
        for item in output_grain.components:
            query_graph.add_node(concept_to_node(item), concept=item)
            query_graph.add_edge(operator, concept_to_node(item))
        ds_label = datasource_to_node(datasource)
        query_graph.add_node(ds_label, ds=datasource)
        query_graph.add_edge(ds_label, operator)
    else:
        raise ValueError(
            f"NO CONNECTION CAN BE MADE FOR {concept.name} grain {concept.grain} target_grain {output_grain}"
        )

    for item in concept.sources:
        find_upstream(
            item,
            output_grain,
            environment=environment,
            g=g,
            query_graph=query_graph,
            virtual=True,
        )


def print_select_graph(input: str, G, indentation: int = 1):
    if input.startswith("select-"):
        output = "SELECT"
    else:
        output = input
    print("\t" * indentation + output)
    for node in G.predecessors(input):
        print_select_graph(node, G, indentation + 1)


def walk_query_graph(input: str, G, query_grain: Grain, index: int = 1) -> List[CTE]:
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


def process_query(environment: Environment, statement: Select) -> ProcessedQuery:
    import matplotlib.pyplot as plt

    plt.clf()
    # environment graph
    graph = generate_graph(environment)

    # graph of query operations
    query_graph = nx.DiGraph()
    root = get_operation_node("output")
    query_graph.add_node(root)
    for concept in statement.output_components + statement.grain.components:
        cname = concept_to_node(concept)
        query_graph.add_node(cname, concept=concept, virtual=False)
        query_graph.add_edge(cname, root)
        find_upstream(concept, statement.grain, environment, graph, query_graph)
    nx.draw_spring(query_graph, with_labels=True)
    # plt.show()
    print_select_graph(root, query_graph)
    starts = [
        n for n in query_graph.nodes if query_graph.in_degree(n) == 0
    ]  # type: ignore
    ctes = []
    for node in starts:
        ctes += walk_query_graph(node, query_graph, statement.grain)
    joins = []
    for cte in ctes:
        print(cte.grain)
        print(statement.grain)
    try:

        base = [cte for cte in ctes if cte.base][0]
    except IndexError as e:
        plt.show()
        raise e
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
