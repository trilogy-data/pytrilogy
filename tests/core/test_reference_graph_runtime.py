from trilogy.core.enums import Purpose
from trilogy.core.graph_models import (
    ReferenceGraph,
    concept_to_node,
    datasource_to_node,
)
from trilogy.core.models.build import BuildConcept, BuildDatasource, BuildGrain
from trilogy.core.models.core import DataType


def make_concept(name: str, *, purpose: Purpose = Purpose.KEY) -> BuildConcept:
    return BuildConcept(
        name=name,
        canonical_name=name,
        namespace="test",
        datatype=DataType.STRING,
        purpose=purpose,
        build_is_aggregate=False,
        grain=BuildGrain(),
    )


def make_datasource(name: str, concepts: list[BuildConcept]) -> BuildDatasource:
    from trilogy.core.models.build import BuildColumnAssignment

    return BuildDatasource(
        name=name,
        address=name,
        columns=[
            BuildColumnAssignment(concept=concept, alias=concept.name, modifiers=[])
            for concept in concepts
        ],
        grain=BuildGrain.from_concepts(
            [concept for concept in concepts if concept.purpose == Purpose.KEY]
        ),
    )


def build_reference_graph() -> tuple[ReferenceGraph, list[str], list[str]]:
    customer_id = make_concept("customer_id")
    order_id = make_concept("order_id")
    customer_name = make_concept("customer_name", purpose=Purpose.PROPERTY)
    order_total = make_concept("order_total", purpose=Purpose.PROPERTY)

    orders = make_datasource("orders", [order_id, order_total, customer_id])
    customers = make_datasource("customers", [customer_id, customer_name])

    concept_nodes = [
        concept_to_node(order_total),
        concept_to_node(customer_name),
        concept_to_node(order_id),
        concept_to_node(customer_id),
    ]
    datasource_nodes = [datasource_to_node(orders), datasource_to_node(customers)]

    graph = ReferenceGraph()
    graph.concepts[concept_nodes[0]] = order_total
    graph.add_node(concept_nodes[0])
    graph.datasources[datasource_nodes[0]] = orders
    graph.add_datasource_node(datasource_nodes[0], orders)
    graph.concepts[concept_nodes[1]] = customer_name
    graph.add_node(concept_nodes[1])
    graph.datasources[datasource_nodes[1]] = customers
    graph.add_datasource_node(datasource_nodes[1], customers)
    graph.concepts[concept_nodes[2]] = order_id
    graph.add_node(concept_nodes[2])
    graph.concepts[concept_nodes[3]] = customer_id
    graph.add_node(concept_nodes[3])

    graph.add_edge(datasource_nodes[0], concept_nodes[2])
    graph.add_edge(datasource_nodes[0], concept_nodes[0])
    graph.add_edge(datasource_nodes[0], concept_nodes[3])
    graph.add_edge(datasource_nodes[1], concept_nodes[3])
    graph.add_edge(datasource_nodes[1], concept_nodes[1])
    graph.pseudonyms = {(concept_nodes[3], concept_nodes[1])}
    return graph, concept_nodes, datasource_nodes


def test_reference_graph_copy_preserves_metadata_order():
    graph, concept_nodes, datasource_nodes = build_reference_graph()

    copied = graph.copy()

    assert list(copied.nodes) == list(graph.nodes)
    assert list(copied.edges) == list(graph.edges)
    assert list(copied.concepts) == concept_nodes
    assert list(copied.concepts) == list(graph.concepts)
    assert list(copied.datasources) == datasource_nodes
    assert list(copied.datasources) == list(graph.datasources)
    assert copied.pseudonyms == graph.pseudonyms


def test_reference_graph_subgraph_preserves_filtered_metadata_order():
    graph, concept_nodes, datasource_nodes = build_reference_graph()
    keep = [
        datasource_nodes[1],
        concept_nodes[1],
        concept_nodes[3],
    ]

    subgraph = graph.subgraph(keep)

    assert list(subgraph.nodes) == [node for node in graph.nodes if node in set(keep)]
    assert list(subgraph.concepts) == [
        node for node in concept_nodes if node in set(keep)
    ]
    assert list(subgraph.datasources) == [
        node for node in datasource_nodes if node in set(keep)
    ]
    assert subgraph.pseudonyms == {(concept_nodes[3], concept_nodes[1])}


def test_reference_graph_remove_nodes_from_preserves_metadata_contract():
    graph, concept_nodes, datasource_nodes = build_reference_graph()

    graph.remove_nodes_from([datasource_nodes[0], concept_nodes[0], "missing"])

    assert datasource_nodes[0] not in graph.nodes
    assert concept_nodes[0] not in graph.nodes
    assert datasource_nodes[0] in graph.datasources
    assert concept_nodes[0] in graph.concepts
