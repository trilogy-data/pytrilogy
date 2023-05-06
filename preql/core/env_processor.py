from preql.core.graph_models import ReferenceGraph, concept_to_node, datasource_to_node
from preql.core.models import Environment


def generate_graph(
    environment: Environment,
) -> ReferenceGraph:
    g = ReferenceGraph()

    # add all parsed concepts
    for name, concept in environment.concepts.items():
        g.add_node(concept)

        # if we have sources, recursively add them
        if concept.sources:
            node_name = concept_to_node(concept)
            for source in concept.sources:
                generic = source.with_default_grain()
                g.add_edge(generic, node_name)
    for key, dataset in environment.datasources.items():
        node = datasource_to_node(dataset)
        g.add_node(dataset, type="datasource", datasource=dataset)
        for concept in dataset.concepts:
            g.add_edge(node, concept)
            g.add_edge(concept, node)
            # if there is a key on a table at a different grain
            # add an FK edge to the canonical source, if it exists
            # for example, order ID on order product table
            g.add_edge(concept, concept.with_default_grain())

        # TODO: evaluate better way to handle scalar function associations
        # for _, concept in environment.concepts.items():
        #     if isinstance(concept.lineage, Function) and concept.lineage.operator not in FunctionClass.AGGREGATE_FUNCTIONS.value:
        #         if not all([c in dataset.concepts for c in concept.sources]):
        #             continue
        #         g.add_edge(dataset, concept.with_grain(dataset.grain))

    return g
