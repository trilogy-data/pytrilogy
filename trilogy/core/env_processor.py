from trilogy.core.graph_models import (
    ReferenceGraph,
    concept_to_node,
    datasource_to_node,
)
from trilogy.core.models import Environment, Concept, Datasource


def add_concept(concept: Concept, g: ReferenceGraph):
    g.add_node(concept)
    # if we have sources, recursively add them
    node_name = concept_to_node(concept)
    if concept.sources:
        for source in concept.sources:
            generic = source.with_default_grain()
            g.add_edge(generic, node_name)
    for _, pseudonym in concept.pseudonyms.items():
        pseudonym = pseudonym.with_default_grain()
        pseudonym_node = concept_to_node(pseudonym)
        if (pseudonym_node, node_name) in g.edges and (
            node_name,
            pseudonym_node,
        ) in g.edges:
            continue
        if pseudonym_node.split("@")[0] == node_name.split("@")[0]:
            continue
        g.add_edge(pseudonym_node, node_name, pseudonym=True)
        g.add_edge(node_name, pseudonym_node, pseudonym=True)
        add_concept(pseudonym, g)


def generate_adhoc_graph(
    concepts: list[Concept],
    datasources: list[Datasource],
    restrict_to_listed: bool = False,
) -> ReferenceGraph:
    g = ReferenceGraph()

    # add all parsed concepts
    for concept in concepts:
        add_concept(concept, g)

    for dataset in datasources:
        node = datasource_to_node(dataset)
        g.add_node(dataset, type="datasource", datasource=dataset)
        for concept in dataset.concepts:
            if restrict_to_listed:
                if concept_to_node(concept) not in g.nodes:
                    continue
            g.add_edge(node, concept)
            g.add_edge(concept, node)
            # if there is a key on a table at a different grain
            # add an FK edge to the canonical source, if it exists
            # for example, order ID on order product table
            default = concept.with_default_grain()
            if concept != default:
                g.add_edge(concept, default)
                g.add_edge(default, concept)
    return g


def generate_graph(
    environment: Environment,
) -> ReferenceGraph:

    return generate_adhoc_graph(
        list(environment.concepts.values()), list(environment.datasources.values())
    )
