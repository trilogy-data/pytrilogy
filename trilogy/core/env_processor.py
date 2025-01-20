from trilogy.core.graph_models import (
    ReferenceGraph,
    concept_to_node,
    datasource_to_node,
)
from trilogy.core.models.build import BuildConcept, BuildDatasource
from trilogy.core.models.build_environment import BuildEnvironment


def add_concept(
    concept: BuildConcept, g: ReferenceGraph, concept_mapping: dict[str, BuildConcept]
):
    g.add_node(concept)
    # if we have sources, recursively add them
    node_name = concept_to_node(concept)
    if concept.concept_arguments:
        for source in concept.concept_arguments:
            if not isinstance(source, BuildConcept):
                raise ValueError(
                    f"Invalid non-build concept {source} passed into graph generation from {concept}"
                )
            generic = source.with_default_grain()
            add_concept(generic, g, concept_mapping)

            g.add_edge(generic, node_name)
    for ps_address in concept.pseudonyms:
        if ps_address not in concept_mapping:
            raise SyntaxError(f"Concept {concept} has invalid pseudonym {ps_address}")
        pseudonym = concept_mapping[ps_address]
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
        add_concept(pseudonym, g, concept_mapping)


def generate_adhoc_graph(
    concepts: list[BuildConcept],
    datasources: list[BuildDatasource],
    restrict_to_listed: bool = False,
) -> ReferenceGraph:
    g = ReferenceGraph()
    concept_mapping = {x.address: x for x in concepts}
    for concept in concepts:
        if not isinstance(concept, BuildConcept):
            raise ValueError(f"Invalid non-build concept {concept}")

    # add all parsed concepts
    for concept in concepts:

        add_concept(concept, g, concept_mapping)

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
    environment: BuildEnvironment,
) -> ReferenceGraph:

    return generate_adhoc_graph(
        list(environment.concepts.values())
        + list(environment.alias_origin_lookup.values()),
        list(environment.datasources.values()),
    )
