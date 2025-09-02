from trilogy.core.graph_models import (
    ReferenceGraph,
    concept_to_node,
    datasource_to_node,
)
from trilogy.core.models.build import BuildConcept, BuildDatasource
from trilogy.core.models.build_environment import BuildEnvironment


def add_concept(
    concept: BuildConcept,
    g: ReferenceGraph,
    concept_mapping: dict[str, BuildConcept],
    default_concept_graph: dict[str, BuildConcept],
    seen: set[str],
):

    # if we have sources, recursively add them
    node_name = concept_to_node(concept)
    if node_name in seen:
        return
    seen.add(node_name)
    g.concepts[node_name] = concept
    g.add_node(node_name)
    if concept.concept_arguments:
        for source in concept.concept_arguments:
            if not isinstance(source, BuildConcept):
                raise ValueError(
                    f"Invalid non-build concept {source} passed into graph generation from {concept}"
                )
            generic = get_default_grain_concept(source, default_concept_graph)
            generic_node = concept_to_node(generic)
            add_concept(generic, g, concept_mapping, default_concept_graph, seen)

            g.add_edge(generic_node, node_name, fast=True)
    for ps_address in concept.pseudonyms:
        if ps_address not in concept_mapping:
            raise SyntaxError(f"Concept {concept} has invalid pseudonym {ps_address}")
        pseudonym = concept_mapping[ps_address]
        pseudonym = get_default_grain_concept(pseudonym, default_concept_graph)
        pseudonym_node = concept_to_node(pseudonym)
        if (pseudonym_node, node_name) in g.edges and (
            node_name,
            pseudonym_node,
        ) in g.edges:
            continue
        if pseudonym_node.split("@")[0] == node_name.split("@")[0]:
            continue
        g.add_edge(pseudonym_node, node_name, fast=True)
        g.add_edge(node_name, pseudonym_node, fast=True)
        g.pseudonyms.add((pseudonym_node, node_name))
        g.pseudonyms.add((node_name, pseudonym_node))
        add_concept(pseudonym, g, concept_mapping, default_concept_graph, seen)


def get_default_grain_concept(
    concept: BuildConcept, default_concept_graph: dict[str, BuildConcept]
) -> BuildConcept:
    """Get the default grain concept from the graph."""
    if concept.address in default_concept_graph:
        return default_concept_graph[concept.address]
    default = concept.with_default_grain()
    default_concept_graph[concept.address] = default
    return default


def generate_adhoc_graph(
    concepts: list[BuildConcept],
    datasources: list[BuildDatasource],
    default_concept_graph: dict[str, BuildConcept],
    restrict_to_listed: bool = False,
) -> ReferenceGraph:
    g = ReferenceGraph()
    concept_mapping = {x.address: x for x in concepts}
    seen: set[str] = set()
    for concept in concepts:
        if not isinstance(concept, BuildConcept):
            raise ValueError(f"Invalid non-build concept {concept}")

    # add all parsed concepts
    for concept in concepts:

        add_concept(concept, g, concept_mapping, default_concept_graph, seen)

    for dataset in datasources:
        node = datasource_to_node(dataset)
        g.add_datasource_node(node, dataset)
        for concept in dataset.concepts:
            cnode = concept_to_node(concept)
            g.concepts[cnode] = concept
            g.add_node(cnode)
            if restrict_to_listed:
                if cnode not in g.nodes:
                    continue
            g.add_edge(node, cnode, fast=True)
            g.add_edge(cnode, node, fast=True)
            # if there is a key on a table at a different grain
            # add an FK edge to the canonical source, if it exists
            # for example, order ID on order product table
            default = get_default_grain_concept(concept, default_concept_graph)

            if concept != default:
                dcnode = concept_to_node(default)
                g.concepts[dcnode] = default
                g.add_node(dcnode)
                g.add_edge(cnode, dcnode, fast=True)
                g.add_edge(dcnode, cnode, fast=True)
    return g


def generate_graph(
    environment: BuildEnvironment,
) -> ReferenceGraph:
    default_concept_graph: dict[str, BuildConcept] = {}
    return generate_adhoc_graph(
        list(environment.concepts.values())
        + list(environment.alias_origin_lookup.values()),
        list(environment.datasources.values()),
        default_concept_graph=default_concept_graph,
    )
