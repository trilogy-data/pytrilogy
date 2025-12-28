from dataclasses import dataclass
from typing import Iterator

from trilogy.core.enums import Derivation
from trilogy.core.graph_models import (
    ReferenceGraph,
    concept_to_node,
    datasource_to_node,
)
from trilogy.core.models.build import BuildConcept, BuildDatasource
from trilogy.core.models.build_environment import BuildEnvironment


@dataclass(slots=True)
class BasicConceptGraph:
    """Pre-computed dependency graph for BASIC derived concepts."""

    # concept address -> concept
    concepts: dict[str, BuildConcept]
    # concept address -> set of input addresses required
    dependencies: dict[str, frozenset[str]]
    # input address -> concepts that depend on it
    reverse_deps: dict[str, list[str]]
    # concepts with no dependencies (can be computed from any base concept)
    roots: list[str]


def build_basic_concept_graph(concepts: list[BuildConcept]) -> BasicConceptGraph:
    """Build dependency graph for BASIC derived concepts.

    Returns a structure that allows efficient single-pass computation
    of which derived concepts can be added to a datasource.
    """
    concept_map: dict[str, BuildConcept] = {}
    dependencies: dict[str, frozenset[str]] = {}
    reverse_deps: dict[str, list[str]] = {}
    roots: list[str] = []

    for concept in concepts:
        if concept.derivation != Derivation.BASIC or not concept.concept_arguments:
            continue

        addr = concept.canonical_address
        concept_map[addr] = concept
        input_addrs = frozenset(c.canonical_address for c in concept.concept_arguments)
        dependencies[addr] = input_addrs

        # Build reverse dependency map
        for input_addr in input_addrs:
            if input_addr not in reverse_deps:
                reverse_deps[input_addr] = []
            reverse_deps[input_addr].append(addr)

    # Find roots - concepts whose inputs are all non-BASIC
    for addr, deps in dependencies.items():
        if all(d not in dependencies for d in deps):
            roots.append(addr)

    return BasicConceptGraph(
        concepts=concept_map,
        dependencies=dependencies,
        reverse_deps=reverse_deps,
        roots=roots,
    )


def get_derivable_concepts(
    graph: BasicConceptGraph,
    available: set[str],
    already_present: set[str],
) -> Iterator[BuildConcept]:
    """Yield concepts derivable from available concepts in topological order.

    Uses the pre-computed dependency graph to traverse in a single pass,
    yielding each concept as soon as its dependencies are satisfied.

    Args:
        graph: Pre-computed dependency graph for BASIC concepts
        available: Set of canonical addresses of complete concepts (will be mutated)
        already_present: Set of canonical addresses already in the datasource (skip these)
    """
    if not graph.roots:
        return

    # Track which concepts we've already processed
    processed: set[str] = set()
    # Queue of concept addresses to check
    to_check: list[str] = list(graph.roots)

    while to_check:
        addr = to_check.pop()
        if addr in processed:
            continue

        deps = graph.dependencies[addr]
        if deps.issubset(available):
            processed.add(addr)
            available.add(addr)

            # Only yield if not already in the datasource
            if addr not in already_present:
                yield graph.concepts[addr]

            # Add concepts that depend on this one to the check queue
            for dependent in graph.reverse_deps.get(addr, []):
                if dependent not in processed:
                    to_check.append(dependent)


def add_concept(
    concept: BuildConcept,
    g: ReferenceGraph,
    concept_mapping: dict[str, BuildConcept],
    default_concept_graph: dict[str, BuildConcept],
    seen: set[str],
    node_stash: dict[str, str] = {},
):

    # if we have sources, recursively add them
    node_name = concept_to_node(concept, node_stash)
    if node_name in seen:
        return
    seen.add(node_name)
    g.concepts[node_name] = concept
    g.add_node(node_name)
    root_name = node_name.split("@", 1)[0]
    if concept.concept_arguments:
        for source in concept.concept_arguments:
            if not isinstance(source, BuildConcept):
                raise ValueError(
                    f"Invalid non-build concept {source} passed into graph generation from {concept}"
                )
            generic = get_default_grain_concept(source, default_concept_graph)
            generic_node = concept_to_node(generic, stash=node_stash)
            add_concept(
                generic, g, concept_mapping, default_concept_graph, seen, node_stash
            )

            g.add_edge(generic_node, node_name)
    for ps_address in concept.pseudonyms:
        if ps_address not in concept_mapping:
            raise SyntaxError(f"Concept {concept} has invalid pseudonym {ps_address}")
        pseudonym = concept_mapping[ps_address]
        pseudonym = get_default_grain_concept(pseudonym, default_concept_graph)
        pseudonym_node = concept_to_node(pseudonym, stash=node_stash)
        if (pseudonym_node, node_name) in g.edges and (
            node_name,
            pseudonym_node,
        ) in g.edges:
            continue
        if pseudonym_node.split("@", 1)[0] == root_name:
            continue
        g.add_edge(pseudonym_node, node_name)
        g.add_edge(node_name, pseudonym_node)
        g.pseudonyms.add((pseudonym_node, node_name))
        g.pseudonyms.add((node_name, pseudonym_node))
        add_concept(
            pseudonym, g, concept_mapping, default_concept_graph, seen, node_stash
        )


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
) -> ReferenceGraph:
    g = ReferenceGraph()
    concept_mapping = {x.address: x for x in concepts}
    node_stash: dict[str, str] = {}
    seen: set[str] = set()
    for concept in concepts:
        if not isinstance(concept, BuildConcept):
            raise ValueError(f"Invalid non-build concept {concept}")

    # add all parsed concepts
    for concept in concepts:
        add_concept(
            concept, g, concept_mapping, default_concept_graph, seen, node_stash
        )

    basic_graph = build_basic_concept_graph(concepts)

    for dataset in datasources:
        node = datasource_to_node(dataset)
        g.datasources[node] = dataset
        g.add_datasource_node(node, dataset)
        eligible = dataset.concepts
        already_present = set(x.canonical_address for x in eligible)
        complete_contains = set(
            c.concept.canonical_address for c in dataset.columns if c.is_complete
        )
        for derived in get_derivable_concepts(
            basic_graph, complete_contains, already_present
        ):
            eligible.append(derived)

        for concept in eligible:
            cnode = concept_to_node(concept, node_stash)
            g.concepts[cnode] = concept

            g.add_node(cnode)
            g.add_edge(node, cnode)
            g.add_edge(cnode, node)
            # if there is a key on a table at a different grain
            # add an FK edge to the canonical source, if it exists
            # for example, order ID on order product table
            default = get_default_grain_concept(concept, default_concept_graph)

            if concept != default:

                dcnode = concept_to_node(default, node_stash)
                g.concepts[dcnode] = default
                g.add_node(dcnode)
                g.add_edge(cnode, dcnode)
                g.add_edge(dcnode, cnode)
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
