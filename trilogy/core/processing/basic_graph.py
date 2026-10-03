"""The dependency graph of BASIC derived concepts: which derivations a
source can compute inline from the complete columns it carries."""

from collections.abc import Iterator
from dataclasses import dataclass

from trilogy.core.enums import Derivation
from trilogy.core.models.build import BuildConcept


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
