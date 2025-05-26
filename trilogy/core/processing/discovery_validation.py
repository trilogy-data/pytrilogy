from collections import defaultdict
from enum import Enum
from typing import List

from trilogy.core.models.build import (
    BuildConcept,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import (
    StrategyNode,
)
from trilogy.core.processing.utility import (
    get_disconnected_components,
)


class ValidationResult(Enum):
    COMPLETE = 1
    DISCONNECTED = 2
    INCOMPLETE = 3
    INCOMPLETE_CONDITION = 4


def validate_concept(
    concept: BuildConcept,
    node: StrategyNode,
    found_addresses: set[str],
    non_partial_addresses: set[str],
    partial_addresses: set[str],
    virtual_addresses: set[str],
    found_map: dict[str, set[BuildConcept]],
    accept_partial: bool,
    seen: set[str],
    environment: BuildEnvironment,
):
    found_map[str(node)].add(concept)
    seen.add(concept.address)
    if concept not in node.partial_concepts:
        found_addresses.add(concept.address)
        non_partial_addresses.add(concept.address)
        # remove it from our partial tracking
        if concept.address in partial_addresses:
            partial_addresses.remove(concept.address)
        if concept.address in virtual_addresses:
            virtual_addresses.remove(concept.address)
    if concept in node.partial_concepts:
        if concept.address in non_partial_addresses:
            return None
        partial_addresses.add(concept.address)
        if accept_partial:
            found_addresses.add(concept.address)
            found_map[str(node)].add(concept)
    for v_address in concept.pseudonyms:
        if v_address in seen:
            return
        v = environment.concepts[v_address]
        if v.address in seen:
            return
        if v.address == concept.address:
            return
        validate_concept(
            v,
            node,
            found_addresses,
            non_partial_addresses,
            partial_addresses,
            virtual_addresses,
            found_map,
            accept_partial,
            seen=seen,
            environment=environment,
        )


def validate_stack(
    environment: BuildEnvironment,
    stack: List[StrategyNode],
    concepts: List[BuildConcept],
    mandatory_with_filter: List[BuildConcept],
    conditions: BuildWhereClause | None = None,
    accept_partial: bool = False,
) -> tuple[ValidationResult, set[str], set[str], set[str], set[str]]:
    found_map: dict[str, set[BuildConcept]] = defaultdict(set)
    found_addresses: set[str] = set()
    non_partial_addresses: set[str] = set()
    partial_addresses: set[str] = set()
    virtual_addresses: set[str] = set()
    seen: set[str] = set()

    for node in stack:
        resolved = node.resolve()

        for concept in resolved.output_concepts:
            if concept.address in resolved.hidden_concepts:
                continue

            validate_concept(
                concept,
                node,
                found_addresses,
                non_partial_addresses,
                partial_addresses,
                virtual_addresses,
                found_map,
                accept_partial,
                seen,
                environment,
            )
        for concept in node.virtual_output_concepts:
            if concept.address in non_partial_addresses:
                continue
            found_addresses.add(concept.address)
            virtual_addresses.add(concept.address)
    if not conditions:
        conditions_met = True
    else:
        conditions_met = all(
            [node.preexisting_conditions == conditions.conditional for node in stack]
        ) or all([c.address in found_addresses for c in mandatory_with_filter])
    # zip in those we know we found
    if not all([c.address in found_addresses for c in concepts]) or not conditions_met:
        if not all([c.address in found_addresses for c in concepts]):
            return (
                ValidationResult.INCOMPLETE,
                found_addresses,
                {c.address for c in concepts if c.address not in found_addresses},
                partial_addresses,
                virtual_addresses,
            )
        return (
            ValidationResult.INCOMPLETE_CONDITION,
            found_addresses,
            {c.address for c in concepts if c.address not in mandatory_with_filter},
            partial_addresses,
            virtual_addresses,
        )

    graph_count, _ = get_disconnected_components(found_map)
    if graph_count in (0, 1):
        return (
            ValidationResult.COMPLETE,
            found_addresses,
            set(),
            partial_addresses,
            virtual_addresses,
        )
    # if we have too many subgraphs, we need to keep searching
    return (
        ValidationResult.DISCONNECTED,
        found_addresses,
        set(),
        partial_addresses,
        virtual_addresses,
    )
