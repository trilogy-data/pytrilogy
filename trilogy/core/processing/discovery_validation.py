from collections import defaultdict
from enum import Enum
from typing import List

from trilogy.core.enums import Granularity
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildParenthetical,
    BuildSubselectComparison,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import (
    condition_implies,
    decompose_condition,
)
from trilogy.core.processing.nodes import (
    StrategyNode,
)
from trilogy.core.processing.utility import (
    get_disconnected_components,
)

ConditionExpression = (
    BuildComparison | BuildConditional | BuildParenthetical | BuildSubselectComparison
)


def _is_scalar_only(node: StrategyNode) -> bool:
    """A node whose visible outputs are all single-row scalars (e.g. a CTE
    aggregate referenced as a constant). Such nodes are cross-joined into the
    consumer; their preexisting_conditions reflect the CTE's own WHERE and are
    independent of the outer query's row-level conditions."""
    resolved = node.resolve()
    visible = [
        c for c in resolved.output_concepts if c.address not in resolved.hidden_concepts
    ]
    if not visible:
        return False
    return all(c.granularity == Granularity.SINGLE_ROW for c in visible)


def _node_condition_implies(
    node: StrategyNode,
    condition: ConditionExpression,
) -> bool:
    return node.preexisting_conditions == condition or (
        node.preexisting_conditions is not None
        and condition_implies(node.preexisting_conditions, condition)
    )


def _condition_atom_met(
    stack: List[StrategyNode],
    found_addresses: set[str],
    condition: ConditionExpression,
) -> bool:
    if all(c.address in found_addresses for c in condition.row_arguments):
        return True
    return all(
        _is_scalar_only(node) or _node_condition_implies(node, condition)
        for node in stack
    )


def _conditions_met(
    stack: List[StrategyNode],
    found_addresses: set[str],
    mandatory_with_filter: List[BuildConcept],
    conditions: BuildWhereClause | None,
) -> bool:
    if not conditions:
        return True
    conditional = conditions.conditional
    if all(
        _is_scalar_only(node) or _node_condition_implies(node, conditional)
        for node in stack
    ):
        return True
    if all(c.address in found_addresses for c in mandatory_with_filter):
        return True
    return all(
        _condition_atom_met(stack, found_addresses, atom)
        for atom in decompose_condition(conditional)
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
    # logger.debug(
    #     f"Validating concept {concept.address} with accept_partial={accept_partial}"
    # )
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
            continue
        if v_address in environment.alias_origin_lookup:
            # logger.debug(
            #     f"Found alias origin for {v_address}: {environment.alias_origin_lookup[v_address]} mapped to {environment.concepts[v_address]}")
            v = environment.alias_origin_lookup[v_address]
        else:
            v = environment.concepts[v_address]

        if v.address in seen:

            continue

        if v.address == concept.address:

            continue
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
    conditions_met = _conditions_met(
        stack,
        found_addresses,
        mandatory_with_filter,
        conditions,
    )
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
