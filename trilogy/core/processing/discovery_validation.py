from collections import defaultdict
from enum import Enum

from trilogy.core.enums import Derivation, Granularity
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
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


def _is_scalar_only(node: StrategyNode, condition: BoolExpr | None = None) -> bool:
    """A node whose visible outputs are all single-row scalars. Such nodes are
    cross-joined into the consumer, so their preexisting_conditions are
    independent of the outer query's row-level conditions. The exemption holds
    only while `condition` does not filter one of this node's own outputs."""
    # A pure-literal condition (no row arguments) belongs to no scope's rows and
    # must always be applied; the overlap check below would vacuously exempt it.
    if condition is not None and not condition.row_arguments:
        return False
    resolved = node.resolve()
    visible = [
        c for c in resolved.output_concepts if c.address not in resolved.hidden_concepts
    ]
    if not visible:
        return False
    if not all(c.granularity == Granularity.SINGLE_ROW for c in visible):
        return False
    if condition is not None:
        visible_addresses = {c.address for c in visible}
        if any(arg.address in visible_addresses for arg in condition.row_arguments):
            return False
    return True


def _is_independent_scope(node: StrategyNode, condition: BoolExpr) -> bool:
    """A node whose visible outputs are all rowset-derived is a self-contained
    subquery scope, so its preexisting_conditions are independent of `condition`
    unless the condition filters one of the rowset's own outputs; that is a
    consumer filter on the rowset's rows and must be applied."""
    if not condition.row_arguments:
        return False
    resolved = node.resolve()
    visible = [
        c for c in resolved.output_concepts if c.address not in resolved.hidden_concepts
    ]
    if not visible:
        return False
    if not all(c.derivation == Derivation.ROWSET for c in visible):
        return False
    visible_addresses = {c.address for c in visible}
    return not any(arg.address in visible_addresses for arg in condition.row_arguments)


def _node_condition_implies(
    node: StrategyNode,
    condition: BoolExpr,
) -> bool:
    return node.preexisting_conditions == condition or (
        node.preexisting_conditions is not None
        and condition_implies(node.preexisting_conditions, condition)
    )


def _stack_exempt_or_implies(stack: list[StrategyNode], condition: BoolExpr) -> bool:
    """Every node either implies the condition or is exempt from carrying it
    (scalar-only / independent scope). Sufficient for a FRAGMENT search: the
    consuming level re-validates with its full sibling stack, where an actual
    applier must appear."""
    return all(
        _is_scalar_only(node, condition)
        or _is_independent_scope(node, condition)
        or _node_condition_implies(node, condition)
        for node in stack
    )


def _stack_applies_condition(stack: list[StrategyNode], condition: BoolExpr) -> bool:
    """A condition counts as already applied only when at least one node's own
    conditions imply it and every other node either implies it or is exempt.
    Exemptions let a node opt OUT of carrying a condition another node
    applies; they never satisfy it, so an all-exempt stack with no applier
    fails rather than silently dropping the filter."""
    return _stack_exempt_or_implies(stack, condition) and any(
        _node_condition_implies(node, condition) for node in stack
    )


def _condition_atom_met(
    stack: list[StrategyNode],
    found_addresses: set[str],
    condition: BoolExpr,
    require_applier: bool,
) -> bool:
    if all(c.address in found_addresses for c in condition.row_arguments):
        return True
    if require_applier:
        return _stack_applies_condition(stack, condition)
    return _stack_exempt_or_implies(stack, condition)


def _conditions_met(
    stack: list[StrategyNode],
    found_addresses: set[str],
    mandatory_with_filter: list[BuildConcept],
    conditions: BuildWhereClause | None,
    require_applier: bool,
) -> bool:
    if not conditions:
        return True
    conditional = conditions.conditional
    if require_applier:
        if _stack_applies_condition(stack, conditional):
            return True
    elif _stack_exempt_or_implies(stack, conditional):
        return True
    if all(c.address in found_addresses for c in mandatory_with_filter):
        return True
    return all(
        _condition_atom_met(stack, found_addresses, atom, require_applier)
        for atom in decompose_condition(conditional)
    )


class ValidationResult(Enum):
    COMPLETE = 1
    DISCONNECTED = 2
    INCOMPLETE = 3
    INCOMPLETE_CONDITION = 4


def _deep_output_addresses(node: StrategyNode) -> set[str]:
    """Every address produced anywhere in `node`'s parent subtree (own
    addresses only, no pseudonyms)."""
    acc: set[str] = set()
    seen: set[int] = set()
    stack = [node]
    while stack:
        current = stack.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        acc.update(c.address for c in current.output_concepts)
        stack.extend(current.parents)
    return acc


def validate_concept(
    concept: BuildConcept,
    node: StrategyNode,
    found_addresses: set[str],
    non_partial_addresses: set[str],
    partial_addresses: set[str],
    found_map: dict[str, set[BuildConcept]],
    accept_partial: bool,
    seen: set[str],
    environment: BuildEnvironment,
    group_mates: dict[str, set[str]],
    node_deep_addresses: set[str],
):
    found_map[str(node)].add(concept)
    seen.add(concept.address)
    if concept not in node.partial_concepts:
        found_addresses.add(concept.address)
        non_partial_addresses.add(concept.address)
        partial_addresses.discard(concept.address)
    if concept in node.partial_concepts:
        if concept.address in non_partial_addresses:
            return
        partial_addresses.add(concept.address)
        if accept_partial:
            found_addresses.add(concept.address)
            found_map[str(node)].add(concept)
    for v_address in concept.pseudonyms:
        if v_address in seen:
            continue
        # A scoped-join key-group member is never satisfied through a group-mate
        # pseudonym: the join needs each side's own column, so counting the mate
        # as found would collapse the join onto one side. A node whose subtree
        # materializes the mate already represents the join, so its coalesced
        # output covers both. The mate still lands in found_map so the stack is
        # connected once each side sources its own.
        if (
            v_address in group_mates.get(concept.address, ())
            and v_address not in node_deep_addresses
        ):
            mate = environment.alias_origin_lookup.get(
                v_address
            ) or environment.concepts.get(v_address)
            if mate is not None:
                found_map[str(node)].add(mate)
            continue
        if v_address in environment.alias_origin_lookup:
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
            found_map,
            accept_partial,
            seen=seen,
            environment=environment,
            group_mates=group_mates,
            node_deep_addresses=node_deep_addresses,
        )


def validate_stack(
    environment: BuildEnvironment,
    stack: list[StrategyNode],
    concepts: list[BuildConcept],
    mandatory_with_filter: list[BuildConcept],
    conditions: BuildWhereClause | None = None,
    accept_partial: bool = False,
    require_condition_applier: bool = False,
) -> tuple[ValidationResult, set[str], set[str], set[str]]:
    found_map: dict[str, set[BuildConcept]] = defaultdict(set)
    found_addresses: set[str] = set()
    non_partial_addresses: set[str] = set()
    partial_addresses: set[str] = set()
    seen: set[str] = set()
    group_mates = environment.pseudonym_unsatisfiable_group_mates()

    for node in stack:
        resolved = node.resolve()
        node_deep_addresses = _deep_output_addresses(node) if group_mates else set()

        for concept in resolved.output_concepts:
            if concept.address in resolved.hidden_concepts:
                continue

            validate_concept(
                concept,
                node,
                found_addresses,
                non_partial_addresses,
                partial_addresses,
                found_map,
                accept_partial,
                seen,
                environment,
                group_mates,
                node_deep_addresses,
            )
    conditions_met = _conditions_met(
        stack,
        found_addresses,
        mandatory_with_filter,
        conditions,
        require_condition_applier,
    )
    if not all(c.address in found_addresses for c in concepts) or not conditions_met:
        if not all(c.address in found_addresses for c in concepts):
            return (
                ValidationResult.INCOMPLETE,
                found_addresses,
                {c.address for c in concepts if c.address not in found_addresses},
                partial_addresses,
            )
        return (
            ValidationResult.INCOMPLETE_CONDITION,
            found_addresses,
            {c.address for c in concepts if c.address not in mandatory_with_filter},
            partial_addresses,
        )

    graph_count, _ = get_disconnected_components(found_map)
    if graph_count in (0, 1):
        return (
            ValidationResult.COMPLETE,
            found_addresses,
            set(),
            partial_addresses,
        )
    return (
        ValidationResult.DISCONNECTED,
        found_addresses,
        set(),
        partial_addresses,
    )
