from trilogy.core.enums import AggregateGroupingMode, Derivation
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import GroupNode, StrategyNode

from .common import parent_outputs_needed

_ROW_PRESERVING_AGGREGATE_INPUT_DERIVATIONS = {
    Derivation.ROOT,
    Derivation.BASIC,
    Derivation.FILTER,
}


def _add_render_inputs(
    concept: BuildConcept,
    input_concepts: list[BuildConcept],
    input_addresses: set[str],
    available_by_address: dict[str, BuildConcept],
    seen: set[str] | None = None,
) -> None:
    if concept.address in input_addresses:
        return
    seen = seen or set()
    if concept.address in seen:
        return
    seen.add(concept.address)
    available = available_by_address.get(concept.address)
    if available is not None:
        input_concepts.append(available)
        input_addresses.add(available.address)
        return
    if concept.lineage is None:
        return
    for arg in concept.lineage.concept_arguments:
        _add_render_inputs(
            arg, input_concepts, input_addresses, available_by_address, seen
        )


def gen_aggregate(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """GROUP BY at the outputs' shared grain over already-built parents.

    Forces a real GROUP source_type when any output has non-standard
    grouping (ROLLUP/CUBE/GROUPING_SETS) — the GroupNode's grain-match
    shortcut would otherwise drop the GROUP BY entirely, losing the
    subtotal rows the rollup adds (q14). Mirrors v3 `gen_group_node`."""
    has_non_standard_grouping = any(
        isinstance(c.lineage, BuildAggregateWrapper)
        and c.lineage.grouping != AggregateGroupingMode.STANDARD
        for c in outputs
    )
    input_concepts = parent_outputs_needed(outputs, parents, conditions)
    input_addresses = {concept.address for concept in input_concepts}
    available_by_address = {
        concept.address: concept
        for parent in parents
        for concept in parent.output_concepts
    }

    for output in outputs:
        if not isinstance(output.lineage, BuildAggregateWrapper):
            continue
        for arg in output.lineage.function.arguments:
            if (
                isinstance(arg, BuildConcept)
                and arg.derivation in _ROW_PRESERVING_AGGREGATE_INPUT_DERIVATIONS
            ):
                _add_render_inputs(
                    arg, input_concepts, input_addresses, available_by_address
                )

    return GroupNode(
        output_concepts=outputs,
        input_concepts=input_concepts,
        environment=environment,
        parents=parents,
        conditions=conditions.conditional if conditions else None,
        preexisting_conditions=(
            preexisting_conditions.conditional if preexisting_conditions else None
        ),
        force_group=True if has_non_standard_grouping else None,
    )
