"""Shared helpers for v4 node generators."""

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import MergeNode, SelectNode, StrategyNode
from trilogy.core.processing.v4_helper.condition_injection import condition_row_args


def parent_outputs_needed(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    conditions: BuildWhereClause | None = None,
) -> list[BuildConcept]:
    """Which parent outputs do `outputs` consume (via their lineage), plus
    any concepts the optional `conditions` references? Returned in parent
    order, deduped by address."""
    referenced: set[str] = set()
    for c in outputs:
        if c.lineage is not None:
            for arg in c.lineage.concept_arguments:
                referenced.add(arg.address)
        # Also pass-through: an output may be a direct parent output.
        referenced.add(c.address)
    for arg in condition_row_args(conditions):
        referenced.add(arg.address)

    result: list[BuildConcept] = []
    seen: set[str] = set()
    for parent in parents:
        for output in parent.output_concepts:
            if output.address in referenced and output.address not in seen:
                seen.add(output.address)
                result.append(output)
    return result


def outputs_with_parent_grain_keys(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
) -> list[BuildConcept]:
    """Expose a derived output's declared grain keys when parents provide them."""
    parent_outputs: dict[str, BuildConcept] = {}
    for parent in parents:
        for concept in parent.output_concepts:
            parent_outputs.setdefault(concept.address, concept)

    result = list(outputs)
    seen = {concept.address for concept in result}
    for output in outputs:
        if output.lineage is None or output.grain is None:
            continue
        for address in sorted(output.grain.components):
            parent_concept = parent_outputs.get(address)
            if parent_concept is None or address in seen:
                continue
            seen.add(address)
            result.append(parent_concept)
    return result


def passthrough_if_materialized(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None,
    preexisting_conditions: BuildWhereClause | None,
) -> StrategyNode | None:
    """If every requested output is already materialized by a parent, return a
    plain projection over the parent(s) instead of re-deriving.

    A row-shape barrier (UNNEST/WINDOW) that the group graph re-derived as a
    condition-phase duplicate arrives here with its own output already supplied
    by the upstream barrier it sits on. Re-running UNNEST/window then would
    double-expand the rows and render the barrier inline in an invalid spot
    (`WHERE unnest(...)`). Projecting the existing column is the correct
    no-op. Returns None when a genuine derivation is still required."""
    if not parents:
        return None
    provided = {o.address for p in parents for o in p.output_concepts}
    if not all(o.address in provided for o in outputs):
        return None
    full_outputs = outputs_with_parent_grain_keys(outputs, parents)
    inputs = parent_outputs_needed(full_outputs, parents, conditions)
    cond = conditions.conditional if conditions else None
    pre = preexisting_conditions.conditional if preexisting_conditions else None
    if len(parents) == 1:
        return SelectNode(
            input_concepts=inputs,
            output_concepts=full_outputs,
            environment=environment,
            parents=parents,
            conditions=cond,
            preexisting_conditions=pre,
        )
    return MergeNode(
        input_concepts=inputs,
        output_concepts=full_outputs,
        environment=environment,
        parents=parents,
        conditions=cond,
        preexisting_conditions=pre,
    )
