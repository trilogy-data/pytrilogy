"""Shared helpers for v4 node generators."""

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.processing.nodes import StrategyNode
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
