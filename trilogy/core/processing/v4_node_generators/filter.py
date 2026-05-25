from typing import List

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import FilterNode, StrategyNode

from .common import parent_outputs_needed


def gen_filter(
    outputs: List[BuildConcept],
    parents: List[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """Apply a row filter over already-built parents. The filter expression
    comes from the filter concept's lineage; `conditions` here is any
    additional clause injected at this group."""
    return FilterNode(
        input_concepts=parent_outputs_needed(outputs, parents, conditions),
        output_concepts=outputs,
        environment=environment,
        parents=parents,
        conditions=conditions.conditional if conditions else None,
        preexisting_conditions=(
            preexisting_conditions.conditional if preexisting_conditions else None
        ),
    )
