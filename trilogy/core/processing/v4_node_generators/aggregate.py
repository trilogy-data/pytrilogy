from typing import List

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import GroupNode, StrategyNode

from .common import parent_outputs_needed


def gen_aggregate(
    outputs: List[BuildConcept],
    parents: List[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """GROUP BY at the outputs' shared grain over already-built parents."""
    return GroupNode(
        output_concepts=outputs,
        input_concepts=parent_outputs_needed(outputs, parents, conditions),
        environment=environment,
        parents=parents,
        conditions=conditions.conditional if conditions else None,
    )
