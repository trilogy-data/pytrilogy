from typing import List

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import StrategyNode, SubselectNode

from .common import parent_outputs_needed


def gen_subselect(
    outputs: List[BuildConcept],
    parents: List[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """Correlated subselect: parents provide the outer/inner inputs."""
    return SubselectNode(
        input_concepts=parent_outputs_needed(outputs, parents, conditions),
        output_concepts=outputs,
        environment=environment,
        parents=parents,
        preexisting_conditions=conditions.conditional if conditions else None,
    )
