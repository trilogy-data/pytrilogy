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
    preexisting_conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """Correlated subselect: parents provide the outer/inner inputs.
    SubselectNode has no `conditions` arg — both this-level and inherited
    atoms collapse into `preexisting_conditions`."""
    new = conditions.conditional if conditions else None
    pre = preexisting_conditions.conditional if preexisting_conditions else None
    combined = new if pre is None else (pre if new is None else (new + pre))
    return SubselectNode(
        input_concepts=parent_outputs_needed(outputs, parents, conditions),
        output_concepts=outputs,
        environment=environment,
        parents=parents,
        preexisting_conditions=combined,
    )
