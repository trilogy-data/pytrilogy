from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import StrategyNode, WindowNode

from .common import parent_outputs_needed, passthrough_if_materialized


def gen_window(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """Window functions (LEAD/LAG/RANK/...) over an already-built parent.
    WindowNode has no `conditions` arg — `conditions` (new at this group)
    and `preexisting_conditions` (atoms applied upstream) collapse into one
    `preexisting_conditions` since the WindowNode can't filter on its own."""
    passthrough = passthrough_if_materialized(
        outputs, parents, environment, conditions, preexisting_conditions
    )
    if passthrough is not None:
        return passthrough
    new = conditions.conditional if conditions else None
    pre = preexisting_conditions.conditional if preexisting_conditions else None
    combined = new if pre is None else (pre if new is None else (new + pre))
    return WindowNode(
        input_concepts=parent_outputs_needed(outputs, parents, conditions),
        output_concepts=outputs,
        environment=environment,
        parents=parents,
        preexisting_conditions=combined,
    )
