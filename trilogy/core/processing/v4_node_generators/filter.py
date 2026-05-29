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
    """Apply a row filter over already-built parents. Filter doesn't reshape
    rows or remove columns — it just selects a subset of rows. So pass
    through every parent output as well as the filter's own primaries; any
    downstream consumer (e.g. an aggregate that needs a grain key) can then
    reach back through the filter without us having to predict its needs at
    build time. The optimizer will prune unused columns later."""
    pass_through: list[BuildConcept] = []
    seen: set[str] = {c.address for c in outputs}
    for parent in parents:
        for output in parent.output_concepts:
            if output.address not in seen:
                pass_through.append(output)
                seen.add(output.address)
    full_outputs = list(outputs) + pass_through
    return FilterNode(
        input_concepts=parent_outputs_needed(full_outputs, parents, conditions),
        output_concepts=full_outputs,
        environment=environment,
        parents=parents,
        conditions=conditions.conditional if conditions else None,
        preexisting_conditions=(
            preexisting_conditions.conditional if preexisting_conditions else None
        ),
    )
