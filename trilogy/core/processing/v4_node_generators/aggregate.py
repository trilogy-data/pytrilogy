from typing import List

from trilogy.core.enums import AggregateGroupingMode
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import GroupNode, StrategyNode

from .common import parent_outputs_needed


def gen_aggregate(
    outputs: List[BuildConcept],
    parents: List[StrategyNode],
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
    return GroupNode(
        output_concepts=outputs,
        input_concepts=parent_outputs_needed(outputs, parents, conditions),
        environment=environment,
        parents=parents,
        conditions=conditions.conditional if conditions else None,
        preexisting_conditions=(
            preexisting_conditions.conditional if preexisting_conditions else None
        ),
        force_group=True if has_non_standard_grouping else None,
    )
