from __future__ import annotations

from typing import TYPE_CHECKING

from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.union_node import build_layers, is_union
from trilogy.core.processing.nodes import SelectNode, StrategyNode, UnionNode
from trilogy.core.processing.v4_helper.source_policy import (
    STRICT_SOURCE_POLICY,
    SourcePolicy,
)

from .common import parent_outputs_needed

if TYPE_CHECKING:
    from trilogy.core.processing.concept_strategies_v4 import V4History


def gen_union(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
    *,
    history: V4History,
    g: ReferenceGraph,
    source_policy: SourcePolicy = STRICT_SOURCE_POLICY,
) -> StrategyNode | None:
    """Stack parent outputs into a UNION ALL. Each parent contributes one
    arm; the union node is responsible for column-aligning them. UnionNode
    has no `conditions` arg, so new-at-this-group atoms collapse into
    `preexisting_conditions` alongside the inherited ones."""
    from trilogy.core.processing.concept_strategies_v4 import search_concepts

    union_outputs = [output for output in outputs if is_union(output)]
    if not union_outputs:
        return None
    layers, resolved = build_layers(union_outputs)
    if not layers or not resolved:
        return None
    parent_nodes: list[StrategyNode] = []
    for layer in layers:
        parent = search_concepts(
            mandatory_list=layer,
            history=history,
            environment=environment,
            depth=0,
            g=g,
            source_policy=source_policy,
            conditions=[conditions] if conditions else [],
        ).strategy_node
        if parent is None:
            return None
        parent.add_output_concepts(resolved)
        parent_nodes.append(
            SelectNode(
                input_concepts=list(parent.output_concepts),
                output_concepts=resolved,
                environment=environment,
                parents=[parent],
            )
        )

    new = conditions.conditional if conditions else None
    pre = preexisting_conditions.conditional if preexisting_conditions else None
    combined = new if pre is None else (pre if new is None else (new + pre))
    return UnionNode(
        input_concepts=parent_outputs_needed(resolved, parent_nodes, conditions),
        output_concepts=resolved,
        environment=environment,
        parents=parent_nodes,
        preexisting_conditions=combined,
    )
