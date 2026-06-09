from __future__ import annotations

from typing import TYPE_CHECKING

from trilogy.core.enums import Derivation
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import (
    BuildConcept,
    BuildSubselectItem,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import StrategyNode, SubselectNode
from trilogy.core.processing.v4_helper.source_policy import (
    STRICT_SOURCE_POLICY,
    SourcePolicy,
)
from trilogy.utility import unique

from .common import parent_outputs_needed

if TYPE_CHECKING:
    from trilogy.core.processing.concept_strategies_v4 import V4History


def gen_subselect(
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
    """Correlated subselect: the group graph supplies the OUTER (correlation)
    parent. For a cross-datasource subselect (`outer_arguments` set), the INNER
    select reads a separate datasource (`@close_warehouses` reads `warehouse_*`
    while the outer correlates on `customer_lat/lon`); plan it recursively and
    add it as a second parent so the SubselectNode can render the correlated
    sub-query. Mirrors v3 `gen_subselect_node`'s cross-datasource branch.

    SubselectNode has no `conditions` arg — both this-level and inherited atoms
    collapse into `preexisting_conditions`."""
    from trilogy.core.processing.concept_strategies_v4 import search_concepts

    new = conditions.conditional if conditions else None
    pre = preexisting_conditions.conditional if preexisting_conditions else None
    combined = new if pre is None else (pre if new is None else (new + pre))

    inner_parents: list[StrategyNode] = []
    inner_inputs: list[BuildConcept] = []
    for concept in outputs:
        if concept.derivation != Derivation.SUBSELECT or not isinstance(
            concept.lineage, BuildSubselectItem
        ):
            continue
        if not concept.lineage.outer_arguments:
            continue
        inner_concepts = unique(concept.lineage.inner_concept_arguments, "address")
        if not inner_concepts:
            continue
        info = search_concepts(
            mandatory_list=inner_concepts,
            history=history,
            environment=environment,
            depth=0,
            g=g,
            conditions=[],
            source_policy=source_policy,
        )
        if info.strategy_node is None:
            return None
        inner_parents.append(info.strategy_node)
        inner_inputs.extend(inner_concepts)

    # The inner select's columns are referenced only INSIDE the correlated
    # sub-query, so they are inputs (made available by the inner parent), not
    # outputs of this node — exposing them as outputs would make the resolver
    # base the CTE on the inner datasource and drop the outer correlation
    # columns from scope. The outer parent stays the driving row source.
    all_parents = list(parents) + inner_parents
    input_concepts = unique(
        parent_outputs_needed(outputs, parents, conditions) + inner_inputs,
        "address",
    )
    return SubselectNode(
        input_concepts=input_concepts,
        output_concepts=list(outputs),
        environment=environment,
        parents=all_parents,
        preexisting_conditions=combined,
    )
