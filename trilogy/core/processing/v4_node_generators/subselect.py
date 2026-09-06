from trilogy.core.enums import Derivation
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import (
    BuildConcept,
    BuildSubselectItem,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import GroupNode, StrategyNode, SubselectNode
from trilogy.core.processing.v4_helper.history import V4History
from trilogy.utility import unique

from .common import collapse_conditions, parent_outputs_needed, search_parent


def gen_subselect(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    preexisting_conditions: BuildWhereClause | None = None,
    *,
    history: V4History,
    g: ReferenceGraph,
) -> StrategyNode | None:
    """Correlated subselect: the group graph supplies the OUTER (correlation)
    parent. For a cross-datasource subselect (`outer_arguments` set), the INNER
    select reads a separate datasource while the outer correlates on its own
    columns; plan it recursively and add it as a second parent so the
    SubselectNode can render the correlated sub-query.

    SubselectNode has no `conditions` slot, so an atom hosted here and the
    atoms an ancestor already applied both collapse into
    `preexisting_conditions`: recorded as applied, never re-rendered. The
    group graph hosts no atom at a SUBSELECT group, so `conditions` is
    expected to be None; a hosted atom would be recorded, not rendered."""
    combined = collapse_conditions(conditions, preexisting_conditions)

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
        inner = search_parent(inner_concepts, environment, history, g)
        if inner is None:
            return None
        inner_parents.append(inner)
        inner_inputs.extend(inner_concepts)

    # The inner select's columns are referenced only INSIDE the correlated
    # sub-query, so they are inputs (made available by the inner parent), not
    # outputs of this node; exposing them as outputs would make the resolver
    # base the CTE on the inner datasource and drop the outer correlation
    # columns from scope. The outer parent stays the driving row source.
    all_parents = list(parents) + inner_parents
    input_concepts = unique(
        parent_outputs_needed(outputs, parents, conditions) + inner_inputs,
        "address",
    )
    node: StrategyNode = SubselectNode(
        input_concepts=input_concepts,
        output_concepts=list(outputs),
        environment=environment,
        parents=all_parents,
        preexisting_conditions=combined,
    )
    # A non-correlated subselect is one global value, but the node computes it
    # once per parent row and its QDS grain (from the outputs) hides that, so
    # when NO output carries the parent's row grain, dedup to the output set
    # (per-row compute CTE, then a GROUP BY collapse CTE).
    if outputs and all(
        c.derivation == Derivation.SUBSELECT
        and isinstance(c.lineage, BuildSubselectItem)
        and not c.lineage.outer_arguments
        for c in outputs
    ):
        node = GroupNode(
            output_concepts=list(outputs),
            input_concepts=list(outputs),
            environment=environment,
            parents=[node],
            preexisting_conditions=combined,
        )
    return node
