"""Shared helpers for v4 node generators."""

from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import BoolExpr, BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import MergeNode, SelectNode, StrategyNode
from trilogy.core.processing.v4_helper.condition_injection import condition_row_args
from trilogy.core.processing.v4_helper.history import V4History


def collapse_conditions(
    conditions: BuildWhereClause | None,
    preexisting_conditions: BuildWhereClause | None,
) -> BoolExpr | None:
    """AND the atoms new at this group with the ones an ancestor already applied.

    For node types with no `conditions` slot of their own (window, union,
    subselect) the distinction has nowhere to live, so both collapse into the
    single `preexisting_conditions` the node does carry."""
    new = conditions.conditional if conditions else None
    pre = preexisting_conditions.conditional if preexisting_conditions else None
    if pre is None:
        return new
    if new is None:
        return pre
    return new + pre


def search_parent(
    concepts: list[BuildConcept],
    environment: BuildEnvironment,
    history: V4History,
    g: ReferenceGraph,
    depth: int = 0,
    conditions: list[BuildWhereClause] | None = None,
    complete_partials: bool = True,
    staged_conditions: list[BuildWhereClause] | None = None,
) -> StrategyNode | None:
    """Plan `concepts` as an independent sub-search and return its producer.

    The single re-entry point into the planner for generators that source
    something the group graph did not hand them as a parent (a union arm, a
    correlated subselect's inner select, a condition feeder)."""
    from trilogy.core.processing.concept_strategies_v4 import search_concepts

    return search_concepts(
        mandatory_list=concepts,
        history=history,
        environment=environment,
        depth=depth,
        g=g,
        conditions=conditions or [],
        complete_partials=complete_partials,
        staged_conditions=staged_conditions,
    ).strategy_node


def parent_outputs_needed(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    conditions: BuildWhereClause | None = None,
) -> list[BuildConcept]:
    """Which parent outputs do `outputs` consume (via their lineage), plus
    any concepts the optional `conditions` references? Returned in parent
    order, deduped by address."""
    referenced: set[str] = set()
    for c in outputs:
        if c.lineage is not None:
            for arg in c.lineage.concept_arguments:
                referenced.add(arg.address)
        # Also pass-through: an output may be a direct parent output.
        referenced.add(c.address)
    for arg in condition_row_args(conditions):
        referenced.add(arg.address)

    result: list[BuildConcept] = []
    seen: set[str] = set()
    for parent in parents:
        for output in parent.output_concepts:
            if output.address in referenced and output.address not in seen:
                seen.add(output.address)
                result.append(output)
    return result


def outputs_with_parent_grain_keys(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
) -> list[BuildConcept]:
    """Expose a derived output's declared grain keys when parents provide them."""
    parent_outputs: dict[str, BuildConcept] = {}
    for parent in parents:
        for concept in parent.output_concepts:
            parent_outputs.setdefault(concept.address, concept)

    result = list(outputs)
    seen = {concept.address for concept in result}
    for output in outputs:
        if output.lineage is None or output.grain is None:
            continue
        for address in sorted(output.grain.components):
            parent_concept = parent_outputs.get(address)
            if parent_concept is None or address in seen:
                continue
            seen.add(address)
            result.append(parent_concept)
    return result


def passthrough_if_materialized(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None,
    preexisting_conditions: BuildWhereClause | None,
) -> StrategyNode | None:
    """If every requested output is already materialized by a parent, return a
    plain projection over the parent(s) instead of re-deriving.

    A row-shape barrier (UNNEST/WINDOW) that the group graph re-derived as a
    condition-phase duplicate arrives here with its own output already supplied
    by the upstream barrier it sits on. Re-running UNNEST/window then would
    double-expand the rows and render the barrier inline in an invalid spot
    (`WHERE unnest(...)`). Projecting the existing column is the correct
    no-op. Returns None when a genuine derivation is still required."""
    if not parents:
        return None
    provided = {o.address for p in parents for o in p.output_concepts}
    if not all(o.address in provided for o in outputs):
        return None
    full_outputs = outputs_with_parent_grain_keys(outputs, parents)
    inputs = parent_outputs_needed(full_outputs, parents, conditions)
    cond = conditions.conditional if conditions else None
    pre = preexisting_conditions.conditional if preexisting_conditions else None
    if len(parents) == 1:
        return SelectNode(
            input_concepts=inputs,
            output_concepts=full_outputs,
            environment=environment,
            parents=parents,
            conditions=cond,
            preexisting_conditions=pre,
        )
    return MergeNode(
        input_concepts=inputs,
        output_concepts=full_outputs,
        environment=environment,
        parents=parents,
        conditions=cond,
        preexisting_conditions=pre,
    )
