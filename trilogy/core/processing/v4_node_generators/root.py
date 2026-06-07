"""ROOT generator: pick or join datasources for requested concepts."""

from typing import cast

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import decompose_condition
from trilogy.core.processing.nodes import History, SelectNode, StrategyNode
from trilogy.core.processing.v4_helper.condition_injection import (
    ConditionSources,
    condition_row_args,
    inject_condition_at_node,
    split_existence_atoms,
)
from trilogy.core.processing.v4_helper.source_planning import SourceRequest, plan_source
from trilogy.core.processing.v4_helper.source_policy import (
    STRICT_SOURCE_POLICY,
    SourcePolicy,
)


def _outputs_with_grain_keys(
    outputs: list[BuildConcept],
    environment: BuildEnvironment,
) -> list[BuildConcept]:
    addresses: set[str] = set()
    for concept in outputs:
        addresses.add(concept.address)
        if concept.grain is not None:
            addresses.update(concept.grain.components)
        addresses.update(concept.keys or set())
    return [
        environment.concepts[address]
        for address in sorted(addresses)
        if address in environment.concepts
    ]


def _resolve_root_condition_sources(
    node: StrategyNode,
    conditions: BuildWhereClause,
    environment: BuildEnvironment,
    g,
    history: History,
    source_policy: SourcePolicy,
) -> ConditionSources:
    from trilogy.core.processing.concept_strategies_v4 import V4History, search_concepts

    sources = ConditionSources()
    v4_history = cast(V4History, history)
    produced = {concept.address for concept in node.usable_outputs}
    row_args = [
        concept
        for concept in condition_row_args(conditions)
        if concept.address not in produced
    ]
    if row_args:
        row_search = _outputs_with_grain_keys(row_args, environment)
        row_info = search_concepts(
            mandatory_list=row_search,
            history=v4_history,
            environment=environment,
            depth=1,
            g=g,
            conditions=[],
            source_policy=source_policy,
        )
        if row_info.strategy_node is None:
            return sources
        sources.row_concepts = row_args
        sources.row_parents.append(row_info.strategy_node)

    seen_existence: set[str] = set()
    seen_parents: set[int] = set()
    for arg_group in conditions.existence_arguments or ():
        existence_args = [
            concept for concept in arg_group if concept.address not in seen_existence
        ]
        if not existence_args:
            continue
        ex_info = search_concepts(
            mandatory_list=existence_args,
            history=v4_history,
            environment=environment,
            depth=1,
            g=g,
            conditions=[],
            source_policy=source_policy,
        )
        if ex_info.strategy_node is None:
            continue
        for concept in existence_args:
            seen_existence.add(concept.address)
            sources.existence_concepts.append(concept)
        if id(ex_info.strategy_node) not in seen_parents:
            seen_parents.add(id(ex_info.strategy_node))
            sources.existence_parents.append(ex_info.strategy_node)
    return sources


def _existence_arg_count(conditions: BuildWhereClause) -> int:
    return len(
        {
            concept.address
            for arg_group in conditions.existence_arguments or ()
            for concept in arg_group
        }
    )


def gen_root(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    *,
    history: History,
    g,
    source_policy: SourcePolicy = STRICT_SOURCE_POLICY,
) -> StrategyNode | None:
    """Source ROOT concepts through the v4 source planner.

    Existence-bearing atoms (`x IN <subselect>`) are applied in a wrapper so
    the existence feeder remains a side-channel parent rather than being pulled
    into the row stream.
    """
    row_conditions, existence_conditions = split_existence_atoms(conditions)

    inner_outputs: list[BuildConcept] = list(outputs)
    if existence_conditions is not None:
        seen = {c.address for c in inner_outputs}
        for atom in decompose_condition(existence_conditions.conditional):
            for arg in atom.row_arguments:
                if arg.address not in seen:
                    inner_outputs.append(arg)
                    seen.add(arg.address)

    node = plan_source(
        SourceRequest(
            outputs=inner_outputs,
            environment=environment,
            graph=g,
            history=history,
            conditions=row_conditions,
            source_policy=source_policy,
        )
    )
    if node is None and conditions is not None:
        fallback_outputs = _outputs_with_grain_keys(inner_outputs, environment)
        node = plan_source(
            SourceRequest(
                outputs=fallback_outputs,
                environment=environment,
                graph=g,
                history=history,
                conditions=None,
                source_policy=source_policy,
            )
        )
        if node is None:
            return None
        sources = _resolve_root_condition_sources(
            node, conditions, environment, g, history, source_policy
        )
        hidden = {concept.address for concept in fallback_outputs} - {
            concept.address for concept in outputs
        }
        return inject_condition_at_node(
            node,
            conditions,
            fallback_outputs,
            environment=environment,
            sources=sources,
            input_concepts=list(node.output_concepts) + sources.row_concepts,
            condition_on_merge=bool(sources.row_parents),
            hidden_concepts=hidden or None,
            combine_existing=False,
        )
    if node is None or existence_conditions is None:
        return node

    sources = (
        _resolve_root_condition_sources(
            node, existence_conditions, environment, g, history, source_policy
        )
        if _existence_arg_count(existence_conditions) == 1
        else ConditionSources()
    )
    if not sources.row_parents:
        return SelectNode(
            input_concepts=list(node.output_concepts),
            output_concepts=list(outputs),
            environment=environment,
            parents=[node, *sources.existence_parents],
            partial_concepts=list(node.partial_concepts),
            conditions=existence_conditions.conditional,
            existence_concepts=sources.existence_concepts,
        )
    return inject_condition_at_node(
        node,
        existence_conditions,
        list(outputs),
        environment=environment,
        sources=sources,
        input_concepts=list(node.output_concepts),
        combine_existing=False,
    )
