from dataclasses import dataclass
from typing import List

from trilogy.constants import logger
from trilogy.core.enums import AggregateGroupingMode, Derivation, FunctionType
from trilogy.core.internal import ALL_ROWS_CONCEPT
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildFilterItem,
    BuildFunction,
    BuildGrain,
    BuildWhereClause,
    LooseBuildConceptList,
    concept_is_relevant,
    resolve_concepts_with_equivalents,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.common import (
    ConditionExpression,
    _condition_available_from_parents,
    _preexisting_conditions_from_parents,
    gen_enrichment_node,
    resolve_function_parent_concepts,
)
from trilogy.core.processing.nodes import GroupNode, History, MergeNode, StrategyNode
from trilogy.core.processing.utility import create_log_lambda, padding
from trilogy.utility import unique

LOGGER_PREFIX = "[GEN_GROUP_NODE]"


@dataclass
class GroupOutputPlan:
    parent_concepts: List[BuildConcept]
    output_concepts: List[BuildConcept]
    grain_components: List[BuildConcept]


@dataclass
class ParentResolution:
    parent_concepts: List[BuildConcept]
    parent_input_concepts: List[BuildConcept]
    parents: List[StrategyNode]
    parent_source: StrategyNode | None
    parent_output_addr: set[str]
    can_reuse_parent_for_enrichment: bool


def _can_use_grouped_materialized_source(concept: BuildConcept) -> bool:
    if not isinstance(concept.lineage, BuildAggregateWrapper):
        return True
    return concept.lineage.function.operator in (FunctionType.COUNT, FunctionType.SUM)


def _shared_nonstandard_grouping(a: BuildConcept, b: BuildConcept) -> bool:
    if not isinstance(a.lineage, BuildAggregateWrapper):
        return False
    if not isinstance(b.lineage, BuildAggregateWrapper):
        return False
    if a.lineage.grouping == AggregateGroupingMode.STANDARD:
        return False
    if a.lineage.grouping != b.lineage.grouping:
        return False
    if [c.address for c in a.lineage.by] != [c.address for c in b.lineage.by]:
        return False
    if [[c.address for c in gs] for gs in a.lineage.grouping_sets] != [
        [c.address for c in gs] for gs in b.lineage.grouping_sets
    ]:
        return False
    return True


def _is_optional_group_output(concept: BuildConcept) -> bool:
    return isinstance(concept.lineage, (BuildAggregateWrapper, BuildFunction))


def _has_concrete_grain(concept: BuildConcept) -> bool:
    return bool(
        concept.grain
        and len(concept.grain.components) > 0
        and not concept.grain.abstract
    )


def _concept_addresses(concepts: List[BuildConcept]) -> set[str]:
    return {x.address for x in concepts}


def _extra_parent_concepts(
    aggregate_parents: List[BuildConcept],
    parent_concepts: List[BuildConcept],
) -> List[BuildConcept]:
    parent_addresses = _concept_addresses(parent_concepts)
    return [x for x in aggregate_parents if x.address not in parent_addresses]


def get_aggregate_grain(
    concept: BuildConcept, environment: BuildEnvironment
) -> BuildGrain:
    raw_parents: List[BuildConcept] = resolve_function_parent_concepts(
        concept, environment=environment
    )
    expanded: List[BuildConcept] = []
    for p in raw_parents:
        if isinstance(p.lineage, BuildAggregateWrapper):
            expanded.extend(p.lineage.by)
        else:
            expanded.append(p)
    parent_concepts = unique(expanded, "address")

    if _has_concrete_grain(concept):
        grain_components = [environment.concepts[c] for c in concept.grain.components]
        parent_concepts += grain_components
    return BuildGrain.from_concepts(parent_concepts)


def get_group_parent_inputs(
    parent_concepts: List[BuildConcept], environment: BuildEnvironment
) -> List[BuildConcept]:
    preserved: List[BuildConcept] = []
    for parent in parent_concepts:
        if not parent.is_aggregate or parent.grain.abstract:
            continue
        preserved += [
            environment.concepts[c]
            for c in parent.grain.components
            if c in environment.concepts
        ]
    return unique(parent_concepts + preserved, "address")


def _can_include_optional_aggregate(
    concept: BuildConcept,
    possible_agg: BuildConcept,
    parent_concepts: List[BuildConcept],
    target_parent_grain: BuildGrain,
    environment: BuildEnvironment,
    depth: int,
    allow_parent_subset: bool,
) -> tuple[bool, List[BuildConcept]]:
    aggregate_parents: List[BuildConcept] = resolve_function_parent_concepts(
        possible_agg,
        environment=environment,
    )
    comp_grain = get_aggregate_grain(possible_agg, environment)
    aggregate_parent_addresses = _concept_addresses(aggregate_parents)
    parent_addresses = _concept_addresses(parent_concepts)
    parent_sets_match = (
        aggregate_parent_addresses.issubset(parent_addresses)
        if allow_parent_subset
        else aggregate_parent_addresses == parent_addresses
    )
    if parent_sets_match:
        return True, []
    if comp_grain == target_parent_grain or _shared_nonstandard_grouping(
        concept, possible_agg
    ):
        return True, _extra_parent_concepts(aggregate_parents, parent_concepts)
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} cannot include optional agg {possible_agg.address}; "
        f"it has mismatched parent grain {comp_grain} vs local parent {target_parent_grain}"
    )
    return False, []


def _add_optional_aggregate_outputs(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    parent_concepts: List[BuildConcept],
    output_concepts: List[BuildConcept],
    target_parent_grain: BuildGrain,
    environment: BuildEnvironment,
    depth: int,
    concrete_grain: bool,
) -> tuple[List[BuildConcept], List[BuildConcept]]:
    for possible_agg in local_optional:
        if not _is_optional_group_output(possible_agg):
            continue
        if concrete_grain:
            if possible_agg.grain and possible_agg.grain != concept.grain:
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} mismatched equivalent group by with grain {possible_agg.grain} for {concept.address}"
                )
            if not possible_agg.grain or possible_agg.grain != concept.grain:
                continue
        else:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} considering optional agg {possible_agg.address} for {concept.address}"
            )
            if not possible_agg.grain.abstract:
                continue
        should_include, extra_parents = _can_include_optional_aggregate(
            concept=concept,
            possible_agg=possible_agg,
            parent_concepts=parent_concepts,
            target_parent_grain=target_parent_grain,
            environment=environment,
            depth=depth,
            allow_parent_subset=concrete_grain,
        )
        if not should_include:
            continue
        parent_concepts += extra_parents
        output_concepts.append(possible_agg)
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} found equivalent group by optional concept {possible_agg.address} for {concept.address}"
        )
    return unique(parent_concepts, "address"), unique(output_concepts, "address")


def _plan_group_outputs(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
) -> GroupOutputPlan:
    parent_concepts: List[BuildConcept] = unique(
        resolve_function_parent_concepts(concept, environment=environment), "address"
    )
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} parent concepts for {concept} {concept.lineage} are {[x.address for x in parent_concepts]} from group grain {concept.grain}"
    )
    output_concepts = [concept]
    grain_components = resolve_concepts_with_equivalents(
        concept.grain.components,
        environment,
        local_optional,
    )
    target_parent_grain = get_aggregate_grain(concept, environment)
    if _has_concrete_grain(concept):
        parent_concepts = unique(parent_concepts + grain_components, "address")
        output_concepts = unique(output_concepts + grain_components, "address")
        parent_concepts, output_concepts = _add_optional_aggregate_outputs(
            concept=concept,
            local_optional=local_optional,
            parent_concepts=parent_concepts,
            output_concepts=output_concepts,
            target_parent_grain=target_parent_grain,
            environment=environment,
            depth=depth,
            concrete_grain=True,
        )
    elif concept.grain.abstract:
        parent_concepts, output_concepts = _add_optional_aggregate_outputs(
            concept=concept,
            local_optional=local_optional,
            parent_concepts=parent_concepts,
            output_concepts=output_concepts,
            target_parent_grain=target_parent_grain,
            environment=environment,
            depth=depth,
            concrete_grain=False,
        )
    output_concepts = unique(
        output_concepts
        + [c for c in local_optional if c.derivation == Derivation.CONSTANT],
        "address",
    )
    return GroupOutputPlan(
        parent_concepts=parent_concepts,
        output_concepts=output_concepts,
        grain_components=grain_components,
    )


def _try_materialized_group_source(
    output_concepts: List[BuildConcept],
    concept: BuildConcept,
    environment: BuildEnvironment,
    g,
    depth: int,
    history: History,
    conditions: BuildWhereClause | None,
) -> StrategyNode | None:
    materialized = history.gen_select_node(
        output_concepts,
        environment,
        g,
        depth + 1,
        fail_if_not_found=False,
        conditions=conditions,
    )
    if materialized:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} found materialized aggregate source for {concept.address}"
        )
    return materialized


def _remaining_optional_outputs(
    local_optional: List[BuildConcept],
    parent_input_concepts: List[BuildConcept],
    output_concepts: List[BuildConcept],
) -> List[BuildConcept]:
    grouped_addresses = _concept_addresses(
        unique(parent_input_concepts + output_concepts, "address")
    )
    return [x for x in local_optional if x.address not in grouped_addresses]


def _can_try_wide_parent(
    remaining_optional: List[BuildConcept],
    parent_input_concepts: List[BuildConcept],
    conditions: BuildWhereClause | None,
) -> bool:
    return (
        bool(remaining_optional)
        and conditions is not None
        and all(
            not isinstance(x.lineage, BuildFilterItem)
            and x.derivation != Derivation.WINDOW
            and not concept_is_relevant(x, parent_input_concepts)
            for x in remaining_optional
        )
    )


def _source_parent_concepts(
    mandatory_list: List[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History,
    conditions: BuildWhereClause | None,
) -> StrategyNode | None:
    return source_concepts(
        mandatory_list=mandatory_list,
        environment=environment,
        g=g,
        depth=depth,
        history=history,
        conditions=conditions,
    )


def _resolve_parent_sources(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    parent_concepts: List[BuildConcept],
    output_concepts: List[BuildConcept],
    grain_components: List[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History,
    conditions: BuildWhereClause | None,
) -> ParentResolution | None:
    parent_concepts = unique(
        [x for x in parent_concepts if not x.name == ALL_ROWS_CONCEPT], "address"
    )
    parent_input_concepts = get_group_parent_inputs(parent_concepts, environment)
    remaining_optional = _remaining_optional_outputs(
        local_optional, parent_input_concepts, output_concepts
    )
    can_try_wide_parent = _can_try_wide_parent(
        remaining_optional, parent_input_concepts, conditions
    )
    if remaining_optional and not can_try_wide_parent:
        narrow_reason = (
            "no conditions to reuse"
            if conditions is None
            else "optional depends on parent inputs or is filter-derived"
        )
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} keeping narrow parent for "
            f"{concept.address}; optional "
            f"{[x.address for x in remaining_optional]} not eligible: "
            f"{narrow_reason}"
        )
    mandatory_parent_concepts = unique(
        parent_input_concepts + (remaining_optional if can_try_wide_parent else []),
        "address",
    )
    parent_source = _source_parent_concepts(
        mandatory_list=mandatory_parent_concepts,
        environment=environment,
        g=g,
        depth=depth,
        source_concepts=source_concepts,
        history=history,
        conditions=conditions,
    )
    if not parent_source:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} group by node parents unresolvable"
        )
        return None
    parent_output_addr = _concept_addresses(parent_source.usable_outputs)
    can_reuse_wide_parent = can_try_wide_parent
    can_reuse_parent_for_enrichment = False
    parent_required = None
    if can_reuse_wide_parent:
        resolved_parent_source = parent_source.resolve()
        parent_required = GroupNode.check_if_required(
            parent_input_concepts,
            [resolved_parent_source],
            environment,
            depth,
        )
        enrichment_output = unique(grain_components + remaining_optional, "address")
        enrichment_required = GroupNode.check_if_required(
            enrichment_output,
            [resolved_parent_source],
            environment,
            depth,
        )
        if enrichment_required.required:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} wide parent for "
                f"{concept.address} would require regroup to extract "
                f"optional output; fetching narrow parent"
            )
            can_reuse_wide_parent = False
            parent_source = _source_parent_concepts(
                mandatory_list=parent_input_concepts,
                environment=environment,
                g=g,
                depth=depth,
                source_concepts=source_concepts,
                history=history,
                conditions=conditions,
            )
            if not parent_source:
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} group by narrow node parents unresolvable"
                )
                return None
            parent_output_addr = _concept_addresses(parent_source.usable_outputs)
        else:
            can_reuse_parent_for_enrichment = True
    parent: StrategyNode
    if can_reuse_wide_parent:
        if parent_required and parent_required.required:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} fetched widened parent for "
                f"{concept.address} with optional "
                f"{[x.address for x in remaining_optional]}; regrouping to "
                f"{[x.address for x in parent_input_concepts]}"
            )
            parent = GroupNode(
                output_concepts=parent_input_concepts,
                input_concepts=parent_input_concepts,
                environment=environment,
                parents=[parent_source],
                depth=depth,
                conditions=_group_conditions_to_apply([parent_source], conditions),
                preexisting_conditions=_preexisting_conditions_from_parents(
                    [parent_source], conditions
                ),
            )
        else:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} fetched widened parent for "
                f"{concept.address} with optional "
                f"{[x.address for x in remaining_optional]}; parent grain "
                "already satisfies group input"
            )
            parent = parent_source
    else:
        parent = parent_source
    return ParentResolution(
        parent_concepts=parent_concepts,
        parent_input_concepts=parent_input_concepts,
        parents=[parent],
        parent_source=parent_source,
        parent_output_addr=parent_output_addr,
        can_reuse_parent_for_enrichment=can_reuse_parent_for_enrichment,
    )


def _empty_parent_resolution() -> ParentResolution:
    return ParentResolution(
        parent_concepts=[],
        parent_input_concepts=[],
        parents=[],
        parent_source=None,
        parent_output_addr=set(),
        can_reuse_parent_for_enrichment=False,
    )


def _group_conditions_to_apply(
    parents: List[StrategyNode],
    conditions: BuildWhereClause | None,
) -> ConditionExpression | None:
    if conditions is None:
        return None
    if _preexisting_conditions_from_parents(parents, conditions):
        return None
    if _condition_available_from_parents(parents, conditions.conditional):
        return conditions.conditional
    return None


def _build_group_node(
    concept: BuildConcept,
    output_concepts: List[BuildConcept],
    parent_resolution: ParentResolution,
    environment: BuildEnvironment,
    depth: int,
    conditions: BuildWhereClause | None,
) -> GroupNode:
    input_concepts = (
        parent_resolution.parent_input_concepts
        if parent_resolution.parent_concepts
        else parent_resolution.parent_concepts
    )
    return GroupNode(
        output_concepts=output_concepts,
        input_concepts=input_concepts,
        environment=environment,
        parents=parent_resolution.parents,
        depth=depth,
        force_group=(
            concept.is_aggregate
            and isinstance(concept.lineage, BuildAggregateWrapper)
            and concept.lineage.grouping != AggregateGroupingMode.STANDARD
        ),
        preexisting_conditions=conditions.conditional if conditions else None,
        required_outputs=input_concepts,
    )


def _reuse_wide_parent_for_enrichment(
    concept: BuildConcept,
    group_node: GroupNode,
    output_concepts: List[BuildConcept],
    missing_optional: List[BuildConcept],
    grain_components: List[BuildConcept],
    parent_resolution: ParentResolution,
    environment: BuildEnvironment,
    depth: int,
    conditions: BuildWhereClause | None,
) -> StrategyNode | None:
    if (
        not parent_resolution.can_reuse_parent_for_enrichment
        or parent_resolution.parent_source is None
    ):
        return None
    missing_addr = _concept_addresses(missing_optional)
    if not missing_addr.issubset(parent_resolution.parent_output_addr):
        return None
    enrich_output = unique(grain_components + missing_optional, "address")
    parent_source = parent_resolution.parent_source
    enrichment_required = GroupNode.check_if_required(
        enrich_output,
        [parent_source.resolve()],
        environment,
        depth,
    )
    enrichment_node: StrategyNode
    if enrichment_required.required:
        enrichment_node = GroupNode(
            output_concepts=enrich_output,
            input_concepts=enrich_output,
            environment=environment,
            parents=[parent_source],
            depth=depth,
            conditions=_group_conditions_to_apply([parent_source], conditions),
            preexisting_conditions=_preexisting_conditions_from_parents(
                [parent_source], conditions
            ),
        )
    else:
        enrichment_node = parent_source
    merged_output = unique(output_concepts + missing_optional, "address")
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} enrichment reuses wide parent for "
        f"{concept.address}, missing {[x.address for x in missing_optional]}"
    )
    return MergeNode(
        input_concepts=merged_output,
        output_concepts=merged_output,
        environment=environment,
        parents=[group_node, enrichment_node],
        depth=depth,
        preexisting_conditions=_preexisting_conditions_from_parents(
            [group_node, enrichment_node], conditions
        ),
    )


def gen_group_node(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    group_plan = _plan_group_outputs(
        concept=concept,
        local_optional=local_optional,
        environment=environment,
        depth=depth,
    )
    parent_concepts = group_plan.parent_concepts
    output_concepts = group_plan.output_concepts
    grain_components = group_plan.grain_components
    materialized_outputs = unique(
        output_concepts
        + [c for c in local_optional if not _is_optional_group_output(c)],
        "address",
    )
    can_use_grouped_materialized = _can_use_grouped_materialized_source(concept)
    if can_use_grouped_materialized and len(materialized_outputs) > len(
        output_concepts
    ):
        materialized = _try_materialized_group_source(
            output_concepts=materialized_outputs,
            concept=concept,
            environment=environment,
            g=g,
            depth=depth,
            history=history,
            conditions=conditions,
        )
        if materialized:
            return materialized
    if parent_concepts:
        target_grain = BuildGrain.from_concepts(parent_concepts)
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} fetching group node parents {LooseBuildConceptList(concepts=parent_concepts)} with expected grain {target_grain}"
        )
        if can_use_grouped_materialized and grain_components:
            materialized = _try_materialized_group_source(
                output_concepts=unique(output_concepts, "address"),
                concept=concept,
                environment=environment,
                g=g,
                depth=depth,
                history=history,
                conditions=conditions,
            )
            if materialized:
                return materialized
        parent_resolution = _resolve_parent_sources(
            concept=concept,
            local_optional=local_optional,
            parent_concepts=parent_concepts,
            output_concepts=output_concepts,
            grain_components=grain_components,
            environment=environment,
            g=g,
            depth=depth,
            source_concepts=source_concepts,
            history=history,
            conditions=conditions,
        )
        if not parent_resolution:
            return None
    else:
        parent_resolution = _empty_parent_resolution()

    group_node = _build_group_node(
        concept=concept,
        output_concepts=output_concepts,
        parent_resolution=parent_resolution,
        environment=environment,
        depth=depth,
        conditions=conditions,
    )

    if not local_optional:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no optional concepts, returning group node"
        )
        return group_node
    grouped_output_addr = _concept_addresses(group_node.usable_outputs)
    missing_optional = [
        x for x in local_optional if x.address not in grouped_output_addr
    ]
    if not missing_optional:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no extra enrichment needed for group node, has all of {[x.address for x in local_optional]}"
        )
        return group_node
    enrichment = _reuse_wide_parent_for_enrichment(
        concept=concept,
        group_node=group_node,
        output_concepts=output_concepts,
        missing_optional=missing_optional,
        grain_components=grain_components,
        parent_resolution=parent_resolution,
        environment=environment,
        depth=depth,
        conditions=conditions,
    )
    if enrichment:
        return enrichment
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} group node for {concept.address} requires enrichment, missing {[x.address for x in missing_optional]}"
    )
    return gen_enrichment_node(
        group_node,
        join_keys=grain_components,
        local_optional=local_optional,
        environment=environment,
        g=g,
        depth=depth,
        source_concepts=source_concepts,
        log_lambda=create_log_lambda(
            LOGGER_PREFIX + f" for {concept.address}", depth, logger
        ),
        history=history,
        conditions=conditions,
    )
