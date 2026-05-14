from typing import List

from trilogy.constants import logger
from trilogy.core.enums import AggregateGroupingMode, FunctionType
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
    gen_enrichment_node,
    resolve_function_parent_concepts,
)
from trilogy.core.processing.nodes import GroupNode, History, MergeNode, StrategyNode
from trilogy.core.processing.utility import create_log_lambda, padding
from trilogy.utility import unique

LOGGER_PREFIX = "[GEN_GROUP_NODE]"


def _can_use_grouped_materialized_source(concept: BuildConcept) -> bool:
    if not isinstance(concept.lineage, BuildAggregateWrapper):
        return True
    return concept.lineage.function.operator in (FunctionType.COUNT, FunctionType.SUM)


def _shared_nonstandard_grouping(a: BuildConcept, b: BuildConcept) -> bool:
    """Two aggregates with the same non-standard grouping mode and same by-list
    must share a GROUP BY clause (ROLLUP/CUBE/GROUPING SETS) and therefore must
    co-locate in a single group node — even if their per-arg parent grains
    differ."""
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


def get_aggregate_grain(
    concept: BuildConcept, environment: BuildEnvironment
) -> BuildGrain:
    raw_parents: List[BuildConcept] = resolve_function_parent_concepts(
        concept, environment=environment
    )
    # Aggregate parents are values, not grain keys. Expand to their `by`
    # so two outer aggregates whose inner aggregates share a structural
    # grain (but differ only by filter/operator) compare equal.
    expanded: List[BuildConcept] = []
    for p in raw_parents:
        if isinstance(p.lineage, BuildAggregateWrapper):
            expanded.extend(p.lineage.by)
        else:
            expanded.append(p)
    parent_concepts = unique(expanded, "address")

    if (
        concept.grain
        and len(concept.grain.components) > 0
        and not concept.grain.abstract
    ):
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
    # aggregates MUST always group to the proper grain
    # except when the
    parent_concepts: List[BuildConcept] = unique(
        resolve_function_parent_concepts(concept, environment=environment), "address"
    )
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} parent concepts for {concept} {concept.lineage} are {[x.address for x in parent_concepts]} from group grain {concept.grain}"
    )

    # if the aggregation has a grain, we need to ensure these are the ONLY optional in the output of the select
    output_concepts = [concept]
    grain_components = resolve_concepts_with_equivalents(
        concept.grain.components,
        environment,
        local_optional,
    )
    if (
        concept.grain
        and len(concept.grain.components) > 0
        and not concept.grain.abstract
    ):

        parent_concepts += grain_components
        build_grain_parents = get_aggregate_grain(concept, environment)
        output_concepts += grain_components
        for possible_agg in local_optional:

            if not isinstance(
                possible_agg.lineage,
                (BuildAggregateWrapper, BuildFunction),
            ):
                continue
            if possible_agg.grain and possible_agg.grain != concept.grain:
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} mismatched equivalent group by with grain {possible_agg.grain} for {concept.address}"
                )

            if possible_agg.grain and possible_agg.grain == concept.grain:
                agg_parents: List[BuildConcept] = resolve_function_parent_concepts(
                    possible_agg,
                    environment=environment,
                )
                comp_grain = get_aggregate_grain(possible_agg, environment)
                if set([x.address for x in agg_parents]).issubset(
                    set([x.address for x in parent_concepts])
                ):
                    output_concepts.append(possible_agg)
                    logger.info(
                        f"{padding(depth)}{LOGGER_PREFIX} found equivalent group by optional concept {possible_agg.address} for {concept.address}"
                    )
                elif comp_grain == build_grain_parents or _shared_nonstandard_grouping(
                    concept, possible_agg
                ):
                    extra = [x for x in agg_parents if x.address not in parent_concepts]
                    parent_concepts += extra
                    output_concepts.append(possible_agg)
                    logger.info(
                        f"{padding(depth)}{LOGGER_PREFIX} found equivalent group by optional concept {possible_agg.address} for {concept.address}"
                    )
                else:
                    logger.info(
                        f"{padding(depth)}{LOGGER_PREFIX} cannot include optional agg {possible_agg.address}; it has mismatched parent grain {comp_grain } vs local parent {build_grain_parents}"
                    )
    elif concept.grain.abstract:
        for possible_agg in local_optional:
            if not isinstance(
                possible_agg.lineage,
                (BuildAggregateWrapper, BuildFunction),
            ):

                continue
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} considering optional agg {possible_agg.address} for {concept.address}"
            )
            agg_parents = resolve_function_parent_concepts(
                possible_agg,
                environment=environment,
            )
            comp_grain = get_aggregate_grain(possible_agg, environment)
            if not possible_agg.grain.abstract:
                continue
            if set([x.address for x in agg_parents]) == set(
                [x.address for x in parent_concepts]
            ):
                output_concepts.append(possible_agg)
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} found equivalent group by optional concept {possible_agg.address} for {concept.address}"
                )
            elif comp_grain == get_aggregate_grain(
                concept, environment
            ) or _shared_nonstandard_grouping(concept, possible_agg):
                extra = [x for x in agg_parents if x.address not in parent_concepts]
                parent_concepts += extra
                output_concepts.append(possible_agg)
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} found equivalent group by optional concept {possible_agg.address} for {concept.address}"
                )
            else:
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} cannot include optional agg {possible_agg.address}; it has mismatched parent grain {comp_grain } vs local parent {get_aggregate_grain(concept, environment)}"
                )
    materialized_outputs = unique(
        output_concepts
        + [
            c
            for c in local_optional
            if not isinstance(c.lineage, (BuildAggregateWrapper, BuildFunction))
        ],
        "address",
    )
    can_use_grouped_materialized = _can_use_grouped_materialized_source(concept)
    if can_use_grouped_materialized and len(materialized_outputs) > len(
        output_concepts
    ):
        materialized = history.gen_select_node(
            materialized_outputs,
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
    if parent_concepts:
        target_grain = BuildGrain.from_concepts(parent_concepts)
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} fetching group node parents {LooseBuildConceptList(concepts=parent_concepts)} with expected grain {target_grain}"
        )
        if can_use_grouped_materialized and grain_components:
            materialized = history.gen_select_node(
                unique(output_concepts, "address"),
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
        parent_concepts = unique(
            [x for x in parent_concepts if not x.name == ALL_ROWS_CONCEPT], "address"
        )
        parent_input_concepts = get_group_parent_inputs(parent_concepts, environment)
        grouped_addresses = {
            x.address
            for x in unique(parent_input_concepts + output_concepts, "address")
        }
        remaining_optional = [
            x for x in local_optional if x.address not in grouped_addresses
        ]
        can_try_wide_parent = (
            bool(remaining_optional)
            and conditions is not None
            and all(
                not isinstance(x.lineage, BuildFilterItem)
                and not concept_is_relevant(x, parent_input_concepts)
                for x in remaining_optional
            )
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
        parent_source: StrategyNode | None = source_concepts(
            mandatory_list=mandatory_parent_concepts,
            environment=environment,
            g=g,
            depth=depth,
            history=history,
            conditions=conditions,
        )
        if not parent_source:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} group by node parents unresolvable"
            )
            return None
        parent_output_addr = {c.address for c in parent_source.usable_outputs}
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
                parent_source = source_concepts(
                    mandatory_list=parent_input_concepts,
                    environment=environment,
                    g=g,
                    depth=depth,
                    history=history,
                    conditions=conditions,
                )
                if not parent_source:
                    logger.info(
                        f"{padding(depth)}{LOGGER_PREFIX} group by narrow node parents unresolvable"
                    )
                    return None
                parent_output_addr = {c.address for c in parent_source.usable_outputs}
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
                    preexisting_conditions=(
                        conditions.conditional if conditions else None
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
        parents: List[StrategyNode] = [parent]
    else:
        parent_input_concepts = []
        parent_source = None
        parent_output_addr = set()
        can_reuse_parent_for_enrichment = False
        parents = []

    group_node = GroupNode(
        output_concepts=output_concepts,
        input_concepts=parent_input_concepts if parent_concepts else parent_concepts,
        environment=environment,
        parents=parents,
        depth=depth,
        force_group=(
            concept.is_aggregate
            and isinstance(concept.lineage, BuildAggregateWrapper)
            and concept.lineage.grouping != AggregateGroupingMode.STANDARD
        ),
        preexisting_conditions=conditions.conditional if conditions else None,
        required_outputs=parent_input_concepts if parent_concepts else parent_concepts,
    )

    # early exit if no optional

    if not local_optional:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no optional concepts, returning group node"
        )
        return group_node
    grouped_output_addr = {x.address for x in group_node.usable_outputs}
    missing_optional = [
        x for x in local_optional if x.address not in grouped_output_addr
    ]
    if not missing_optional:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no extra enrichment needed for group node, has all of {[x.address for x in local_optional]}"
        )
        return group_node
    # If the wide parent already carries every missing optional, reuse it for
    # enrichment rather than making another source_concepts call. Only add a
    # grouping wrapper when the enrichment output grain actually needs one.
    if can_reuse_parent_for_enrichment and parent_source is not None:
        missing_addr = {x.address for x in missing_optional}
        if missing_addr.issubset(parent_output_addr):
            enrich_output = unique(grain_components + missing_optional, "address")
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
                    preexisting_conditions=(
                        conditions.conditional if conditions else None
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
                preexisting_conditions=(conditions.conditional if conditions else None),
            )
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
