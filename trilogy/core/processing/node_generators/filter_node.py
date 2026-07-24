from dataclasses import dataclass

from trilogy.constants import logger
from trilogy.core.enums import Derivation
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildFilterItem,
    BuildWhereClause,
    resolve_concepts_with_equivalents,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import is_scalar_condition
from trilogy.core.processing.node_generators.common import (
    resolve_filter_parent_concepts,
    resolve_function_parent_concepts,
)
from trilogy.core.processing.nodes import (
    FilterNode,
    GroupNode,
    History,
    MergeNode,
    SelectNode,
    StrategyNode,
)
from trilogy.core.processing.utility import padding
from trilogy.utility import unique

LOGGER_PREFIX = "[GEN_FILTER_NODE]"

FILTER_TYPES = (BuildFilterItem,)


@dataclass
class FilterParentPlan:
    parent_row_concepts: list[BuildConcept]
    parent_existence_concepts: list[tuple[BuildConcept, ...]]
    same_filter_optional: list[BuildConcept]
    row_output_concepts: list[BuildConcept]
    optimized_pushdown: bool
    grouped_pushdown: bool
    global_filter_is_local_filter: bool


def _concept_addresses(concepts: list[BuildConcept]) -> set[str]:
    return {concept.address for concept in concepts}


def _concepts_cover_optional(
    output_concepts: list[BuildConcept], local_optional: list[BuildConcept]
) -> bool:
    output_addresses = _concept_addresses(output_concepts)
    return all(x.address in output_addresses for x in local_optional)


def _optional_outputs(
    output_concepts: list[BuildConcept], local_optional: list[BuildConcept]
) -> list[BuildConcept]:
    optional_addresses = _concept_addresses(local_optional)
    return [x for x in output_concepts if x.address in optional_addresses]


def _missing_optional(
    output_concepts: list[BuildConcept], local_optional: list[BuildConcept]
) -> list[BuildConcept]:
    output_addresses = _concept_addresses(output_concepts)
    return [x for x in local_optional if x.address not in output_addresses]


def _same_concepts(left: list[BuildConcept], right: list[BuildConcept]) -> bool:
    return _concept_addresses(left) == _concept_addresses(right)


def _row_grain_outputs(
    output_concepts: list[BuildConcept], row_parent: StrategyNode
) -> list[BuildConcept]:
    # Narrowing outputs to [concept] + optionals can drop the row parent's
    # grain keys, advertising a coarser (false) grain; a downstream merge then
    # re-correlates rows on whatever non-unique columns survive (TPC-DS q74
    # joined web sums back to customers on first/last name). Keep any output
    # that carries a component of the parent grain, directly or by pseudonym.
    components = row_parent.resolve().grain.components
    return [
        x
        for x in output_concepts
        if x.address in components or components.intersection(x.pseudonyms)
    ]


def _aggregate_filter_parent_concepts(
    concept: BuildConcept,
    environment: BuildEnvironment,
) -> list[BuildConcept]:
    if not isinstance(concept.lineage, FILTER_TYPES):
        return []
    parents: list[BuildConcept] = []
    content = concept.lineage.content
    if isinstance(content, BuildConcept):
        parents.append(content)
    for condition_concept in concept.lineage.where.row_arguments:
        if isinstance(condition_concept.lineage, BuildAggregateWrapper):
            parents += resolve_function_parent_concepts(condition_concept, environment)
        else:
            parents.append(condition_concept)
    return unique(parents, "address")


def _resolve_parent_row_outputs(
    parent_row_concepts: list[BuildConcept],
    local_optional: list[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
) -> list[BuildConcept]:
    resolved = resolve_concepts_with_equivalents(
        [x.address for x in parent_row_concepts],
        environment,
        local_optional,
    )
    for parent, output in zip(parent_row_concepts, resolved):
        if parent.address == output.address:
            continue
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} found equivalent row optional "
            f"{output.address} for filter parent {parent.address}"
        )
    return unique(parent_row_concepts + resolved, "address")


def pushdown_filter_to_parent(
    concept: BuildConcept,
    environment: BuildEnvironment,
    local_optional: list[BuildConcept],
    conditions: BuildWhereClause | None,
    filter_where: BuildWhereClause,
    same_filter_optional: list[BuildConcept],
    depth: int,
) -> bool:
    optimized_pushdown = False
    if not is_scalar_condition(filter_where.conditional):
        optimized_pushdown = False
    elif not local_optional:
        optimized_pushdown = True
    elif conditions and conditions == filter_where:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} query conditions are the same as filter conditions, can optimize across all concepts"
        )
        optimized_pushdown = True
    elif _same_concepts(same_filter_optional, local_optional):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} all optional concepts are included in the filter, can optimize across all concepts"
        )
        optimized_pushdown = True
    # A predicate disjoint from local_optional is NOT safe to push into the
    # source WHERE: when the filtered concept feeds an aggregate grouped by an
    # optional key (`sum(x ? cond) by k`), dropping the non-qualifying rows also
    # drops any k-group with no qualifying row, but the aggregate-internal `?`
    # must keep it (emitting NULL/0), unlike a query-level WHERE. gen_filter_node
    # can't see whether a downstream aggregate/filter erases that distinction, so
    # we always keep the per-row CASE WHEN here (fuzzer:
    # edge__function__filtered_aggregates).
    return optimized_pushdown


def _can_pushdown_as_grouped_filter(
    concept: BuildConcept,
    local_optional: list[BuildConcept],
    parent_existence_concepts: list[tuple[BuildConcept, ...]],
    filter_where: BuildWhereClause,
) -> bool:
    if (
        local_optional
        or parent_existence_concepts
        or is_scalar_condition(filter_where.conditional)
    ):
        return False
    # Grouped pushdown collapses the filter and its aggregate predicate into a
    # single GroupNode whose only output is `concept`, with the predicate moved
    # to HAVING and the content rendered as the group key. That is only sound
    # when the content's own grain matches the predicate's grouping grain — i.e.
    # the content *is* the group key. `filter order_id where count(x) by
    # order_id > 1` qualifies (content `order_id` grain == agg by-grain). But
    # `filter customer.address.zip where zip_p_count > 10` does not: zip is a
    # property at `customer.address.id` grain while `zip_p_count` groups by
    # `customer.address.zip`, so there is no real group key to render — the
    # collapsed CTE would group by the content expression with the aggregate
    # inlined as a CASE WHEN, producing an invalid `GROUP BY count(...)`. Keep
    # filter and group separate there so the aggregate's by-grain survives.
    assert isinstance(concept.lineage, FILTER_TYPES)
    content = concept.lineage.content
    agg_args = [
        r for r in filter_where.row_arguments if r.derivation == Derivation.AGGREGATE
    ]
    if isinstance(content, BuildConcept):
        if any(a.grain != content.grain for a in agg_args):
            return False
    # An aggregate-comparison mixed with a row-level predicate on anything but
    # the group key itself can't collapse into one grouped CTE: the row
    # predicate is invalid in HAVING at the group grain, and pre-filtering rows
    # would corrupt the aggregate (it must be computed over all rows). Fall
    # back to the standard plan, which materializes the aggregate at its own
    # grain and evaluates the predicate at row level.
    content_address = content.address if isinstance(content, BuildConcept) else None
    if agg_args and any(
        r.derivation != Derivation.AGGREGATE and r.address != content_address
        for r in filter_where.row_arguments
    ):
        return False
    # A non-aggregate predicate on a *nested* filter concept (another `x ? cond`)
    # alongside the per-key aggregate(s) can't be collapsed into this group's
    # HAVING/WHERE: the nested filter needs its own per-row CASE materialized,
    # which the grouped form pushes into GROUP BY next to the aggregates (invalid
    # `GROUP BY CASE WHEN ... count(...) ...`) or splits off and regroups at the
    # wrong grain. Fall back to the standard filter-node plan, which computes the
    # predicate CASE at row grain over materialized aggregate columns.
    if any(
        r.derivation != Derivation.AGGREGATE and isinstance(r.lineage, BuildFilterItem)
        for r in filter_where.row_arguments
    ):
        return False
    return True


def build_parent_concepts(
    concept: BuildConcept,
    environment: BuildEnvironment,
    local_optional: list[BuildConcept],
    conditions: BuildWhereClause | None = None,
    depth: int = 0,
) -> FilterParentPlan:
    parent_row_concepts, parent_existence_concepts = resolve_filter_parent_concepts(
        concept, environment
    )
    if not isinstance(concept.lineage, FILTER_TYPES):
        raise SyntaxError('Filter node must have a filter type lineage"')
    filter_where = concept.lineage.where

    same_filter_optional: list[BuildConcept] = []
    global_filter_is_local_filter: bool = bool(
        conditions and conditions == filter_where
    )

    exact_partial_matches = True
    for x in local_optional:
        if isinstance(x.lineage, FILTER_TYPES):
            if {arg.address for arg in x.lineage.where.concept_arguments} == {
                arg.address for arg in filter_where.concept_arguments
            }:
                exact_partial_matches = (
                    exact_partial_matches and x.lineage.where == filter_where
                )
                logger.info(
                    f"{padding(depth)}{LOGGER_PREFIX} fetching parents for peer {x.address} (of {concept.address})"
                )

                for arg in x.lineage.content_concept_arguments:
                    if arg.address not in parent_row_concepts:
                        parent_row_concepts.append(arg)
                same_filter_optional.append(x)
                continue
        elif global_filter_is_local_filter:
            same_filter_optional.append(x)
            # also append it to the parent row concepts
            parent_row_concepts.append(x)

    # sometimes, it's okay to include other local optional above the filter
    # in case it is, prep our list
    extra_row_level_optional: list[BuildConcept] = []

    same_filter_optional_addresses = _concept_addresses(same_filter_optional)
    for x in local_optional:
        if x.address in same_filter_optional_addresses:
            continue
        extra_row_level_optional.append(x)
    grouped_pushdown = _can_pushdown_as_grouped_filter(
        concept,
        local_optional,
        parent_existence_concepts,
        filter_where,
    )
    is_optimized_pushdown = grouped_pushdown or (
        exact_partial_matches
        and pushdown_filter_to_parent(
            concept,
            environment,
            local_optional,
            conditions,
            filter_where,
            same_filter_optional,
            depth,
        )
    )

    if grouped_pushdown:
        parent_row_concepts = _aggregate_filter_parent_concepts(concept, environment)
    if not is_optimized_pushdown:
        parent_row_concepts += extra_row_level_optional
    elif not grouped_pushdown:
        # New "disjoint" pushdown branch can leave behind grain-key concepts
        # that weren't pulled in by same_filter_optional's content scan. The
        # parent CTE will carry the pushed WHERE and must still project them,
        # so include them as row-level parents to fetch alongside the source.
        seen = {c.address for c in parent_row_concepts}
        for x in extra_row_level_optional:
            if x.address not in seen:
                parent_row_concepts.append(x)
                seen.add(x.address)
    row_output_concepts = _resolve_parent_row_outputs(
        parent_row_concepts,
        local_optional,
        environment,
        depth,
    )
    return FilterParentPlan(
        parent_row_concepts=parent_row_concepts,
        parent_existence_concepts=parent_existence_concepts,
        same_filter_optional=same_filter_optional,
        row_output_concepts=row_output_concepts,
        optimized_pushdown=is_optimized_pushdown,
        grouped_pushdown=grouped_pushdown,
        global_filter_is_local_filter=global_filter_is_local_filter,
    )


def add_existence_sources(
    core_parent_nodes: list[StrategyNode],
    parent_existence_concepts: list[tuple[BuildConcept, ...]],
    source_concepts,
    environment,
    g,
    depth,
    history,
) -> bool:
    for existence_tuple in parent_existence_concepts:
        if not existence_tuple:
            continue
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} fetching filter node existence parents {[x.address for x in existence_tuple]}"
        )
        parent_existence = source_concepts(
            mandatory_list=list(existence_tuple),
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
        )
        if not parent_existence:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} filter existence node parents could not be found"
            )
            return False
        core_parent_nodes.append(parent_existence)
    return True


def gen_filter_node(
    concept: BuildConcept,
    local_optional: list[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    if not isinstance(concept.lineage, FILTER_TYPES):
        raise SyntaxError('Filter node must have a filter type lineage"')
    where = concept.lineage.where

    parent_plan = build_parent_concepts(
        concept,
        environment=environment,
        local_optional=local_optional,
        conditions=conditions,
        depth=depth,
    )
    parent_row_concepts = parent_plan.parent_row_concepts
    parent_existence_concepts = parent_plan.parent_existence_concepts
    same_filter_optional = parent_plan.same_filter_optional
    row_output_concepts = parent_plan.row_output_concepts
    optimized_pushdown = parent_plan.optimized_pushdown
    grouped_pushdown = parent_plan.grouped_pushdown
    global_filter_is_local_filter = parent_plan.global_filter_is_local_filter

    row_parent: StrategyNode = source_concepts(
        mandatory_list=parent_row_concepts,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
        conditions=conditions,
    )

    core_parent_nodes: list[StrategyNode] = []
    flattened_existence = [x for y in parent_existence_concepts for x in y]
    if parent_existence_concepts:
        if not add_existence_sources(
            core_parent_nodes,
            parent_existence_concepts,
            source_concepts,
            environment,
            g,
            depth,
            history,
        ):
            # An existence subquery (e.g. a membership RHS) couldn't be sourced
            # — bail so the search reports a clean UnresolvableQueryException
            # instead of emitting a dangling INVALID_REFERENCE_BUG CTE.
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} filter node existence parents could not be sourced; abandoning filter node"
            )
            return None

    if not row_parent:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} filter node row parents {[x.address for x in parent_row_concepts]} could not be found"
        )
        return None
    else:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} filter node has row parents {[x.address for x in parent_row_concepts]} from node with output [{[x.address for x in row_parent.output_concepts]}] partial {row_parent.partial_concepts}"
        )
    if grouped_pushdown:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} returning grouped filter node "
            f"with pushdown to HAVING {where.conditional}"
        )
        return GroupNode(
            input_concepts=parent_row_concepts,
            output_concepts=[concept],
            environment=environment,
            parents=[row_parent],
            conditions=where.conditional,
            preexisting_conditions=conditions.conditional if conditions else None,
            force_group=True,
        )
    if global_filter_is_local_filter:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} filter node conditions match global conditions adding row parent {row_parent.output_concepts} with condition {where.conditional}"
        )
        row_parent.add_parents(core_parent_nodes)
        # all local optional will be in the parent already, so we can set outputs
        row_parent.set_output_concepts([concept] + local_optional)
        return row_parent
    if optimized_pushdown:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} returning optimized filter node with pushdown to parent with condition {where.conditional} across {[concept] + same_filter_optional + row_parent.output_concepts} "
        )
        if isinstance(row_parent, SelectNode):
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} nesting select node in strategy node"
            )
            parent = StrategyNode(
                input_concepts=row_parent.output_concepts,
                output_concepts=[concept]
                + same_filter_optional
                + row_output_concepts
                + row_parent.output_concepts,
                environment=row_parent.environment,
                parents=[row_parent],
                depth=row_parent.depth,
                partial_concepts=row_parent.partial_concepts,
                # Inherit the global query condition row_parent already applied
                # so it stays visible as preexisting after the filter's where is
                # appended below; otherwise condition validation can't prove it.
                preexisting_conditions=row_parent.preexisting_conditions,
                force_group=False,
            )
        else:
            parent = row_parent
            parent.add_output_concepts(
                [concept] + same_filter_optional + row_output_concepts
            )
        parent.add_parents(core_parent_nodes)
        if not parent.preexisting_conditions == where.conditional:
            # add_condition appends ``where.conditional`` to the parent's
            # ``conditions`` (correct), but also resets ``preexisting_conditions``
            # to just the filter's where — clobbering any global query
            # conditions previously injected upstream. Downstream condition
            # validation requires the global to still be visible as
            # preexisting on this node, so restore it after appending.
            prior_pre = parent.preexisting_conditions
            parent.add_condition(where.conditional)
            if prior_pre is not None:
                parent.set_preexisting_conditions(prior_pre)
        parent.add_existence_concepts(flattened_existence, False)
        # parent.grain = BuildGrain.from_concepts(
        #     parent.output_concepts,
        #     environment=environment,
        # )
        parent.rebuild_cache()
        filter_node = parent
    else:
        core_parent_nodes.append(row_parent)
        # With no optional concepts requested, non-qualifying rows can never be
        # observed downstream, so apply the predicate as a node condition (the
        # aggregate operands are materialized parent columns, so it renders as
        # a plain WHERE). The per-row CASE alone would leak a NULL group for
        # rows that fail the filter. With optionals (or peer filters with a
        # different where), those rows must survive, so only the CASE applies.
        sole_output = not local_optional and not parent_existence_concepts
        filter_node = FilterNode(
            input_concepts=unique(
                parent_row_concepts + flattened_existence,
                "address",
            ),
            output_concepts=[concept] + same_filter_optional + row_output_concepts,
            environment=environment,
            parents=core_parent_nodes,
            conditions=where.conditional if sole_output else None,
            preexisting_conditions=conditions.conditional if conditions else None,
        )

    if not local_optional or _concepts_cover_optional(
        filter_node.output_concepts, local_optional
    ):
        optional_outputs = _optional_outputs(
            filter_node.output_concepts, local_optional
        )
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no extra enrichment needed for filter node, has all of {[x.address for x in local_optional]}"
        )
        filter_node.set_output_concepts(
            unique(
                [concept]
                + optional_outputs
                + _row_grain_outputs(filter_node.output_concepts, row_parent),
                "address",
            )
        )
        return filter_node
    missing_optional = _missing_optional(filter_node.output_concepts, local_optional)
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} need to enrich filter node with additional concepts {[x.address for x in missing_optional]}"
    )
    enrich_node: StrategyNode = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=parent_row_concepts + missing_optional,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
        conditions=conditions,
    )
    if not enrich_node:
        logger.error(
            f"{padding(depth)}{LOGGER_PREFIX} filter node enrichment node could not be found"
        )
        return filter_node
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} returning filter node and enrich node with {enrich_node.output_concepts} and {enrich_node.input_concepts}"
    )
    return MergeNode(
        input_concepts=filter_node.output_concepts + enrich_node.output_concepts,
        output_concepts=[
            concept,
        ]
        + local_optional,
        environment=environment,
        parents=[
            filter_node,
            enrich_node,
        ],
        preexisting_conditions=conditions.conditional if conditions else None,
    )
