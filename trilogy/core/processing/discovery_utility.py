from typing import List

from trilogy.constants import logger
from trilogy.core.enums import (
    Derivation,
    FunctionType,
    Granularity,
    Purpose,
    SourceType,
)
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildFilterItem,
    BuildFunction,
    BuildGrain,
    BuildRowsetItem,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import QueryDatasource, UnnestJoin
from trilogy.core.processing.constants import ROOT_DERIVATIONS
from trilogy.core.processing.nodes import GroupNode, MergeNode, StrategyNode
from trilogy.core.processing.utility import GroupRequiredResponse
from trilogy.utility import unique


def depth_to_prefix(depth: int) -> str:
    return "\t" * depth


NO_PUSHDOWN_DERIVATIONS: list[Derivation] = ROOT_DERIVATIONS + [
    Derivation.BASIC,
    Derivation.ROWSET,
    Derivation.UNNEST,
]


LOGGER_PREFIX = "[DISCOVERY LOOP]"


def calculate_effective_parent_grain(
    node: QueryDatasource | BuildDatasource,
) -> BuildGrain:
    # calculate the effective grain of the parent node
    # this is the union of all parent grains
    if isinstance(node, QueryDatasource):
        grain = BuildGrain()
        qds = node
        if not qds.joins:
            return qds.datasources[0].grain
        seen = set()
        for join in qds.joins:
            if isinstance(join, UnnestJoin):
                grain += BuildGrain(components=set([x.address for x in join.concepts]))
                continue
            pairs = join.concept_pairs or []
            for key in pairs:
                left = key.existing_datasource
                logger.debug(f"adding left grain {left.grain} for join key {key.left}")
                grain += left.grain
                seen.add(left.name)
            keys = [key.right for key in pairs]
            join_grain = BuildGrain.from_concepts(keys)
            if join_grain == join.right_datasource.grain:
                logger.debug(f"irrelevant right join {join}, does not change grain")
            else:
                logger.debug(
                    f"join changes grain, adding {join.right_datasource.grain} to {grain}"
                )
                grain += join.right_datasource.grain
            seen.add(join.right_datasource.name)
        for x in qds.datasources:
            # if we haven't seen it, it's still contributing to grain
            # unless used ONLY in a subselect
            # so the existence check is a [bad] proxy for that
            if x.name not in seen and not (
                qds.condition
                and qds.condition.existence_arguments
                and any(
                    [
                        c.address in block
                        for c in x.output_concepts
                        for block in qds.condition.existence_arguments
                    ]
                )
            ):
                logger.debug(f"adding unjoined grain {x.grain} for datasource {x.name}")
                grain += x.grain
        return grain
    else:
        return node.grain or BuildGrain()


def check_if_group_required(
    downstream_concepts: List[BuildConcept],
    parents: list[QueryDatasource | BuildDatasource],
    environment: BuildEnvironment,
    depth: int = 0,
) -> GroupRequiredResponse:
    padding = "\t" * depth
    target_grain = BuildGrain.from_concepts(
        downstream_concepts,
        environment=environment,
    )

    comp_grain = BuildGrain()
    for source in parents:
        # comp_grain += source.grain
        comp_grain += calculate_effective_parent_grain(source)

    # dynamically select if we need to group
    # we must avoid grouping if we are already at grain
    if comp_grain.abstract and not target_grain.abstract:
        logger.info(
            f"{padding}{LOGGER_PREFIX} Group requirement check: upstream grain is abstract, cannot determine grouping requirement, assuming group required"
        )
        return GroupRequiredResponse(target_grain, comp_grain, True)
    if comp_grain.issubset(target_grain):

        logger.info(
            f"{padding}{LOGGER_PREFIX} Group requirement check:  {comp_grain}, target: {target_grain}, grain is subset of target, no group node required"
        )
        return GroupRequiredResponse(target_grain, comp_grain, False)
    # find out what extra is in the comp grain vs target grain
    difference = [
        environment.concepts[c] for c in (comp_grain - target_grain).components
    ]
    logger.info(
        f"{padding}{LOGGER_PREFIX} Group requirement check: upstream grain: {comp_grain}, desired grain: {target_grain} from, difference {[x.address for x in difference]}"
    )
    for x in difference:
        logger.info(
            f"{padding}{LOGGER_PREFIX} Difference concept {x.address} purpose {x.purpose} keys {x.keys}"
        )

    # if the difference is all unique properties whose keys are in the source grain
    # we can also suppress the group
    if difference and all(
        [
            x.keys
            and all(
                environment.concepts[z].address in comp_grain.components for z in x.keys
            )
            for x in difference
        ]
    ):
        logger.info(
            f"{padding}{LOGGER_PREFIX} Group requirement check: skipped due to unique property validation"
        )
        return GroupRequiredResponse(target_grain, comp_grain, False)
    if difference and all([x.purpose == Purpose.KEY for x in difference]):
        logger.info(
            f"{padding}{LOGGER_PREFIX} checking if downstream is unique properties of key"
        )
        replaced_grain_raw: list[set[str]] = [
            (
                x.keys or set()
                if x.purpose == Purpose.UNIQUE_PROPERTY
                else set([x.address])
            )
            for x in downstream_concepts
            if x.address in target_grain.components
        ]
        # flatten the list of lists
        replaced_grain = [item for sublist in replaced_grain_raw for item in sublist]
        # if the replaced grain is a subset of the comp grain, we can skip the group
        unique_grain_comp = BuildGrain.from_concepts(
            replaced_grain, environment=environment
        )
        if comp_grain.issubset(unique_grain_comp):
            logger.info(
                f"{padding}{LOGGER_PREFIX} Group requirement check: skipped due to unique property validation"
            )
            return GroupRequiredResponse(target_grain, comp_grain, False)
    logger.info(
        f"{padding}{LOGGER_PREFIX} Checking for grain equivalence for filters and rowsets"
    )
    ngrain = []
    for con in target_grain.components:
        full = environment.concepts[con]
        if full.derivation == Derivation.ROWSET:
            ngrain.append(full.address.split(".", 1)[1])
        elif full.derivation == Derivation.FILTER:
            assert isinstance(full.lineage, BuildFilterItem)
            if isinstance(full.lineage.content, BuildConcept):
                ngrain.append(full.lineage.content.address)
        else:
            ngrain.append(full.address)
    target_grain2 = BuildGrain.from_concepts(
        ngrain,
        environment=environment,
    )
    if comp_grain.issubset(target_grain2):
        logger.info(
            f"{padding}{LOGGER_PREFIX} Group requirement check: {comp_grain}, {target_grain2}, pre rowset grain is subset of target, no group node required"
        )
        return GroupRequiredResponse(target_grain2, comp_grain, False)

    logger.info(f"{padding}{LOGGER_PREFIX} Group requirement check: group required")
    return GroupRequiredResponse(target_grain, comp_grain, True)


def group_if_required_v2(
    root: StrategyNode,
    final: List[BuildConcept],
    environment: BuildEnvironment,
    where_injected: set[str] | None = None,
    depth: int = 0,
):
    where_injected = where_injected or set()
    required = check_if_group_required(
        downstream_concepts=final,
        parents=[root.resolve()],
        environment=environment,
        depth=depth,
    )
    targets = [
        x
        for x in root.output_concepts
        if x.address in final or any(c in final for c in x.pseudonyms)
    ]
    if required.required:
        if isinstance(root, MergeNode):
            root.force_group = True
            root.set_output_concepts(targets, rebuild=False, change_visibility=False)
            root.rebuild_cache()
            return root
        elif isinstance(root, GroupNode) and root.source_type == SourceType.BASIC:
            # we need to group this one more time
            pass
        elif isinstance(root, GroupNode):
            if set(x.address for x in final) != set(
                x.address for x in root.output_concepts
            ):
                allowed_outputs = [
                    x
                    for x in root.output_concepts
                    if not (
                        x.address in where_injected
                        and x.address not in (root.required_outputs or set())
                    )
                ]

                logger.info(
                    f"Adjusting group node outputs to remove injected concepts {where_injected}: remaining {allowed_outputs}"
                )
                root.set_output_concepts(allowed_outputs)
            return root
        return GroupNode(
            output_concepts=targets,
            input_concepts=targets,
            environment=environment,
            parents=[root],
            partial_concepts=root.partial_concepts,
            preexisting_conditions=root.preexisting_conditions,
        )
    elif isinstance(root, GroupNode):

        return root
    else:
        root.set_output_concepts(targets, rebuild=False, change_visibility=False)
    return root


def get_upstream_concepts(base: BuildConcept, nested: bool = False) -> set[str]:
    upstream = set()
    if nested:
        upstream.add(base.address)
    if not base.lineage:
        return upstream
    for x in base.lineage.concept_arguments:
        # if it's derived from any value in a rowset, ALL rowset items are upstream
        if x.derivation == Derivation.ROWSET:
            assert isinstance(x.lineage, BuildRowsetItem), type(x.lineage)
            for y in x.lineage.rowset.select.output_components:
                upstream.add(f"{x.lineage.rowset.name}.{y.address}")
                # upstream = upstream.union(get_upstream_concepts(y, nested=True))
        upstream = upstream.union(get_upstream_concepts(x, nested=True))
    return upstream


def evaluate_loop_condition_pushdown(
    mandatory: list[BuildConcept],
    conditions: BuildWhereClause | None,
    depth: int,
    force_no_condition_pushdown: bool,
    forced_pushdown: list[BuildConcept],
) -> BuildWhereClause | None:
    # filter evaluation
    # always pass the filter up when we aren't looking at all filter inputs
    # or there are any non-filter complex types
    if not conditions:
        return None
    # first, check if we *have* to push up conditions above complex derivations
    if forced_pushdown:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Force including conditions to push filtering above complex concepts {forced_pushdown} that are not condition row inputs {conditions.row_arguments} or parent"
        )
        return conditions
    # otherwise, only prevent pushdown
    # (forcing local condition evaluation)
    # only if all condition inputs are here and we only have roots
    should_evaluate_filter_on_this_level_not_push_down = all(
        [x.address in mandatory for x in conditions.row_arguments]
    ) and not any(
        [
            x.derivation not in (ROOT_DERIVATIONS + [Derivation.BASIC])
            for x in mandatory
            if x.address not in conditions.row_arguments
        ]
    )

    if (
        force_no_condition_pushdown
        or should_evaluate_filter_on_this_level_not_push_down
    ):
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Forcing condition evaluation at this level: all basic_no_agg: {should_evaluate_filter_on_this_level_not_push_down}"
        )
        return None

    return conditions


def generate_candidates_restrictive(
    priority_concept: BuildConcept,
    candidates: list[BuildConcept],
    exhausted: set[str],
    # conditions_exist: bool,
) -> list[BuildConcept]:
    unselected_candidates = [
        x for x in candidates if x.address != priority_concept.address
    ]
    local_candidates = [
        x
        for x in unselected_candidates
        if x.address not in exhausted
        and x.granularity != Granularity.SINGLE_ROW
        and x.address not in priority_concept.pseudonyms
        and priority_concept.address not in x.pseudonyms
    ]

    # if it's single row, joins are irrelevant. Fetch without keys.
    if priority_concept.granularity == Granularity.SINGLE_ROW:
        logger.info("Have single row concept, including only other single row optional")
        optional = (
            [
                x
                for x in unselected_candidates
                if x.granularity == Granularity.SINGLE_ROW
                and x.address not in priority_concept.pseudonyms
                and priority_concept.address not in x.pseudonyms
            ]
            if priority_concept.derivation == Derivation.AGGREGATE
            else []
        )
        return optional
    return local_candidates


def get_priority_concept(
    all_concepts: List[BuildConcept],
    attempted_addresses: set[str],
    found_concepts: set[str],
    partial_concepts: set[str],
    depth: int,
) -> BuildConcept:
    # optimized search for missing concepts
    all_concepts_local = all_concepts
    pass_one = sorted(
        [
            c
            for c in all_concepts_local
            if c.address not in attempted_addresses
            and (c.address not in found_concepts or c.address in partial_concepts)
        ],
        key=lambda x: x.address,
    )

    priority = (
        # then multiselects to remove them from scope
        [c for c in pass_one if c.derivation == Derivation.MULTISELECT]
        +
        # then rowsets to remove them from scope, as they cannot get partials
        [c for c in pass_one if c.derivation == Derivation.ROWSET]
        +
        # then rowsets to remove them from scope, as they cannot get partials
        [c for c in pass_one if c.derivation == Derivation.UNION]
        # we should be home-free here
        + [c for c in pass_one if c.derivation == Derivation.BASIC]
        +
        # then aggregates to remove them from scope, as they cannot get partials
        [c for c in pass_one if c.derivation == Derivation.AGGREGATE]
        # then windows to remove them from scope, as they cannot get partials
        + [c for c in pass_one if c.derivation == Derivation.WINDOW]
        # then filters to remove them from scope, also cannot get partials
        + [c for c in pass_one if c.derivation == Derivation.FILTER]
        # unnests are weird?
        + [c for c in pass_one if c.derivation == Derivation.UNNEST]
        + [c for c in pass_one if c.derivation == Derivation.RECURSIVE]
        + [c for c in pass_one if c.derivation == Derivation.GROUP_TO]
        + [c for c in pass_one if c.derivation == Derivation.CONSTANT]
        # finally our plain selects
        + [
            c for c in pass_one if c.derivation == Derivation.ROOT
        ]  # and any non-single row constants
    )

    priority += [c for c in pass_one if c.address not in [x.address for x in priority]]
    final = []
    # if any thing is derived from another concept
    # get the derived copy first
    # as this will usually resolve cleaner
    for x in priority:
        if any(
            [
                x.address
                in get_upstream_concepts(
                    c,
                )
                for c in priority
            ]
        ):
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} delaying fetch of {x.address} as parent of another concept"
            )
            continue
        final.append(x)
    # then append anything we didn't get
    for x2 in priority:
        if x2 not in final:
            final.append(x2)
    if final:
        return final[0]
    raise ValueError(
        f"Cannot resolve query. No remaining priority concepts, have attempted {attempted_addresses} out of {all_concepts} with found {found_concepts}"
    )


def is_pushdown_aliased_concept(c: BuildConcept) -> bool:
    return (
        isinstance(c.lineage, BuildFunction)
        and c.lineage.operator == FunctionType.ALIAS
        and isinstance(c.lineage.arguments[0], BuildConcept)
        and c.lineage.arguments[0].derivation not in NO_PUSHDOWN_DERIVATIONS
    )


def get_inputs_that_require_pushdown(
    conditions: BuildWhereClause | None, mandatory: list[BuildConcept]
) -> list[BuildConcept]:
    if not conditions:
        return []
    return [
        x
        for x in mandatory
        if x.address not in conditions.row_arguments
        and (
            x.derivation not in NO_PUSHDOWN_DERIVATIONS
            or is_pushdown_aliased_concept(x)
        )
    ]


def get_loop_iteration_targets(
    mandatory: list[BuildConcept],
    conditions: BuildWhereClause | None,
    attempted: set[str],
    force_conditions: bool,
    found: set[str],
    partial: set[str],
    depth: int,
    materialized_canonical: set[str],
) -> tuple[BuildConcept, List[BuildConcept], BuildWhereClause | None]:
    # objectives
    # 1. if we have complex types; push any conditions further up until we only have roots
    # 2. if we only have roots left, push all condition inputs into the candidate list
    # 3. from the final candidate list, select the highest priority concept to attempt next
    force_pushdown_to_complex_input = False

    pushdown_targets = get_inputs_that_require_pushdown(conditions, mandatory)
    if pushdown_targets:
        force_pushdown_to_complex_input = True
    # a list of all non-materialized concepts, or all concepts
    # if a pushdown is required
    all_concepts_local: list[BuildConcept] = [
        x
        for x in mandatory
        if force_pushdown_to_complex_input
        or (x.canonical_address not in materialized_canonical)
        # keep Root/Constant
        or x.derivation in (Derivation.ROOT, Derivation.CONSTANT)
    ]
    remaining_concrete = [x for x in mandatory if x.address not in all_concepts_local]

    for x in remaining_concrete:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX}  Adding materialized concept {x.address} as root instead of derived."
        )
        all_concepts_local.append(x.with_materialized_source())

    remaining = [x for x in all_concepts_local if x.address not in attempted]
    conditions = evaluate_loop_condition_pushdown(
        mandatory=all_concepts_local,
        conditions=conditions,
        depth=depth,
        force_no_condition_pushdown=force_conditions,
        forced_pushdown=pushdown_targets,
    )
    local_all = [*all_concepts_local]

    if all([x.derivation in (Derivation.ROOT,) for x in remaining]) and conditions:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} All remaining mandatory concepts are roots or constants, injecting condition inputs into candidate list"
        )
        local_all = unique(
            list(conditions.row_arguments) + remaining,
            "address",
        )
        conditions = None
    if conditions and force_conditions:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} condition evaluation at this level forced"
        )
        local_all = unique(
            list(conditions.row_arguments) + remaining,
            "address",
        )
        # if we have a forced pushdown, also push them down while keeping them at this level too
        conditions = conditions if force_pushdown_to_complex_input else None

    priority_concept = get_priority_concept(
        all_concepts=local_all,
        attempted_addresses=attempted,
        found_concepts=found,
        partial_concepts=partial,
        depth=depth,
    )

    optional = generate_candidates_restrictive(
        priority_concept=priority_concept,
        candidates=local_all,
        exhausted=attempted,
        # conditions_exist = conditions is not None,
        # depth=depth,
    )
    return priority_concept, optional, conditions
