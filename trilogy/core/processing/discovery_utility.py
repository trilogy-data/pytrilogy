from typing import List

from trilogy.constants import logger
from trilogy.core.enums import Derivation, Purpose, SourceType
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildFilterItem,
    BuildGrain,
    BuildRowsetItem,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import QueryDatasource, UnnestJoin
from trilogy.core.processing.nodes import GroupNode, MergeNode, StrategyNode
from trilogy.core.processing.utility import GroupRequiredResponse


def depth_to_prefix(depth: int) -> str:
    return "\t" * depth


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
                logger.info(f"adding left grain {left.grain} for join key {key.left}")
                grain += left.grain
                seen.add(left.name)
            keys = [key.right for key in pairs]
            join_grain = BuildGrain.from_concepts(keys)
            if join_grain == join.right_datasource.grain:
                logger.info(f"irrelevant right join {join}, does not change grain")
            else:
                logger.info(
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
                logger.info(f"adding unjoined grain {x.grain} for datasource {x.name}")
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
        f"{padding}{LOGGER_PREFIX} Group requirement check: upstream grain: {comp_grain}, desired grain: {target_grain} from , difference {[x.address for x in difference]}"
    )
    for x in difference:
        logger.info(
            f"{padding}{LOGGER_PREFIX} Difference concept {x.address} purpose {x.purpose} keys {x.keys}"
        )

    # if the difference is all unique properties whose keys are in the source grain
    # we can also suppress the group
    if all(
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
    if all([x.purpose == Purpose.KEY for x in difference]):
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


def get_priority_concept(
    all_concepts: List[BuildConcept],
    attempted_addresses: set[str],
    found_concepts: set[str],
    partial_concepts: set[str],
    depth: int,
) -> BuildConcept:
    # optimized search for missing concepts
    pass_one = sorted(
        [
            c
            for c in all_concepts
            if c.address not in attempted_addresses
            and (c.address not in found_concepts or c.address in partial_concepts)
        ],
        key=lambda x: x.address,
    )
    # sometimes we need to scan intermediate concepts to get merge keys or filter keys,
    # so do an exhaustive search
    # pass_two = [c for c in all_concepts if c.address not in attempted_addresses]

    for remaining_concept in (pass_one,):
        priority = (
            # then multiselects to remove them from scope
            [c for c in remaining_concept if c.derivation == Derivation.MULTISELECT]
            +
            # then rowsets to remove them from scope, as they cannot get partials
            [c for c in remaining_concept if c.derivation == Derivation.ROWSET]
            +
            # then rowsets to remove them from scope, as they cannot get partials
            [c for c in remaining_concept if c.derivation == Derivation.UNION]
            # we should be home-free here
            +
            # then aggregates to remove them from scope, as they cannot get partials
            [c for c in remaining_concept if c.derivation == Derivation.AGGREGATE]
            # then windows to remove them from scope, as they cannot get partials
            + [c for c in remaining_concept if c.derivation == Derivation.WINDOW]
            # then filters to remove them from scope, also cannot get partials
            + [c for c in remaining_concept if c.derivation == Derivation.FILTER]
            # unnests are weird?
            + [c for c in remaining_concept if c.derivation == Derivation.UNNEST]
            + [c for c in remaining_concept if c.derivation == Derivation.RECURSIVE]
            + [c for c in remaining_concept if c.derivation == Derivation.BASIC]
            + [c for c in remaining_concept if c.derivation == Derivation.GROUP_TO]
            + [c for c in remaining_concept if c.derivation == Derivation.CONSTANT]
            # finally our plain selects
            + [
                c for c in remaining_concept if c.derivation == Derivation.ROOT
            ]  # and any non-single row constants
        )

        priority += [
            c
            for c in remaining_concept
            if c.address not in [x.address for x in priority]
        ]
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
