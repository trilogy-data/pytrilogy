from collections import defaultdict
from itertools import combinations
from typing import List, Optional


from preql.constants import logger
from preql.core.enums import PurposeLineage
from preql.core.env_processor import generate_graph
from preql.core.graph_models import ReferenceGraph
from preql.core.models import (
    Concept,
    Environment,
)
from preql.core.processing.utility import (
    get_disconnected_components,
)
from preql.utility import unique
from preql.core.processing.nodes import (
    ConstantNode,
    MergeNode,
    GroupNode,
    StrategyNode,
)
from preql.core.processing.nodes.base_node import concept_list_to_grain
from preql.core.processing.node_generators import (
    gen_filter_node,
    gen_window_node,
    gen_group_node,
    gen_basic_node,
    gen_select_node,
    gen_static_select_node,
    gen_unnest_node,
)

LOGGER_PREFIX = "[CONCEPT DETAIL]"


def throw_helpful_error(
    mandatory_concepts: list[Concept],
    optional_concepts: List[Concept],
    extra_msg: Optional[str] = "",
):
    error_msg_required = [c.address for c in mandatory_concepts]
    error_msg_optional = [c.address for c in optional_concepts]
    raise ValueError(
        f"Could not find any way to associate required concepts {error_msg_required} and optional {error_msg_optional}. {extra_msg}"
    )


def get_priority_concept(
    all_concepts: List[Concept], attempted_addresses: List[str]
) -> Concept:
    remaining_concept = [
        c for c in all_concepts if c.address not in attempted_addresses
    ]
    priority = (
        [c for c in remaining_concept if c.derivation == PurposeLineage.AGGREGATE]
        + [c for c in remaining_concept if c.derivation == PurposeLineage.WINDOW]
        + [c for c in remaining_concept if c.derivation == PurposeLineage.FILTER]
        + [c for c in remaining_concept if c.derivation == PurposeLineage.UNNEST]
        + [c for c in remaining_concept if c.derivation == PurposeLineage.BASIC]
        + [c for c in remaining_concept if not c.lineage]
        + [c for c in remaining_concept if c.derivation == PurposeLineage.CONSTANT]
    )
    if not priority:
        raise ValueError(
            f"Cannot resolve query. No remaining priority concepts, have attempted {attempted_addresses}"
        )
    return priority[0]


def get_local_optional(
    optional_concepts: List[Concept],
    mandatory_concepts: List[Concept],
    concept: Concept,
    local_prefix: str,
) -> tuple[List[Concept], List[Concept]]:
    # we don't  want to look for multiple aggregates at the same time
    # don't ever push a constant upstream - apply at end
    local_optional_staging = unique(
        [
            x
            for x in optional_concepts + mandatory_concepts
            if x.address != concept.address
        ],
        "address",
    )

    local_mandatory_staging = unique(
        [
            x
            for x in optional_concepts + mandatory_concepts
            if x.address != concept.address
        ],
        "address",
    )

    # reduce search space to actual grain
    return (
        concept_list_to_grain(local_optional_staging, []).components_copy,
        concept_list_to_grain(local_mandatory_staging, []).components_copy,
    )


def recurse_or_fail(
    depth,
    environment: Environment,
    g,
    found_concepts: set[Concept],
    mandatory_concepts: List[Concept],
    optional_concepts: List[Concept],
    local_prefix,
    accept_partial: bool = False,
) -> StrategyNode:
    candidates = [
        x for x in found_concepts if x.purpose and x not in mandatory_concepts
    ]
    if not candidates:
        # terminal state one - no options to discard
        throw_helpful_error(
            mandatory_concepts, optional_concepts, "No candidates found in iteration"
        )
    # want to make the miimum amount of new concepts
    # mandatory, as finding a match is less and less likely
    # as we require more
    combos = []
    for x in range(0, len(candidates) + 1):
        for combo in combinations(candidates, x):
            combos.append(mandatory_concepts + list(combo))
    attempt = []
    if depth > 50:
        throw_helpful_error(
            mandatory_concepts, optional_concepts, "Recursion violation"
        )
    for new_mandatory in reversed(combos):
        attempt = new_mandatory
        if (
            set(x.address for x in new_mandatory)
            == set(x.address for x in mandatory_concepts)
            and not optional_concepts
        ):
            continue
        logger.info(
            f"{local_prefix}{LOGGER_PREFIX} attempting to modified subset of option concepts"
            f" {','.join([str(c) for c in new_mandatory])}"
        )
        try:
            return source_concepts(
                mandatory_concepts=new_mandatory,
                optional_concepts=[
                    x for x in optional_concepts if x not in new_mandatory
                ],
                environment=environment,
                g=g,
                depth=depth + 1,
                accept_partial=accept_partial,
            )
        except ValueError:
            logger.debug(
                f"failed to find {[c.address for c in new_mandatory]} and optional {[c.address for c in optional_concepts]}"
            )
            continue
    # terminal state two - have gone through all options
    process_optional = [x.address for x in attempt]
    throw_helpful_error(
        mandatory_concepts, optional_concepts, "Last attempt: " + str(process_optional)
    )
    raise ValueError("Should never get here, for type-hinting")


def source_concepts(
    mandatory_concepts: List[Concept],
    optional_concepts: List[Concept],
    environment: Environment,
    g: Optional[ReferenceGraph] = None,
    depth: int = 0,
    accept_partial: bool = False,
) -> StrategyNode:
    """Mandatory concepts are those which must be included in the output
    Optional concepts may be dropped"""
    g = g or generate_graph(environment)
    local_prefix = "\t" * depth
    stack: List[StrategyNode] = []
    all_concepts: List[Concept] = unique(
        mandatory_concepts + optional_concepts, "address"
    )
    if not all_concepts:
        raise SyntaxError(
            f"Cannot source empty concept inputs, had {mandatory_concepts} and {optional_concepts}"
        )
    # may be able to directly find everything we need
    matched = gen_static_select_node(
        mandatory_concepts + optional_concepts, environment, g, depth
    )
    if matched and (accept_partial or len(matched.partial_concepts) == 0):
        logger.info(
            f"{local_prefix}{LOGGER_PREFIX} found direct select node with all {[x.address for x in mandatory_concepts + optional_concepts]} "
            f"concepts and {accept_partial} for partial with partial match {len(matched.partial_concepts)}, returning."
        )
        return matched

    # now start the fun portion
    # Loop through all possible grains + subgrains
    # Starting with the most grain
    found_addresses: set[str] = set()
    partial_addresses: set[str] = set()
    non_partial_addresses: set[str] = set()
    found_concepts: set[Concept] = set()
    found_map = defaultdict(set)

    logger.info(
        f"{local_prefix}{LOGGER_PREFIX} Beginning sourcing loop for {[str(c) for c in all_concepts]}"
    )
    attempted_priority_concepts: set[str] = set()
    valid_graph = False
    while not valid_graph:
        # pick the concept we're trying to get
        concept = get_priority_concept(all_concepts, list(attempted_priority_concepts))
        attempted_priority_concepts.add(concept.address)
        # process into a deduped list of optional join concepts to try to pull through
        local_optional, local_required = get_local_optional(
            optional_concepts, mandatory_concepts, concept, local_prefix
        )
        logger.info(
            f"{local_prefix}{LOGGER_PREFIX} For {concept.address}, have local required {[str(c) for c in local_required]} and optional {[str(c) for c in local_optional]}"
        )

        if concept.lineage:
            if concept.derivation == PurposeLineage.WINDOW:
                logger.info(
                    f"{local_prefix}{LOGGER_PREFIX} for {concept.address}, generating window node"
                )
                stack.append(
                    gen_window_node(
                        concept, local_optional, environment, g, depth, source_concepts
                    )
                )
            elif concept.derivation == PurposeLineage.FILTER:
                logger.info(
                    f"{local_prefix}{LOGGER_PREFIX} for {concept.address}, generating filter node"
                )
                stack.append(
                    gen_filter_node(
                        concept, local_optional, environment, g, depth, source_concepts
                    )
                )
            elif concept.derivation == PurposeLineage.UNNEST:
                logger.info(
                    f"{local_prefix}{LOGGER_PREFIX} for {concept.address}, generating unnest node"
                )
                stack.append(
                    gen_unnest_node(
                        concept, local_optional, environment, g, depth, source_concepts
                    )
                )
            elif concept.derivation == PurposeLineage.AGGREGATE:
                # don't push constants up before aggregation
                # if not required
                # to avoid constants multiplication changing default aggregation results
                # ex sum(x) * 2 w/ no grain should return sum(x) * 2, not sum(x*2)
                # these should always be sourceable independently
                agg_optional = [
                    x
                    for x in local_optional
                    if not x.derivation == PurposeLineage.CONSTANT
                ]
                logger.info(
                    f"{local_prefix}{LOGGER_PREFIX} for {concept.address}, generating aggregate node with {agg_optional}"
                )

                stack.append(
                    gen_group_node(
                        concept, agg_optional, environment, g, depth, source_concepts
                    )
                )
            elif concept.derivation == PurposeLineage.CONSTANT:
                logger.info(
                    f"{local_prefix}{LOGGER_PREFIX} for {concept.address}, generating constant node"
                )
                stack.append(
                    ConstantNode(
                        input_concepts=[],
                        output_concepts=[concept],
                        environment=environment,
                        g=g,
                        parents=[],
                        depth=depth + 1,
                    )
                )
            elif concept.derivation == PurposeLineage.BASIC:
                logger.info(
                    f"{local_prefix}{LOGGER_PREFIX} for {concept.address}, generating basic with {[x.address for x in local_optional]}"
                )
                stack.append(
                    gen_basic_node(
                        concept, local_optional, environment, g, depth, source_concepts
                    )
                )
            else:
                raise ValueError(f"Unknown lineage type {concept.derivation}")
        else:
            # if there's no lineage, we can go ahead and try to source the concept
            # from a table or set of tables via a join
            # 2023-10-20 - open it up to ANYTHING here
            # selectable = [x for x in local_optional if not x.lineage]
            stack.append(
                gen_select_node(
                    concept,
                    local_optional,
                    environment,
                    g,
                    depth,
                    accept_partial=accept_partial,
                )
            )
        for node in stack:
            for concept in node.resolve().output_concepts:
                found_concepts.add(concept)
                found_map[str(node)].add(concept)

                if concept not in node.partial_concepts:
                    found_addresses.add(concept.address)
                    non_partial_addresses.add(concept.address)
                if concept in node.partial_concepts:
                    partial_addresses.add(concept.address)

        logger.info(
            f"{local_prefix}{LOGGER_PREFIX} finished a loop iteration looking for {[c.address for c in all_concepts]} from"
            f" {[n for n in stack]}, have {found_addresses} and partial {partial_addresses}"
        )
        if all([c.address in found_addresses for c in all_concepts]) or (
            accept_partial
            and all(
                [
                    c.address in [found_addresses.union(partial_addresses)]
                    for c in all_concepts
                ]
            )
        ):
            logger.info(
                f"{local_prefix}{LOGGER_PREFIX} have all concepts, have {[c.address for c in all_concepts]} from"
                f" {[n for n in stack]}"
                f" checking for single connected graph"
            )
            graph_count, graphs = get_disconnected_components(found_map)
            logger.info(
                f"{local_prefix}{LOGGER_PREFIX} Graph analysis: {graph_count} subgraphs found"
            )
            # if we have too many subgraphs, we need to add more mandatory concepts
            if graph_count in (0, 1):
                valid_graph = True
                logger.info(
                    f"{local_prefix}{LOGGER_PREFIX} One fully connected subgraph returned, sourcing {[c.address for c in mandatory_concepts]} successful."
                )
            elif graph_count > 1 and all(
                [c.address in attempted_priority_concepts for c in all_concepts]
            ):
                logger.info(
                    f"{local_prefix}{LOGGER_PREFIX} fetched nodes are not a connected graph - have {graph_count} as {graphs},"
                    f"rerunning with more mandatory concepts"
                )

                return recurse_or_fail(
                    depth,
                    environment,
                    g,
                    found_concepts,
                    mandatory_concepts,
                    optional_concepts,
                    local_prefix,
                    accept_partial=True,
                )

    partials = [
        c
        for c in all_concepts
        if c.address in partial_addresses and c.address not in non_partial_addresses
    ]

    output = MergeNode(
        input_concepts=mandatory_concepts + optional_concepts,
        output_concepts=mandatory_concepts + optional_concepts,
        environment=environment,
        g=g,
        parents=stack,
        depth=depth,
        partial_concepts=partials,
    )

    # ensure we can resolve our final merge
    output.resolve()

    return output


def source_query_concepts(
    output_concepts,
    environment: Environment,
    g: Optional[ReferenceGraph] = None,
):
    if not output_concepts:
        raise ValueError(f"No output concepts provided {output_concepts}")
    root = source_concepts(output_concepts, [], environment, g, depth=0)
    return GroupNode(
        output_concepts=output_concepts,
        input_concepts=output_concepts,
        environment=environment,
        g=g,
        parents=[root],
        partial_concepts=root.partial_concepts,
    )
