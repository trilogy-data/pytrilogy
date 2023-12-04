from collections import defaultdict
from itertools import combinations
from typing import List, Optional


from preql.constants import logger
from preql.core.enums import Purpose, PurposeLineage
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
)

LOGGER_PREFIX = "[CONCEPT DETAIL]"


def throw_helpful_error(
    mandatory_concepts: list[Concept], optional_concepts: List[Concept]
):
    error_msg_required = [c.address for c in mandatory_concepts]
    error_msg_optional = [c.address for c in optional_concepts]
    raise ValueError(
        f"Could not find any way to associate required concepts {error_msg_required} and optional {error_msg_optional}"
    )


def get_priority_concept(all_concepts:List[Concept], found_addresses:List[str]) -> Concept:
    remaining_concept = [c for c in all_concepts if c.address not in found_addresses]
    priority = (
        [c for c in remaining_concept if c.derivation == PurposeLineage.AGGREGATE]
        + [c for c in remaining_concept if c.derivation == PurposeLineage.WINDOW]
        + [c for c in remaining_concept if c.derivation == PurposeLineage.FILTER]
        + [c for c in remaining_concept if c.derivation == PurposeLineage.BASIC]
        + [c for c in remaining_concept if not c.lineage]
        + [c for c in remaining_concept if c.derivation == PurposeLineage.CONSTANT]
    )
    # concept: Concept = priority[0]
    return priority[0]


def get_local_optional(optional_concepts, mandatory_concepts, concept):
    # we don't  want to look for multiple aggregates at the same time
    # local optional should be relevant keys, but not metrics
    local_optional_staging = unique(
        [
            x
            for x in optional_concepts + mandatory_concepts
            if x.address
            != concept.address  # and #not x.derivation== PurposeLineage.AGGREGATE
        ],
        "address",
    )

    # reduce search space to actual grain
    return concept_list_to_grain(local_optional_staging, []).components_copy


def recurse_or_fail(
    depth,
    environment,
    g,
    found_concepts,
    mandatory_concepts,
    optional_concepts,
    local_prefix,
    accept_partial: bool = False,
):
    candidates = [
        x
        for x in found_concepts
        if x.purpose in (Purpose.KEY, Purpose.PROPERTY) and x not in mandatory_concepts
    ]
    if not candidates:
        # terminal state one - no options to discard
        throw_helpful_error(mandatory_concepts, optional_concepts)
    # want to make the minimum amount of new concepts
    # mandatory, as finding a match is less and less likely
    # as we require more
    for x in range(1, len(candidates) + 1):
        for combo in combinations(candidates, x):
            new_mandatory: List[Concept] = mandatory_concepts + list(combo)
            logger.info(
                f"{local_prefix}{LOGGER_PREFIX} Attempting to resolve joins to reach"
                f" {','.join([str(c) for c in new_mandatory])}"
            )
            try:
                return source_concepts(
                    mandatory_concepts=new_mandatory,
                    optional_concepts=optional_concepts,
                    environment=environment,
                    g=g,
                    depth=depth + 1,
                    accept_partial=accept_partial,
                )
            except ValueError:
                print(f"failed to find {[c.address for c in new_mandatory]}")
                continue
    # terminal state two - have gone through all options
    throw_helpful_error(mandatory_concepts, optional_concepts)


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
    try:
        matched = gen_static_select_node(
            mandatory_concepts + optional_concepts, environment, g, depth
        )
        if matched:
            logger.info(
                f"{local_prefix}{LOGGER_PREFIX} found direct select node with all {len(mandatory_concepts+optional_concepts)} concepts, returning static selection"
            )
            return matched
    except Exception as e:
        logger.info(
            f"{local_prefix}{LOGGER_PREFIX} error with finding constant source: {str(e)}"
        )
        pass

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

    while not all(c.address in found_addresses for c in all_concepts):
        # pick the concept we're trying to get
        concept = get_priority_concept(all_concepts, list(found_addresses))

        # process into a deduped list of optional join concepts to try to pull through
        local_optional = get_local_optional(
            optional_concepts, mandatory_concepts, concept
        )

        if concept.lineage:
            if concept.derivation == PurposeLineage.WINDOW:
                stack.append(
                    gen_window_node(
                        concept, local_optional, environment, g, depth, source_concepts
                    )
                )
            elif concept.derivation == PurposeLineage.FILTER:
                stack.append(
                    gen_filter_node(
                        concept, local_optional, environment, g, depth, source_concepts
                    )
                )
            elif concept.derivation == PurposeLineage.AGGREGATE:
                stack.append(
                    gen_group_node(
                        concept, local_optional, environment, g, depth, source_concepts
                    )
                )
            elif concept.derivation == PurposeLineage.CONSTANT:
                stack.append(
                    ConstantNode(
                        [concept], [], environment, g, parents=[], depth=depth + 1
                    )
                )
            elif concept.derivation == PurposeLineage.BASIC:
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
                    concept, local_optional, environment, g, depth, source_concepts
                )
            )

        for node in stack:
            for concept in node.resolve().output_concepts:
                if concept not in node.partial_concepts:
                    found_addresses.add(concept.address)
                    non_partial_addresses.add(concept.address)
                    found_concepts.add(concept)
                    found_map[str(node)].add(concept)
                if concept in node.partial_concepts:
                    partial_addresses.add(concept.address)
                    found_concepts.add(concept)
                    found_map[str(node)].add(concept)
        logger.info(
            f"{local_prefix}{LOGGER_PREFIX} finished a loop iteration looking for {[c.address for c in all_concepts]} from"
            f" {[n for n in stack]}, have {found_addresses} and partial {partial_addresses}"
        )
        if all([c.address in found_addresses for c in all_concepts]) or (
            accept_partial and all([c.address in [found_addresses.union(partial_addresses)]
            for c in all_concepts ])
        ):
            logger.info(
                f"{local_prefix}{LOGGER_PREFIX} have all concepts, have {[c.address for c in all_concepts]} from"
                f" {[n for n in stack]}"
                " checking for single connected graph"
            )
            graph_count, graphs = get_disconnected_components(found_map)
            logger.info(
                f"{local_prefix}{LOGGER_PREFIX} Graph analysis: {graph_count} subgraphs found"
            )
            # if we have too many subgraphs, we need to add more mandatory concepts
            if graph_count > 1:
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
            logger.info(
                f"{local_prefix}{LOGGER_PREFIX} One fully connected subgraph returned, sourcing {[c.address for c in mandatory_concepts]} successful."
            )
    partials = [
        c
        for c in all_concepts
        if c.address in partial_addresses and c.address not in non_partial_addresses
    ]

    output = MergeNode(
        mandatory_concepts,
        optional_concepts,
        environment,
        g,
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
        output_concepts,
        [],
        environment,
        g,
        parents=[root],
        partial_concepts=root.partial_concepts,
    )
