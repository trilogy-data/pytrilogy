from collections import defaultdict
from typing import List, Optional, Callable


from preql.constants import logger
from preql.core.enums import PurposeLineage, Granularity, FunctionType
from preql.core.env_processor import generate_graph
from preql.core.graph_models import ReferenceGraph
from preql.core.models import Concept, Environment, Function, Grain
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
from preql.core.processing.node_generators import (
    gen_filter_node,
    gen_window_node,
    gen_group_node,
    gen_basic_node,
    gen_select_node,
    gen_unnest_node,
    gen_merge_node,
    gen_group_to_node,
    gen_rowset_node,
)

from enum import Enum


class ValidationResult(Enum):
    COMPLETE = 1
    DISCONNECTED = 2
    INCOMPLETE = 3


LOGGER_PREFIX = "[CONCEPT DETAIL]"


def get_priority_concept(
    all_concepts: List[Concept], attempted_addresses: set[str]
) -> Concept:
    remaining_concept = [
        c for c in all_concepts if c.address not in attempted_addresses
    ]
    priority = (
        # find anything that needs no joins first, so we can exit early
        [
            c
            for c in remaining_concept
            if c.derivation == PurposeLineage.CONSTANT
            and c.granularity == Granularity.SINGLE_ROW
        ]
        +
        # then rowsets to remove them from scope, as they cannot get partials
        [c for c in remaining_concept if c.derivation == PurposeLineage.ROWSET]
        +
        # then aggregates to remove them from scope, as they cannot get partials
        [
            c
            for c in remaining_concept
            if c.derivation == PurposeLineage.AGGREGATE
            and not c.granularity == Granularity.SINGLE_ROW
        ]
        # then windows to remove them from scope, as they cannot get partials
        + [
            c
            for c in remaining_concept
            if c.derivation == PurposeLineage.WINDOW
            and not c.granularity == Granularity.SINGLE_ROW
        ]
        # then filters to remove them from scope, also cannot get partials
        + [
            c
            for c in remaining_concept
            if c.derivation == PurposeLineage.FILTER
            and not c.granularity == Granularity.SINGLE_ROW
        ]
        # unnests are weird?
        + [
            c
            for c in remaining_concept
            if c.derivation == PurposeLineage.UNNEST
            and not c.granularity == Granularity.SINGLE_ROW
        ]
        # we should be home-free here
        + [
            c
            for c in remaining_concept
            if c.derivation == PurposeLineage.BASIC
            and not c.granularity == Granularity.SINGLE_ROW
        ]
        # finally our plain selects
        + [
            c
            for c in remaining_concept
            if c.derivation == PurposeLineage.ROOT
            and not c.granularity == Granularity.SINGLE_ROW
        ]
        # and any non-single row constants
        + [
            c
            for c in remaining_concept
            if c.derivation == PurposeLineage.CONSTANT
            and not c.granularity == Granularity.SINGLE_ROW
        ]
        # catch all
        + [
            c
            for c in remaining_concept
            if c.derivation != PurposeLineage.CONSTANT
            and c.granularity == Granularity.SINGLE_ROW
        ]
    )
    if not priority:
        raise ValueError(
            f"Cannot resolve query. No remaining priority concepts, have attempted {attempted_addresses}"
        )
    return priority[0]


def generate_candidates_restrictive(
    priority_concept: Concept, candidates: list[Concept], exhausted: set[str]
) -> List[List[Concept]]:
    # if it's single row, joins are irrelevant. Fetch without keys.
    if priority_concept.granularity == Granularity.SINGLE_ROW:
        return [[]]
    combos: list[list[Concept]] = []
    combos.append(
        Grain(
            components=[
                x
                for x in list(candidates)
                if x.address not in exhausted
                and x.granularity != Granularity.SINGLE_ROW
            ]
        ).components_copy
    )
    # append the empty set for sourcing concept by itself last
    combos.append([])
    return combos


def generate_node(
    concept: Concept,
    local_optional: List[Concept],
    environment: Environment,
    g: ReferenceGraph,
    depth: int,
    source_concepts: Callable,
    accept_partial: bool = False,
) -> StrategyNode | None:
    # first check in case there is a materialized_concept
    candidate = gen_select_node(
        concept,
        local_optional,
        environment,
        g,
        depth,
        fail_if_not_found=False,
        accept_partial=accept_partial,
        accept_partial_optional=False,
    )
    if candidate:
        return candidate

    if concept.derivation == PurposeLineage.WINDOW:
        return gen_window_node(
            concept, local_optional, environment, g, depth, source_concepts
        )
    elif concept.derivation == PurposeLineage.ROWSET:
        return gen_rowset_node(
            concept, local_optional, environment, g, depth, source_concepts
        )
    elif concept.derivation == PurposeLineage.FILTER:
        return gen_filter_node(
            concept, local_optional, environment, g, depth, source_concepts
        )
    elif concept.derivation == PurposeLineage.UNNEST:
        return gen_unnest_node(
            concept, local_optional, environment, g, depth, source_concepts
        )
    elif concept.derivation == PurposeLineage.AGGREGATE:
        # don't push constants up before aggregation
        # if not required
        # to avoid constants multiplication changing default aggregation results
        # ex sum(x) * 2 w/ no grain should return sum(x) * 2, not sum(x*2)
        # these should always be sourceable independently
        agg_optional = [
            x for x in local_optional if x.granularity != Granularity.SINGLE_ROW
        ]

        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating aggregate node with {[x.address for x in agg_optional]}"
        )
        return gen_group_node(
            concept, agg_optional, environment, g, depth, source_concepts
        )
    elif concept.derivation == PurposeLineage.CONSTANT:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating constant node"
        )
        return ConstantNode(
            input_concepts=[],
            output_concepts=[concept],
            environment=environment,
            g=g,
            parents=[],
            depth=depth + 1,
        )
    elif concept.derivation == PurposeLineage.BASIC:
        # this is special case handling for group bys
        if (
            isinstance(concept.lineage, Function)
            and concept.lineage.operator == FunctionType.GROUP
        ):
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating group to grain node with {[x.address for x in local_optional]}"
            )
            return gen_group_to_node(
                concept, local_optional, environment, g, depth, source_concepts
            )
        return gen_basic_node(
            concept, local_optional, environment, g, depth, source_concepts
        )
    elif concept.derivation == PurposeLineage.ROOT:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating select node"
        )
        return gen_select_node(
            concept,
            local_optional,
            environment,
            g,
            depth,
            fail_if_not_found=False,
            accept_partial=accept_partial,
        )
    else:
        raise ValueError(f"Unknown derivation {concept.derivation}")


def validate_stack(
    stack: List[StrategyNode], concepts: List[Concept], accept_partial: bool = False
) -> tuple[ValidationResult, set[str], set[str], set[str]]:
    found_map = defaultdict(set)
    found_addresses: set[str] = set()
    non_partial_addresses: set[str] = set()
    partial_addresses: set[str] = set()
    for node in stack:
        for concept in node.resolve().output_concepts:
            found_map[str(node)].add(concept)
            if concept not in node.partial_concepts:
                found_addresses.add(concept.address)
                non_partial_addresses.add(concept.address)
                # remove it from our partial tracking
                if concept.address in partial_addresses:
                    partial_addresses.remove(concept.address)
            if concept in node.partial_concepts:
                partial_addresses.add(concept.address)
                if accept_partial:
                    found_addresses.add(concept.address)
                    found_map[str(node)].add(concept)
    if not all([c.address in found_addresses for c in concepts]):
        return (
            ValidationResult.INCOMPLETE,
            found_addresses,
            {c.address for c in concepts if c.address not in found_addresses},
            partial_addresses,
        )
    graph_count, graphs = get_disconnected_components(found_map)
    if graph_count in (0, 1):
        return ValidationResult.COMPLETE, found_addresses, set(), partial_addresses
    # if we have too many subgraphs, we need to keep searching
    return ValidationResult.DISCONNECTED, found_addresses, set(), partial_addresses


def depth_to_prefix(depth: int) -> str:
    return "\t" * depth


def search_concepts(
    mandatory_list: List[Concept],
    environment: Environment,
    depth: int,
    g: ReferenceGraph,
    accept_partial: bool = False,
) -> StrategyNode | None:
    mandatory_list = unique(mandatory_list, "address")
    all_mandatory = set(c.address for c in mandatory_list)
    attempted: set[str] = set()

    found: set[str] = set()
    skip: set[str] = set()
    stack: List[StrategyNode] = []
    complete = ValidationResult.INCOMPLETE

    while attempted != all_mandatory:
        priority_concept = get_priority_concept(mandatory_list, attempted)

        candidates = [
            c for c in mandatory_list if c.address != priority_concept.address
        ]
        candidate_lists = generate_candidates_restrictive(
            priority_concept, candidates, skip
        )

        for list in candidate_lists:
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Beginning sourcing loop for {str(priority_concept)}, accept_partial {accept_partial} optional {[str(v) for v in list]}"
            )
            node = generate_node(
                priority_concept,
                list,
                environment,
                g,
                depth + 1,
                source_concepts=search_concepts,
                accept_partial=accept_partial,
            )
            if node:
                stack.append(node)
                found.add(priority_concept.address)
                # these concepts should not be attempted to be sourced again
                # as fetching them requires operating on a subset of concepts
                if priority_concept.derivation in [
                    PurposeLineage.AGGREGATE,
                    PurposeLineage.FILTER,
                    PurposeLineage.WINDOW,
                    PurposeLineage.BASIC,
                    PurposeLineage.UNNEST,
                    PurposeLineage.ROWSET,
                ]:
                    skip.add(priority_concept.address)
                break
        attempted.add(priority_concept.address)
        complete, found, missing, partial = validate_stack(
            stack, mandatory_list, accept_partial
        )
        # early exit if we have a complete stack with one node
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} finished concept loop for {priority_concept} flag for accepting partial addresses is "
            f" {accept_partial} (complete: {complete}), have {found} from {[n for n in stack]} (missing {missing} partial {partial}), attempted {attempted}"
        )
        # we can only early exit if we have a complete stack
        # and we are not looking for more non-partial sources
        if complete == ValidationResult.COMPLETE and (
            not accept_partial or (accept_partial and not partial)
        ):
            break

    logger.info(
        f"{depth_to_prefix(depth)}{LOGGER_PREFIX} finished sourcing loop (complete: {complete}), have {found} from {[n for n in stack]} (missing {all_mandatory - found}), attempted {attempted}"
    )
    if complete == ValidationResult.COMPLETE:
        all_partial = [
            c
            for c in mandatory_list
            if all(
                [
                    c.address in [x.address for x in p.partial_concepts]
                    for p in stack
                    if [c in p.output_concepts]
                ]
            )
        ]
        if len(stack) == 1:
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Source stack has single node, returning just that node"
            )
            return stack[0]

        output = MergeNode(
            input_concepts=mandatory_list,
            output_concepts=mandatory_list,
            environment=environment,
            g=g,
            parents=stack,
            depth=depth,
            partial_concepts=all_partial,
        )

        # ensure we can resolve our final merge
        output.resolve()
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Graph is connected, returning merge node, partial {[c.address for c in all_partial]}"
        )
        return output

    logger.info(
        f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Stack is not connected graph, flag for accepting partial addresses is {accept_partial}"
    )
    # see if we can expand our mandatory list by adding join concepts
    expanded = gen_merge_node(
        all_concepts=mandatory_list,
        environment=environment,
        g=g,
        depth=depth,
        source_concepts=search_concepts,
    )
    if expanded:
        expanded.resolve()
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Found connections for {[c.address for c in mandatory_list]} via concept addition;"
        )
        return expanded
    # if we can't find it after expanding to a merge, then
    # attempt to accept partials in join paths
    elif not accept_partial:
        partial_search = search_concepts(
            mandatory_list=mandatory_list,
            environment=environment,
            depth=depth,
            g=g,
            accept_partial=True,
        )
        if partial_search:
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Found {[c.address for c in mandatory_list]} by accepting partials"
            )
            return partial_search
    logger.error(
        f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Could not resolve concepts {[c.address for c in mandatory_list]}, network outcome was {complete}, missing {all_mandatory - found}"
    )
    return None


def source_query_concepts(
    output_concepts: List[Concept],
    environment: Environment,
    g: Optional[ReferenceGraph] = None,
):
    if not g:
        g = generate_graph(environment)
    if not output_concepts:
        raise ValueError(f"No output concepts provided {output_concepts}")
    root = search_concepts(
        mandatory_list=output_concepts, environment=environment, g=g, depth=0
    )

    if not root:
        error_strings = [
            f"{c.address}<{c.purpose}>{c.derivation}>" for c in output_concepts
        ]
        raise ValueError(
            f"Could not resolve conections between {error_strings} from environment graph."
        )
    return GroupNode(
        output_concepts=output_concepts,
        input_concepts=output_concepts,
        environment=environment,
        g=g,
        parents=[root],
        partial_concepts=root.partial_concepts,
    )
