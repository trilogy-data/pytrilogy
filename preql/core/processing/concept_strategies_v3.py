from collections import defaultdict
from typing import List, Optional, Callable


from preql.constants import logger
from preql.core.enums import PurposeLineage, Granularity
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
from preql.core.processing.node_generators import (
    gen_filter_node,
    gen_window_node,
    gen_group_node,
    gen_basic_node,
    gen_select_node,
    gen_unnest_node,
    gen_merge_node,
)

LOGGER_PREFIX = "[CONCEPT DETAIL]"


def get_priority_concept(
    all_concepts: List[Concept], attempted_addresses: set[str]
) -> Concept:
    remaining_concept = [
        c for c in all_concepts if c.address not in attempted_addresses
    ]
    priority = (
        [
            c
            for c in remaining_concept
            if c.derivation == PurposeLineage.AGGREGATE
            and not c.granularity == Granularity.SINGLE_ROW
        ]
        + [
            c
            for c in remaining_concept
            if c.derivation == PurposeLineage.WINDOW
            and not c.granularity == Granularity.SINGLE_ROW
        ]
        + [
            c
            for c in remaining_concept
            if c.derivation == PurposeLineage.FILTER
            and not c.granularity == Granularity.SINGLE_ROW
        ]
        + [
            c
            for c in remaining_concept
            if c.derivation == PurposeLineage.UNNEST
            and not c.granularity == Granularity.SINGLE_ROW
        ]
        + [
            c
            for c in remaining_concept
            if c.derivation == PurposeLineage.BASIC
            and not c.granularity == Granularity.SINGLE_ROW
        ]
        + [
            c
            for c in remaining_concept
            if c.derivation == PurposeLineage.ROOT
            and not c.granularity == Granularity.SINGLE_ROW
        ]
        + [
            c
            for c in remaining_concept
            if c.derivation == PurposeLineage.CONSTANT
            and not c.granularity == Granularity.SINGLE_ROW
        ]
        + [c for c in remaining_concept if c.granularity == Granularity.SINGLE_ROW]
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
        [
            x
            for x in list(candidates)
            if x.address not in exhausted
            and not x.granularity == Granularity.SINGLE_ROW
        ]
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
    local_prefix: str,
) -> StrategyNode | None:
    candidate = gen_select_node(
        concept, local_optional, environment, g, depth, fail_if_not_found=False
    )
    if candidate:
        return candidate

    if concept.derivation == PurposeLineage.WINDOW:
        return gen_window_node(
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
            f"{local_prefix}{LOGGER_PREFIX} for {concept.address}, generating aggregate node with {[x.address for x in agg_optional]}"
        )

        return gen_group_node(
            concept, agg_optional, environment, g, depth, source_concepts
        )
    elif concept.derivation == PurposeLineage.CONSTANT:
        logger.info(
            f"{local_prefix}{LOGGER_PREFIX} for {concept.address}, generating constant node"
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
        return gen_basic_node(
            concept, local_optional, environment, g, depth, source_concepts
        )
    elif concept.derivation == PurposeLineage.ROOT:
        return gen_select_node(
            concept, local_optional, environment, g, depth, fail_if_not_found=False
        )
    else:
        raise ValueError(f"Unknown derivation {concept.derivation}")


def validate_stack(
    stack: List[StrategyNode], concepts: List[Concept]
) -> tuple[bool, set[str]]:
    found_concepts = set()
    found_map = defaultdict(set)
    found_addresses = set()
    non_partial_addresses = set()
    partial_addresses = set()
    for node in stack:
        for concept in node.resolve().output_concepts:
            found_concepts.add(concept)
            found_map[str(node)].add(concept)

            if concept not in node.partial_concepts:
                found_addresses.add(concept.address)
                non_partial_addresses.add(concept.address)
            if concept in node.partial_concepts:
                partial_addresses.add(concept.address)
    if not all([c.address in found_addresses for c in concepts]):
        return False, found_addresses
    graph_count, graphs = get_disconnected_components(found_map)

    if graph_count in (0, 1):
        return True, found_addresses
    # if we have too many subgraphs, we need to keep searching
    return False, found_addresses


def depth_to_prefix(depth: int) -> str:
    return "\t" * depth


def search_concepts(
    mandatory_list: List[Concept],
    environment: Environment,
    depth: int,
    g: ReferenceGraph,
) -> StrategyNode:
    mandatory_list = unique(mandatory_list, "address")
    all = set(c.address for c in mandatory_list)
    attempted: set[str] = set()

    found: set[str] = set()
    skip: set[str] = set()
    stack: List[StrategyNode] = []
    complete = False

    while attempted != all:
        priority_concept = get_priority_concept(mandatory_list, attempted)

        candidates = [
            c for c in mandatory_list if c.address != priority_concept.address
        ]
        candidate_lists = generate_candidates_restrictive(
            priority_concept, candidates, skip
        )

        for list in candidate_lists:
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Beginning sourcing loop for {str(priority_concept)}, optional {[str(v) for v in list]}"
            )
            node = generate_node(
                priority_concept,
                list,
                environment,
                g,
                depth + 1,
                source_concepts=search_concepts,
                local_prefix="",
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
                ]:
                    skip.add(priority_concept.address)
                break
        attempted.add(priority_concept.address)
        complete, found = validate_stack(stack, mandatory_list)
        # early exit if we have a complete stack with one node
        if complete and len(stack) == 1:
            break

    logger.info(
        f"{depth_to_prefix(depth)}{LOGGER_PREFIX} finished sourcing loop (complete: {complete}), have {found} from {[n for n in stack]} (missing {all - found}), attempted {attempted}"
    )
    if complete:
        output = MergeNode(
            input_concepts=mandatory_list,
            output_concepts=mandatory_list,
            environment=environment,
            g=g,
            parents=stack,
            depth=depth,
            # partial_concepts=partials,
        )

        # ensure we can resolve our final merge
        output.resolve()
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Graph is connected, returning merge node"
        )
        return output

    logger.info(f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Stack is not connected graph")
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
        return expanded
    logger.error(
        f"{depth_to_prefix(depth)}{LOGGER_PREFIX}Could not resolve concepts {[c.address for c in mandatory_list]}"
    )
    raise ValueError(
        f"{depth_to_prefix(depth)}{LOGGER_PREFIX}Could not resolve concepts {[c.address for c in mandatory_list]}"
    )


def source_query_concepts(
    output_concepts,
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
    return GroupNode(
        output_concepts=output_concepts,
        input_concepts=output_concepts,
        environment=environment,
        g=g,
        parents=[root],
        partial_concepts=root.partial_concepts,
    )
