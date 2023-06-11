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
    FilterItem,
    Function,
    WindowItem,
    AggregateWrapper,
)
from preql.core.processing.utility import (
    get_disconnected_components,
)
from preql.utility import unique
from preql.core.processing.nodes import (
    FilterNode,
    SelectNode,
    MergeNode,
    GroupNode,
    WindowNode,
    StrategyNode,
)
from preql.core.processing.nodes.base_node import concept_list_to_grain


LOGGER_PREFIX = "[CONCEPT DETAIL]"


def resolve_window_parent_concepts(concept: Concept) -> List[Concept]:
    if not isinstance(concept.lineage, WindowItem):
        raise ValueError
    base = [concept.lineage.content]
    if concept.lineage.over:
        base += concept.lineage.over
    if concept.lineage.order_by:
        for item in concept.lineage.order_by:
            base += [item.expr.output]
    return unique(base, "address")


def resolve_filter_parent_concepts(concept: Concept) -> List[Concept]:
    if not isinstance(concept.lineage, FilterItem):
        raise ValueError
    base = [concept.lineage.content]
    base += concept.lineage.where.input
    return unique(base, "address")


def resolve_function_parent_concepts(concept: Concept) -> List[Concept]:
    if not isinstance(concept.lineage, (Function, AggregateWrapper)):
        raise ValueError(f"Concept {concept} is not an aggregate function")
    if concept.derivation == PurposeLineage.AGGREGATE:
        if concept.grain:
            return unique(
                concept.lineage.concept_arguments + concept.grain.components_copy,
                "address",
            )
        return concept.lineage.concept_arguments
    # TODO: handle basic lineage chains?

    return unique(concept.lineage.concept_arguments, "address")


def source_concepts(
    mandatory_concepts: List[Concept],
    optional_concepts: List[Concept],
    environment: Environment,
    g: Optional[ReferenceGraph] = None,
) -> StrategyNode:
    """Mandatory concepts are those which must be included in the output
    Optional concepts may be dropped"""
    g = g or generate_graph(environment)
    stack: List[StrategyNode] = []
    all_concepts = unique(mandatory_concepts + optional_concepts, "address")
    if not all_concepts:
        raise SyntaxError("Cannot source empty concept inputs")
    # TODO
    # Loop through all possible grains + subgrains
    # Starting with the most grain
    found_addresses: list[str] = []
    found_concepts: set[Concept] = set()
    found_map = defaultdict(set)

    # early exit when we have found all concepts
    while not all(c.address in found_addresses for c in all_concepts):
        remaining_concept = [
            c for c in all_concepts if c.address not in found_addresses
        ]
        priority = (
            [c for c in remaining_concept if c.derivation == PurposeLineage.AGGREGATE]
            + [c for c in remaining_concept if c.derivation == PurposeLineage.WINDOW]
            + [c for c in remaining_concept if c.derivation == PurposeLineage.FILTER]
            + [c for c in remaining_concept if c.derivation == PurposeLineage.BASIC]
            + [c for c in remaining_concept if not c.lineage]
            + [c for c in remaining_concept if c.derivation == PurposeLineage.CONSTANT]
        )
        concept = priority[0]
        # we don't actually want to look for multiple aggregates at the same time
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
        local_optional = concept_list_to_grain(
            local_optional_staging, []
        ).components_copy
        if concept.lineage:
            if concept.derivation == PurposeLineage.WINDOW:
                parent_concepts = resolve_window_parent_concepts(concept)
                stack.append(
                    WindowNode(
                        [concept],
                        local_optional,
                        environment,
                        g,
                        parents=[
                            source_concepts(
                                parent_concepts, local_optional, environment, g
                            )
                        ],
                    )
                )
            elif concept.derivation == PurposeLineage.FILTER:
                parent_concepts = resolve_filter_parent_concepts(concept)
                stack.append(
                    FilterNode(
                        [concept],
                        local_optional,
                        environment,
                        g,
                        parents=[
                            source_concepts(
                                parent_concepts, local_optional, environment, g
                            )
                        ],
                    )
                )
            elif concept.derivation == PurposeLineage.AGGREGATE:
                # aggregates MUST always group to the proper grain
                # except when the
                parent_concepts = unique(
                    resolve_function_parent_concepts(concept), "address"
                )

                # if the aggregation has a grain, we need to ensure these are the ONLY optional in the output of the select
                if len(concept.grain.components_copy) > 0:
                    local_optional = concept.grain.components_copy
                # otherwise, local optional are mandatory
                else:
                    parent_concepts += local_optional
                if parent_concepts:
                    parents = [source_concepts(parent_concepts, [], environment, g)]
                else:
                    parents = []
                stack.append(
                    GroupNode(
                        [concept],
                        local_optional,
                        environment,
                        g,
                        parents=parents,
                    )
                )
            elif concept.derivation == PurposeLineage.CONSTANT:
                stack.append(SelectNode([concept], [], environment, g, parents=[]))
            elif concept.derivation == PurposeLineage.BASIC:
                # directly select out a basic derivation
                parent_concepts = resolve_function_parent_concepts(concept)
                stack.append(
                    SelectNode(
                        [concept],
                        local_optional,
                        environment,
                        g,
                        parents=[
                            source_concepts(
                                parent_concepts, local_optional, environment, g
                            )
                        ],
                    )
                )
            else:
                raise ValueError(f"Unknown lineage type {concept.derivation}")
        else:
            basic_inputs = [x for x in local_optional if x.lineage is None]
            stack.append(SelectNode([concept], basic_inputs, environment, g))

        for node in stack:
            for concept in node.resolve().output_concepts:
                found_addresses.append(concept.address)
                found_concepts.add(concept)
                found_map[str(node)].add(concept)
        logger.info(
            f"{LOGGER_PREFIX} finished a loop iteration looking for {[c.address for c in all_concepts]} from"
            f" {[n for n in stack]}, have {found_addresses}"
        )
        if all(c.address in found_addresses for c in all_concepts):
            logger.info(
                f"{LOGGER_PREFIX} have all concepts, have {[c.address for c in all_concepts]} from"
                f" {[n for n in stack]}"
                " checking for convergence"
            )

            graph_count, graphs = get_disconnected_components(found_map)
            if graph_count > 1:
                candidates = [
                    x
                    for x in found_concepts
                    if x.purpose in (Purpose.KEY, Purpose.PROPERTY)
                    and x not in mandatory_concepts
                ]
                logger.info(
                    f"{LOGGER_PREFIX} fetched nodes are not a connected graph - have {graph_count} as {graphs},"
                    f"rerunning with more mandatory concepts than {[c.address for c in mandatory_concepts]} from {candidates}"
                )

                for x in range(1, len(candidates) + 1):
                    for combo in combinations(candidates, x):
                        new_mandatory = mandatory_concepts + list(combo)
                        logger.info(
                            f"{LOGGER_PREFIX} Attempting to resolve joins to reach"
                            f" {','.join([str(c) for c in new_mandatory])}"
                        )
                        try:
                            return source_concepts(
                                mandatory_concepts=new_mandatory,
                                optional_concepts=optional_concepts,
                                environment=environment,
                                g=g,
                            )
                        except ValueError:
                            continue
                required = [c.address for c in mandatory_concepts]
                raise ValueError(
                    f"Could not find any way to associate required concepts {required}"
                )
            logger.info(
                f"{LOGGER_PREFIX} One fully connected subgraph returned, sourcing {[c.address for c in mandatory_concepts]} successful."
            )

    return MergeNode(
        mandatory_concepts, optional_concepts, environment, g, parents=stack
    )


def source_query_concepts(
    output_concepts,
    grain_components,
    environment: Environment,
    g: Optional[ReferenceGraph] = None,
):
    root = source_concepts(output_concepts, grain_components, environment, g)
    return GroupNode(output_concepts, grain_components, environment, g, parents=[root])
