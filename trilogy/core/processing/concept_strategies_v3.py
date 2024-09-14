from collections import defaultdict
from typing import List, Optional, Callable

from trilogy.constants import logger
from trilogy.core.enums import PurposeLineage, Granularity, FunctionType
from trilogy.core.env_processor import generate_graph
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models import Concept, Environment, Function, Grain, WhereClause
from trilogy.core.processing.utility import (
    get_disconnected_components,
)
from trilogy.utility import unique
from trilogy.core.processing.nodes import (
    ConstantNode,
    MergeNode,
    GroupNode,
    StrategyNode,
    History,
)
from trilogy.core.processing.node_generators import (
    gen_filter_node,
    gen_window_node,
    gen_group_node,
    gen_basic_node,
    gen_unnest_node,
    gen_merge_node,
    gen_group_to_node,
    gen_rowset_node,
    gen_multiselect_node,
)

from enum import Enum


class ValidationResult(Enum):
    COMPLETE = 1
    DISCONNECTED = 2
    INCOMPLETE = 3


LOGGER_PREFIX = "[CONCEPT DETAIL]"


def get_upstream_concepts(base: Concept, nested: bool = False) -> set[str]:
    upstream = set()
    if nested:
        upstream.add(base.address)
    if not base.lineage:
        return upstream
    for x in base.lineage.concept_arguments:
        upstream = upstream.union(get_upstream_concepts(x, nested=True))
    return upstream


def get_priority_concept(
    all_concepts: List[Concept],
    attempted_addresses: set[str],
    found_concepts: set[str],
    depth: int,
) -> Concept:
    # optimized search for missing concepts
    pass_one = [
        c
        for c in all_concepts
        if c.address not in attempted_addresses and c.address not in found_concepts
    ]
    # sometimes we need to scan intermediate concepts to get merge keys, so fall back
    # to exhaustive search
    pass_two = [c for c in all_concepts if c.address not in attempted_addresses]
    for remaining_concept in (pass_one, pass_two):
        priority = (
            # find anything that needs no joins first, so we can exit early
            [
                c
                for c in remaining_concept
                if c.derivation == PurposeLineage.CONSTANT
                and c.granularity == Granularity.SINGLE_ROW
            ]
            +
            # then multiselects to remove them from scope
            [c for c in remaining_concept if c.derivation == PurposeLineage.MULTISELECT]
            +
            # then rowsets to remove them from scope, as they cannot get partials
            [c for c in remaining_concept if c.derivation == PurposeLineage.ROWSET]
            # we should be home-free here
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
            if any([x.address in get_upstream_concepts(c) for c in priority]):
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
        f"Cannot resolve query. No remaining priority concepts, have attempted {attempted_addresses}"
    )


def generate_candidates_restrictive(
    priority_concept: Concept,
    candidates: list[Concept],
    exhausted: set[str],
) -> List[List[Concept]]:
    # if it's single row, joins are irrelevant. Fetch without keys.
    if priority_concept.granularity == Granularity.SINGLE_ROW:
        return [[]]

    local_candidates = [
        x
        for x in list(candidates)
        if x.address not in exhausted
        and x.granularity != Granularity.SINGLE_ROW
        and x.address not in priority_concept.pseudonyms
        and priority_concept.address not in x.pseudonyms
    ]
    combos: list[list[Concept]] = []
    grain_check = Grain(components=[*local_candidates]).components_copy
    # for simple operations these, fetch as much as possible.
    if priority_concept.derivation in (PurposeLineage.BASIC, PurposeLineage.ROOT):
        if set([x.address for x in grain_check]) != set(
            [x.address for x in local_candidates]
        ):
            combos.append(local_candidates)
    combos.append(grain_check)
    # combos.append(local_candidates)
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
    history: History | None = None,
    conditions: WhereClause | None = None,
) -> StrategyNode | None:
    # first check in case there is a materialized_concept
    history = history or History()
    candidate = history.gen_select_node(
        concept,
        local_optional,
        environment,
        g,
        depth + 1,
        fail_if_not_found=False,
        accept_partial=accept_partial,
        accept_partial_optional=False,
        source_concepts=source_concepts,
        conditions=conditions,
    )

    if candidate:
        return candidate

    if concept.derivation == PurposeLineage.WINDOW:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating window node with optional {[x.address for x in local_optional]}"
        )
        return gen_window_node(
            concept,
            local_optional,
            environment,
            g,
            depth + 1,
            source_concepts,
            history,
            conditions=conditions,
        )

    elif concept.derivation == PurposeLineage.FILTER:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating filter node with optional {[x.address for x in local_optional]}"
        )
        return gen_filter_node(
            concept,
            local_optional,
            environment,
            g,
            depth + 1,
            source_concepts=source_concepts,
            history=history,
            conditions=conditions,
        )
    elif concept.derivation == PurposeLineage.UNNEST:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating unnest node with optional {[x.address for x in local_optional]}"
        )
        return gen_unnest_node(
            concept,
            local_optional,
            environment,
            g,
            depth + 1,
            source_concepts,
            history,
            conditions=conditions,
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
            concept,
            agg_optional,
            environment,
            g,
            depth + 1,
            source_concepts,
            history,
            conditions=conditions,
        )
    elif concept.derivation == PurposeLineage.ROWSET:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating rowset node with optional {[x.address for x in local_optional]}"
        )
        return gen_rowset_node(
            concept, local_optional, environment, g, depth + 1, source_concepts, history
        )
    elif concept.derivation == PurposeLineage.MULTISELECT:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating multiselect node with optional {[x.address for x in local_optional]}"
        )
        return gen_multiselect_node(
            concept, local_optional, environment, g, depth + 1, source_concepts, history
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
                concept,
                local_optional,
                environment,
                g,
                depth + 1,
                source_concepts,
                history,
            )
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating basic node with optional {[x.address for x in local_optional]}"
        )
        return gen_basic_node(
            concept, local_optional, environment, g, depth + 1, source_concepts, history
        )

    elif concept.derivation == PurposeLineage.ROOT:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating select node with optional {[x.address for x in local_optional]}"
        )
        return history.gen_select_node(
            concept,
            local_optional,
            environment,
            g,
            depth + 1,
            fail_if_not_found=False,
            accept_partial=accept_partial,
            accept_partial_optional=True,
            source_concepts=source_concepts,
            conditions=conditions,
        )
    else:
        raise ValueError(f"Unknown derivation {concept.derivation}")


def validate_concept(
    concept: Concept,
    node: StrategyNode,
    found_addresses: set[str],
    non_partial_addresses: set[str],
    partial_addresses: set[str],
    virtual_addresses: set[str],
    found_map: dict[str, set[Concept]],
    accept_partial: bool,
    seen: set[str],
):

    found_map[str(node)].add(concept)
    seen.add(concept.address)
    if concept not in node.partial_concepts:

        found_addresses.add(concept.address)
        non_partial_addresses.add(concept.address)
        # remove it from our partial tracking
        if concept.address in partial_addresses:
            partial_addresses.remove(concept.address)
        if concept.address in virtual_addresses:
            virtual_addresses.remove(concept.address)
    if concept in node.partial_concepts:
        if concept.address in non_partial_addresses:
            return None
        partial_addresses.add(concept.address)
        if accept_partial:
            found_addresses.add(concept.address)
            found_map[str(node)].add(concept)
    for _, v in concept.pseudonyms.items():
        if v.address == concept.address:
            return
        if v.address in seen:
            return
        validate_concept(
            v,
            node,
            found_addresses,
            non_partial_addresses,
            partial_addresses,
            virtual_addresses,
            found_map,
            accept_partial,
            seen=seen,
        )


def validate_stack(
    stack: List[StrategyNode],
    concepts: List[Concept],
    accept_partial: bool = False,
) -> tuple[ValidationResult, set[str], set[str], set[str], set[str]]:
    found_map: dict[str, set[Concept]] = defaultdict(set)
    found_addresses: set[str] = set()
    non_partial_addresses: set[str] = set()
    partial_addresses: set[str] = set()
    virtual_addresses: set[str] = set()
    seen: set[str] = set()
    for node in stack:
        resolved = node.resolve()

        for concept in resolved.output_concepts:
            validate_concept(
                concept,
                node,
                found_addresses,
                non_partial_addresses,
                partial_addresses,
                virtual_addresses,
                found_map,
                accept_partial,
                seen,
            )
        for concept in node.virtual_output_concepts:
            if concept.address in non_partial_addresses:
                continue
            found_addresses.add(concept.address)
            virtual_addresses.add(concept.address)

    # zip in those we know we found
    if not all([c.address in found_addresses for c in concepts]):
        return (
            ValidationResult.INCOMPLETE,
            found_addresses,
            {c.address for c in concepts if c.address not in found_addresses},
            partial_addresses,
            virtual_addresses,
        )

    graph_count, _ = get_disconnected_components(found_map)
    if graph_count in (0, 1):
        return (
            ValidationResult.COMPLETE,
            found_addresses,
            set(),
            partial_addresses,
            virtual_addresses,
        )
    # if we have too many subgraphs, we need to keep searching
    return (
        ValidationResult.DISCONNECTED,
        found_addresses,
        set(),
        partial_addresses,
        virtual_addresses,
    )


def depth_to_prefix(depth: int) -> str:
    return "\t" * depth


def search_concepts(
    mandatory_list: List[Concept],
    environment: Environment,
    depth: int,
    g: ReferenceGraph,
    accept_partial: bool = False,
    history: History | None = None,
    conditions: WhereClause | None = None,
) -> StrategyNode | None:

    history = history or History()
    hist = history.get_history(
        search=mandatory_list, accept_partial=accept_partial, conditions=conditions
    )
    if hist is not False:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Returning search node from history ({'exists' if hist is not None else 'does not exist'}) for {[c.address for c in mandatory_list]} with accept_partial {accept_partial}"
        )
        assert not isinstance(hist, bool)
        return hist

    result = _search_concepts(
        mandatory_list,
        environment,
        depth=depth,
        g=g,
        accept_partial=accept_partial,
        history=history,
        conditions=conditions,
    )
    # a node may be mutated after be cached; always store a copy
    history.search_to_history(
        mandatory_list,
        accept_partial,
        result.copy() if result else None,
        conditions=conditions,
    )
    return result


def _search_concepts(
    mandatory_list: List[Concept],
    environment: Environment,
    depth: int,
    g: ReferenceGraph,
    history: History,
    accept_partial: bool = False,
    conditions: WhereClause | None = None,
) -> StrategyNode | None:

    mandatory_list = unique(mandatory_list, "address")

    all_mandatory = set(c.address for c in mandatory_list)
    attempted: set[str] = set()

    found: set[str] = set()
    skip: set[str] = set()
    virtual: set[str] = set()
    stack: List[StrategyNode] = []
    complete = ValidationResult.INCOMPLETE

    while attempted != all_mandatory:
        priority_concept = get_priority_concept(
            mandatory_list, attempted, found_concepts=found, depth=depth
        )

        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} priority concept is {str(priority_concept)}"
        )

        candidates = [
            c for c in mandatory_list if c.address != priority_concept.address
        ]
        candidate_lists = generate_candidates_restrictive(
            priority_concept, candidates, skip
        )
        for clist in candidate_lists:
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Beginning sourcing loop for {str(priority_concept)}, accept_partial {accept_partial}, optional {[str(v) for v in clist]}, exhausted {[str(c) for c in skip]}"
            )
            node = generate_node(
                priority_concept,
                clist,
                environment,
                g,
                depth,
                source_concepts=search_concepts,
                accept_partial=accept_partial,
                history=history,
                conditions=conditions,
            )
            if node:
                stack.append(node)
                node.resolve()
                # these concepts should not be attempted to be sourced again
                # as fetching them requires operating on a subset of concepts
                if priority_concept.derivation in [
                    PurposeLineage.AGGREGATE,
                    PurposeLineage.FILTER,
                    PurposeLineage.WINDOW,
                    PurposeLineage.UNNEST,
                    PurposeLineage.ROWSET,
                    PurposeLineage.BASIC,
                    PurposeLineage.MULTISELECT,
                ]:
                    skip.add(priority_concept.address)
                break
        attempted.add(priority_concept.address)
        complete, found, missing, partial, virtual = validate_stack(
            stack, mandatory_list, accept_partial
        )

        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} finished concept loop for {priority_concept} flag for accepting partial addresses is"
            f" {accept_partial} (complete: {complete}), have {found} from {[n for n in stack]} (missing {missing} partial {partial} virtual {virtual}), attempted {attempted}"
        )
        # early exit if we have a complete stack with one node
        # we can only early exit if we have a complete stack
        # and we are not looking for more non-partial sources
        if complete == ValidationResult.COMPLETE and (
            not accept_partial or (accept_partial and not partial)
        ):
            break

    logger.info(
        f"{depth_to_prefix(depth)}{LOGGER_PREFIX} finished sourcing loop (complete: {complete}), have {found} from {[n for n in stack]} (missing {all_mandatory - found}), attempted {attempted}, virtual {virtual}"
    )
    if complete == ValidationResult.COMPLETE:
        non_virtual = [c for c in mandatory_list if c.address not in virtual]
        if len(stack) == 1:
            output = stack[0]
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Source stack has single node, returning that {type(output)} with output {[x.address for x in output.output_concepts]} and {output.resolve().source_map}"
            )
            return output

        output = MergeNode(
            input_concepts=non_virtual,
            output_concepts=non_virtual,
            environment=environment,
            g=g,
            parents=stack,
            depth=depth,
        )

        # ensure we can resolve our final merge
        output.resolve()
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Graph is connected, returning merge node, partial {[c.address for c in output.partial_concepts]}"
        )
        return output

    # check that we're not already in a discovery loop
    if not history.check_started(
        mandatory_list, accept_partial=accept_partial, conditions=conditions
    ):
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Stack is not connected graph, flag for accepting partial addresses is {accept_partial}, checking for expanded concepts"
        )
        # gate against further recursion into this
        history.log_start(
            mandatory_list, accept_partial=accept_partial, conditions=conditions
        )
        expanded = gen_merge_node(
            all_concepts=mandatory_list,
            environment=environment,
            g=g,
            depth=depth,
            source_concepts=search_concepts,
            history=history,
            search_conditions=conditions,
        )

        if expanded:
            # we don't need to return the entire list; just the ones we needed pre-expansion
            ex_resolve = expanded.resolve()
            extra = [
                x
                for x in ex_resolve.output_concepts
                if x.address not in [y.address for y in mandatory_list]
                and x not in ex_resolve.grain.components
            ]
            expanded.set_output_concepts(mandatory_list)

            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Found connections for {[c.address for c in mandatory_list]} via concept addition; removing extra {[c.address for c in extra]}"
            )
            return expanded
    # if we can't find it after expanding to a merge, then
    # attempt to accept partials in join paths

    if not accept_partial:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Stack is not connected graph, flag for accepting partial addresses is {accept_partial}, changing flag"
        )
        partial_search = search_concepts(
            mandatory_list=mandatory_list,
            environment=environment,
            depth=depth,
            g=g,
            accept_partial=True,
            history=history,
            conditions=conditions,
        )
        if partial_search:
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Found {[c.address for c in mandatory_list]} by accepting partials"
            )
            return partial_search
    logger.error(
        f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Could not resolve concepts {[c.address for c in mandatory_list]}, network outcome was {complete}, missing {all_mandatory - found},"
    )
    return None


def source_query_concepts(
    output_concepts: List[Concept],
    environment: Environment,
    g: Optional[ReferenceGraph] = None,
    conditions: Optional[WhereClause] = None,
    history: Optional[History] = None,
):
    if not output_concepts:
        raise ValueError(f"No output concepts provided {output_concepts}")
    if not g:
        g = generate_graph(environment)

    history = history or History()
    root = search_concepts(
        mandatory_list=output_concepts,
        environment=environment,
        g=g,
        depth=0,
        history=history,
        conditions=conditions,
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
