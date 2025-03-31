from collections import defaultdict
from enum import Enum
from typing import List, Optional, Protocol, Union

from trilogy.constants import logger
from trilogy.core.enums import Derivation, FunctionType, Granularity
from trilogy.core.env_processor import generate_graph
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.author import (
    UndefinedConcept,
)
from trilogy.core.models.build import (
    BuildConcept,
    BuildFunction,
    BuildRowsetItem,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators import (
    gen_basic_node,
    gen_filter_node,
    gen_group_node,
    gen_group_to_node,
    gen_merge_node,
    gen_multiselect_node,
    gen_rowset_node,
    gen_synonym_node,
    gen_union_node,
    gen_unnest_node,
    gen_window_node,
)
from trilogy.core.processing.nodes import (
    ConstantNode,
    GroupNode,
    History,
    MergeNode,
    StrategyNode,
)
from trilogy.core.processing.utility import (
    get_disconnected_components,
)
from trilogy.utility import unique


class ValidationResult(Enum):
    COMPLETE = 1
    DISCONNECTED = 2
    INCOMPLETE = 3
    INCOMPLETE_CONDITION = 4


LOGGER_PREFIX = "[CONCEPT DETAIL]"


class SearchConceptsType(Protocol):
    def __call__(
        self,
        mandatory_list: List[BuildConcept],
        history: History,
        environment: BuildEnvironment,
        depth: int,
        g: ReferenceGraph,
        accept_partial: bool = False,
        conditions: Optional[BuildWhereClause] = None,
    ) -> Union[StrategyNode, None]: ...


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


def restrict_node_outputs_targets(
    node: StrategyNode, targets: list[BuildConcept], depth: int
) -> list[BuildConcept]:
    ex_resolve = node.resolve()
    extra = [
        x
        for x in ex_resolve.output_concepts
        if x.address not in [y.address for y in targets]
    ]

    base = [x for x in ex_resolve.output_concepts if x.address not in extra]
    logger.info(
        f"{depth_to_prefix(depth)}{LOGGER_PREFIX} reducing final outputs, was {[c.address for c in ex_resolve.output_concepts]} with extra {[c.address for c in extra]}, remaining {base}"
    )
    for x in targets:
        if x.address not in base:
            base.append(x)
    node.set_output_concepts(base)
    return extra


def get_priority_concept(
    all_concepts: List[BuildConcept],
    attempted_addresses: set[str],
    found_concepts: set[str],
    depth: int,
) -> BuildConcept:
    # optimized search for missing concepts
    pass_one = [
        c
        for c in all_concepts
        if c.address not in attempted_addresses and c.address not in found_concepts
    ]
    # sometimes we need to scan intermediate concepts to get merge keys or filter keys,
    # so do an exhaustive search
    # pass_two = [c for c in all_concepts+filter_only if c.address not in attempted_addresses]
    for remaining_concept in (pass_one,):
        priority = (
            # find anything that needs no joins first, so we can exit early
            [
                c
                for c in remaining_concept
                if c.derivation == Derivation.CONSTANT
                and c.granularity == Granularity.SINGLE_ROW
            ]
            +
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
            + [c for c in remaining_concept if c.derivation == Derivation.BASIC]
            # finally our plain selects
            + [
                c for c in remaining_concept if c.derivation == Derivation.ROOT
            ]  # and any non-single row constants
            + [c for c in remaining_concept if c.derivation == Derivation.CONSTANT]
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
        f"Cannot resolve query. No remaining priority concepts, have attempted {attempted_addresses}"
    )


def generate_candidates_restrictive(
    priority_concept: BuildConcept,
    candidates: list[BuildConcept],
    exhausted: set[str],
    depth: int,
    conditions: BuildWhereClause | None = None,
) -> List[List[BuildConcept]]:
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
    if conditions and priority_concept.derivation in (
        Derivation.ROOT,
        Derivation.CONSTANT,
    ):
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Injecting additional conditional row arguments as all remaining concepts are roots or constant"
        )
        return [unique(list(conditions.row_arguments) + local_candidates, "address")]
    return [local_candidates]


def generate_node(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    environment: BuildEnvironment,
    g: ReferenceGraph,
    depth: int,
    source_concepts: SearchConceptsType,
    history: History,
    accept_partial: bool = False,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    # first check in case there is a materialized_concept
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

    if concept.derivation == Derivation.WINDOW:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating window node with optional {[x.address for x in local_optional]}"
        )
        return gen_window_node(
            concept,
            local_optional,
            history=history,
            environment=environment,
            g=g,
            depth=depth + 1,
            source_concepts=source_concepts,
            conditions=conditions,
        )

    elif concept.derivation == Derivation.FILTER:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating filter node with optional {[x.address for x in local_optional]}"
        )
        return gen_filter_node(
            concept,
            local_optional,
            history=history,
            environment=environment,
            g=g,
            depth=depth + 1,
            source_concepts=source_concepts,
            conditions=conditions,
        )
    elif concept.derivation == Derivation.UNNEST:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating unnest node with optional {[x.address for x in local_optional]} and condition {conditions}"
        )
        return gen_unnest_node(
            concept,
            local_optional,
            history=history,
            environment=environment,
            g=g,
            depth=depth + 1,
            source_concepts=source_concepts,
            conditions=conditions,
        )
    elif concept.derivation == Derivation.UNION:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating union node with optional {[x.address for x in local_optional]} and condition {conditions}"
        )
        return gen_union_node(
            concept,
            local_optional,
            environment,
            g,
            depth + 1,
            source_concepts,
            history,
            conditions=conditions,
        )
    elif concept.derivation == Derivation.AGGREGATE:
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
            history=history,
            environment=environment,
            g=g,
            depth=depth + 1,
            source_concepts=source_concepts,
            conditions=conditions,
        )
    elif concept.derivation == Derivation.ROWSET:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating rowset node with optional {[x.address for x in local_optional]}"
        )
        return gen_rowset_node(
            concept,
            local_optional,
            environment,
            g,
            depth + 1,
            source_concepts,
            history,
            conditions=conditions,
        )
    elif concept.derivation == Derivation.MULTISELECT:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating multiselect node with optional {[x.address for x in local_optional]}"
        )
        return gen_multiselect_node(
            concept,
            local_optional,
            environment,
            g,
            depth + 1,
            source_concepts,
            history,
            conditions=conditions,
        )
    elif concept.derivation == Derivation.CONSTANT:
        constant_targets = [concept] + local_optional
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating constant node"
        )
        if any([x.derivation != Derivation.CONSTANT for x in local_optional]):
            non_root = [
                x.address for x in local_optional if x.derivation != Derivation.CONSTANT
            ]
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} including filter concepts, there are non root/non constant concepts we should find first: {non_root}. Recursing with all of these as mandatory"
            )

            if not history.check_started(
                constant_targets, accept_partial=accept_partial, conditions=conditions
            ):
                history.log_start(
                    constant_targets,
                    accept_partial=accept_partial,
                    conditions=conditions,
                )
                return source_concepts(
                    mandatory_list=constant_targets,
                    environment=environment,
                    g=g,
                    depth=depth + 1,
                    accept_partial=accept_partial,
                    history=history,
                    # we DO NOT pass up conditions at this point, as we are now expanding to include conditions in search
                    # which we do whenever we hit a root node
                    # conditions=conditions,
                )
            else:
                return None
        return ConstantNode(
            input_concepts=[],
            output_concepts=constant_targets,
            environment=environment,
            parents=[],
            depth=depth + 1,
            preexisting_conditions=conditions.conditional if conditions else None,
        )
    elif concept.derivation == Derivation.BASIC:
        # this is special case handling for group bys
        if (
            isinstance(concept.lineage, BuildFunction)
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
                conditions=conditions,
            )
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating basic node with optional {[x.address for x in local_optional]}"
        )
        return gen_basic_node(
            concept,
            local_optional,
            history=history,
            environment=environment,
            g=g,
            depth=depth + 1,
            source_concepts=source_concepts,
            conditions=conditions,
        )

    elif concept.derivation == Derivation.ROOT:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating select node with optional including condition inputs {[x.address for x in local_optional]}"
        )
        # we've injected in any conditional concepts that may exist
        # so if we don't still have just roots, we need to go up
        root_targets = [concept] + local_optional

        if any(
            [
                x.derivation not in (Derivation.ROOT, Derivation.CONSTANT)
                for x in local_optional
            ]
        ):
            non_root = [
                x.address
                for x in local_optional
                if x.derivation not in (Derivation.ROOT, Derivation.CONSTANT)
            ]
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} including any filters, there are non-root concepts we should expand first: {non_root}. Recursing with all of these as mandatory"
            )

            if not history.check_started(
                root_targets, accept_partial=accept_partial, conditions=conditions
            ):
                history.log_start(
                    root_targets, accept_partial=accept_partial, conditions=conditions
                )
                return source_concepts(
                    mandatory_list=root_targets,
                    environment=environment,
                    g=g,
                    depth=depth + 1,
                    accept_partial=accept_partial,
                    history=history,
                    # we DO NOT pass up conditions at this point, as we are now expanding to include conditions in search
                    # which we do whenever we hit a root node
                    # conditions=conditions,
                )

        check = history.gen_select_node(
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
        if not check:

            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Could not resolve root concepts, checking for expanded concepts"
            )
            for accept_partial in [False, True]:
                expanded = gen_merge_node(
                    all_concepts=root_targets,
                    environment=environment,
                    g=g,
                    depth=depth + 1,
                    source_concepts=source_concepts,
                    history=history,
                    search_conditions=conditions,
                    accept_partial=accept_partial,
                )

                if expanded:
                    extra = restrict_node_outputs_targets(expanded, root_targets, depth)
                    pseudonyms = [
                        x
                        for x in extra
                        if any(x.address in y.pseudonyms for y in root_targets)
                    ]
                    if pseudonyms:
                        expanded.add_output_concepts(pseudonyms)
                        logger.info(
                            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Hiding pseudonyms{[c.address for c in pseudonyms]}"
                        )
                        expanded.hide_output_concepts(pseudonyms)

                    logger.info(
                        f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Found connections for {[c.address for c in root_targets]} via concept addition; removing extra {[c.address for c in extra]}"
                    )
                    return expanded

            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} could not find additional concept(s) to inject"
            )
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Could not resolve root concepts, checking for synonyms"
            )
            if not history.check_started(
                root_targets, accept_partial=accept_partial, conditions=conditions
            ):
                history.log_start(
                    root_targets, accept_partial=accept_partial, conditions=conditions
                )
                resolved = gen_synonym_node(
                    all_concepts=root_targets,
                    environment=environment,
                    g=g,
                    depth=depth + 1,
                    source_concepts=source_concepts,
                    history=history,
                    conditions=conditions,
                    accept_partial=accept_partial,
                )
                if resolved:
                    logger.info(
                        f"{depth_to_prefix(depth)}{LOGGER_PREFIX} resolved concepts through synonyms"
                    )
                    return resolved

            return None
    else:
        raise ValueError(f"Unknown derivation {concept.derivation} on {concept}")
    return None


def validate_concept(
    concept: BuildConcept,
    node: StrategyNode,
    found_addresses: set[str],
    non_partial_addresses: set[str],
    partial_addresses: set[str],
    virtual_addresses: set[str],
    found_map: dict[str, set[BuildConcept]],
    accept_partial: bool,
    seen: set[str],
    environment: BuildEnvironment,
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
    for v_address in concept.pseudonyms:
        if v_address in seen:
            return
        v = environment.concepts[v_address]
        if v.address in seen:
            return
        if v.address == concept.address:
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
            environment=environment,
        )


def validate_stack(
    environment: BuildEnvironment,
    stack: List[StrategyNode],
    concepts: List[BuildConcept],
    mandatory_with_filter: List[BuildConcept],
    conditions: BuildWhereClause | None = None,
    accept_partial: bool = False,
) -> tuple[ValidationResult, set[str], set[str], set[str], set[str]]:
    found_map: dict[str, set[BuildConcept]] = defaultdict(set)
    found_addresses: set[str] = set()
    non_partial_addresses: set[str] = set()
    partial_addresses: set[str] = set()
    virtual_addresses: set[str] = set()
    seen: set[str] = set()

    for node in stack:
        resolved = node.resolve()

        for concept in resolved.output_concepts:
            if concept.address in resolved.hidden_concepts:
                continue
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
                environment,
            )
        for concept in node.virtual_output_concepts:
            if concept.address in non_partial_addresses:
                continue
            found_addresses.add(concept.address)
            virtual_addresses.add(concept.address)
    if not conditions:
        conditions_met = True
    else:
        conditions_met = all(
            [node.preexisting_conditions == conditions.conditional for node in stack]
        ) or all([c.address in found_addresses for c in mandatory_with_filter])
    # zip in those we know we found
    if not all([c.address in found_addresses for c in concepts]) or not conditions_met:
        if not all([c.address in found_addresses for c in concepts]):
            return (
                ValidationResult.INCOMPLETE,
                found_addresses,
                {c.address for c in concepts if c.address not in found_addresses},
                partial_addresses,
                virtual_addresses,
            )
        return (
            ValidationResult.INCOMPLETE_CONDITION,
            found_addresses,
            {c.address for c in concepts if c.address not in mandatory_with_filter},
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


def append_existence_check(
    node: StrategyNode,
    environment: BuildEnvironment,
    graph: ReferenceGraph,
    where: BuildWhereClause,
    history: History,
):
    # we if we have a where clause doing an existence check
    # treat that as separate subquery
    if where.existence_arguments:
        for subselect in where.existence_arguments:
            if not subselect:
                continue
            if all([x.address in node.input_concepts for x in subselect]):
                logger.info(
                    f"{LOGGER_PREFIX} existance clause inputs already found {[str(c) for c in subselect]}"
                )
                continue
            logger.info(
                f"{LOGGER_PREFIX} fetching existence clause inputs {[str(c) for c in subselect]}"
            )
            parent = source_query_concepts(
                [*subselect],
                history=history,
                environment=environment,
                g=graph,
            )
            assert parent, "Could not resolve existence clause"
            node.add_parents([parent])
            logger.info(
                f"{LOGGER_PREFIX} fetching existence clause inputs {[str(c) for c in subselect]}"
            )
            node.add_existence_concepts([*subselect])


def search_concepts(
    mandatory_list: List[BuildConcept],
    history: History,
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    accept_partial: bool = False,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
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
    mandatory_list: List[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    history: History,
    accept_partial: bool = False,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    # these are the concepts we need in the output projection
    mandatory_list = unique(mandatory_list, "address")
    for x in mandatory_list:
        if isinstance(x, UndefinedConcept):
            raise SyntaxError(f"Undefined concept {x.address}")
    all_mandatory = set(c.address for c in mandatory_list)

    must_evaluate_condition_on_this_level_not_push_down = False

    # if we have a filter, we may need to get more values to support that.
    if conditions:
        completion_mandatory = unique(
            mandatory_list + list(conditions.row_arguments), "address"
        )
        # if anything we need to get is in the filter set and it's a computed value
        # we need to get _everything_ in this loop
        required_filters = [
            x
            for x in mandatory_list
            if x.derivation not in (Derivation.ROOT, Derivation.CONSTANT)
            and not (
                x.derivation == Derivation.AGGREGATE
                and x.granularity == Granularity.SINGLE_ROW
            )
            and x.address in conditions.row_arguments
        ]
        if any(required_filters):
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} derived condition row inputs {[x.address for x in required_filters]} present in mandatory list, forcing condition evaluation at this level. "
            )
            mandatory_list = completion_mandatory
            must_evaluate_condition_on_this_level_not_push_down = True
        else:
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Do not need to evaluate conditions yet."
            )
    else:

        completion_mandatory = mandatory_list
    attempted: set[str] = set()

    found: set[str] = set()
    skip: set[str] = set()
    virtual: set[str] = set()
    stack: List[StrategyNode] = []
    complete = ValidationResult.INCOMPLETE

    while attempted != all_mandatory:
        priority_concept = get_priority_concept(
            mandatory_list,
            attempted,
            found_concepts=found,
            depth=depth,
        )
        # filter evaluation
        # always pass the filter up when we aren't looking at all filter inputs
        # or there are any non-filter complex types
        if conditions:
            should_evaluate_filter_on_this_level_not_push_down = all(
                [x.address in mandatory_list for x in conditions.row_arguments]
            ) and not any(
                [
                    x.derivation not in (Derivation.ROOT, Derivation.CONSTANT)
                    for x in mandatory_list
                    if x.address not in conditions.row_arguments
                ]
            )
        else:
            should_evaluate_filter_on_this_level_not_push_down = True
        local_conditions = (
            conditions
            if conditions
            and not must_evaluate_condition_on_this_level_not_push_down
            and not should_evaluate_filter_on_this_level_not_push_down
            else None
        )
        # but if it's not basic, and it's not condition;
        # we do need to push it down (and have another layer of filter evaluation)
        # to ensure filtering happens before something like a SUM
        if (
            conditions
            and priority_concept.derivation
            not in (Derivation.ROOT, Derivation.CONSTANT)
            and priority_concept.address not in conditions.row_arguments
        ):
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Force including conditions to push filtering above complex condition that is not condition member or parent"
            )
            local_conditions = conditions

        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} priority concept is {str(priority_concept)} derivation {priority_concept.derivation} granularity {priority_concept.granularity} with conditions {local_conditions}"
        )

        candidates = [
            c for c in mandatory_list if c.address != priority_concept.address
        ]
        candidate_lists = generate_candidates_restrictive(
            priority_concept, candidates, skip, depth=depth, conditions=conditions
        )
        for clist in candidate_lists:
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Beginning sourcing loop for {priority_concept.address}, accept_partial {accept_partial}, optional {[v.address for v in clist]}, exhausted {[c for c in skip]}"
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
                conditions=local_conditions,
            )
            if node:
                stack.append(node)
                try:
                    node.resolve()
                except Exception as e:
                    logger.error(
                        f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Could not resolve node {node} {e}"
                    )
                    raise e
                # these concepts should not be attempted to be sourced again
                # as fetching them requires operating on a subset of concepts
                if priority_concept.derivation in [
                    Derivation.AGGREGATE,
                    Derivation.FILTER,
                    Derivation.WINDOW,
                    Derivation.UNNEST,
                    Derivation.ROWSET,
                    Derivation.BASIC,
                    Derivation.MULTISELECT,
                    Derivation.UNION,
                ]:
                    skip.add(priority_concept.address)
                break
        attempted.add(priority_concept.address)
        complete, found, missing, partial, virtual = validate_stack(
            environment,
            stack,
            mandatory_list,
            completion_mandatory,
            conditions=conditions,
            accept_partial=accept_partial,
        )
        mandatory_completion = [c.address for c in completion_mandatory]
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} finished concept loop for {priority_concept} {priority_concept.derivation} condition {conditions} flag for accepting partial addresses is"
            f" {accept_partial} (complete: {complete}), have {found} from {[n for n in stack]} (missing {missing} partial {partial} virtual {virtual}), attempted {attempted}, mandatory w/ filter {mandatory_completion}"
        )
        if complete == ValidationResult.INCOMPLETE_CONDITION:
            cond_dict = {str(node): node.preexisting_conditions for node in stack}
            logger.error(f"Have {cond_dict} and need {str(conditions)}")
            raise SyntaxError(f"Have {cond_dict} and need {str(conditions)}")
        # early exit if we have a complete stack with one node
        # we can only early exit if we have a complete stack
        # and we are not looking for more non-partial sources
        if complete == ValidationResult.COMPLETE and (
            not accept_partial or (accept_partial and not partial)
        ):
            break
        # if we have attempted on root node, we've tried them all.
        # inject in another search with filter concepts
        if priority_concept.derivation == Derivation.ROOT:
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Breaking as attempted root with no results"
            )
            break

    logger.info(
        f"{depth_to_prefix(depth)}{LOGGER_PREFIX} finished sourcing loop (complete: {complete}), have {found} from {[n for n in stack]} (missing {all_mandatory - found}), attempted {attempted}, virtual {virtual}"
    )
    if complete == ValidationResult.COMPLETE:
        condition_required = True
        non_virtual = [c for c in completion_mandatory if c.address not in virtual]
        if not conditions:
            condition_required = False
            non_virtual = [c for c in mandatory_list if c.address not in virtual]

        elif all([x.preexisting_conditions == conditions.conditional for x in stack]):
            condition_required = False
            non_virtual = [c for c in mandatory_list if c.address not in virtual]

        if conditions and not condition_required:
            parent_map = {
                str(x): x.preexisting_conditions == conditions.conditional
                for x in stack
            }
            logger.info(
                f"Condition {conditions} not required, parents included filtering! {parent_map }"
            )
        if len(stack) == 1:
            output: StrategyNode = stack[0]
            # _ = restrict_node_outputs_targets(output, mandatory_list, depth)
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Source stack has single node, returning that {type(output)}"
            )
        else:
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} wrapping multiple parent nodes {[type(x) for x in stack]} in merge node"
            )
            output = MergeNode(
                input_concepts=non_virtual,
                output_concepts=non_virtual,
                environment=environment,
                parents=stack,
                depth=depth,
            )

        # ensure we can resolve our final merge
        output.resolve()
        if condition_required and conditions:
            output.add_condition(conditions.conditional)
            if conditions.existence_arguments:
                append_existence_check(
                    output, environment, g, where=conditions, history=history
                )
        elif conditions:
            output.preexisting_conditions = conditions.conditional
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Graph is connected, returning {type(output)} node partial {[c.address for c in output.partial_concepts]}"
        )
        return output

    # if we can't find it after expanding to a merge, then
    # accept partials in join paths

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
    output_concepts: List[BuildConcept],
    history: History,
    environment: BuildEnvironment,
    g: Optional[ReferenceGraph] = None,
    conditions: Optional[BuildWhereClause] = None,
):
    if not output_concepts:
        raise ValueError(f"No output concepts provided {output_concepts}")
    if not g:
        g = generate_graph(environment)

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
            f"Could not resolve connections between {error_strings} from environment graph."
        )
    final = [x for x in root.output_concepts if x.address not in root.hidden_concepts]
    logger.info(
        f"{depth_to_prefix(0)}{LOGGER_PREFIX} final concepts are {[x.address for x in final]}"
    )
    if GroupNode.check_if_required(
        downstream_concepts=final,
        parents=[root.resolve()],
        environment=environment,
    ).required:
        candidate: StrategyNode = GroupNode(
            output_concepts=final,
            input_concepts=final,
            environment=environment,
            parents=[root],
            partial_concepts=root.partial_concepts,
        )
    else:
        candidate = root

    return candidate
