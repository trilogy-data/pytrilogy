from typing import List, Optional

from trilogy.constants import logger
from trilogy.core.enums import Derivation, Granularity
from trilogy.core.env_processor import generate_graph
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.author import (
    UndefinedConcept,
)
from trilogy.core.models.build import (
    BuildConcept,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.discovery_node_factory import generate_node
from trilogy.core.processing.discovery_utility import (
    LOGGER_PREFIX,
    depth_to_prefix,
    get_priority_concept,
)
from trilogy.core.processing.discovery_validation import (
    ValidationResult,
    validate_stack,
)
from trilogy.core.processing.nodes import (
    GroupNode,
    History,
    MergeNode,
    StrategyNode,
)
from trilogy.utility import unique


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
    logger.error(f"starting search for {mandatory_list}")
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
    # cache our values before an filter injection
    original_mandatory = [*mandatory_list]
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
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Force including conditions in {priority_concept.address} to push filtering above complex condition that is not condition member or parent"
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
                    Derivation.RECURSIVE,
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
            f" {accept_partial} (complete: {complete}), have {found} from {[n for n in stack]} (missing {missing} synonyms  partial {partial} virtual {virtual}), attempted {attempted}, mandatory w/ filter {mandatory_completion}"
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
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} breaking loop, complete"
            )
            break
        elif complete == ValidationResult.COMPLETE and accept_partial and partial:
            if len(attempted) == len(mandatory_list):
                logger.info(
                    f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Breaking as we have attempted all nodes"
                )
                break
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Found complete stack with partials {partial}, continuing search, attempted {attempted} all {len(mandatory_list)}"
            )
        else:
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Not complete, continuing search"
            )
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
        non_virtual_output = [c for c in original_mandatory if c.address not in virtual]
        non_virtual_different = len(completion_mandatory) != len(original_mandatory)
        non_virtual_difference_values = set(
            [x.address for x in completion_mandatory]
        ).difference(set([x.address for x in original_mandatory]))
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
            if non_virtual_different:
                logger.info(
                    f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Found different non-virtual output concepts ({non_virtual_difference_values}), removing condition injected values"
                )
                output.set_output_concepts(
                    [
                        x
                        for x in output.output_concepts
                        if x.address in non_virtual_output
                    ],
                    rebuild=False,
                )

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
        if condition_required and conditions and non_virtual_different:
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Conditions {conditions} were injected, checking if we need a group to restore grain"
            )
            result = GroupNode.check_if_required(
                downstream_concepts=original_mandatory,
                parents=[output.resolve()],
                environment=environment,
                depth=depth,
            )
            if result.required:
                logger.info(
                    f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Adding group node"
                )
                return GroupNode(
                    output_concepts=original_mandatory,
                    input_concepts=original_mandatory,
                    environment=environment,
                    parents=[output],
                    partial_concepts=output.partial_concepts,
                    preexisting_conditions=conditions.conditional,
                    depth=depth,
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
        raise UnresolvableQueryException(
            f"Could not resolve connections for query with output {error_strings} from current model."
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
