from dataclasses import dataclass
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

SKIPPED_DERIVATIONS = [
    Derivation.AGGREGATE,
    Derivation.FILTER,
    Derivation.WINDOW,
    Derivation.UNNEST,
    Derivation.RECURSIVE,
    Derivation.ROWSET,
    Derivation.BASIC,
    Derivation.GROUP_TO,
    Derivation.MULTISELECT,
    Derivation.UNION,
]

ROOT_DERIVATIONS = [Derivation.ROOT, Derivation.CONSTANT]


def generate_candidates_restrictive(
    priority_concept: BuildConcept,
    candidates: list[BuildConcept],
    exhausted: set[str],
    depth: int,
    conditions: BuildWhereClause | None = None,
) -> tuple[list[BuildConcept], BuildWhereClause | None]:
    local_candidates = [
        x
        for x in list(candidates)
        if x.address not in exhausted
        and x.granularity != Granularity.SINGLE_ROW
        and x.address not in priority_concept.pseudonyms
        and priority_concept.address not in x.pseudonyms
    ]
    if conditions and priority_concept.derivation in ROOT_DERIVATIONS:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Injecting additional conditional row arguments as all remaining concepts are roots or constant"
        )
        # otherwise, we can ignore the conditions now that we've injected inputs
        return (
            unique(list(conditions.row_arguments) + local_candidates, "address"),
            None,
        )
    # if it's single row, joins are irrelevant. Fetch without keys.
    if priority_concept.granularity == Granularity.SINGLE_ROW:
        return [], conditions

    return local_candidates, conditions


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
            logger.info(f"{LOGGER_PREFIX} found {[str(c) for c in subselect]}")
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


@dataclass
class LoopContext:
    mandatory_list: List[BuildConcept]
    environment: BuildEnvironment
    depth: int
    g: ReferenceGraph
    history: History
    attempted: set[str]
    found: set[str]
    skip: set[str]
    all_mandatory: set[str]
    original_mandatory: List[BuildConcept]
    completion_mandatory: List[BuildConcept]
    stack: List[StrategyNode]
    complete: ValidationResult = ValidationResult.INCOMPLETE
    accept_partial: bool = False
    must_evaluate_condition_on_this_level_not_push_down: bool = False
    conditions: BuildWhereClause | None = None

    @property
    def incomplete(self) -> bool:
        return self.attempted != self.all_mandatory


def initialize_loop_context(
    mandatory_list: List[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    history: History,
    accept_partial: bool = False,
    conditions: BuildWhereClause | None = None,
):
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
            if x.derivation not in ROOT_DERIVATIONS
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
    logger.info(
        f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Initialized loop context with mandatory list {[c.address for c in mandatory_list]} and completion mandatory {[c.address for c in completion_mandatory]}"
    )
    return LoopContext(
        mandatory_list=mandatory_list,
        environment=environment,
        depth=depth,
        g=g,
        history=history,
        attempted=set(),
        found=set(),
        skip=set(),
        all_mandatory=all_mandatory,
        original_mandatory=original_mandatory,
        completion_mandatory=completion_mandatory,
        stack=[],
        complete=ValidationResult.INCOMPLETE,
        accept_partial=accept_partial,
        must_evaluate_condition_on_this_level_not_push_down=must_evaluate_condition_on_this_level_not_push_down,
        conditions=conditions,
    )


def evaluate_loop_conditions(
    context: LoopContext, priority_concept: BuildConcept
) -> BuildWhereClause | None:
    # filter evaluation
    # always pass the filter up when we aren't looking at all filter inputs
    # or there are any non-filter complex types
    if context.conditions:
        should_evaluate_filter_on_this_level_not_push_down = all(
            [
                x.address in context.mandatory_list
                for x in context.conditions.row_arguments
            ]
        ) and not any(
            [
                x.derivation not in ROOT_DERIVATIONS
                for x in context.mandatory_list
                if x.address not in context.conditions.row_arguments
            ]
        )
    else:
        should_evaluate_filter_on_this_level_not_push_down = True
    local_conditions = (
        context.conditions
        if context.conditions
        and not context.must_evaluate_condition_on_this_level_not_push_down
        and not should_evaluate_filter_on_this_level_not_push_down
        else None
    )
    # but if it's not basic, and it's not condition;
    # we do need to push it down (and have another layer of filter evaluation)
    # to ensure filtering happens before something like a SUM
    if (
        context.conditions
        and priority_concept.derivation not in ROOT_DERIVATIONS
        and priority_concept.address not in context.conditions.row_arguments
    ):
        logger.info(
            f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} Force including conditions in {priority_concept.address} to push filtering above complex condition that is not condition member or parent"
        )
        local_conditions = context.conditions
    return local_conditions


def check_for_early_exit(
    complete, partial, missing, context: LoopContext, priority_concept: BuildConcept
) -> bool:
    if complete == ValidationResult.INCOMPLETE_CONDITION:
        cond_dict = {str(node): node.preexisting_conditions for node in context.stack}
        for node in context.stack:
            logger.info(
                f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} Node {node} has conditions {node.preexisting_conditions} and {node.conditions}"
            )
        raise SyntaxError(f"Have {cond_dict} and need {str(context.conditions)}")
    # early exit if we have a complete stack with one node
    # we can only early exit if we have a complete stack
    # and we are not looking for more non-partial sources
    if complete == ValidationResult.COMPLETE and (
        not context.accept_partial or (context.accept_partial and not partial)
    ):
        logger.info(
            f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} breaking loop, complete"
        )
        return True
    elif complete == ValidationResult.COMPLETE and context.accept_partial and partial:
        if len(context.attempted) == len(context.mandatory_list):
            logger.info(
                f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} Breaking as we have attempted all nodes"
            )
            return True
        logger.info(
            f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} Found complete stack with partials {partial}, continuing search, attempted {context.attempted} all {len(context.mandatory_list)}"
        )
    else:
        logger.info(
            f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} Not complete (missing {missing}), continuing search"
        )
    # if we have attempted on root node, we've tried them all.
    # inject in another search with filter concepts
    if priority_concept.derivation == Derivation.ROOT:
        logger.info(
            f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} Breaking as attempted root with no results"
        )
        return True
    return False


def generate_loop_completion(context: LoopContext, virtual: set[str]) -> StrategyNode:
    condition_required = True
    non_virtual = [c for c in context.completion_mandatory if c.address not in virtual]
    non_virtual_output = [
        c for c in context.original_mandatory if c.address not in virtual
    ]
    non_virtual_different = len(context.completion_mandatory) != len(
        context.original_mandatory
    )
    non_virtual_difference_values = set(
        [x.address for x in context.completion_mandatory]
    ).difference(set([x.address for x in context.original_mandatory]))
    if not context.conditions:
        condition_required = False
        non_virtual = [c for c in context.mandatory_list if c.address not in virtual]

    elif all(
        [
            x.preexisting_conditions == context.conditions.conditional
            for x in context.stack
        ]
    ):
        condition_required = False
        non_virtual = [c for c in context.mandatory_list if c.address not in virtual]

    if context.conditions and not condition_required:
        parent_map = {
            str(x): x.preexisting_conditions == context.conditions.conditional
            for x in context.stack
        }
        logger.info(
            f"Condition {context.conditions} not required, parents included filtering! {parent_map }"
        )
    if len(context.stack) == 1:
        output: StrategyNode = context.stack[0]
        if non_virtual_different:
            logger.info(
                f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} Found different non-virtual output concepts ({non_virtual_difference_values}), removing condition injected values by setting outputs to {[x.address for x in output.output_concepts if x.address in non_virtual_output]}"
            )
            output.set_output_concepts(
                [
                    x
                    for x in output.output_concepts
                    if x.address in non_virtual_output
                    or any(c in non_virtual_output for c in x.pseudonyms)
                ],
                rebuild=False,
            )

        logger.info(
            f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} Source stack has single node, returning that {type(output)}"
        )
    else:
        logger.info(
            f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} wrapping multiple parent nodes {[type(x) for x in context.stack]} in merge node"
        )
        output = MergeNode(
            input_concepts=non_virtual,
            output_concepts=non_virtual,
            environment=context.environment,
            parents=context.stack,
            depth=context.depth,
        )

    # ensure we can resolve our final merge
    output.resolve()
    if condition_required and context.conditions:
        output.add_condition(context.conditions.conditional)
        if context.conditions.existence_arguments:
            append_existence_check(
                output,
                context.environment,
                context.g,
                where=context.conditions,
                history=context.history,
            )
    elif context.conditions:
        output.preexisting_conditions = context.conditions.conditional
    logger.info(
        f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} Graph is connected, returning {type(output)} node output {[x.address for x in output.usable_outputs]} partial {[c.address for c in output.partial_concepts or []]} with {context.conditions}"
    )
    if condition_required and context.conditions and non_virtual_different:
        logger.info(
            f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} Conditions {context.conditions} were injected, checking if we need a group to restore grain"
        )

        result = GroupNode.check_if_required(
            downstream_concepts=output.usable_outputs,
            parents=[output.resolve()],
            environment=context.environment,
            depth=context.depth,
        )
        if result.required:
            logger.info(
                f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} Adding group node with outputs {[x.address for x in context.original_mandatory]}"
            )
            return GroupNode(
                output_concepts=context.original_mandatory,
                input_concepts=output.usable_outputs,
                environment=context.environment,
                parents=[output],
                partial_concepts=output.partial_concepts,
                preexisting_conditions=context.conditions.conditional,
                depth=context.depth,
            )
    return output


def _search_concepts(
    mandatory_list: List[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    history: History,
    accept_partial: bool = False,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    # check for direct materialization first
    candidate = history.gen_select_node(
        mandatory_list,
        environment,
        g,
        depth + 1,
        fail_if_not_found=False,
        accept_partial=accept_partial,
        conditions=conditions,
    )

    if candidate:
        return candidate
    context = initialize_loop_context(
        mandatory_list=mandatory_list,
        environment=environment,
        depth=depth,
        g=g,
        history=history,
        accept_partial=accept_partial,
        conditions=conditions,
    )

    while context.incomplete:

        priority_concept = get_priority_concept(
            context.mandatory_list,
            context.attempted,
            found_concepts=context.found,
            depth=depth,
        )

        local_conditions = evaluate_loop_conditions(context, priority_concept)

        candidates = [
            c for c in context.mandatory_list if c.address != priority_concept.address
        ]
        # the local conditions list may be overriden if we end up injecting conditions
        candidate_list, local_conditions = generate_candidates_restrictive(
            priority_concept,
            candidates,
            context.skip,
            depth=depth,
            conditions=local_conditions,
        )

        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} priority concept is {str(priority_concept)} derivation {priority_concept.derivation} granularity {priority_concept.granularity} with conditions {local_conditions}"
        )

        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Beginning sourcing loop for {priority_concept.address}, accept_partial {accept_partial}, optional {[v.address for v in candidate_list]}, exhausted {[c for c in context.skip]}"
        )
        node = generate_node(
            priority_concept,
            candidate_list,
            environment,
            g,
            depth,
            source_concepts=search_concepts,
            accept_partial=accept_partial,
            history=history,
            conditions=local_conditions,
        )
        if node:
            context.stack.append(node)
            node.resolve()
            # these concepts should not be attempted to be sourced again
            # as fetching them requires operating on a subset of concepts
            if priority_concept.derivation in SKIPPED_DERIVATIONS:
                context.skip.add(priority_concept.address)
        context.attempted.add(priority_concept.address)
        complete, found_c, missing_c, partial, virtual = validate_stack(
            environment,
            context.stack,
            context.mandatory_list,
            context.completion_mandatory,
            conditions=context.conditions,
            accept_partial=accept_partial,
        )
        # assign
        context.found = found_c
        early_exit = check_for_early_exit(
            complete, partial, missing_c, context, priority_concept
        )
        if early_exit:
            break

    logger.info(
        f"{depth_to_prefix(depth)}{LOGGER_PREFIX} finished sourcing loop (complete: {complete}), have {context.found} from {[n for n in context.stack]} (missing {context.all_mandatory - context.found}), attempted {context.attempted}, virtual {virtual}"
    )
    if complete == ValidationResult.COMPLETE:
        return generate_loop_completion(context, virtual)

    # if we can't find it after expanding to a merge, then
    # accept partials in join paths
    if not accept_partial:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Stack is not connected graph, flag for accepting partial addresses is {accept_partial}, changing flag"
        )
        partial_search = search_concepts(
            # use the original mandatory list
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
        f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Could not resolve concepts {[c.address for c in mandatory_list]}, network outcome was {complete}, missing {context.all_mandatory - context.found},"
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
