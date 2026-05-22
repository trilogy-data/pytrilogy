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
from trilogy.core.processing.condition_utility import condition_implies
from trilogy.core.processing.condition_context import (
    BuildConditionContext,
    ConditionInput,
)
from trilogy.core.processing.constants import ROOT_DERIVATIONS, SKIPPED_DERIVATIONS
from trilogy.core.processing.discovery_node_factory import generate_node
from trilogy.core.processing.discovery_utility import (
    LOGGER_PREFIX,
    depth_to_prefix,
    get_loop_iteration_targets,
    group_if_required_v2,
)
from trilogy.core.processing.discovery_validation import (
    ValidationResult,
    _is_scalar_only,
    validate_stack,
)
from trilogy.core.processing.nodes import (
    History,
    MergeNode,
    StrategyNode,
)
from trilogy.utility import unique


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
    conditions: ConditionInput = None,
) -> StrategyNode | None:
    condition_context = BuildConditionContext.normalize(conditions)
    active_conditions = (
        condition_context.active_where if condition_context is not None else None
    )
    hist = history.get_history(
        search=mandatory_list,
        accept_partial=accept_partial,
        conditions=condition_context,
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
        conditions=condition_context,
    )
    # a node may be mutated after be cached; always store a copy
    history.search_to_history(
        mandatory_list,
        accept_partial,
        result.copy() if result else None,
        conditions=condition_context,
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
    conditions: BuildConditionContext | None = None

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
    conditions: BuildConditionContext | None = None,
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
    discovery_conditions = conditions.discovery_where if conditions else None
    if discovery_conditions:
        discovery_condition_row_addresses = {
            concept.address for concept in discovery_conditions.row_arguments
        }
        completion_mandatory = unique(
            mandatory_list + list(discovery_conditions.row_arguments), "address"
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
            and x.address in discovery_condition_row_addresses
        ]
        if not required_filters:
            required_filters = [
                x for x in discovery_conditions.row_arguments if x.is_aggregate
            ]
        if any(required_filters):
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} derived condition row inputs {[x.address for x in required_filters]} present in mandatory list, forcing condition evaluation at this level. "
            )
            mandatory_list = completion_mandatory
            all_mandatory = set(c.address for c in completion_mandatory)
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


def _node_satisfies_condition(
    node: StrategyNode,
    condition: BuildConditionContext | None,
) -> bool:
    active = condition.active_where if condition else None
    if active is None:
        return True
    return node.preexisting_conditions == active.conditional or (
        node.preexisting_conditions is not None
        and condition_implies(node.preexisting_conditions, active.conditional)
    )


def _advance_condition_result(
    result: StrategyNode | None,
    mandatory_list: list[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    history: History,
    accept_partial: bool,
    conditions: BuildConditionContext | None,
) -> StrategyNode | None:
    if result is None or conditions is None or conditions.is_complete:
        return result
    if not _node_satisfies_condition(result, conditions):
        return result
    advanced = conditions.advance()
    if advanced.is_complete:
        return result
    stage_where = advanced.current_where
    can_merge_stage_inputs = bool(
        stage_where
        and stage_where.row_arguments
        and all(x.is_aggregate for x in stage_where.row_arguments)
    )
    if stage_where is not None and can_merge_stage_inputs:
        applied = BuildConditionContext(applied=advanced.applied)
        condition_inputs = unique(list(stage_where.row_arguments), "address")
        condition_node = search_concepts(
            mandatory_list=condition_inputs,
            environment=environment,
            depth=depth,
            g=g,
            accept_partial=accept_partial,
            history=history,
            conditions=applied,
        )
        if condition_node:
            merged_outputs = unique(mandatory_list + condition_inputs, "address")
            merged = MergeNode(
                input_concepts=merged_outputs,
                output_concepts=merged_outputs,
                environment=environment,
                parents=[result, condition_node],
                depth=depth,
                preexisting_conditions=applied.active_where.conditional
                if applied.active_where
                else None,
            )
            merged.add_condition(stage_where.conditional)
            if advanced.active_where:
                merged.set_preexisting_conditions(advanced.active_where.conditional)
            if stage_where.existence_arguments:
                append_existence_check(
                    merged,
                    environment,
                    g,
                    where=stage_where,
                    history=history,
                )
            return _advance_condition_result(
                merged,
                mandatory_list,
                environment,
                depth,
                g,
                history,
                accept_partial,
                advanced,
            )
    return search_concepts(
        mandatory_list=mandatory_list,
        environment=environment,
        depth=depth,
        g=g,
        accept_partial=accept_partial,
        history=history,
        conditions=advanced,
    )


def _condition_references_node_output(
    node: StrategyNode,
    condition: BuildWhereClause,
) -> bool:
    output_addresses = {concept.address for concept in node.usable_outputs}
    return any(arg.address in output_addresses for arg in condition.row_arguments)


def check_for_early_exit(
    complete: ValidationResult,
    found: set[str],
    partial: set[str],
    missing: set[str],
    context: LoopContext,
    priority_concept: BuildConcept,
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
        elif all(
            [
                x.address in found and x.address not in partial
                for x in context.mandatory_list
            ]
        ):
            logger.info(
                f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} Breaking as we have found all mandatory nodes without partials"
            )
            return True
        logger.info(
            f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} Found complete stack with partials {partial}, continuing search, attempted {context.attempted} of total {len(context.mandatory_list)}."
        )
    else:
        logger.info(
            f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} Not complete (missing {missing}), continuing search"
        )
    # if we have attempted one root node, we've tried them all.
    # unless it's a single row property, in which case we can keep looking
    if (
        priority_concept.derivation == Derivation.ROOT
        and not priority_concept.granularity == Granularity.SINGLE_ROW
    ):
        logger.info(
            f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} Breaking as attempted root with no results"
        )
        return True
    return False


def generate_loop_completion(context: LoopContext, virtual: set[str]) -> StrategyNode:
    condition_required = True
    active_conditions = context.conditions.active_where if context.conditions else None
    discovery_conditions = (
        context.conditions.discovery_where if context.conditions else None
    )
    non_virtual = [c for c in context.completion_mandatory if c.address not in virtual]
    non_virtual_different = len(context.completion_mandatory) != len(
        context.original_mandatory
    )
    non_virtual_difference_values = set(
        [x.address for x in context.completion_mandatory]
    ).difference(set([x.address for x in context.original_mandatory]))
    if not discovery_conditions:
        condition_required = False
        non_virtual = [c for c in context.mandatory_list if c.address not in virtual]

    elif all(
        [
            x.preexisting_conditions == discovery_conditions.conditional
            or (
                x.preexisting_conditions is not None
                and condition_implies(
                    x.preexisting_conditions, discovery_conditions.conditional
                )
            )
            or _is_scalar_only(x)
            and not _condition_references_node_output(x, discovery_conditions)
            for x in context.stack
        ]
    ):
        condition_required = False
        non_virtual = [c for c in context.mandatory_list if c.address not in virtual]

    if active_conditions and not condition_required:
        parent_map = {
            str(x): x.preexisting_conditions == active_conditions.conditional
            for x in context.stack
        }
        logger.info(
            f"Condition {context.conditions} not required, parents included filtering! {parent_map}"
        )

    if len(context.stack) == 1:
        output: StrategyNode = context.stack[0]
        if non_virtual_different:
            logger.info(
                f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} Found added non-virtual output concepts ({non_virtual_difference_values})"
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
    if condition_required and discovery_conditions:
        output.add_condition(discovery_conditions.conditional)
        if active_conditions is not None:
            output.set_preexisting_conditions(active_conditions.conditional)
        if discovery_conditions.existence_arguments:
            append_existence_check(
                output,
                context.environment,
                context.g,
                where=discovery_conditions,
                history=context.history,
            )
    elif active_conditions and discovery_conditions:
        output.preexisting_conditions = active_conditions.conditional
    logger.info(
        f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} Graph is connected, returning {type(output)} node output {[x.address for x in output.usable_outputs]} partial {[c.address for c in output.partial_concepts or []]} with {context.conditions}"
    )

    if condition_required and discovery_conditions and non_virtual_different:
        logger.info(
            f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} Conditions {context.conditions} were injected, checking if we need a group to restore grain"
        )
        return group_if_required_v2(
            output,
            context.original_mandatory,
            context.environment,
            non_virtual_difference_values,
            depth=context.depth,
        )

    return group_if_required_v2(
        output,
        context.original_mandatory,
        context.environment,
        non_virtual_difference_values,
        depth=context.depth,
    )


def _search_concepts(
    mandatory_list: List[BuildConcept],
    environment: BuildEnvironment,
    depth: int,
    g: ReferenceGraph,
    history: History,
    accept_partial: bool = False,
    conditions: BuildConditionContext | None = None,
) -> StrategyNode | None:
    active_conditions = conditions.active_where if conditions else None
    discovery_conditions = conditions.discovery_where if conditions else None
    # check for direct materialization first
    candidate = history.gen_select_node(
        mandatory_list,
        environment,
        g,
        depth + 1,
        fail_if_not_found=False,
        accept_partial=accept_partial,
        conditions=active_conditions,
    )

    # if we get a can
    if candidate:
        return _advance_condition_result(
            candidate,
            mandatory_list,
            environment,
            depth,
            g,
            history,
            accept_partial,
            conditions,
        )

    context = initialize_loop_context(
        mandatory_list=mandatory_list,
        environment=environment,
        depth=depth,
        g=g,
        history=history,
        accept_partial=accept_partial,
        conditions=conditions,
    )
    partial: set[str] = set()
    virtual: set[str] = set()
    complete = ValidationResult.INCOMPLETE
    while context.incomplete:
        priority_concept, candidate_list, local_conditions = get_loop_iteration_targets(
            mandatory=context.mandatory_list,
            conditions=discovery_conditions,
            attempted=context.attempted,
            force_conditions=context.must_evaluate_condition_on_this_level_not_push_down,
            found=context.found,
            partial=partial,
            depth=depth,
            materialized_canonical=(
                environment.non_partial_materialized_canonical_concepts
                if not accept_partial
                else environment.materialized_canonical_concepts
            ),
            environment=environment,
        )
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} priority concept is {str(priority_concept)} derivation {priority_concept.derivation} granularity {priority_concept.granularity} with conditions {local_conditions}"
        )

        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Beginning sourcing loop for {priority_concept.address}, accept_partial {accept_partial}, optional {[v.address for v in candidate_list]}, exhausted {[c for c in context.skip]}"
        )
        node_conditions = (
            conditions.focus(local_conditions)
            if conditions
            else BuildConditionContext.from_where_clause(local_conditions)
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
            conditions=node_conditions,
            required_concepts=context.mandatory_list,
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
            conditions=active_conditions,
            accept_partial=accept_partial,
        )
        # assign
        context.found = found_c
        early_exit = check_for_early_exit(
            complete, found_c, partial, missing_c, context, priority_concept
        )
        if early_exit:
            break

    logger.info(
        f"{depth_to_prefix(depth)}{LOGGER_PREFIX} finished sourcing loop (complete: {complete}), have {context.found} from {[n for n in context.stack]} (missing {context.all_mandatory - context.found}), attempted {context.attempted}, virtual {virtual}"
    )
    if complete == ValidationResult.COMPLETE:
        return _advance_condition_result(
            generate_loop_completion(context, virtual),
            mandatory_list,
            environment,
            depth,
            g,
            history,
            accept_partial,
            conditions,
        )

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
    logger.info(
        f"{depth_to_prefix(depth)}{LOGGER_PREFIX} Could not resolve concepts {[c.address for c in mandatory_list]}, network outcome was {complete}, missing {context.all_mandatory - context.found},"
    )

    return None


def source_query_concepts(
    output_concepts: List[BuildConcept],
    history: History,
    environment: BuildEnvironment,
    g: Optional[ReferenceGraph] = None,
    conditions: Optional[BuildWhereClause] = None,
    where_clauses: list[BuildWhereClause] | None = None,
):
    if not output_concepts:
        raise ValueError(f"No output concepts provided {output_concepts}")
    if not g:
        g = generate_graph(environment)

    condition_context = BuildConditionContext.normalize(conditions)
    if where_clauses:
        condition_context = BuildConditionContext.from_where_clauses(where_clauses)
    root = search_concepts(
        mandatory_list=output_concepts,
        environment=environment,
        g=g,
        depth=0,
        history=history,
        conditions=condition_context,
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
    return group_if_required_v2(root, output_concepts, environment, depth=0)
