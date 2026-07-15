from dataclasses import dataclass
from typing import List, Optional

from trilogy.constants import logger
from trilogy.core.enums import AggregateGroupingMode, Derivation, Granularity
from trilogy.core.env_processor import generate_graph
from trilogy.core.exceptions import (
    DisconnectedConceptsException,
    UnresolvableQueryException,
)
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.author import (
    UndefinedConcept,
)
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import (
    combine_condition_atoms,
    decompose_condition,
)
from trilogy.core.processing.constants import ROOT_DERIVATIONS, SKIPPED_DERIVATIONS
from trilogy.core.processing.discovery_node_factory import generate_node
from trilogy.core.processing.discovery_utility import (
    LOGGER_PREFIX,
    depth_to_prefix,
    disconnected_components,
    format_disconnected_subgraphs_error,
    get_loop_iteration_targets,
    get_upstream_concepts,
    group_if_required_v2,
    membership_span_note,
    raise_if_filter_disconnected,
)
from trilogy.core.processing.discovery_validation import (
    ValidationResult,
    _stack_applies_condition,
    _stack_exempt_or_implies,
    validate_stack,
)
from trilogy.core.processing.node_generators.presence_probe import (
    gen_coalescing_axis_node,
    is_presence_probe,
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
    conditions: BuildWhereClause | None = None,
):
    # we if we have a where clause doing an existence check
    # treat that as separate subquery
    if where.existence_arguments:
        already_sourced = {c.address for c in node.input_concepts} | {
            c.address for c in node.existence_concepts
        }
        for subselect in where.existence_arguments:
            if not subselect:
                continue
            if all(x.address in already_sourced for x in subselect):
                logger.info(
                    f"{LOGGER_PREFIX} existance clause inputs already found {[str(c) for c in subselect]}"
                )
                continue
            logger.info(
                f"{LOGGER_PREFIX} fetching existence clause inputs {[str(c) for c in subselect]}"
            )
            # A HAVING-derived membership subselect (`conditions` set) is this
            # query's own post-aggregation semijoin: the query WHERE must be
            # pushed pre-aggregate into its aggregate inputs, exactly as it is on
            # the output path — else the membership recomputes the aggregate over
            # the unfiltered universe and its value never matches the filtered
            # output (q44 silent-empty). A user `x in (select ...)` RHS is an
            # independent set (no conditions) and stays unfiltered.
            parent = source_query_concepts(
                [*subselect],
                history=history,
                environment=environment,
                g=graph,
                conditions=conditions,
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
        # A filter-only rowset output (referenced in the WHERE but not selected)
        # must eventually be injected because its opaque lineage cannot be rebuilt
        # in the outer scope. Do that only after every non-terminal output has been
        # sourced: promoting the WHERE while an aggregate is still pending lets
        # the aggregate be built condition-free as an optional of the rowset.
        mandatory_addresses = {x.address for x in mandatory_list}
        # ...UNLESS the rowset input is UPSTREAM of a ROLLUP/CUBE/GROUPING SETS
        # aggregate at this level (`sum(ch.total_sales) by rollup (...)`): it feeds
        # that aggregation, so its filter must apply BELOW the group, at the input's
        # own grain. Such grouping adds NULL-key subtotal rows; forcing the filter
        # here drags the finer input into the rollup output, enriches it back over
        # the NULL-keyed subtotal grain, and re-applies the filter — silently
        # dropping every subtotal row. Push those inputs down instead. The upstream
        # check keys off lineage (its rowset expansion also covers a *sibling* field
        # of the aggregated one); a filter-only rowset output that is NOT upstream of
        # the grouping (an independent membership operand, q23) still forces here.
        # Pseudonym twins count as upstream: a coalescing scoped join (`subset join
        # a.k = b.k`) collapses the sides to one same-valued column, so a filter on
        # the far side's address is a filter on the rollup's own input dim.
        nonstandard_grouping_upstream: set[str] = set()
        for m in mandatory_list:
            if (
                isinstance(m.lineage, BuildAggregateWrapper)
                and m.lineage.grouping != AggregateGroupingMode.STANDARD
            ):
                nonstandard_grouping_upstream |= get_upstream_concepts(m, nested=True)
        pushed_below_grouping: set[str] = set()
        candidate_filters = []
        condition_terminal = all(
            x.derivation in ROOT_DERIVATIONS or x.derivation == Derivation.ROWSET
            for x in mandatory_list
        )
        if condition_terminal:
            for x in conditions.row_arguments:
                if (
                    x.address in mandatory_addresses
                    or x.derivation != Derivation.ROWSET
                    or x.granularity == Granularity.SINGLE_ROW
                ):
                    continue
                if {x.address, *x.pseudonyms} & nonstandard_grouping_upstream:
                    pushed_below_grouping.add(x.address)
                else:
                    candidate_filters.append(x)
        required_filters += candidate_filters
        if pushed_below_grouping:
            # the filter on these inputs is enforced below the grouping, at the
            # input's own grain; requiring them here would drag the finer input
            # back into the rollup output over the NULL-keyed subtotal rows
            completion_mandatory = [
                c
                for c in completion_mandatory
                if c.address not in pushed_below_grouping
            ]
        if any(required_filters):
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} derived condition row inputs {[x.address for x in required_filters]} present in mandatory list, forcing condition evaluation at this level. "
            )
            mandatory_list = completion_mandatory
            all_mandatory = set(c.address for c in completion_mandatory)
            must_evaluate_condition_on_this_level_not_push_down = True
            # NOTE: this deliberately withholds even the non-self-referential
            # row atoms from the computed values' sourcing. An aggregate
            # referenced in the WHERE is evaluated over its own unfiltered
            # scope (`where f = 1 and sx > 5` reads the full-population sx) —
            # restrict its input by binding the condition inside the aggregate
            # (`sum(x ? cond) by k`) instead. A residual-routing variant that
            # pushed the row atoms into the computed values' builds was tried
            # and reverted: it silently narrowed WHERE-referenced sibling
            # aggregates (test_where_aggregate_input_not_filtered_by_where).
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


def _condition_input_unsourceable(
    priority_concept: BuildConcept, context: LoopContext, node_found: bool
) -> bool:
    """True if the priority is a derived concept that appears only in the WHERE
    clause (not in this level's outputs) and could not be sourced.

    Restricted to SKIPPED_DERIVATIONS: those concepts get a single sourcing
    attempt as their own node (the loop never reattempts them, see `skip`), so
    they can't be picked up later as a free optional of another node. A pure
    WHERE input of that class that generate_node can't produce is therefore
    genuinely unsourceable — its filter can never be applied at or above this
    level. Failing the search lets the normal fall-through raise the same clean
    UnresolvableQueryException a SELECT-only version of the query would, rather
    than completing on outputs alone and tripping the INCOMPLETE_CONDITION
    guardrail downstream. ROOT/CONSTANT inputs are excluded — they can still be
    sourced as an optional when a different concept is the priority.
    """
    if node_found or not context.conditions:
        return False
    if priority_concept.derivation not in SKIPPED_DERIVATIONS:
        return False
    if priority_concept.address in {c.address for c in context.original_mandatory}:
        return False
    return any(
        c.address == priority_concept.address for c in context.conditions.row_arguments
    )


def check_for_early_exit(
    complete: ValidationResult,
    found: set[str],
    partial: set[str],
    missing: set[str],
    context: LoopContext,
    priority_concept: BuildConcept,
) -> bool:
    if complete == ValidationResult.INCOMPLETE_CONDITION:
        # The outputs are all sourced but the WHERE/HAVING can't be applied yet.
        # If a condition input simply isn't in the stack (e.g. it lives in a
        # disconnected subgraph and hasn't been sourced), this is NOT a terminal
        # invalid state: keep searching like a plain INCOMPLETE. The loop either
        # sources it (-> COMPLETE/DISCONNECTED) or exhausts and fails cleanly,
        # yielding the same DisconnectedConceptsException a SELECT of those
        # concepts would. Only when every condition input is already sourced and
        # the condition is *still* unsatisfiable is the planner in a genuinely
        # invalid state worth the loud sentinel below.
        condition_inputs_sourced = not context.conditions or all(
            c.address in found for c in context.conditions.row_arguments
        )
        if not condition_inputs_sourced:
            return False
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


def _restrict_completion_conditions(
    conditions: BuildWhereClause,
    non_virtual: list[BuildConcept],
    stack: list[StrategyNode],
    original_addresses: set[str],
) -> tuple[BuildWhereClause, list[BuildConcept]]:
    """Re-apply at the completion merge only the WHERE atoms not already applied
    to *every* parent. An atom is dropped solely when each parent has genuinely
    applied it (its preexisting_conditions imply it, or the node is a scalar /
    independent-scope source) — never merely because its column is absent from
    the merge's outputs. A filter parent that consumed a column while a sibling
    parent did *not* filter on it still needs that atom re-applied; keeping it
    means its row args stay projected, and if a column is genuinely unavailable
    the downstream input validation fails loudly rather than silently dropping a
    filter. Returns the residual where clause and ``non_virtual`` trimmed of any
    row args that belonged only to dropped (already-applied) atoms.
    """
    atoms = decompose_condition(conditions.conditional)
    kept = [a for a in atoms if not _stack_applies_condition(stack, a)]
    if len(kept) == len(atoms):
        return conditions, non_virtual
    residual = combine_condition_atoms(kept)
    kept_row_args = {r.address for a in kept for r in a.row_arguments}
    reduced = [
        c
        for c in non_virtual
        if c.address in original_addresses or c.address in kept_row_args
    ]
    where = (
        BuildWhereClause(conditional=residual) if residual is not None else conditions
    )
    return where, reduced


def generate_loop_completion(context: LoopContext, virtual: set[str]) -> StrategyNode:
    condition_required = True
    non_virtual = [c for c in context.completion_mandatory if c.address not in virtual]
    non_virtual_different = len(context.completion_mandatory) != len(
        context.original_mandatory
    )
    non_virtual_difference_values = set(
        [x.address for x in context.completion_mandatory]
    ).difference(set([x.address for x in context.original_mandatory]))
    if not context.conditions:
        condition_required = False
        non_virtual = [c for c in context.mandatory_list if c.address not in virtual]

    elif _stack_exempt_or_implies(context.stack, context.conditions.conditional):
        condition_required = False
        non_virtual = [c for c in context.mandatory_list if c.address not in virtual]

    if context.conditions and not condition_required:
        parent_map = {
            str(x): x.preexisting_conditions == context.conditions.conditional
            for x in context.stack
        }
        logger.info(
            f"Condition {context.conditions} not required, parents included filtering! {parent_map}"
        )

    # When re-applying conditions at this merge, drop atoms already applied to
    # every parent (e.g. a scalar `state = 'GA'` filtered in all arms) so we do
    # not demand their now-consumed columns as merge outputs. Atoms not applied
    # to all parents (e.g. an existence `order_number in feeder` filtered in one
    # arm only) stay and are re-applied here.
    reapply_conditions = context.conditions
    if condition_required and context.conditions:
        reapply_conditions, non_virtual = _restrict_completion_conditions(
            context.conditions,
            non_virtual,
            context.stack,
            {x.address for x in context.original_mandatory},
        )
        non_virtual_difference_values = set(x.address for x in non_virtual).difference(
            set(x.address for x in context.original_mandatory)
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
        # A presence probe a parent materialized on its own side (a coalescing
        # member's null marker) must survive this merge: the outer WHERE reads it
        # post-merge, and omitting it forces recomputation off the fused key,
        # never NULL (TPC-DS q35). Carry every parent-exposed probe up.
        non_virtual_addresses = {c.address for c in non_virtual}
        probe_carry = unique(
            [
                c
                for node in context.stack
                for c in node.usable_outputs
                if is_presence_probe(c.address)
                and c.address not in non_virtual_addresses
            ],
            "address",
        )
        output = MergeNode(
            input_concepts=non_virtual + probe_carry,
            output_concepts=non_virtual + probe_carry,
            environment=context.environment,
            parents=context.stack,
            depth=context.depth,
        )

    # ensure we can resolve our final merge
    output.resolve()
    if condition_required and reapply_conditions:
        output.add_condition(reapply_conditions.conditional)
        if reapply_conditions.existence_arguments:
            append_existence_check(
                output,
                context.environment,
                context.g,
                where=reapply_conditions,
                history=context.history,
            )
        # If we re-applied only a residual subset, the dropped atoms are still
        # guaranteed (every parent applied them), so advertise the FULL condition
        # as satisfied — otherwise a higher-level search sees this node as
        # missing those atoms and re-filters or fails as incomplete.
        if reapply_conditions is not context.conditions and context.conditions:
            output.set_preexisting_conditions(context.conditions.conditional)
    elif context.conditions and _stack_applies_condition(
        context.stack, context.conditions.conditional
    ):
        # Advertise the condition as pre-applied only when some parent actually
        # applied it. An all-exempt stack (scalar / rowset scopes) may be
        # returned unfiltered as a fragment, but claiming the condition here
        # would make every consumer treat this node as the applier and the
        # filter would silently drop end to end.
        output.preexisting_conditions = context.conditions.conditional
    logger.info(
        f"{depth_to_prefix(context.depth)}{LOGGER_PREFIX} Graph is connected, returning {type(output)} node output {[x.address for x in output.usable_outputs]} partial {[c.address for c in output.partial_concepts or []]} with {context.conditions}"
    )

    if condition_required and context.conditions and non_virtual_different:
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

    # if we get a can
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
    partial: set[str] = set()
    virtual: set[str] = set()
    complete = ValidationResult.INCOMPLETE
    while context.incomplete:
        try:
            (
                priority_concept,
                candidate_list,
                local_conditions,
            ) = get_loop_iteration_targets(
                mandatory=context.mandatory_list,
                conditions=context.conditions,
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
        except DisconnectedConceptsException:
            if partial:
                logger.info(
                    f"{depth_to_prefix(depth)}{LOGGER_PREFIX} search exhausted with partial concepts {sorted(partial)}"
                )
                break
            raise
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
        if _condition_input_unsourceable(priority_concept, context, bool(node)):
            logger.info(
                f"{depth_to_prefix(depth)}{LOGGER_PREFIX} WHERE input {priority_concept.address} could not be sourced; "
                "abandoning level so the query fails cleanly rather than completing without its filter"
            )
            break
        complete, found_c, missing_c, partial, virtual = validate_stack(
            environment,
            context.stack,
            context.mandatory_list,
            context.completion_mandatory,
            conditions=context.conditions,
            accept_partial=accept_partial,
            # depth 0 is a final scope (statement root, union arm, rowset
            # body): its conditions render here or nowhere, so exemption
            # without an actual applier would silently drop the filter. A
            # deeper search is a fragment whose consumer re-validates with
            # the full sibling stack.
            require_condition_applier=depth == 0,
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
):
    if not output_concepts:
        raise ValueError(f"No output concepts provided {output_concepts}")
    if not g:
        g = generate_graph(environment)
    # keep the outermost statement's WHERE (existence sub-sourcing re-enters
    # here with derived conditions; the probe axis ruling wants the original)
    if history.statement_conditions is None and conditions is not None:
        history.statement_conditions = conditions

    root: StrategyNode | None = None
    # A statement whose SOLE output is a ROWSET member of a coalescing
    # (`full`/`union`) key group projects the unified axis — assemble the
    # coalesce of every member side. Statement-level only: within discovery a
    # bare member request is routinely a SIDE sub-request from the rowset
    # exposure machinery, which must stay one-sided. (Bound ROOT members are
    # canonicalized and intercepted in generate_node instead.)
    if (
        len(output_concepts) == 1
        and conditions is None
        and output_concepts[0].derivation == Derivation.ROWSET
    ):
        root = gen_coalescing_axis_node(
            output_concepts[0],
            environment,
            0,
            g=g,
            source_concepts=search_concepts,
            history=history,
        )

    if root is None:
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
        # Partition the full required set (outputs + filter row args, i.e. the
        # resolver's `completion_mandatory`) by true join reachability in the
        # reference graph. When it splits, name the groups and point at the
        # join/merge fix. R
        required = output_concepts
        if conditions:
            required = unique(
                list(output_concepts) + list(conditions.row_arguments), "address"
            )
        groups = disconnected_components(environment, required, g)
        if len(groups) > 1:
            message = format_disconnected_subgraphs_error(groups, environment, g)
            note = membership_span_note(conditions, groups, environment, g)
            if note:
                message = f"{message}\n{note}"
            raise DisconnectedConceptsException(
                message,
                subgraphs=[[c.address for c in group] for group in groups],
            )
        # A FILTER output hides its `? <cond>` concepts inside its lineage, so the
        # check above can't see them. Re-check with them surfaced: a filter whose
        # condition can't be related to the value it filters is just a disconnected
        # grouping, and should report the standard 'add a join or merge' error
        # (with rowset-specific context) instead of dead-ending on the virtual
        # concept's hashed address.
        raise_if_filter_disconnected(
            output_concepts,
            environment,
            g,
            extra_required=list(conditions.row_arguments) if conditions else None,
        )
        raise UnresolvableQueryException(
            "Could not resolve connections for query with output"
            f" {error_strings} from current model."
        )
    final = [x for x in root.output_concepts if x.address not in root.hidden_concepts]
    logger.info(
        f"{depth_to_prefix(0)}{LOGGER_PREFIX} final concepts are {[x.address for x in final]}"
    )
    return group_if_required_v2(root, output_concepts, environment, depth=0)
