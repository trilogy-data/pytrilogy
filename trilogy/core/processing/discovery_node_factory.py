from typing import List, Optional, Protocol, Union

from trilogy.constants import logger
from trilogy.core.enums import Derivation, FunctionType, Granularity
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import (
    BuildConcept,
    BuildFunction,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.discovery_utility import LOGGER_PREFIX, depth_to_prefix
from trilogy.core.processing.node_generators import (
    gen_basic_node,
    gen_filter_node,
    gen_group_node,
    gen_group_to_node,
    gen_merge_node,
    gen_multiselect_node,
    gen_recursive_node,
    gen_rowset_node,
    gen_synonym_node,
    gen_union_node,
    gen_unnest_node,
    gen_window_node,
)
from trilogy.core.processing.nodes import (
    ConstantNode,
    History,
    StrategyNode,
)


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
    elif concept.derivation == Derivation.RECURSIVE:
        logger.info(
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating recursive node with optional {[x.address for x in local_optional]} and condition {conditions}"
        )
        return gen_recursive_node(
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
            f"{depth_to_prefix(depth)}{LOGGER_PREFIX} for {concept.address}, generating aggregate node with {[x for x in agg_optional]}"
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
                logger.info(
                    f"{depth_to_prefix(depth)}{LOGGER_PREFIX} skipping search, already in a recursion fot these concepts"
                )
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

            # if not history.check_started(
            #     root_targets, accept_partial=accept_partial, conditions=conditions
            # ) or 1==1:
            if True:
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
            else:
                logger.info(
                    f"{depth_to_prefix(depth)}{LOGGER_PREFIX} skipping root search, already in a recursion for these concepts"
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
            else:
                logger.info(
                    f"{depth_to_prefix(depth)}{LOGGER_PREFIX} skipping synonym search, already in a recursion for these concepts"
                )
            return None
    else:
        raise ValueError(f"Unknown derivation {concept.derivation} on {concept}")
    return None
