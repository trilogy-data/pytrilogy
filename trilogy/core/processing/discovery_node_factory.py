from dataclasses import dataclass
from typing import List, Optional, Protocol, Union

from trilogy.constants import logger
from trilogy.core.enums import Derivation, Granularity
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import (
    BuildConcept,
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


@dataclass
class NodeGenerationContext:
    """Encapsulates common parameters for node generation."""

    concept: BuildConcept
    local_optional: List[BuildConcept]
    environment: BuildEnvironment
    g: ReferenceGraph
    depth: int
    source_concepts: SearchConceptsType
    history: History
    accept_partial: bool = False
    conditions: Optional[BuildWhereClause] = None

    @property
    def next_depth(self) -> int:
        return self.depth + 1

    def log_generation(self, node_type: str, extra_info: str = "") -> None:
        """Centralized logging for node generation."""
        optional_addresses = [x.address for x in self.local_optional]
        base_msg = f"for {self.concept.address}, generating {node_type} node with optional {optional_addresses}"

        if extra_info:
            base_msg += f" and {extra_info}"

        logger.info(f"{depth_to_prefix(self.depth)}{LOGGER_PREFIX} {base_msg}")


def restrict_node_outputs_targets(
    node: StrategyNode, targets: list[BuildConcept], depth: int
) -> list[BuildConcept]:
    """Restricts node outputs to target concepts and returns extra concepts."""
    ex_resolve = node.resolve()
    target_addresses = {y.address for y in targets}

    extra = [x for x in ex_resolve.output_concepts if x.address not in target_addresses]

    base = [
        x
        for x in ex_resolve.output_concepts
        if x.address not in {c.address for c in extra}
    ]

    logger.info(
        f"{depth_to_prefix(depth)}{LOGGER_PREFIX} reducing final outputs, "
        f"was {[c.address for c in ex_resolve.output_concepts]} "
        f"with extra {[c.address for c in extra]}, remaining {base}"
    )

    # Add missing targets
    for target in targets:
        if target.address not in {c.address for c in base}:
            base.append(target)

    node.set_output_concepts(base)
    return extra


# Simple factory functions for basic derivation types
def _generate_window_node(ctx: NodeGenerationContext) -> StrategyNode | None:
    ctx.log_generation("window")
    return gen_window_node(
        ctx.concept,
        ctx.local_optional,
        history=ctx.history,
        environment=ctx.environment,
        g=ctx.g,
        depth=ctx.next_depth,
        source_concepts=ctx.source_concepts,
        conditions=ctx.conditions,
    )


def _generate_filter_node(ctx: NodeGenerationContext) -> StrategyNode | None:
    ctx.log_generation("filter")
    return gen_filter_node(
        ctx.concept,
        ctx.local_optional,
        history=ctx.history,
        environment=ctx.environment,
        g=ctx.g,
        depth=ctx.next_depth,
        source_concepts=ctx.source_concepts,
        conditions=ctx.conditions,
    )


def _generate_unnest_node(ctx: NodeGenerationContext) -> StrategyNode | None:
    ctx.log_generation("unnest", f"condition {ctx.conditions}")
    return gen_unnest_node(
        ctx.concept,
        ctx.local_optional,
        history=ctx.history,
        environment=ctx.environment,
        g=ctx.g,
        depth=ctx.next_depth,
        source_concepts=ctx.source_concepts,
        conditions=ctx.conditions,
    )


def _generate_recursive_node(ctx: NodeGenerationContext) -> StrategyNode | None:
    ctx.log_generation("recursive", f"condition {ctx.conditions}")
    return gen_recursive_node(
        ctx.concept,
        ctx.local_optional,
        history=ctx.history,
        environment=ctx.environment,
        g=ctx.g,
        depth=ctx.next_depth,
        source_concepts=ctx.source_concepts,
        conditions=ctx.conditions,
    )


def _generate_union_node(ctx: NodeGenerationContext) -> StrategyNode | None:
    ctx.log_generation("union", f"condition {ctx.conditions}")
    return gen_union_node(
        ctx.concept,
        ctx.local_optional,
        ctx.environment,
        ctx.g,
        ctx.next_depth,
        ctx.source_concepts,
        ctx.history,
        conditions=ctx.conditions,
    )


def _generate_aggregate_node(ctx: NodeGenerationContext) -> StrategyNode | None:
    # Filter out constants to avoid multiplication issues
    agg_optional = [
        x for x in ctx.local_optional if x.granularity != Granularity.SINGLE_ROW
    ]

    logger.info(
        f"{depth_to_prefix(ctx.depth)}{LOGGER_PREFIX} "
        f"for {ctx.concept.address}, generating aggregate node with {agg_optional}"
    )

    return gen_group_node(
        ctx.concept,
        agg_optional,
        history=ctx.history,
        environment=ctx.environment,
        g=ctx.g,
        depth=ctx.next_depth,
        source_concepts=ctx.source_concepts,
        conditions=ctx.conditions,
    )


def _generate_rowset_node(ctx: NodeGenerationContext) -> StrategyNode | None:
    ctx.log_generation("rowset")
    return gen_rowset_node(
        ctx.concept,
        ctx.local_optional,
        ctx.environment,
        ctx.g,
        ctx.next_depth,
        ctx.source_concepts,
        ctx.history,
        conditions=ctx.conditions,
    )


def _generate_multiselect_node(ctx: NodeGenerationContext) -> StrategyNode | None:
    ctx.log_generation("multiselect")
    return gen_multiselect_node(
        ctx.concept,
        ctx.local_optional,
        ctx.environment,
        ctx.g,
        ctx.next_depth,
        ctx.source_concepts,
        ctx.history,
        conditions=ctx.conditions,
    )


def _generate_group_to_node(ctx: NodeGenerationContext) -> StrategyNode | None:
    ctx.log_generation("group to grain")
    return gen_group_to_node(
        ctx.concept,
        ctx.local_optional,
        ctx.environment,
        ctx.g,
        ctx.next_depth,
        ctx.source_concepts,
        ctx.history,
        conditions=ctx.conditions,
    )


def _generate_basic_node(ctx: NodeGenerationContext) -> StrategyNode | None:
    ctx.log_generation("basic")
    return gen_basic_node(
        ctx.concept,
        ctx.local_optional,
        history=ctx.history,
        environment=ctx.environment,
        g=ctx.g,
        depth=ctx.next_depth,
        source_concepts=ctx.source_concepts,
        conditions=ctx.conditions,
    )


class RootNodeHandler:
    """Handles complex root node generation logic."""

    def __init__(self, context: NodeGenerationContext):
        self.ctx = context

    def generate(self) -> Optional[StrategyNode]:
        self.ctx.log_generation("select", "including condition inputs")

        root_targets = [self.ctx.concept] + self.ctx.local_optional

        if self._has_non_root_concepts():
            return self._handle_non_root_concepts(root_targets)

        return self._resolve_root_concepts(root_targets)

    def _has_non_root_concepts(self) -> bool:
        return any(
            x.derivation not in (Derivation.ROOT, Derivation.CONSTANT)
            for x in self.ctx.local_optional
        )

    def _handle_non_root_concepts(
        self, root_targets: List[BuildConcept]
    ) -> Optional[StrategyNode]:
        non_root = [
            x.address
            for x in self.ctx.local_optional
            if x.derivation not in (Derivation.ROOT, Derivation.CONSTANT)
        ]

        logger.info(
            f"{depth_to_prefix(self.ctx.depth)}{LOGGER_PREFIX} "
            f"including any filters, there are non-root concepts we should expand first: {non_root}. "
            f"Recursing with all of these as mandatory"
        )

        self.ctx.history.log_start(
            root_targets,
            accept_partial=self.ctx.accept_partial,
            conditions=self.ctx.conditions,
        )

        return self.ctx.source_concepts(
            mandatory_list=root_targets,
            environment=self.ctx.environment,
            g=self.ctx.g,
            depth=self.ctx.next_depth,
            accept_partial=self.ctx.accept_partial,
            history=self.ctx.history,
        )

    def _resolve_root_concepts(
        self, root_targets: List[BuildConcept]
    ) -> Optional[StrategyNode]:
        expanded_node = self._try_merge_expansion(root_targets)
        if expanded_node:
            return expanded_node
        if self.ctx.accept_partial:
            synonym_node = self._try_synonym_resolution(root_targets)
            if synonym_node:
                logger.info(
                    f"{depth_to_prefix(self.ctx.depth)}{LOGGER_PREFIX} "
                    f"resolved root concepts through synonyms"
                )
                return synonym_node

        return None

    def _try_merge_expansion(
        self, root_targets: List[BuildConcept]
    ) -> Optional[StrategyNode]:
        for accept_partial in [False, True]:
            expanded = gen_merge_node(
                all_concepts=root_targets,
                environment=self.ctx.environment,
                g=self.ctx.g,
                depth=self.ctx.next_depth,
                source_concepts=self.ctx.source_concepts,
                history=self.ctx.history,
                search_conditions=self.ctx.conditions,
                accept_partial=accept_partial,
            )

            if expanded:
                self._handle_expanded_node(expanded, root_targets)
                return expanded

        logger.info(
            f"{depth_to_prefix(self.ctx.depth)}{LOGGER_PREFIX} "
            f"could not find additional concept(s) to inject"
        )
        return None

    def _handle_expanded_node(
        self, expanded: StrategyNode, root_targets: List[BuildConcept]
    ) -> None:
        extra = restrict_node_outputs_targets(expanded, root_targets, self.ctx.depth)

        pseudonyms = [
            x
            for x in extra
            if any(x.address in y.pseudonyms for y in root_targets)
            and x.address not in root_targets
        ]

        if pseudonyms:
            expanded.add_output_concepts(pseudonyms)
            logger.info(
                f"{depth_to_prefix(self.ctx.depth)}{LOGGER_PREFIX} "
                f"Hiding pseudonyms {[c.address for c in pseudonyms]}"
            )
            expanded.hide_output_concepts(pseudonyms)

        logger.info(
            f"{depth_to_prefix(self.ctx.depth)}{LOGGER_PREFIX} "
            f"Found connections for {[c.address for c in root_targets]} "
            f"via concept addition; removing extra {[c.address for c in extra]}"
        )

    def _try_synonym_resolution(
        self, root_targets: List[BuildConcept]
    ) -> Optional[StrategyNode]:
        logger.info(
            f"{depth_to_prefix(self.ctx.depth)}{LOGGER_PREFIX} "
            f"Could not resolve root concepts, checking for synonyms for {root_targets}"
        )

        if not self.ctx.history.check_started(
            root_targets,
            accept_partial=self.ctx.accept_partial,
            conditions=self.ctx.conditions,
        ):
            self.ctx.history.log_start(
                root_targets,
                accept_partial=self.ctx.accept_partial,
                conditions=self.ctx.conditions,
            )

            resolved = gen_synonym_node(
                all_concepts=root_targets,
                environment=self.ctx.environment,
                g=self.ctx.g,
                depth=self.ctx.next_depth,
                source_concepts=self.ctx.source_concepts,
                history=self.ctx.history,
                conditions=self.ctx.conditions,
                accept_partial=self.ctx.accept_partial,
            )

            if resolved:
                logger.info(
                    f"{depth_to_prefix(self.ctx.depth)}{LOGGER_PREFIX} "
                    f"resolved concepts through synonyms"
                )
                return resolved
        else:
            logger.info(
                f"{depth_to_prefix(self.ctx.depth)}{LOGGER_PREFIX} "
                f"skipping synonym search, already in a recursion for these concepts"
            )

        return None


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

    context = NodeGenerationContext(
        concept=concept,
        local_optional=local_optional,
        environment=environment,
        g=g,
        depth=depth,
        source_concepts=source_concepts,
        history=history,
        accept_partial=accept_partial,
        conditions=conditions,
    )

    # Try materialized concept first
    # this is worth checking every loop iteration
    candidate = history.gen_select_node(
        [concept] + local_optional,
        environment,
        g,
        depth + 1,
        fail_if_not_found=False,
        accept_partial=accept_partial,
        conditions=conditions,
    )

    if candidate:
        return candidate

    # Delegate to appropriate handler based on derivation
    derivation_handlers = {
        Derivation.WINDOW: lambda: _generate_window_node(context),
        Derivation.FILTER: lambda: _generate_filter_node(context),
        Derivation.UNNEST: lambda: _generate_unnest_node(context),
        Derivation.RECURSIVE: lambda: _generate_recursive_node(context),
        Derivation.UNION: lambda: _generate_union_node(context),
        Derivation.AGGREGATE: lambda: _generate_aggregate_node(context),
        Derivation.ROWSET: lambda: _generate_rowset_node(context),
        Derivation.MULTISELECT: lambda: _generate_multiselect_node(context),
        Derivation.GROUP_TO: lambda: _generate_group_to_node(context),
        Derivation.BASIC: lambda: _generate_basic_node(context),
        Derivation.ROOT: lambda: RootNodeHandler(context).generate(),
        Derivation.CONSTANT: lambda: RootNodeHandler(context).generate(),
    }

    handler = derivation_handlers.get(concept.derivation)
    if not handler:
        raise ValueError(f"Unknown derivation {concept.derivation} on {concept}")

    return handler()
