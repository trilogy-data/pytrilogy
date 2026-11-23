from trilogy.constants import logger
from trilogy.core.enums import ZERO_ON_EMPTY_AGGREGATES, SourceType
from trilogy.core.functions import propagates_argument_nulls
from trilogy.core.models.build import (
    BoolExpr,
    BuildAggregateWrapper,
    BuildConcept,
    BuildDatasource,
    BuildGroupingSets,
    BuildOrderBy,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import QueryDatasource
from trilogy.core.processing.aggregate_rollup import pending_rollups
from trilogy.core.processing.condition_utility import (
    condition_proves_non_null,
    is_scalar_condition,
)
from trilogy.core.processing.discovery_utility import check_if_group_required
from trilogy.core.processing.grain_utility import is_identity_group
from trilogy.core.processing.nodes.base_node import (
    StrategyNode,
    resolve_concept_map,
    resolve_existence_map,
)
from trilogy.core.processing.utility import find_nullable_concepts
from trilogy.utility import unique

LOGGER_PREFIX = "[CONCEPT DETAIL - GROUP NODE]"


def null_on_null_group(
    concept: BuildConcept, nullable: set[str], memo: dict[str, bool]
) -> bool:
    """Whether a value computed at a group can be NULL: an aggregate other
    than a count is NULL on a group whose argument values are all NULL, and a
    null-propagating expression over such a value is too."""
    if concept.address in memo:
        return memo[concept.address]
    memo[concept.address] = False
    if concept.equivalent_addresses & nullable:
        result = True
    elif isinstance(concept.lineage, BuildAggregateWrapper):
        result = (
            concept.lineage.function.operator not in ZERO_ON_EMPTY_AGGREGATES
            and any(
                null_on_null_group(a, nullable, memo) for a in concept.concept_arguments
            )
        )
    else:
        result = propagates_argument_nulls(concept) and any(
            null_on_null_group(a, nullable, memo) for a in concept.concept_arguments
        )
    memo[concept.address] = result
    return result


class GroupNode(StrategyNode):
    source_type = SourceType.GROUP

    def __init__(
        self,
        output_concepts: list[BuildConcept],
        input_concepts: list[BuildConcept],
        environment: BuildEnvironment,
        parents: list["StrategyNode"] | None = None,
        depth: int = 0,
        partial_concepts: list[BuildConcept] | None = None,
        rollup_concepts: list[BuildConcept] | None = None,
        nullable_concepts: list[BuildConcept] | None = None,
        force_group: bool | None = None,
        conditions: BoolExpr | None = None,
        preexisting_conditions: BoolExpr | None = None,
        existence_concepts: list[BuildConcept] | None = None,
        hidden_concepts: set[str] | None = None,
        ordering: BuildOrderBy | None = None,
    ):
        super().__init__(
            input_concepts=input_concepts,
            output_concepts=output_concepts,
            environment=environment,
            parents=parents,
            depth=depth,
            partial_concepts=partial_concepts,
            rollup_concepts=rollup_concepts,
            nullable_concepts=nullable_concepts,
            force_group=force_group,
            conditions=conditions,
            existence_concepts=existence_concepts,
            preexisting_conditions=preexisting_conditions,
            hidden_concepts=hidden_concepts,
            ordering=ordering,
        )

    def grouping_pass(self) -> BuildGroupingSets | None:
        """The select's ROLLUP/CUBE/GROUPING SETS pass when this group
        computes one of its aggregates itself: the pass is the group's."""
        inputs = {c.address for c in self.input_concepts}
        if any(
            c.address not in inputs and self.environment.pass_of(c) is not None
            for c in self.output_concepts
        ):
            return self.environment.grouping
        return None

    def _resolve(self) -> QueryDatasource:
        parent_sources: list[QueryDatasource | BuildDatasource] = [
            p.resolve() for p in self.parents
        ]
        grouping_sets = self.grouping_pass()

        grains = check_if_group_required(
            self.output_concepts, parent_sources, self.environment, self.depth
        )
        target_grain = grains.target
        comp_grain = grains.upstream
        # skip the group when parents already sit at the target grain
        if (
            not grains.required
            and grouping_sets is None
            and (
                self.force_group is not True
                or is_identity_group(
                    parent_sources,
                    [],
                    target_grain,
                    self.conditions,
                    self.output_concepts,
                    self.rollup_concepts,
                )
            )
        ):
            source_type = SourceType.SELECT
        else:
            logger.info(
                f"{self.logging_prefix}{LOGGER_PREFIX} Group node has different grain than parents; group is required."
                f" Upstream grains {[str(source.grain) for source in parent_sources]}"
                f" with final grain {comp_grain} vs"
                f" target grain {target_grain}"
                f" delta: {comp_grain - target_grain}"
            )
            source_type = SourceType.GROUP
        source_map = resolve_concept_map(
            parent_sources,
            targets=(
                unique(
                    self.output_concepts + self.conditions.concept_arguments,
                    "address",
                )
                if self.conditions
                else self.output_concepts
            ),
            inherited_inputs=self.input_concepts + self.existence_concepts,
        )
        rollup_addresses = {c.address for c in self.rollup_concepts}
        input_addresses = {c.address for c in self.input_concepts}
        # a rollup reads its partial where the parents hold it
        for address, partial in self.partial_sources.items():
            source_map[address] = set(source_map.get(partial.address, set()))
        for concept in self.output_concepts:
            if concept.is_aggregate and concept.address not in rollup_addresses:
                # An aggregate arriving via input_concepts is passed through from
                # an upstream node; keep its parent source so the precomputed
                # value is projected rather than the lineage re-rendered.
                if concept.address in input_addresses:
                    continue
                source_map[concept.address] = set()
        nullable_addresses = find_nullable_concepts(
            source_map=source_map, joins=[], datasources=parent_sources
        )
        # A scalar condition applied at or below this group (often only visible
        # here as a preexisting condition) filters the rows, so concepts it
        # proves non-null must not be re-marked nullable by the recompute above;
        # otherwise the join scorer emits a null-safe OUTER comparison. See
        # StrategyNode._refine_nullable_for_conditions for the consumer caveat.
        applied = self.preexisting_conditions or self.conditions
        proven_non_null = (
            condition_proves_non_null(applied)
            if applied and is_scalar_condition(applied)
            else set()
        )
        # node-level nullables carry inferred nullability for concepts COMPUTED
        # in this subtree (e.g. a derived join key over a nullable column)
        node_nullable = {x.address for x in self.nullable_concepts}
        parent_nullable = {
            a
            for p in parent_sources
            for c in p.nullable_concepts
            for a in c.equivalent_addresses
        }
        parent_outputs = {c.address for p in parent_sources for c in p.output_concepts}
        memo: dict[str, bool] = {}
        node_nullable |= {
            x.address
            for x in self.output_concepts
            if x.address not in parent_outputs
            and null_on_null_group(x, parent_nullable, memo)
        }
        nullable_concepts = [
            x
            for x in self.output_concepts
            if (x.address in nullable_addresses or x.address in node_nullable)
            and not proven_non_null.intersection(x.all_spellings)
        ]
        # ROLLUP/CUBE/GROUPING SETS inject NULLs into grouping-key dims on the
        # subtotal rows. Mark those dims, and any dim derived from them,
        # nullable so downstream joins use null-safe semantics.
        rollup_by_addresses = grouping_sets.keys if grouping_sets else set()
        if rollup_by_addresses:
            from trilogy.core.processing.discovery_utility import (
                get_upstream_concepts,
            )

            already_nullable = {x.address for x in nullable_concepts}
            nullable_concepts = nullable_concepts + [
                x
                for x in self.output_concepts
                if x.address not in already_nullable
                and (
                    x.address in rollup_by_addresses
                    or rollup_by_addresses & get_upstream_concepts(x)
                )
            ]
        # partial keys from upstream datasources propagate through grouping
        output_addresses = {c.address for c in self.output_concepts}
        inherited_partials = unique(
            self.partial_concepts
            + [
                c
                for ps in parent_sources
                if isinstance(ps, QueryDatasource)
                for c in ps.partial_concepts
                if c.address in output_addresses
            ],
            "address",
        )
        inherited_rollups = unique(
            self.rollup_concepts
            + [
                c
                for ps in parent_sources
                if isinstance(ps, QueryDatasource)
                for c in pending_rollups(ps.rollup_concepts, ps.grain)
                if c.address in output_addresses
            ],
            "address",
        )
        base = QueryDatasource(
            input_concepts=self.input_concepts,
            output_concepts=self.output_concepts,
            datasources=parent_sources,
            source_type=source_type,
            source_map=source_map,
            existence_source_map=resolve_existence_map(
                parent_sources, self.existence_concepts
            ),
            joins=[],
            grain=target_grain,
            partial_concepts=inherited_partials,
            rollup_concepts=inherited_rollups,
            nullable_concepts=nullable_concepts,
            hidden_concepts=self.hidden_concepts,
            condition=self.conditions,
            ordering=self.ordering,
            distinct_counts=self.distinct_counts,
            partial_sources=dict(self.partial_sources),
            grouping_sets=grouping_sets,
        )
        return base

    def copy(self) -> "GroupNode":
        node = GroupNode(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            parents=self.parents,
            depth=self.depth,
            partial_concepts=list(self.partial_concepts),
            rollup_concepts=list(self.rollup_concepts),
            nullable_concepts=list(self.nullable_concepts),
            force_group=self.force_group,
            conditions=self.conditions,
            preexisting_conditions=self.preexisting_conditions,
            existence_concepts=list(self.existence_concepts),
            hidden_concepts=set(self.hidden_concepts),
            ordering=self.ordering,
        )
        return self.with_marks(node)
