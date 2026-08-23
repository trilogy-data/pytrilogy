from trilogy.constants import logger
from trilogy.core.enums import SourceType
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildDatasource,
    BuildOrderBy,
    nonstandard_grouping_lineage,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import QueryDatasource
from trilogy.core.processing.condition_utility import (
    condition_proves_non_null,
    is_scalar_condition,
)
from trilogy.core.processing.discovery_utility import check_if_group_required
from trilogy.core.processing.nodes.base_node import (
    StrategyNode,
    resolve_concept_map,
    resolve_existence_map,
)
from trilogy.core.processing.utility import find_nullable_concepts
from trilogy.utility import unique

LOGGER_PREFIX = "[CONCEPT DETAIL - GROUP NODE]"


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

    def _resolve(self) -> QueryDatasource:
        parent_sources: list[QueryDatasource | BuildDatasource] = [
            p.resolve() for p in self.parents
        ]

        grains = check_if_group_required(
            self.output_concepts, parent_sources, self.environment, self.depth
        )
        target_grain = grains.target
        comp_grain = grains.upstream
        # dynamically select if we need to group
        # because sometimes, we are already at required grain
        if not grains.required and self.force_group is not True:
            # otherwise if no group by, just treat it as a select
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
        for concept in self.output_concepts:
            if concept.is_aggregate and concept.address not in rollup_addresses:
                # An aggregate that arrives via input_concepts is being
                # passed through from an upstream node (e.g. a wrapper
                # GroupNode added by group_if_required_v2 over a node that
                # already aggregated). Keep its parent source so we project
                # the precomputed value instead of re-rendering the lineage
                # against inputs that may no longer be available.
                if concept.address in input_addresses:
                    continue
                source_map[concept.address] = set()
        nullable_addresses = find_nullable_concepts(
            source_map=source_map, joins=[], datasources=parent_sources
        )
        # A scalar condition already applied at/below this group (e.g.
        # ``store.id IS NOT NULL``, often pushed into an upstream scan so it
        # only shows up here as a preexisting condition) filters the rows, so
        # any concept it proves non-null must not be re-marked nullable by the
        # parent-derived recompute above — otherwise the join scorer emits an
        # OUTER ``is not distinct from`` (defeats hash joins). Consumers that
        # judge the condition itself must not trust the resulting absence —
        # see StrategyNode._refine_nullable_for_conditions.
        applied = self.preexisting_conditions or self.conditions
        proven_non_null = (
            condition_proves_non_null(applied)
            if applied and is_scalar_condition(applied)
            else set()
        )
        # union the source-analysis nullables with node-level nullables — the
        # latter carry inferred nullability for concepts COMPUTED in this
        # subtree (e.g. a derived join key over a nullable column)
        node_nullable = {x.address for x in self.nullable_concepts}
        nullable_concepts = [
            x
            for x in self.output_concepts
            if (x.address in nullable_addresses or x.address in node_nullable)
            and not proven_non_null.intersection(
                {x.address, x.canonical_address, *x.pseudonyms}
            )
        ]
        # A ROLLUP/CUBE/GROUPING SETS injects NULLs into its grouping-key dims at
        # the subtotal/grand-total rows. Mark those dims — and any dim derived
        # from them (e.g. ``concat('x', txt)``, which propagates the NULL) —
        # nullable, so downstream joins on them use null-safe (OUTER) semantics
        # and preserve the rollup rows instead of dropping or doubling them.
        rollup_by_addresses: set[str] = set()
        for c in self.output_concepts:
            if (wrapper := nonstandard_grouping_lineage(c)) is not None:
                rollup_by_addresses.update(b.address for b in wrapper.by)
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
        # Merge partial concepts from parent resolved sources
        # so partial keys from upstream datasources propagate through grouping.
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
                for c in ps.rollup_concepts
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
        )
        return base

    def copy(self) -> "GroupNode":
        return GroupNode(
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
