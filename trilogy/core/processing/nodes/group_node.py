from typing import List, Optional

from trilogy.constants import logger
from trilogy.core.enums import SourceType
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildOrderBy,
    BuildParenthetical,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import QueryDatasource
from trilogy.core.processing.nodes.base_node import (
    StrategyNode,
    resolve_concept_map,
)
from trilogy.core.processing.utility import (
    GroupRequiredResponse,
    find_nullable_concepts,
    is_scalar_condition,
)
from trilogy.utility import unique

LOGGER_PREFIX = "[CONCEPT DETAIL - GROUP NODE]"


class GroupNode(StrategyNode):
    source_type = SourceType.GROUP

    def __init__(
        self,
        output_concepts: List[BuildConcept],
        input_concepts: List[BuildConcept],
        environment: BuildEnvironment,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
        depth: int = 0,
        partial_concepts: Optional[List[BuildConcept]] = None,
        nullable_concepts: Optional[List[BuildConcept]] = None,
        force_group: bool | None = None,
        conditions: (
            BuildConditional | BuildComparison | BuildParenthetical | None
        ) = None,
        preexisting_conditions: (
            BuildConditional | BuildComparison | BuildParenthetical | None
        ) = None,
        existence_concepts: List[BuildConcept] | None = None,
        hidden_concepts: set[str] | None = None,
        ordering: BuildOrderBy | None = None,
        required_outputs: List[BuildConcept] | None = None,
    ):
        super().__init__(
            input_concepts=input_concepts,
            output_concepts=output_concepts,
            environment=environment,
            whole_grain=whole_grain,
            parents=parents,
            depth=depth,
            partial_concepts=partial_concepts,
            nullable_concepts=nullable_concepts,
            force_group=force_group,
            conditions=conditions,
            existence_concepts=existence_concepts,
            preexisting_conditions=preexisting_conditions,
            hidden_concepts=hidden_concepts,
            ordering=ordering,
        )
        # the set of concepts required to preserve grain
        # set by group by node generation with aggregates
        self.required_outputs = required_outputs

    @classmethod
    def check_if_required(
        cls,
        downstream_concepts: List[BuildConcept],
        parents: list[QueryDatasource | BuildDatasource],
        environment: BuildEnvironment,
        depth: int = 0,
    ) -> GroupRequiredResponse:
        from trilogy.core.processing.discovery_utility import check_if_group_required

        return check_if_group_required(downstream_concepts, parents, environment, depth)

    def _resolve(self) -> QueryDatasource:
        parent_sources: List[QueryDatasource | BuildDatasource] = [
            p.resolve() for p in self.parents
        ]

        grains = self.check_if_required(
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
        nullable_addresses = find_nullable_concepts(
            source_map=source_map, joins=[], datasources=parent_sources
        )
        nullable_concepts = [
            x for x in self.output_concepts if x.address in nullable_addresses
        ]
        base = QueryDatasource(
            input_concepts=self.input_concepts,
            output_concepts=self.output_concepts,
            datasources=parent_sources,
            source_type=source_type,
            source_map=source_map,
            joins=[],
            grain=target_grain,
            partial_concepts=self.partial_concepts,
            nullable_concepts=nullable_concepts,
            hidden_concepts=self.hidden_concepts,
            condition=self.conditions,
            ordering=self.ordering,
        )
        # if there is a condition on a group node and it's not scalar
        # inject an additional CTE
        if self.conditions and not is_scalar_condition(self.conditions):
            base.condition = None
            base.output_concepts = unique(
                list(base.output_concepts) + list(self.conditions.row_arguments),
                "address",
            )
            # re-visible any hidden concepts
            base.hidden_concepts = set(
                [x for x in base.hidden_concepts if x not in base.output_concepts]
            )
            source_map = resolve_concept_map(
                [base],
                targets=self.output_concepts,
                inherited_inputs=base.output_concepts,
            )
            return QueryDatasource(
                input_concepts=base.output_concepts,
                output_concepts=self.output_concepts,
                datasources=[base],
                source_type=SourceType.SELECT,
                source_map=source_map,
                joins=[],
                grain=target_grain,
                nullable_concepts=base.nullable_concepts,
                partial_concepts=self.partial_concepts,
                condition=self.conditions,
                hidden_concepts=self.hidden_concepts,
                ordering=self.ordering,
            )
        return base

    def copy(self) -> "GroupNode":
        return GroupNode(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            whole_grain=self.whole_grain,
            parents=self.parents,
            depth=self.depth,
            partial_concepts=list(self.partial_concepts),
            nullable_concepts=list(self.nullable_concepts),
            force_group=self.force_group,
            conditions=self.conditions,
            preexisting_conditions=self.preexisting_conditions,
            existence_concepts=list(self.existence_concepts),
            hidden_concepts=set(self.hidden_concepts),
            ordering=self.ordering,
            required_outputs=(
                list(self.required_outputs) if self.required_outputs else None
            ),
        )
