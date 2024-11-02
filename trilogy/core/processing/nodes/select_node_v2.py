from typing import List, Optional


from trilogy.constants import logger
from trilogy.core.constants import CONSTANT_DATASET
from trilogy.core.enums import Purpose, PurposeLineage
from trilogy.core.models import (
    Function,
    Grain,
    QueryDatasource,
    SourceType,
    Concept,
    Environment,
    UnnestJoin,
    Datasource,
    Conditional,
    Comparison,
    Parenthetical,
)
from trilogy.utility import unique
from trilogy.core.processing.nodes.base_node import StrategyNode, resolve_concept_map


LOGGER_PREFIX = "[CONCEPT DETAIL - SELECT NODE]"


class SelectNode(StrategyNode):
    """Select nodes actually fetch raw data from a table
    Responsible for selecting the cheapest option from which to select.
    """

    source_type = SourceType.SELECT

    def __init__(
        self,
        input_concepts: List[Concept],
        output_concepts: List[Concept],
        environment: Environment,
        g,
        datasource: Datasource | None = None,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
        depth: int = 0,
        partial_concepts: List[Concept] | None = None,
        nullable_concepts: List[Concept] | None = None,
        accept_partial: bool = False,
        grain: Optional[Grain] = None,
        force_group: bool | None = False,
        conditions: Conditional | Comparison | Parenthetical | None = None,
        preexisting_conditions: Conditional | Comparison | Parenthetical | None = None,
        hidden_concepts: List[Concept] | None = None,
    ):
        super().__init__(
            input_concepts=input_concepts,
            output_concepts=output_concepts,
            environment=environment,
            g=g,
            whole_grain=whole_grain,
            parents=parents,
            depth=depth,
            partial_concepts=partial_concepts,
            nullable_concepts=nullable_concepts,
            force_group=force_group,
            grain=grain,
            conditions=conditions,
            preexisting_conditions=preexisting_conditions,
            hidden_concepts=hidden_concepts,
        )
        self.accept_partial = accept_partial
        self.datasource = datasource

    def resolve_from_provided_datasource(
        self,
    ) -> QueryDatasource:
        if not self.datasource:
            raise ValueError("Datasource not provided")
        datasource: Datasource = self.datasource

        all_concepts_final: List[Concept] = unique(self.all_concepts, "address")
        source_map: dict[str, set[Datasource | QueryDatasource | UnnestJoin]] = {
            concept.address: {datasource} for concept in self.input_concepts
        }

        derived_concepts = [
            c
            for c in datasource.columns
            if isinstance(c.alias, Function) and c.concept.address in source_map
        ]
        for c in derived_concepts:
            if not isinstance(c.alias, Function):
                continue
            for x in c.alias.concept_arguments:
                source_map[x.address] = {datasource}
        for x in all_concepts_final:
            if x.address not in source_map and x.derivation in (
                PurposeLineage.MULTISELECT,
                PurposeLineage.FILTER,
                PurposeLineage.BASIC,
                PurposeLineage.ROWSET,
                PurposeLineage.BASIC,
            ):
                source_map[x.address] = set()

        # if we're not grouping
        # force grain to datasource grain
        # so that we merge on the same grain
        if self.force_group is False:
            grain = datasource.grain
        else:
            grain = self.grain or Grain()
        return QueryDatasource(
            input_concepts=self.input_concepts,
            output_concepts=all_concepts_final,
            source_map=source_map,
            datasources=[datasource],
            grain=grain,
            joins=[],
            partial_concepts=[
                c.concept for c in datasource.columns if not c.is_complete
            ],
            nullable_concepts=[c.concept for c in datasource.columns if c.is_nullable],
            source_type=SourceType.DIRECT_SELECT,
            # we can skip rendering conditions
            condition=self.conditions,
            # select nodes should never group
            force_group=self.force_group,
            hidden_concepts=self.hidden_concepts,
        )

    def resolve_from_constant_datasources(self) -> QueryDatasource:
        datasource = Datasource(
            name=CONSTANT_DATASET, address=CONSTANT_DATASET, columns=[]
        )
        return QueryDatasource(
            input_concepts=[],
            output_concepts=unique(self.all_concepts, "address"),
            source_map={concept.address: set() for concept in self.all_concepts},
            datasources=[datasource],
            grain=datasource.grain,
            condition=self.conditions,
            joins=[],
            partial_concepts=[],
            source_type=SourceType.CONSTANT,
            hidden_concepts=self.hidden_concepts,
        )

    def _resolve(self) -> QueryDatasource:
        # if we have parent nodes, we do not need to go to a datasource
        resolution: QueryDatasource | None = None
        if all(
            [
                (
                    c.derivation == PurposeLineage.CONSTANT
                    or (
                        c.purpose == Purpose.CONSTANT
                        and c.derivation == PurposeLineage.MULTISELECT
                    )
                )
                for c in self.all_concepts
            ]
        ):
            logger.info(
                f"{self.logging_prefix}{LOGGER_PREFIX} have a constant datasource"
            )
            resolution = self.resolve_from_constant_datasources()
        if self.datasource and not resolution:
            resolution = self.resolve_from_provided_datasource()

        if self.parents:
            if not resolution:
                return super()._resolve()
            # zip in our parent source map
            parent_sources: List[QueryDatasource | Datasource] = [
                p.resolve() for p in self.parents
            ]

            resolution.datasources += parent_sources

            source_map = resolve_concept_map(
                parent_sources,
                targets=self.output_concepts,
                inherited_inputs=self.input_concepts + self.existence_concepts,
            )
            for k, v in source_map.items():
                if v and k not in resolution.source_map:
                    resolution.source_map[k] = v
        if not resolution:
            raise ValueError("No select node could be generated")
        return resolution

    def copy(self) -> "SelectNode":
        return SelectNode(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            g=self.g,
            datasource=self.datasource,
            depth=self.depth,
            parents=self.parents,
            whole_grain=self.whole_grain,
            partial_concepts=list(self.partial_concepts),
            nullable_concepts=list(self.nullable_concepts),
            accept_partial=self.accept_partial,
            grain=self.grain,
            force_group=self.force_group,
            conditions=self.conditions,
            preexisting_conditions=self.preexisting_conditions,
            hidden_concepts=self.hidden_concepts,
        )


class ConstantNode(SelectNode):
    """Represents a constant value."""

    def copy(self) -> "ConstantNode":
        return ConstantNode(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            g=self.g,
            datasource=self.datasource,
            depth=self.depth,
            partial_concepts=list(self.partial_concepts),
            conditions=self.conditions,
            preexisting_conditions=self.preexisting_conditions,
            hidden_concepts=self.hidden_concepts,
        )
