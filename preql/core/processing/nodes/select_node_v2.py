from typing import List, Optional


from preql.constants import logger
from preql.core.constants import CONSTANT_DATASET
from preql.core.enums import Purpose
from preql.core.models import (
    Datasource,
    QueryDatasource,
    SourceType,
    Environment,
    Concept,
    Grain,
    Function,
    UnnestJoin,
)
from preql.utility import unique
from preql.core.processing.nodes.base_node import StrategyNode
from preql.core.exceptions import NoDatasourceException


LOGGER_PREFIX = "[CONCEPT DETAIL - SELECT NODE]"


class StaticSelectNode(StrategyNode):
    """Static select nodes."""

    source_type = SourceType.SELECT

    def __init__(
        self,
        input_concepts: List[Concept],
        output_concepts: List[Concept],
        environment: Environment,
        g,
        datasource: QueryDatasource,
        depth: int = 0,
        partial_concepts: List[Concept] | None = None,
    ):
        super().__init__(
            input_concepts=input_concepts,
            output_concepts=output_concepts,
            environment=environment,
            g=g,
            whole_grain=True,
            parents=[],
            depth=depth,
            partial_concepts=partial_concepts,
        )
        self.datasource = datasource

    def _resolve(self):
        if self.datasource.grain == Grain():
            raise NotImplementedError
        return self.datasource


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
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
        depth: int = 0,
        partial_concepts: List[Concept] | None = None,
        accept_partial: bool = False,
        datasource: Optional[Datasource] = None,
        grain: Optional[Grain] = None,
        force_group: bool = False,
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
            force_group=force_group,
            grain=grain,
        )
        self.accept_partial = accept_partial
        self.datasource = datasource

    def resolve_from_provided_datasource(
        self,
    ):
        datasource = self.datasource
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
            # ensure that if this select needs to merge, the grain components are present
            all_concepts_final = all_concepts_final + datasource.grain.components_copy

        return QueryDatasource(
            input_concepts=self.input_concepts,
            output_concepts=all_concepts_final,
            source_map=source_map,
            datasources=[datasource],
            grain=self.grain,
            joins=[],
            partial_concepts=[
                c.concept for c in datasource.columns if not c.is_complete
            ],
            source_type=SourceType.DIRECT_SELECT,
            # select nodes should never group
            force_group=False,
        )

    def resolve_from_constant_datasources(self) -> QueryDatasource:
        datasource = Datasource(
            identifier=CONSTANT_DATASET, address=CONSTANT_DATASET, columns=[]
        )
        return QueryDatasource(
            input_concepts=[],
            output_concepts=unique(self.all_concepts, "address"),
            source_map={concept.address: set() for concept in self.all_concepts},
            datasources=[datasource],
            grain=datasource.grain,
            joins=[],
            partial_concepts=[],
            source_type=SourceType.CONSTANT,
        )

    def _resolve(self) -> QueryDatasource:
        # if we have parent nodes, we do not need to go to a datasource
        if self.parents:
            return super()._resolve()
        resolution: QueryDatasource | None
        if all([c.purpose == Purpose.CONSTANT for c in self.all_concepts]):
            logger.info(
                f"{self.logging_prefix}{LOGGER_PREFIX} have a constant datasource"
            )
            resolution = self.resolve_from_constant_datasources()
            if resolution:
                return resolution
        if self.datasource:
            resolution = self.resolve_from_provided_datasource()
            if resolution:
                return resolution
        required = [c.address for c in self.all_concepts]
        raise NoDatasourceException(
            f"Could not find any way to associate required concepts {required}"
        )


class ConstantNode(SelectNode):
    """Represents a constant value."""

    pass
