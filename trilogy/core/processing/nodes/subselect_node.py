from trilogy.core.enums import SourceType
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildOrderBy,
)
from trilogy.core.processing.nodes.base_node import StrategyNode


class SubselectNode(StrategyNode):
    source_type = SourceType.SUBSELECT

    def __init__(
        self,
        input_concepts: list[BuildConcept],
        output_concepts: list[BuildConcept],
        environment,
        parents: list["StrategyNode"] | None = None,
        depth: int = 0,
        ordering: BuildOrderBy | None = None,
        preexisting_conditions: BoolExpr | None = None,
    ):
        super().__init__(
            input_concepts=input_concepts,
            output_concepts=output_concepts,
            environment=environment,
            parents=parents,
            depth=depth,
            ordering=ordering,
            preexisting_conditions=preexisting_conditions,
        )

    def copy(self) -> "SubselectNode":
        return SubselectNode(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            parents=self.parents,
            depth=self.depth,
            ordering=self.ordering,
            preexisting_conditions=self.preexisting_conditions,
        )
