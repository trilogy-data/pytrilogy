from trilogy.core.enums import SourceType
from trilogy.core.models.build import BuildConcept
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes.base_node import StrategyNode


class RecursiveNode(StrategyNode):
    """The recursive CTE over a seed and a self-referencing step, stacked
    as a single source."""

    source_type = SourceType.RECURSIVE

    def __init__(
        self,
        input_concepts: list[BuildConcept],
        output_concepts: list[BuildConcept],
        environment: BuildEnvironment,
        parents: list["StrategyNode"] | None = None,
        depth: int = 0,
    ):
        super().__init__(
            input_concepts=input_concepts,
            output_concepts=output_concepts,
            environment=environment,
            parents=parents,
            depth=depth,
        )

    def copy(self) -> "RecursiveNode":
        return RecursiveNode(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            parents=self.parents,
            depth=self.depth,
        )
