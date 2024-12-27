from typing import List

from trilogy.core.models import BoundConcept, SourceType
from trilogy.core.processing.nodes.base_node import QueryDatasource, StrategyNode


class WindowNode(StrategyNode):
    source_type = SourceType.WINDOW

    def __init__(
        self,
        input_concepts: List[BoundConcept],
        output_concepts: List[BoundConcept],
        environment,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
        depth: int = 0,
    ):
        super().__init__(
            input_concepts=input_concepts,
            output_concepts=output_concepts,
            environment=environment,
            whole_grain=whole_grain,
            parents=parents,
            depth=depth,
        )

    def _resolve(self) -> QueryDatasource:
        base = super()._resolve()
        return base

    def copy(self) -> "WindowNode":
        return WindowNode(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            whole_grain=self.whole_grain,
            parents=self.parents,
            depth=self.depth,
        )
