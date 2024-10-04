from typing import List


from trilogy.core.models import SourceType, Concept
from trilogy.core.processing.nodes.base_node import StrategyNode, QueryDatasource


class WindowNode(StrategyNode):
    source_type = SourceType.WINDOW

    def __init__(
        self,
        input_concepts: List[Concept],
        output_concepts: List[Concept],
        environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
        depth: int = 0,
    ):
        super().__init__(
            input_concepts=input_concepts,
            output_concepts=output_concepts,
            environment=environment,
            g=g,
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
            g=self.g,
            whole_grain=self.whole_grain,
            parents=self.parents,
            depth=self.depth,
        )
