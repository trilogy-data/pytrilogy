from typing import List


from trilogy.core.models import (
    SourceType,
    Concept,
    Conditional,
    Comparison,
    Parenthetical,
)
from trilogy.core.processing.nodes.base_node import StrategyNode


class FilterNode(StrategyNode):
    """Filter nodes represent a restriction operation
    on a concept that creates a new derived concept.

    They should only output a concept and it's filtered
    version, but will have parents that provide all required
    filtering keys as inputs.
    """

    source_type = SourceType.FILTER

    def __init__(
        self,
        input_concepts: List[Concept],
        output_concepts: List[Concept],
        environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
        depth: int = 0,
        conditions: Conditional | Comparison | Parenthetical | None = None,
        partial_concepts: List[Concept] | None = None,
        force_group: bool | None = False,
    ):
        super().__init__(
            output_concepts=output_concepts,
            environment=environment,
            g=g,
            whole_grain=whole_grain,
            parents=parents,
            depth=depth,
            input_concepts=input_concepts,
            conditions=conditions,
            partial_concepts=partial_concepts,
            force_group=force_group,
        )

    def copy(self) -> "FilterNode":
        return FilterNode(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            g=self.g,
            whole_grain=self.whole_grain,
            parents=self.parents,
            depth=self.depth,
            conditions=self.conditions,
            partial_concepts=list(self.partial_concepts),
            force_group=self.force_group,
        )
