from typing import List

from trilogy.core.execute_models import (
    Comparison,
    BoundConcept,
    Conditional,
    BoundGrain,
    Parenthetical,
    SourceType,
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
        input_concepts: List[BoundConcept],
        output_concepts: List[BoundConcept],
        environment,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
        depth: int = 0,
        conditions: Conditional | Comparison | Parenthetical | None = None,
        preexisting_conditions: Conditional | Comparison | Parenthetical | None = None,
        partial_concepts: List[BoundConcept] | None = None,
        force_group: bool | None = False,
        grain: BoundGrain | None = None,
        existence_concepts: List[BoundConcept] | None = None,
    ):
        super().__init__(
            output_concepts=output_concepts,
            environment=environment,
            whole_grain=whole_grain,
            parents=parents,
            depth=depth,
            input_concepts=input_concepts,
            conditions=conditions,
            preexisting_conditions=preexisting_conditions,
            partial_concepts=partial_concepts,
            force_group=force_group,
            grain=grain,
            existence_concepts=existence_concepts,
        )

    def copy(self) -> "FilterNode":
        return FilterNode(
            input_concepts=list(self.input_concepts),
            output_concepts=list(self.output_concepts),
            environment=self.environment,
            whole_grain=self.whole_grain,
            parents=self.parents,
            depth=self.depth,
            conditions=self.conditions,
            preexisting_conditions=self.preexisting_conditions,
            partial_concepts=list(self.partial_concepts),
            force_group=self.force_group,
            grain=self.grain,
            existence_concepts=list(self.existence_concepts),
        )
