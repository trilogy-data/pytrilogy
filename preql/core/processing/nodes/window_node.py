from typing import List


from preql.core.models import (
    SourceType,
)
from preql.core.processing.nodes.base_node import StrategyNode


class WindowNode(StrategyNode):
    source_type = SourceType.WINDOW

    def __init__(
        self,
        mandatory_concepts,
        optional_concepts,
        environment,
        g,
        whole_grain: bool = False,
        parents: List["StrategyNode"] | None = None,
    ):
        super().__init__(
            mandatory_concepts,
            optional_concepts,
            environment,
            g,
            whole_grain=whole_grain,
            parents=parents,
        )
