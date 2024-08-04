from trilogy.core.models import (
    CTE,
    SelectStatement,
    PersistStatement,
    Datasource,
    MultiSelectStatement,
    Conditional,
    BooleanOperator,
)
from trilogy.core.enums import PurposeLineage
from trilogy.constants import logger, CONFIG
from abc import ABC

from trilogy.core.optimizations.base_optimization import OptimizationRule

class InlineConstant(OptimizationRule):

    def optimize(self, cte: CTE, inverse_map: dict[str, list[CTE]]) -> bool:
        to_inline = []
        for x in cte.source.input_concepts:
            if x.derivation == PurposeLineage.CONSTANT:
                self.log(f"Found constant {x.address}")
                to_inline.append(x)
        
        if to_inline:
            for c in to_inline:
                cte.inline_constant(c)
            return True
        return False