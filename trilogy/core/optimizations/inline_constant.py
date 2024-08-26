from trilogy.core.models import (
    CTE,
    Concept,
)
from trilogy.core.enums import PurposeLineage

from trilogy.core.optimizations.base_optimization import OptimizationRule


class InlineConstant(OptimizationRule):

    def optimize(self, cte: CTE, inverse_map: dict[str, list[CTE]]) -> bool:

        to_inline: list[Concept] = []
        for x in cte.source.input_concepts:
            if x.address not in cte.source_map:
                continue
            if x.derivation == PurposeLineage.CONSTANT:
                self.log(f"Found constant {x.address} on {cte.name}")
                to_inline.append(x)
        if to_inline:
            inlined = False
            for c in to_inline:
                self.log(f"Attempting to inline constant {c.address} on {cte.name}")
                test = cte.inline_constant(c)
                if test:
                    self.log(f"Successfully inlined constant to {cte.name}")
                    inlined = True
                else:
                    self.log(f"Could not inline constant to {cte.name}")
            return inlined
        return False
