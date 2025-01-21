from trilogy.core.enums import Derivation
from trilogy.core.models.build import BuildConcept
from trilogy.core.models.execute import (
    CTE,
    UnionCTE,
)
from trilogy.core.optimizations.base_optimization import OptimizationRule


class InlineConstant(OptimizationRule):
    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> bool:
        if isinstance(cte, UnionCTE):
            return any(self.optimize(x, inverse_map) for x in cte.internal_ctes)

        to_inline: list[BuildConcept] = []
        for x in cte.source.input_concepts:
            if x.address not in cte.source_map:
                continue
            if x.derivation == Derivation.CONSTANT:
                self.log(f"Found constant {x.address} on {cte.name}")
                to_inline.append(x)
        if to_inline:
            inlined = False
            for c in to_inline:
                self.log(f"Attempting to inline constant {c.address} on {cte.name}")
                test = cte.inline_constant(c)
                if test:
                    self.log(f"Successfully inlined constant {c.address} to {cte.name}")
                    inlined = True
                else:
                    self.log(f"Could not inline constant to {cte.name}")
            return inlined
        return False
