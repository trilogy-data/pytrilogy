from trilogy.core.models.build import (
    BuildConcept,
)
from trilogy.core.models.execute import CTE, UnionCTE
from trilogy.core.optimizations.base_optimization import OptimizationRule


class HideUnusedConcepts(OptimizationRule):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> bool:
        used = set()
        from trilogy.dialect.base import BaseDialect

        renderer = BaseDialect()
        children = inverse_map.get(cte.name, [])
        if not children:
            return False
        for v in children:
            self.log(f"Analyzing usage of {cte.name} in {v.name}")
            renderer.render_cte(v)
        used = renderer.used_map.get(cte.name, set())
        self.log(f"Used concepts for {cte.name}: {used} from {renderer.used_map}")
        add_to_hidden: list[BuildConcept] = []
        for concept in cte.output_columns:
            if concept.address not in used:
                add_to_hidden.append(concept)
        newly_hidden = [
            x.address for x in add_to_hidden if x.address not in cte.hidden_concepts
        ]
        non_hidden = [
            x for x in cte.output_columns if x.address not in cte.hidden_concepts
        ]
        if not newly_hidden or len(non_hidden) <= 1:
            return False
        self.log(
            f"Hiding unused concepts {[x.address for x in add_to_hidden]} from {cte.name} (used: {used}, all: {[x.address for x in cte.output_columns]})"
        )
        candidates = [x.address for x in cte.output_columns if x.address not in used]
        if len(candidates) == len(set([x.address for x in cte.output_columns])):
            # pop one out
            candidates.pop()
        cte.hidden_concepts = set(candidates)
        return True
