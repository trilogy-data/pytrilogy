from trilogy.core.models.build import (
    BuildConcept,
)
from trilogy.core.models.execute import CTE, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import render_cte_used_map


class HideUnusedConcepts(OptimizationRule):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        children = inverse_map.get(cte.name, [])
        if not children:
            return False, None
        used: set[str] = set()
        for v in children:
            self.log(f"Analyzing usage of {cte.name} in {v.name}")
            child_used_map = render_cte_used_map(v)
            used.update(child_used_map.get(cte.name, set()))
        # Grain concepts pin the CTE's row grain (used by the rendered
        # GROUP BY). Hiding them would force the renderer to use qualified
        # column names in the GROUP BY where SELECT positional indices
        # would otherwise work, and would also remove a concept that is
        # actually still referenced — just by the CTE itself, not by its
        # consumers.
        grain_components = getattr(getattr(cte, "grain", None), "components", None)
        if grain_components:
            used.update(grain_components)
        self.log(f"Used concepts for {cte.name}: {used}")
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
            return False, None
        self.log(
            f"Hiding unused concepts {[x.address for x in add_to_hidden]} from {cte.name} (used: {used}, all: {[x.address for x in cte.output_columns]})"
        )
        candidates = [x.address for x in cte.output_columns if x.address not in used]
        visible_addresses = {
            x.address
            for x in cte.output_columns
            if x.address not in cte.hidden_concepts
        }
        if visible_addresses.issubset(set(candidates)):
            # Keep one projected address so an anchor CTE cannot render as SELECT FROM.
            keep_address = next(
                x.address
                for x in reversed(cte.output_columns)
                if x.address in visible_addresses
            )
            candidates = [x for x in candidates if x != keep_address]
        if not candidates:
            return False, None
        cte.hidden_concepts = set(candidates)
        # UnionCTE rendering joins the per-branch CTEs with UNION ALL; each
        # branch's SELECT list is filtered by *that branch's* hidden_concepts,
        # not the union's. Propagate the hide so the branches also drop the
        # unused columns from their projections (e.g. q66's pushed-up
        # ``sales.ship_mode.carrier`` / ``sales.time.time`` that no consumer
        # references after stripping).
        if isinstance(cte, UnionCTE):
            for branch in cte.internal_ctes:
                if not isinstance(branch, CTE):
                    continue
                branch_outputs = {c.address for c in branch.output_columns}
                branch.hidden_concepts |= {
                    addr for addr in candidates if addr in branch_outputs
                }
        return True, None
