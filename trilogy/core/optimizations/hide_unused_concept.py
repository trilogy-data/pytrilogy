from trilogy.core.models.build import (
    BuildConcept,
)
from trilogy.core.models.execute import CTE, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import render_cte_used_map


class HideUnusedConcepts(OptimizationRule):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def _hide_branch_only_outputs(self, cte: UnionCTE) -> bool:
        """Hide any concept that appears in a branch's ``output_columns`` but
        not in the union's ``output_columns`` — those columns are projected
        by the branch SELECT yet aren't reachable from any consumer (the
        union doesn't expose them). E.g. ``UnionDimPushdown`` filter-only
        mode adds a dim's projected concepts to each branch for the WHERE
        atom to render, but consumers keep their own dim join so the union
        never advertises them.
        """
        union_addrs = {c.address for c in cte.output_columns}
        changed = False
        for branch in cte.internal_ctes:
            if not isinstance(branch, CTE):
                continue
            to_hide = {
                c.address
                for c in branch.output_columns
                if c.address not in union_addrs
                and c.address not in branch.hidden_concepts
            }
            if not to_hide:
                continue
            visible = [
                c.address
                for c in branch.output_columns
                if c.address not in branch.hidden_concepts
            ]
            if len(visible) - len(to_hide) < 1:
                # Always leave at least one projected column so the branch
                # SELECT renders.
                continue
            branch.hidden_concepts |= to_hide
            self.log(
                f"Hiding branch-only outputs {sorted(to_hide)} from {branch.name} "
                f"(union {cte.name} doesn't expose them)"
            )
            changed = True
        return changed

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        children = inverse_map.get(cte.name, [])
        if not children:
            return False, None
        used: set[str] = set()
        for v in children:
            self.debug(f"Analyzing usage of {cte.name} in {v.name}")
            child_used_map = render_cte_used_map(v)
            used.update(child_used_map.get(cte.name, set()))
        self.debug(f"Used concepts for {cte.name}: {used}")
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
        branch_only_hidden = False
        if isinstance(cte, UnionCTE):
            branch_only_hidden = self._hide_branch_only_outputs(cte)
        if not newly_hidden or len(non_hidden) <= 1:
            return branch_only_hidden, None
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
            return branch_only_hidden, None
        new_hidden = set(candidates)
        changed = new_hidden != cte.hidden_concepts
        if changed:
            self.log(
                f"Hiding unused concepts {candidates} from {cte.name} "
                f"(used: {used}, all: {[x.address for x in cte.output_columns]})"
            )
            cte.hidden_concepts = new_hidden
        # UnionCTE rendering joins the per-branch CTEs with UNION ALL; each
        # branch's SELECT list is filtered by *that branch's* hidden_concepts,
        # not the union's. Propagate the hide so the branches also drop the
        # unused columns from their projections (e.g. q66's pushed-up
        # ``sales.ship_mode.carrier`` / ``sales.time.time`` that no consumer
        # references after stripping). Re-checked every loop: a branch may only
        # gain the column on a later pass.
        if isinstance(cte, UnionCTE):
            for branch in cte.internal_ctes:
                if not isinstance(branch, CTE):
                    continue
                branch_outputs = {c.address for c in branch.output_columns}
                to_hide = {
                    addr for addr in candidates if addr in branch_outputs
                } - branch.hidden_concepts
                if to_hide:
                    branch.hidden_concepts |= to_hide
                    changed = True
        # Report True only on a real change: returning True on a no-op (the set
        # is already applied) keeps the driver re-running this phase until
        # MAX_OPTIMIZATION_LOOPS, spamming the log every loop.
        return changed or branch_only_hidden, None
