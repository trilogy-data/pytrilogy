"""Read a coalesced join key off one side once the join types are final."""

from __future__ import annotations

from trilogy.core.models.execute import CTE, UnionCTE, preserved_key_pairs
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import join_padded_ctes


def prune_preserved_join_keys(cte: CTE) -> bool:
    """Read a coalesced join key off one side once the join types are final.

    A key several left sides share renders `coalesce(a.k, b.k) = c.k`, and the
    planner keeps every side an earlier join pads (any of them may hold the
    NULL). Once an upgrade narrows a FULL to LEFT, its left side is padded by
    nothing and equals the key on every row, so the other sides only bloat the
    coalesce."""
    padded: set[str] = set()
    changed = False
    for join, pads in join_padded_ctes(cte):
        if join.pairs:
            kept = preserved_key_pairs(join.pairs, padded, lambda pair: pair.node.name)
            if len(kept) != len(join.pairs):
                join.pairs = kept
                changed = True
        padded |= {c.name for c in pads}
    return changed


class PrunePreservedJoinKeys(OptimizationRule):
    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE) or not prune_preserved_join_keys(cte):
            return False, None
        self.log(f"Read coalesced join keys off a preserved side in {cte.name}")
        return True, None
