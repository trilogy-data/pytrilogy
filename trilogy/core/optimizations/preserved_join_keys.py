"""Read a coalesced join key off one side once the join types are final.

A key several left sides share renders `coalesce(a.k, b.k) = c.k`, and the
planner keeps every side an earlier join pads (any of them may hold the
NULL). The upgrade passes can narrow those joins afterwards: once a FULL is
LEFT, its left side is padded by nothing, equals the key on every row, and the
other sides only bloat the coalesce (and pin the join behind theirs).
"""

from __future__ import annotations

from trilogy.core.models.execute import (
    CTE,
    CTEConceptPair,
    UnionCTE,
    preserved_key_pairs,
)
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import join_padded_ctes


def _pair_cte(pair: CTEConceptPair) -> str:
    return pair.cte.name


def prune_preserved_join_keys(cte: CTE) -> bool:
    padded: set[str] = set()
    changed = False
    for join, pads in join_padded_ctes(cte):
        if join.joinkey_pairs:
            kept = preserved_key_pairs(join.joinkey_pairs, padded, _pair_cte)
            if len(kept) != len(join.joinkey_pairs):
                join.joinkey_pairs = kept
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
