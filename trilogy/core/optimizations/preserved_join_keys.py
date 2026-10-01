"""Read a coalesced join key off one side once the join types are final.

A key several left sides share renders `coalesce(a.k, b.k) = c.k`, and the
planner keeps every side an earlier join pads (any of them may hold the
NULL). The upgrade passes can narrow those joins afterwards: once a FULL is
LEFT, its left side is padded by nothing, equals the key on every row, and the
other sides only bloat the coalesce (and pin the join behind theirs).
"""

from __future__ import annotations

from trilogy.core.enums import JoinType
from trilogy.core.models.execute import CTE, CTEConceptPair, Join, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule


def _left_sources(join: Join) -> set[str]:
    if join.left_cte is not None:
        return {join.left_cte.name}
    return {pair.cte.name for pair in join.joinkey_pairs or []}


def _preserved_pairs(
    pairs: list[CTEConceptPair], padded: set[str]
) -> list[CTEConceptPair]:
    groups: dict[tuple[str, str], list[CTEConceptPair]] = {}
    for pair in pairs:
        groups.setdefault((pair.right.address, pair.left.address), []).append(pair)
    out: list[CTEConceptPair] = []
    for group in groups.values():
        preserved = [pair for pair in group if pair.cte.name not in padded]
        if len(group) == 1 or not preserved:
            out.extend(group)
            continue
        out.append(min(preserved, key=lambda p: p.cte.name))
    return out


def prune_preserved_join_keys(cte: CTE) -> bool:
    joined: set[str] = {cte.base_name} if cte.base_name else set()
    padded: set[str] = set()
    changed = False
    for join in cte.joins:
        if not isinstance(join, Join):
            continue
        if join.joinkey_pairs:
            kept = _preserved_pairs(join.joinkey_pairs, padded)
            if len(kept) != len(join.joinkey_pairs):
                join.joinkey_pairs = kept
                changed = True
        joined |= _left_sources(join)
        if join.jointype in (JoinType.RIGHT_OUTER, JoinType.FULL):
            padded |= joined
        if join.jointype in (JoinType.LEFT_OUTER, JoinType.FULL):
            padded.add(join.right_cte.name)
        joined.add(join.right_cte.name)
    return changed


class PrunePreservedJoinKeys(OptimizationRule):
    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE) or not prune_preserved_join_keys(cte):
            return False, None
        self.log(f"Read coalesced join keys off a preserved side in {cte.name}")
        return True, None
