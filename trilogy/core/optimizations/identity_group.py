"""Drop a GROUP BY that only deduplicates rows already unique at its keys.

A plan-time merge groups when its parents' grain claims do not cover the
output, and those claims must hold on every row an outer join could pad. Once
the join upgrades have made those joins INNER (or a LEFT onto a side's whole
grain), the joined rows may already be unique at the group keys: the GROUP BY
is then a DISTINCT that removes nothing.

Sound only when the CTE groups no aggregate (the GROUP BY is a pure DISTINCT)
and every source's row uniqueness is known: a datasource's declared grain, a
filtered projection of one that keeps that grain, or a grouped CTE's keys.
"""

from __future__ import annotations

from trilogy.core.enums import Derivation, JoinType, Purpose, SourceType
from trilogy.core.models.execute import CTE, DatasourceCTE, Join, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule

_PRESERVING = (JoinType.INNER, JoinType.LEFT_OUTER)


def _outputs(node: CTE | UnionCTE) -> set[str]:
    return {c.address for c in node.output_columns}


def unique_key(node: CTE | UnionCTE) -> set[str] | None:
    """Addresses `node`'s rows are unique on, when that is known."""
    if isinstance(node, UnionCTE):
        return None
    if isinstance(node, DatasourceCTE):
        grain = set(node.datasource.grain.components)
        return grain if grain and grain <= _outputs(node) else None
    if node.group_to_grain:
        return {c.address for c in node.group_concepts}
    if node.source.source_type == SourceType.GROUP or node.joins:
        return None
    parents = [b.node for b in node.source_bindings() if b.node is not None]
    if len(parents) != 1:
        return None
    parent_key = unique_key(parents[0])
    if parent_key is None or not parent_key <= _outputs(node):
        return None
    return parent_key


def _is_pure_distinct(cte: CTE) -> bool:
    if cte.rollup_concepts:
        return False
    read = list(cte.output_columns)
    if cte.condition is not None:
        read.extend(cte.condition.concept_arguments)
    return not any(
        c.derivation in (Derivation.AGGREGATE, Derivation.WINDOW)
        or c.purpose == Purpose.METRIC
        for c in read
    )


def _closure(keys: set[str], joins: list[Join]) -> set[str]:
    """`keys` plus every address an INNER join equates to one of them."""
    out = set(keys)
    changed = True
    while changed:
        changed = False
        for join in joins:
            if join.jointype != JoinType.INNER:
                continue
            for pair in join.joinkey_pairs or []:
                ends = {pair.left.address, pair.right.address}
                if ends & out and not ends <= out:
                    out |= ends
                    changed = True
    return out


def rows_unique_at_group(cte: CTE) -> bool:
    if not cte.group_to_grain or not _is_pure_distinct(cte):
        return False
    if not all(isinstance(j, Join) for j in cte.joins):
        return False
    joins: list[Join] = [j for j in cte.joins if isinstance(j, Join)]
    nodes = {b.node.name: b.node for b in cte.source_bindings() if b.node is not None}
    rights = {j.right_cte.name for j in joins}
    bases = [node for name, node in nodes.items() if name not in rights]
    if len(bases) != 1 or len(rights) != len(joins):
        return False
    keys = _closure({c.address for c in cte.group_concepts}, joins)
    base_key = unique_key(bases[0])
    if base_key is None or not base_key <= keys:
        return False
    for join in joins:
        if join.jointype not in _PRESERVING:
            return False
        right_key = unique_key(join.right_cte)
        if right_key is None:
            return False
        joined_on = {pair.right.address for pair in join.joinkey_pairs or []}
        if not (right_key <= joined_on or right_key <= keys):
            return False
    return True


class DropIdentityGroup(OptimizationRule):
    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE) or not rows_unique_at_group(cte):
            return False, None
        self.log(f"{cte.name} rows are unique at its group keys; dropping GROUP BY")
        cte.group_to_grain = False
        return True, None
