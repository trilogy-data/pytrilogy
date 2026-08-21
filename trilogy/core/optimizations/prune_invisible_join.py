"""Remove LEFT_OUTER joins whose right side is output-invisible.

A LEFT join preserves every left row whether or not it matches; the right
side can only (a) attach its columns to matched rows or (b) multiply a left
row that matches more than one right row. When the consumer renders no
reference to the right CTE, (a) is moot; when the right side's grain sits
within the join's right key addresses, each left row matches at most one
right row and (b) cannot occur. The join is then row-identical to not
joining at all, so drop it and let the driver's irrelevant-CTE filter sweep
the orphaned producer. Null-safe pairs prune the same as plain equality:
with no rendered right column and multiplicity at most one, matched and
unmatched left rows are indistinguishable.

This is the terminal step of the consumer-invisibility chain (see
``UpgradeJoinOnGuards``): a padded merge contributor consumed only through
plain-equality joins on a solid key gets its FULL join narrowed to
LEFT_OUTER first, and lands here once nothing reads its columns."""

from __future__ import annotations

from trilogy.core.enums import JoinType
from trilogy.core.models.build import BuildConcept
from trilogy.core.models.execute import CTE, Join, RecursiveCTE, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule


def _right_source_keys(cte: CTE, right: CTE | UnionCTE) -> tuple[set[str], bool]:
    """Tokens the consumer can use for the right side: its name and
    safe_identifier, plus the consumer's source key for it (an inlined
    datasource renders under its alias-map token, and source_map entries hold
    that token, not the CTE name). Returns ``(keys, ambiguous)``; ambiguous is true
    when another parent claims one of these tokens (role-played copies of one
    datasource can collide on ``safe_identifier``), in which case both the
    reference check and the scrub would be unreliable."""
    keys = {right.name, right.safe_identifier, cte.source_key_for(right)}
    other_tokens = {
        token for name, token in cte.inlined_alias_map.items() if name != right.name
    } | {p.name for p in cte.parent_ctes if p.name != right.name}
    return keys, bool(keys & other_tokens)


def _rendered_reference_blocks(cte: CTE, join: Join, right_keys: set[str]) -> bool:
    """True when anything the CTE renders reads the join's right side: a
    visible output column (single-source or COALESCE), a condition or ORDER BY
    argument, an existence subselect, a generated semi-join feeder, or another
    join using it as an endpoint or key contributor. A concept with no
    source_map binding renders as an expression here, so its lineage
    arguments render too, so close over those."""
    seeds: list[BuildConcept] = [
        column
        for column in cte.output_columns
        if column.address not in cte.hidden_concepts
    ]
    if cte.condition is not None:
        seeds.extend(cte.condition.row_arguments)
        for group in cte.condition.existence_arguments:
            seeds.extend(group)
    if cte.order_by is not None:
        for expr in cte.order_by.concept_arguments:
            if isinstance(expr, BuildConcept):
                seeds.append(expr)
            else:
                # An expression ORDER BY item can reference columns we can't
                # cheaply enumerate; don't prune under it.
                return True
    visited: set[str] = set()
    while seeds:
        concept = seeds.pop()
        if concept.address in visited:
            continue
        visited.add(concept.address)
        sources = set(cte.source_map.get(concept.address, ()))
        if sources & right_keys:
            return True
        if not sources and concept.lineage is not None:
            seeds.extend(concept.lineage.concept_arguments)
    for existence_sources in cte.existence_source_map.values():
        if set(existence_sources) & right_keys:
            return True
    if any(semi.feeder in right_keys for semi in cte.semi_join_filters):
        return True
    for other in cte.joins or []:
        if other is join or not isinstance(other, Join):
            continue
        if other.right_cte.name in right_keys:
            return True
        if other.left_cte is not None and other.left_cte.name in right_keys:
            return True
        if any(
            pair.cte is not None and pair.cte.name in right_keys
            for pair in other.joinkey_pairs or []
        ):
            return True
    return False


def _unique_on_join_keys(join: Join) -> bool:
    """Each left row matches at most one right row: the right CTE's grain is
    covered by the right-side pair addresses. Null-safe pairs group NULL keys,
    so grain uniqueness carries over unchanged."""
    right = join.right_cte
    if isinstance(right, RecursiveCTE) or not isinstance(right, CTE):
        return False
    if right.limit is not None:
        return False
    pairs = join.joinkey_pairs or []
    if not pairs:
        return False
    key_addresses = {pair.right.address for pair in pairs}
    components = set(right.grain.components) if right.grain else set()
    if not components:
        return False
    return components <= key_addresses


class PruneInvisibleOuterJoins(OptimizationRule):
    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE) or not cte.joins:
            return False, None
        changed = False
        for join in list(cte.joins):
            if not isinstance(join, Join) or join.jointype != JoinType.LEFT_OUTER:
                continue
            right_keys, ambiguous = _right_source_keys(cte, join.right_cte)
            if ambiguous or not _unique_on_join_keys(join):
                continue
            if _rendered_reference_blocks(cte, join, right_keys):
                continue
            self.log(
                f"Removing invisible LEFT join to {join.right_cte.name} on"
                f" {cte.name}: no rendered reference and unique join keys"
            )
            cte.joins.remove(join)
            for address in list(cte.source_map):
                sources = [s for s in cte.source_map[address] if s not in right_keys]
                if sources or not cte.source_map[address]:
                    cte.source_map[address] = sources
                else:
                    # Exclusively right-sourced: only reachable for hidden
                    # addresses (visible ones blocked above), so retire the
                    # column outright.
                    del cte.source_map[address]
                    cte.output_columns = [
                        c for c in cte.output_columns if c.address != address
                    ]
                    cte.hidden_concepts.discard(address)
            cte.parent_ctes = [
                parent for parent in cte.parent_ctes if parent.name not in right_keys
            ]
            changed = True
        return changed, None
