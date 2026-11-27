"""Upgrade outer joins when the WHERE proves they can be stricter.

Outer joins preserve unmatched rows by NULL-padding one side. When the
surrounding WHERE rejects rows where those NULL-padded columns appear (directly
via ``IS NOT NULL`` or via any null-propagating predicate that mentions a
concept on that side), the unmatched rows can never satisfy the filter, so the
OUTER join produces the same surviving rows as a stricter join.

A predicate "forces a concept non-null in surviving rows" if it can never be
TRUE when that concept is NULL: direct ``IS NOT NULL``, null-propagating
comparisons against literals, or any concept reference inside a null-
propagating expression. ``COALESCE``/``NULLIF``/``CASE``/``COUNT`` are opaque
(they can be non-null even when an arg is NULL), so they are not recursed into.

Upgrades by current join type:

  - ``FULL``
      both sides forced non-null  → ``INNER``
      only left forced (drops left-unmatched rows that have NULL left-side
      columns) → ``LEFT_OUTER``
      only right forced (drops right-unmatched rows with NULL right-side
      columns) → ``RIGHT_OUTER``
  - ``LEFT_OUTER`` (NULL-padding lives on the right side)
      right forced → ``INNER``
  - ``RIGHT_OUTER`` (NULL-padding lives on the left side)
      left forced → ``INNER``
"""

from __future__ import annotations

from dataclasses import dataclass, field

from trilogy.core.enums import (
    JoinType,
)
from trilogy.core.models.build import BoolExpr, BuildFunction, BuildParenthetical
from trilogy.core.models.execute import (
    CTE,
    ConceptPair,
    Join,
    UnionCTE,
    coalesced_key_groups,
    pair_matches_nulls,
    preserved_key_pairs,
)
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import (
    accumulated_left_ctes,
    join_padded_ctes,
    output_addresses,
    seed_ctes,
    zero_filled_reads,
)
from trilogy.core.processing.condition_utility import (
    concepts_implied_non_null,
    gather_non_null_proofs,
    gather_or_groups,
    opaque_binding_addresses,
    partial_addresses,
)
from trilogy.core.processing.join_resolution import OUTER_JOIN_TYPES
from trilogy.core.processing.utility import unpadded_join_type


@dataclass
class _ProofState:
    """``cte_keys`` maps a (CTE, key) an INNER join's predicate proves
    non-null to the last such join's index: it rejects the rows any join
    before it padded, and none of the rows a later join pads."""

    direct: set[str]
    cte_keys: dict[tuple[str, str], int] = field(default_factory=dict)
    or_groups: list[list[set[str]]] = field(default_factory=list)

    def direct_intersects(self, addresses: set[str]) -> bool:
        return bool(self.direct & addresses)

    def side_forced_by_or(self, side_only: set[str]) -> bool:
        """An OR atom forces a side present when every disjunct proves a
        concept unique to that side: whichever disjunct made the row survive,
        a side-only column is non-null, so no NULL-padded row of that side
        can survive."""
        return any(
            all(bool(disjunct & side_only) for disjunct in group)
            for group in self.or_groups
        )

    def proves_after(self, cte: CTE | UnionCTE, address: str, idx: int) -> bool:
        return self.cte_keys.get((cte.name, address), -1) > idx

    def proves_cte_key(self, cte: CTE | UnionCTE, address: str, idx: int) -> bool:
        return address in self.direct or self.proves_after(cte, address, idx)

    def proves_key(
        self, cte: CTE | UnionCTE, address: str, idx: int, blocked: set[str]
    ) -> bool:
        """A blocked partial needs the side-specific proof."""
        if address in blocked:
            return self.proves_after(cte, address, idx)
        return self.proves_cte_key(cte, address, idx)

    def proves_cte_present(
        self, cte: CTE | UnionCTE, addresses: set[str], idx: int
    ) -> bool:
        return any(self.proves_after(cte, address, idx) for address in addresses)

    def add_cte_key(self, cte: CTE | UnionCTE, address: str, idx: int) -> bool:
        key = (cte.name, address)
        if self.cte_keys.get(key, -1) >= idx:
            return False
        self.cte_keys[key] = idx
        return True


def _source_datasources(source: CTE | UnionCTE) -> set[str]:
    """The tokens a consumer's ``source_map`` names an operand by, which
    ``_blocked_partials`` intersects against: the operand's own CTE name
    (`generate_source_map` writes ``cte.safe_identifier``) and the physical
    tables it renders from, which the consumer names once a leaf scan is
    inlined."""
    return {source.safe_identifier} | {
        d for vals in source.source_map.values() for d in vals
    }


def _blocked_partials(
    cte: CTE | UnionCTE,
    partial: set[str],
    operand_ds: set[str],
) -> set[str]:
    """Partial concepts that cannot force ``operand`` present: a partial value
    only blocks promotion when it can render from outside the operand (a
    complete copy lives elsewhere and the predicate can be satisfied by that
    column). A partial concept that renders exclusively from the operand still
    forces it.

    Exclusivity, not mere overlap, is load-bearing: a shared join key
    registered on both sides renders as ``COALESCE(operand, other)``, which
    stays non-null on the operand's NULL-padded rows via the other side, so a
    non-null proof on it cannot reject those rows."""
    blocked: set[str] = set()
    for addr in partial:
        binds = set(cte.source_map.get(addr, ()))
        if binds and not (binds <= operand_ds):
            blocked.add(addr)
    return blocked


def _seed_addresses(cte: CTE | UnionCTE) -> set[str]:
    """Addresses available from the CTE's FROM clause (see ``seed_ctes``),
    falling back to a direct base datasource (raw table FROM)."""
    seeds = seed_ctes(cte)
    if seeds:
        return {a for seed in seeds for a in output_addresses(seed)}
    if not isinstance(cte, CTE) or not cte.joins:
        return set()
    if not isinstance(cte.joins[0], Join):
        return set()
    base = cte.source.base_datasource
    if base is not None:
        return {c.address for c in base.output_concepts}
    return set()


def _accumulated_left_addresses(cte: CTE | UnionCTE, idx: int) -> set[str]:
    """Columns visible on the LEFT side of join ``idx``: the seed plus every
    prior join's ``right``. Without the accumulation, ``right_only`` for a
    downstream join over-includes columns the left already carries, and a
    WHERE proof on a shared column would falsely promote the join."""
    if not isinstance(cte, CTE):
        return set()
    return _seed_addresses(cte) | {
        a for left in accumulated_left_ctes(cte, idx) for a in output_addresses(left)
    }


def _side_addresses(
    cte: CTE | UnionCTE,
    idx: int,
    join: Join,
) -> tuple[set[str], set[str]]:
    """Addresses unique to each side of the join. Filters that touch only
    one side are unambiguous about which side they constrain."""
    left_all = _accumulated_left_addresses(cte, idx)
    right_all = output_addresses(join.right)
    return left_all - right_all, right_all - left_all


def _downgrade(
    cte: CTE | UnionCTE,
    idx: int,
    join: Join,
    proofs: _ProofState,
) -> JoinType:
    """Pick the strictest join that still produces the same surviving rows."""
    pairs = join.pairs or []
    left_ctes = accumulated_left_ctes(cte, idx)
    right_all = output_addresses(join.right)
    left_only, right_only = _side_addresses(cte, idx, join)

    # A filter on a concept the operand only partially covers cannot force the
    # operand present when the predicate renders from a complete copy off the
    # operand (see _blocked_partials). Blocked partials need a side-specific
    # cte_keys proof instead.
    right_ds = _source_datasources(join.right)
    left_ds = {d for lc in left_ctes for d in _source_datasources(lc)}
    # A source both sides read is exclusively neither's: the solid stream of
    # a region reads the domain's dimension scan for an attribute, and the
    # span it binds partially renders from that scan on the domain's side.
    left_names = {lc.name for lc in left_ctes}
    right_block = _blocked_partials(
        cte, partial_addresses(join.right), right_ds - left_ds - left_names
    )
    left_block = _blocked_partials(
        cte,
        {a for lc in left_ctes for a in partial_addresses(lc)},
        left_ds - right_ds - {join.right.name},
    )
    # Only a column read off a side is NULL on that side's padded rows; a
    # concept computed here from its lineage is judged by the lineage instead
    # (see `_lineage_proofs`).
    read = {a for a, sources in cte.source_map.items() if sources}
    right_only = (right_only - right_block) & read
    left_only = (left_only - left_block) & read

    # A non-null proof on a concept whose column binding is structurally
    # non-null (a CASE/raw expression) doesn't prove its side matched.
    right_only = right_only - opaque_binding_addresses(join.right)
    left_opaque = {a for lc in left_ctes for a in opaque_binding_addresses(lc)}
    left_only = left_only - left_opaque

    # A side is forced present when the WHERE references a concept that only
    # exists on it, or when every join key is proven non-null for the specific
    # CTE that supplies it. Either rules out NULL-padded rows on that side.
    left_forced = (
        proofs.direct_intersects(left_only)
        or proofs.side_forced_by_or(left_only)
        or any(
            proofs.proves_cte_present(left_cte, output_addresses(left_cte), idx)
            for left_cte in left_ctes
        )
        or (
            bool(pairs)
            and all(
                proofs.proves_key(p.node, p.left.address, idx, left_block)
                for p in pairs
            )
        )
    )
    right_forced = (
        proofs.direct_intersects(right_only)
        or proofs.side_forced_by_or(right_only)
        or proofs.proves_cte_present(join.right, right_all, idx)
        or (
            bool(pairs)
            and all(
                proofs.proves_key(join.right, p.right.address, idx, right_block)
                for p in pairs
            )
        )
    )
    return unpadded_join_type(join.join_type, left_forced, right_forced)


def _lineage_proofs(cte: CTE, condition: BoolExpr, proofs: set[str]) -> set[str]:
    """Add what each proven concept's lineage proves, for the concepts this CTE
    computes inline (no ``source_map`` column). Such a value is computed after
    the join, so it can be non-null on a padded row (``ret is not null`` is
    ``false`` there); only its strict inputs are NULL with the padding."""
    known = {c.address: c for c in condition.concept_arguments}
    out = set(proofs)
    todo = [a for a in proofs if not cte.source_map.get(a)]
    while todo:
        concept = known.get(todo.pop())
        if concept is None or concept.lineage is None:
            continue
        lineage = concept.lineage
        if isinstance(lineage, BuildFunction | BuildParenthetical):
            known.update({c.address: c for c in lineage.concept_arguments})
        for address in concepts_implied_non_null(lineage) - out:
            out.add(address)
            if not cte.source_map.get(address):
                todo.append(address)
    return out


def _cte_side(pair: ConceptPair) -> str:
    return pair.node.name


def _add_inner_join_key_proofs(join: Join, idx: int, proofs: _ProofState) -> bool:
    """Propagate only key non-nullness proven by rendered INNER predicates."""
    changed = False
    for pairs in coalesced_key_groups(join.pairs or [], _cte_side):
        left_ctes = {p.node.name for p in pairs}
        if len(left_ctes) > 1:
            # Renders as COALESCE(left1, left2, ...) = right, which proves the
            # right key but no individual left key.
            right = pairs[0].right.address
            changed = proofs.add_cte_key(join.right, right, idx) or changed
            continue

        pair = pairs[0]
        if pair_matches_nulls(pair, join.modifiers):
            continue
        changed = proofs.add_cte_key(pair.node, pair.left.address, idx) or changed
        changed = proofs.add_cte_key(join.right, pair.right.address, idx) or changed
    return changed


class UpgradeJoinOnGuards(OptimizationRule):
    """Upgrade FULL/LEFT_OUTER/RIGHT_OUTER joins to a stricter form when the
    CTE's own WHERE rejects the unmatched rows the OUTER join was preserving.

    The planner narrows on every WHERE and join it places
    (`consumer_proofs`, `grain_utility.downgrade_join_for_proofs`); this
    reads only what the optimizer moved into a CTE after planning."""

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE):
            return False, None
        if not any(
            isinstance(j, Join) and j.join_type in OUTER_JOIN_TYPES
            for j in cte.joins or []
        ):
            return False, None

        direct_proofs = (
            gather_non_null_proofs(cte.condition) if cte.condition else set()
        )
        or_groups = gather_or_groups(cte.condition) if cte.condition else []
        # `count = 0` over a COUNT this CTE coalesces to 0 on the padded rows
        # accepts those rows: it proves nothing about the side that padded them
        zero = zero_filled_reads(cte, cte.condition)
        if zero:
            direct_proofs -= zero
            or_groups = [[d - zero for d in group] for group in or_groups]
        if cte.condition is not None:
            direct_proofs = _lineage_proofs(cte, cte.condition, direct_proofs)
            or_groups = [
                [_lineage_proofs(cte, cte.condition, d) for d in group]
                for group in or_groups
            ]
        if not direct_proofs and not or_groups:
            return False, None
        proofs = _ProofState(direct=direct_proofs, or_groups=or_groups)

        # Iterate to fixpoint: an INNER join with null-rejecting key predicates
        # proves its sources' keys non-null, which can unlock further upgrades.
        # A merged key proves some branch supplied a value, not every branch
        # sharing that address.
        changed = False
        while True:
            proof_changed = False
            join_changed = False
            for idx, join in enumerate(cte.joins or []):
                if isinstance(join, Join) and join.join_type == JoinType.INNER:
                    proof_changed = (
                        _add_inner_join_key_proofs(join, idx, proofs) or proof_changed
                    )

            for idx, join in enumerate(cte.joins or []):
                if not isinstance(join, Join) or join.join_type not in OUTER_JOIN_TYPES:
                    continue
                target = _downgrade(cte, idx, join, proofs)
                if target == join.join_type:
                    continue
                dropped = {
                    JoinType.INNER: "unmatched",
                    JoinType.LEFT_OUTER: "right-unmatched",
                    JoinType.RIGHT_OUTER: "left-unmatched",
                }[target]
                self.log(
                    f"{join.join_type.value}→{target.value} on {cte.name} for join with"
                    f" {join.right.name}: WHERE filters out"
                    f" {dropped} rows that the OUTER join was preserving"
                )
                join.join_type = target
                if target == JoinType.INNER:
                    _add_inner_join_key_proofs(join, idx, proofs)
                join_changed = True

            if not proof_changed and not join_changed:
                break
            changed = changed or join_changed
        return changed, None


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
