"""upgrades joins when the WHERE proves they can be inner.

Outer joins exist to preserve unmatched rows by NULL-padding one side. When the
surrounding WHERE rejects rows where those NULL-padded columns appear (directly
via ``IS NOT NULL`` or via any null-propagating predicate that mentions a
concept on that side), the unmatched rows can never satisfy the filter — so
the OUTER join produces the same surviving rows as a stricter join, and we
can upgrade.

A predicate "forces a concept non-null in surviving rows" if it can never be
TRUE when that concept is NULL — direct ``IS NOT NULL``, null-propagating
comparisons against literals, or any concept reference inside a null-
propagating expression. ``COALESCE``/``NULLIF``/``CASE``/``COUNT`` are treated
as opaque (they can be non-null even when an arg is NULL), so we don't recurse
into them.

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

from trilogy.core.enums import ComparisonOperator, JoinType
from trilogy.core.models.build import (
    BuildComparison,
    BuildConditional,
    BuildParenthetical,
)
from trilogy.core.models.execute import CTE, BaseJoin, CTEConceptPair, Join, UnionCTE
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.processing.condition_utility import (
    NULL_PROPAGATING_OPS,
    concepts_implied_non_null,
    decompose_condition,
    is_null_literal,
)

# Joins whose surviving rows can be a strict subset under a null-rejecting
# WHERE. INNER joins already drop both unmatched sides, so they're never
# eligible to be downgraded further.
_OUTER_JOIN_TYPES = (JoinType.FULL, JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER)


def _proves_non_null(
    atom: BuildComparison | BuildConditional | BuildParenthetical,
) -> set[str]:
    """Concept addresses that this AND-atom forces non-null in surviving rows."""
    if isinstance(atom, BuildParenthetical):
        return _proves_non_null(atom.content)  # type: ignore[arg-type]
    if not isinstance(atom, BuildComparison):
        return set()

    left, right, op = atom.left, atom.right, atom.operator

    if op == ComparisonOperator.IS_NOT:
        # `<expr> IS NOT NULL` — every concept inside the expression must be
        # non-null, modulo null-opaque functions.
        if is_null_literal(right):
            return concepts_implied_non_null(left)
        if is_null_literal(left):
            return concepts_implied_non_null(right)
        return set()

    # `concept IS NULL` specifically wants NULLs — never proves non-null.
    if op == ComparisonOperator.IS:
        return set()

    if op in NULL_PROPAGATING_OPS:
        return concepts_implied_non_null(left) | concepts_implied_non_null(right)

    return set()


def _gather_proofs(
    cond: BuildComparison | BuildConditional | BuildParenthetical,
) -> set[str]:
    return {
        addr for atom in decompose_condition(cond) for addr in _proves_non_null(atom)
    }


def _cte_addresses(cte: CTE | UnionCTE | None) -> set[str]:
    if cte is None:
        return set()
    return {c.address for c in cte.output_columns}


def _seed_addresses(cte: CTE | UnionCTE) -> set[str]:
    """Addresses available from the CTE's FROM clause — the LEFT side of the
    chain's first join.

    The "left" of an in-rendered join can come from any of:
      - ``joins[0].left_cte`` (explicit).
      - A ``parent_cte`` that isn't consumed as any join's right side.
      - A ``joinkey_pair.cte`` attached to a join — set when an upstream CTE
        was inlined into this one. Without this branch a chain whose left
        side is an inlined CTE (e.g. gcat ``vehicle_lv_info``) shows up with
        no ``parent_cte`` and no explicit ``left_cte``, so the rule would
        otherwise see an empty left and (wrongly) treat shared join-key
        columns as right-only proofs.
      - A direct base datasource (raw table FROM).

    Each non-seed parent gets consumed as some join's ``right_cte``; the
    FROM-clause table is the leftover."""
    if not isinstance(cte, CTE) or not cte.joins:
        return set()
    first = cte.joins[0]
    if not isinstance(first, Join):
        return set()
    if first.left_cte is not None:
        return _cte_addresses(first.left_cte)
    right_names = {j.right_cte.name for j in cte.joins if isinstance(j, Join)}
    for parent in cte.parent_ctes:
        if isinstance(parent, (CTE, UnionCTE)) and parent.name not in right_names:
            return _cte_addresses(parent)
    # Inlined-left case: a join carries its own left CTE on the joinkey_pairs.
    for j in cte.joins:
        if not isinstance(j, Join):
            continue
        for pair in j.joinkey_pairs or []:
            if not isinstance(pair, CTEConceptPair):
                continue
            if pair.cte.name not in right_names:
                return _cte_addresses(pair.cte)
    base = cte.source.base_datasource
    if base is not None:
        return {c.address for c in base.output_concepts}
    return set()


def _accumulated_left_addresses(cte: CTE | UnionCTE, idx: int) -> set[str]:
    """Columns visible on the LEFT side of join ``idx`` — the accumulated FROM.

    The seed (FROM clause) contributes its columns; for ``idx > 0`` every prior
    join's ``right_cte`` also contributes (already merged into the LEFT by the
    time this join is evaluated). Without the accumulation, ``right_only`` for
    a downstream join over-includes columns the left already carried, and a
    WHERE proof on a shared column would falsely promote the join."""
    addrs: set[str] = set()
    if not isinstance(cte, CTE):
        return addrs
    join = cte.joins[idx] if idx < len(cte.joins) else None
    if isinstance(join, Join) and join.left_cte is not None:
        addrs |= _cte_addresses(join.left_cte)
    addrs |= _seed_addresses(cte)
    for prior_idx in range(idx):
        prior = cte.joins[prior_idx]
        if not isinstance(prior, Join):
            continue
        addrs |= _cte_addresses(prior.right_cte)
    return addrs


def _side_addresses(
    cte: CTE | UnionCTE,
    idx: int,
    join: Join,
) -> tuple[set[str], set[str]]:
    """Addresses unique to each side of the join. Filters that touch only
    one side are unambiguous about which side they constrain."""
    left_all = _accumulated_left_addresses(cte, idx)
    right_all = _cte_addresses(join.right_cte)
    return left_all - right_all, right_all - left_all


def _downgrade(
    cte: CTE | UnionCTE,
    idx: int,
    join: Join,
    proofs: set[str],
) -> JoinType | None:
    """Pick the strictest join that still produces the same surviving rows."""
    current = join.jointype
    if current not in _OUTER_JOIN_TYPES:
        return None

    pairs = join.joinkey_pairs or []
    left_keys = {p.left.address for p in pairs}
    right_keys = {p.right.address for p in pairs}
    left_only, right_only = _side_addresses(cte, idx, join)

    # The left side is "forced present" when the WHERE references a concept
    # that only exists on the left, OR when every left join key is individually
    # proven non-null. Either case rules out unmatched rows whose left columns
    # were filled with NULL. Mirror for the right side.
    left_forced = bool(proofs & left_only) or (
        bool(left_keys) and left_keys.issubset(proofs)
    )
    right_forced = bool(proofs & right_only) or (
        bool(right_keys) and right_keys.issubset(proofs)
    )

    if current == JoinType.FULL:
        if left_forced and right_forced:
            return JoinType.INNER
        # left_forced → filter drops left-unmatched rows (NULL left columns)
        # → LEFT_OUTER drops the same set. Mirror for right_forced.
        if left_forced:
            return JoinType.LEFT_OUTER
        if right_forced:
            return JoinType.RIGHT_OUTER
        return None

    if current == JoinType.LEFT_OUTER:
        # NULL-padding lives on the right side; right_forced removes the
        # padded rows, leaving only matched rows → INNER.
        if right_forced:
            return JoinType.INNER
        return None

    if current == JoinType.RIGHT_OUTER:
        if left_forced:
            return JoinType.INNER
        return None

    return None


def _downgrade_base_join(
    cte: CTE,
    base_join: BaseJoin,
    proofs: set[str],
) -> JoinType | None:
    """Mirror of ``_downgrade`` for BaseJoins on ``cte.source.joins``, but
    restricted to LEFT_OUTER→INNER. The right side is a datasource (BD or
    QDS) and ``left_all`` (everything other than the right) tends to
    over-include concepts from inline-joined dims that aren't really on the
    "left" of this specific join — which is fine for proving the right side
    non-null (LEFT_OUTER→INNER) but risks false-positive ``left_forced``
    for the FULL→INNER path. Conservative: only handle the LEFT_OUTER case.

    ``BaseJoin.right_datasource`` is typically a QDS wrapper over the
    underlying BD; the same BD is usually also registered directly on
    ``cte.source.datasources``. Treat the wrapper and its base as the same
    logical right side so the BD's dim attrs don't leak into ``left_all``
    and erase the ``right_only`` we need for the proof check.
    """
    current = base_join.join_type
    if current != JoinType.LEFT_OUTER:
        return None

    right_ds = base_join.right_datasource
    right_base = getattr(right_ds, "base_datasource", None)
    right_all = {c.address for c in right_ds.output_concepts}
    if right_base is not None:
        right_all |= {c.address for c in right_base.output_concepts}
    left_all: set[str] = set()
    for ds in cte.source.datasources:
        if ds is right_ds or ds is right_base:
            continue
        left_all |= {c.address for c in ds.output_concepts}
    right_only = right_all - left_all

    pairs = base_join.concept_pairs or []
    right_keys = {p.right.address for p in pairs}

    right_forced = bool(proofs & right_only) or (
        bool(right_keys) and right_keys.issubset(proofs)
    )
    # NULL-padding lives on the right side; right_forced removes the padded
    # rows, leaving only matched rows → INNER.
    if right_forced:
        return JoinType.INNER
    return None


class UpgradeJoinOnGuards(OptimizationRule):
    """Upgrade FULL/LEFT_OUTER/RIGHT_OUTER joins to a stricter form when the
    enclosing WHERE rejects the unmatched rows the OUTER join was preserving.

    ``base_join_only=True`` restricts the rule to BaseJoins on
    ``cte.source.joins``; intended for an early pass before UnionDimPushdown
    so dim joins become INNER without disturbing CTE-to-CTE joins (which
    other optimizations like CollapseSingleParent may have moved into
    structures that change row visibility under upgrade).
    """

    def __init__(self, base_join_only: bool = False) -> None:
        super().__init__()
        self.base_join_only = base_join_only

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE) or not cte.condition:
            return False, None
        has_outer_cte_join = not self.base_join_only and any(
            isinstance(j, Join) and j.jointype in _OUTER_JOIN_TYPES
            for j in (cte.joins or [])
        )
        has_outer_base_join = any(
            isinstance(j, BaseJoin) and j.join_type in _OUTER_JOIN_TYPES
            for j in (cte.source.joins or [])
        )
        if not has_outer_cte_join and not has_outer_base_join:
            return False, None

        proofs = _gather_proofs(cte.condition)
        if not proofs:
            return False, None

        # Iterate to fixpoint: when a join becomes INNER, both sides' join keys
        # must be non-null on surviving rows. Adding those keys to the proof
        # set can unlock further upgrades — e.g. a downstream dim INNER-joined
        # under a null-rejecting filter forces the join-key column on its
        # upstream LEFT_OUTER-joined fact non-null, which then promotes that
        # earlier join too.
        #
        # Skip adding a join key whose address appears on 3+ datasources in
        # this CTE: that's a merged concept (e.g. `customer.id` unified across
        # multiple sales-source CTEs by `merge`). Its non-null in surviving
        # rows means *some* source contributed via COALESCE — it doesn't pin
        # any specific source side, and treating it as a side-bound proof
        # would wrongly promote sibling joins that the OR/COALESCE semantics
        # are protecting (q10's web/catalog/store union over customer).
        #
        # A clean FK↔PK pair shows up on exactly 2 datasources (the FK on the
        # fact and the PK on the dim) and is safe.
        address_source_count: dict[str, int] = {}
        for ds in cte.source.datasources:
            for c in ds.output_concepts:
                address_source_count[c.address] = (
                    address_source_count.get(c.address, 0) + 1
                )

        def _safe_proof_add(address: str) -> bool:
            return address_source_count.get(address, 0) <= 2
        changed = False
        while True:
            iter_changed = False
            if not self.base_join_only:
                for idx, join in enumerate(cte.joins or []):
                    if (
                        not isinstance(join, Join)
                        or join.jointype not in _OUTER_JOIN_TYPES
                    ):
                        continue
                    target = _downgrade(cte, idx, join, proofs)
                    if target is None or target == join.jointype:
                        continue
                    dropped = {
                        JoinType.INNER: "unmatched",
                        JoinType.LEFT_OUTER: "right-unmatched",
                        JoinType.RIGHT_OUTER: "left-unmatched",
                    }[target]
                    self.log(
                        f"{join.jointype.value}→{target.value} on {cte.name} for join with"
                        f" {join.right_cte.name}: WHERE filters out"
                        f" {dropped} rows that the OUTER join was preserving"
                    )
                    join.jointype = target
                    if target == JoinType.INNER:
                        for pair in join.joinkey_pairs or []:
                            if _safe_proof_add(pair.left.address):
                                proofs.add(pair.left.address)
                            if _safe_proof_add(pair.right.address):
                                proofs.add(pair.right.address)
                    iter_changed = True

            for base_join in cte.source.joins or []:
                if (
                    not isinstance(base_join, BaseJoin)
                    or base_join.join_type not in _OUTER_JOIN_TYPES
                ):
                    continue
                target = _downgrade_base_join(cte, base_join, proofs)
                if target is None or target == base_join.join_type:
                    continue
                dropped = {
                    JoinType.INNER: "unmatched",
                    JoinType.LEFT_OUTER: "right-unmatched",
                    JoinType.RIGHT_OUTER: "left-unmatched",
                }[target]
                self.log(
                    f"{base_join.join_type.value}→{target.value} on {cte.name} for "
                    f"base join with {base_join.right_datasource.identifier}: WHERE "
                    f"filters out {dropped} rows that the OUTER join was preserving"
                )
                base_join.join_type = target
                if target == JoinType.INNER:
                    for bpair in base_join.concept_pairs or []:
                        if _safe_proof_add(bpair.left.address):
                            proofs.add(bpair.left.address)
                        if _safe_proof_add(bpair.right.address):
                            proofs.add(bpair.right.address)
                iter_changed = True

            if not iter_changed:
                break
            changed = True
        return changed, None
