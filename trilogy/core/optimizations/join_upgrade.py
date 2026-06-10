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

from dataclasses import dataclass, field

from trilogy.core.enums import (
    BooleanOperator,
    JoinType,
    Modifier,
)
from trilogy.core.models.build import (
    BoolExpr,
    BuildBetween,
    BuildComparison,
    BuildConditional,
    BuildDatasource,
    BuildFunction,
    BuildParenthetical,
)
from trilogy.core.models.datasource import RawColumnExpr
from trilogy.core.models.execute import (
    CTE,
    BaseJoin,
    ConceptPair,
    CTEConceptPair,
    Join,
    QueryDatasource,
    UnionCTE,
)
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.processing.condition_utility import (
    comparison_proves_non_null,
    concepts_implied_non_null,
    decompose_condition,
)

# Joins whose surviving rows can be a strict subset under a null-rejecting
# WHERE. INNER joins already drop both unmatched sides, so they're never
# eligible to be downgraded further.
_OUTER_JOIN_TYPES = (JoinType.FULL, JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER)


def _base_datasource(
    datasource: BuildDatasource | QueryDatasource,
) -> BuildDatasource | QueryDatasource | None:
    if isinstance(datasource, QueryDatasource):
        return datasource.base_datasource
    return None


@dataclass
class _ProofState:
    direct: set[str]
    cte_keys: set[tuple[str, str]] = field(default_factory=set)
    datasource_keys: set[tuple[str, str]] = field(default_factory=set)
    or_groups: list[list[set[str]]] = field(default_factory=list)

    def direct_intersects(self, addresses: set[str]) -> bool:
        return bool(self.direct & addresses)

    def side_forced_by_or(self, side_only: set[str]) -> bool:
        """An OR atom forces a side present when every disjunct proves a
        concept unique to that side — whichever disjunct made the row survive,
        a side-only column is non-null, so no unmatched (NULL-padded) row of
        that side can survive."""
        return any(
            all(bool(disjunct & side_only) for disjunct in group)
            for group in self.or_groups
        )

    def proves_cte_key(self, cte: CTE | UnionCTE, address: str) -> bool:
        return address in self.direct or (cte.name, address) in self.cte_keys

    def proves_datasource_key(
        self, datasource: BuildDatasource | QueryDatasource, address: str
    ) -> bool:
        return (
            address in self.direct
            or (datasource.identifier, address) in self.datasource_keys
        )

    def proves_cte_present(self, cte: CTE | UnionCTE, addresses: set[str]) -> bool:
        return any((cte.name, address) in self.cte_keys for address in addresses)

    def proves_datasource_present(
        self,
        datasource: BuildDatasource | QueryDatasource,
        addresses: set[str],
    ) -> bool:
        return any(
            (datasource.identifier, address) in self.datasource_keys
            for address in addresses
        )

    def add_cte_key(self, cte: CTE | UnionCTE, address: str) -> bool:
        key = (cte.name, address)
        if key in self.cte_keys:
            return False
        self.cte_keys.add(key)
        return True

    def add_datasource_key(
        self, datasource: BuildDatasource | QueryDatasource, address: str
    ) -> bool:
        key = (datasource.identifier, address)
        if key in self.datasource_keys:
            return False
        self.datasource_keys.add(key)
        return True


def _partial_addresses(
    source: CTE | UnionCTE | BuildDatasource | QueryDatasource,
) -> set[str]:
    """Addresses a source covers only PARTIALLY — it doesn't hold the full set
    of that concept's values.

    A filter on a partial concept can't promote an OUTER join to a stricter
    one: the right side's unmatched rows are meaningful (their value simply
    isn't in this partial source), and the predicate is rendered from the
    complete side anyway — so requiring the value non-null doesn't reject the
    NULL-padded rows. This is the difference between ``customer LEFT JOIN
    orders`` (orders is partial for ``customer.id`` — promoting would drop
    no-order customers) and ``customer LEFT JOIN customer_address`` (the
    address PK is complete, so a non-null filter legitimately forces INNER)."""
    if isinstance(source, BuildDatasource):
        return set()
    return {c.address for c in source.partial_concepts}


def _opaque_binding_addresses(
    source: CTE | UnionCTE | BuildDatasource | QueryDatasource | None,
) -> set[str]:
    """Addresses whose datasource column binding can be non-null even on a
    join's NULL-padded (unmatched) rows.

    A plain column is NULL-padded when its row is unmatched, so a non-null
    proof on it genuinely forces that side present. But a ``RawColumnExpr``
    (opaque SQL we can't analyze, e.g. ``CASE WHEN <rightcol> IS NOT NULL THEN
    True ELSE False END``) or a null-opaque function can yield a non-null value
    for an unmatched row — so proving such a concept non-null proves nothing
    about the join. These must NOT force a LEFT/FULL→stricter upgrade. (The
    ``is_returned = false`` bug: the predicate is satisfied by exactly the
    unmatched rows, yet the CASE-derived flag is non-null, so the gate wrongly
    upgraded the join to INNER and dropped them.)"""
    out: set[str] = set()
    seen: set[int] = set()

    def scan(ds: CTE | UnionCTE | BuildDatasource | QueryDatasource | None) -> None:
        if ds is None or id(ds) in seen:
            return
        seen.add(id(ds))
        if isinstance(ds, BuildDatasource):
            for col in ds.columns:
                alias = col.alias
                if isinstance(alias, RawColumnExpr) or (
                    isinstance(alias, BuildFunction)
                    and not concepts_implied_non_null(alias)
                ):
                    out.add(col.concept.address)
        elif isinstance(ds, QueryDatasource):
            for child in ds.datasources:
                scan(child)
            scan(ds.base_datasource)
        else:  # CTE | UnionCTE
            scan(ds.source)

    scan(source)
    return out


def _source_datasources(
    source: CTE | UnionCTE | BuildDatasource | QueryDatasource,
) -> set[str]:
    """Physical datasource identifiers an operand renders from. For a CTE this
    is every source in its ``source_map``; for a base datasource, itself."""
    sm = getattr(source, "source_map", None)
    if sm:
        return {d for vals in sm.values() for d in vals}
    ident = getattr(source, "identifier", None) or getattr(source, "name", None)
    return {ident} if ident else set()


def _blocked_partials(
    cte: CTE | UnionCTE,
    partial: set[str],
    operand_ds: set[str],
) -> set[str]:
    """Partial concepts that can't force ``operand`` present: a partial value
    only blocks promotion when it renders from OUTSIDE the operand (a complete
    copy lives elsewhere, and that's the column the predicate touches — q22's
    customer key, q95's web_sales order number). A partial concept that renders
    from the operand itself still forces it (the operand is genuinely where the
    filtered value comes from)."""
    blocked: set[str] = set()
    for addr in partial:
        binds = set(cte.source_map.get(addr, ()))
        if binds and not (binds & operand_ds):
            blocked.add(addr)
    return blocked


def _pair_can_match_nulls(
    pair: CTEConceptPair | ConceptPair,
    join_modifiers: list[Modifier],
) -> bool:
    return Modifier.NULLABLE in (
        pair.modifiers
        + (pair.left.modifiers or [])
        + (pair.right.modifiers or [])
        + (join_modifiers or [])
    )


def _or_disjuncts(
    atom: BoolExpr,
) -> list[BoolExpr]:
    """Flatten an OR tree (unwrapping parentheticals) into its disjuncts.

    A non-OR node returns ``[node]`` (a single "disjunct")."""
    if isinstance(atom, BuildParenthetical):
        return _or_disjuncts(atom.content)  # type: ignore[arg-type]
    if isinstance(atom, BuildConditional) and atom.operator == BooleanOperator.OR:
        return _or_disjuncts(atom.left) + _or_disjuncts(atom.right)  # type: ignore[arg-type]
    return [atom]


def _proves_non_null(
    atom: BoolExpr,
) -> set[str]:
    """Concept addresses that this AND-atom forces non-null in surviving rows."""
    if isinstance(atom, BuildParenthetical):
        return _proves_non_null(atom.content)  # type: ignore[arg-type]
    if isinstance(atom, BuildConditional) and atom.operator == BooleanOperator.OR:
        # A surviving row satisfies at least one disjunct but we don't know
        # which — only concepts non-null under *every* disjunct are proven.
        sets = [_gather_proofs(d) for d in _or_disjuncts(atom)]
        return set.intersection(*sets) if sets else set()
    if isinstance(atom, BuildConditional) and atom.operator == BooleanOperator.AND:
        # ``decompose_condition`` returns the whole AND as one chunk when a
        # child isn't in ``CONDITION_TYPES`` (e.g. a ``raw(...)`` predicate
        # arrives as a bare ``BuildFunction``). Walk both sides ourselves so
        # ordinary Comparison proofs sitting next to the opaque child still
        # contribute (q64 ``is_returned`` + ``C_CURRENT_ADDR_SK is not null``).
        return _proves_non_null(atom.left) | _proves_non_null(atom.right)  # type: ignore[arg-type]
    if isinstance(atom, BuildBetween):
        # `x BETWEEN low AND high` requires all three operands to be non-null
        # for the row to survive.
        return (
            concepts_implied_non_null(atom.left)
            | concepts_implied_non_null(atom.low)
            | concepts_implied_non_null(atom.high)
        )
    if not isinstance(atom, BuildComparison):
        return set()
    # Leaf comparison logic (IS NOT/IS/null-propagating + coalesce peering) is
    # shared with condition_utility.condition_proves_non_null.
    return comparison_proves_non_null(atom)


def _gather_proofs(
    cond: BoolExpr,
) -> set[str]:
    return {
        addr for atom in decompose_condition(cond) for addr in _proves_non_null(atom)
    }


def _gather_or_groups(
    cond: BoolExpr,
) -> list[list[set[str]]]:
    """Per-OR-atom disjunct proof sets, for side-level (not concept-level)
    proofs. ``(a.x = 1 OR a.y = 2)`` proves no single concept non-null, but
    every disjunct forces the ``a`` side present — enough to drop ``a``'s
    unmatched rows. Only kept when every disjunct proves something."""
    groups: list[list[set[str]]] = []
    for atom in decompose_condition(cond):
        disjuncts = _or_disjuncts(atom)
        if len(disjuncts) < 2:
            continue
        sets = [_gather_proofs(d) for d in disjuncts]
        if all(sets):
            groups.append(sets)
    return groups


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
    for parent in cte.dependency_nodes(include_inlined=True):
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


def _seed_ctes(cte: CTE | UnionCTE) -> list[CTE | UnionCTE]:
    if not isinstance(cte, CTE) or not cte.joins:
        return []
    first = cte.joins[0]
    if not isinstance(first, Join):
        return []
    if first.left_cte is not None:
        return [first.left_cte]
    right_names = {j.right_cte.name for j in cte.joins if isinstance(j, Join)}
    for parent in cte.dependency_nodes(include_inlined=True):
        if isinstance(parent, (CTE, UnionCTE)) and parent.name not in right_names:
            return [parent]
    for j in cte.joins:
        if not isinstance(j, Join):
            continue
        for pair in j.joinkey_pairs or []:
            if isinstance(pair, CTEConceptPair) and pair.cte.name not in right_names:
                return [pair.cte]
    return []


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


def _accumulated_left_ctes(cte: CTE | UnionCTE, idx: int) -> list[CTE | UnionCTE]:
    left_ctes: list[CTE | UnionCTE] = []
    names: set[str] = set()

    def add_left_cte(left_cte: CTE | UnionCTE) -> None:
        if left_cte.name in names:
            return
        names.add(left_cte.name)
        left_ctes.append(left_cte)

    if not isinstance(cte, CTE):
        return left_ctes
    join = cte.joins[idx] if idx < len(cte.joins) else None
    if isinstance(join, Join) and join.left_cte is not None:
        add_left_cte(join.left_cte)
    for seed_cte in _seed_ctes(cte):
        add_left_cte(seed_cte)
    for prior_idx in range(idx):
        prior = cte.joins[prior_idx]
        if isinstance(prior, Join):
            add_left_cte(prior.right_cte)
    return left_ctes


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
    proofs: _ProofState,
) -> JoinType | None:
    """Pick the strictest join that still produces the same surviving rows."""
    current = join.jointype
    if current not in _OUTER_JOIN_TYPES:
        return None

    pairs = join.joinkey_pairs or []
    left_ctes = _accumulated_left_ctes(cte, idx)
    right_all = _cte_addresses(join.right_cte)
    left_only, right_only = _side_addresses(cte, idx, join)

    # A filter on a concept the operand only PARTIALLY covers can't force that
    # operand present *when the predicate renders from a complete copy living
    # off the operand* — the missing rows are meaningful and the filter touches
    # the other side. Partials that render from the operand itself still force
    # it (see _blocked_partials). Blocked partials need a side-specific
    # cte_keys proof instead.
    right_ds = _source_datasources(join.right_cte)
    left_ds = {d for lc in left_ctes for d in _source_datasources(lc)}
    right_block = _blocked_partials(cte, _partial_addresses(join.right_cte), right_ds)
    left_block = _blocked_partials(
        cte, {a for lc in left_ctes for a in _partial_addresses(lc)}, left_ds
    )
    right_only = right_only - right_block
    left_only = left_only - left_block

    # A non-null proof on a concept whose column binding is structurally
    # non-null (a CASE/raw expression) doesn't prove its side matched.
    right_only = right_only - _opaque_binding_addresses(join.right_cte)
    left_opaque = {a for lc in left_ctes for a in _opaque_binding_addresses(lc)}
    left_only = left_only - left_opaque

    def proves_left_key(c: CTE | UnionCTE, address: str) -> bool:
        if address in left_block:
            return (c.name, address) in proofs.cte_keys
        return proofs.proves_cte_key(c, address)

    def proves_right_key(address: str) -> bool:
        if address in right_block:
            return (join.right_cte.name, address) in proofs.cte_keys
        return proofs.proves_cte_key(join.right_cte, address)

    # The left side is "forced present" when the WHERE references a concept
    # that only exists on the left, OR when every left join key is proven
    # non-null for the specific CTE that supplies it. Either case rules out
    # unmatched rows whose left columns were filled with NULL. Mirror for the
    # right side.
    left_forced = (
        proofs.direct_intersects(left_only)
        or proofs.side_forced_by_or(left_only)
        or any(
            proofs.proves_cte_present(left_cte, _cte_addresses(left_cte))
            for left_cte in left_ctes
        )
        or (bool(pairs) and all(proves_left_key(p.cte, p.left.address) for p in pairs))
    )
    right_forced = (
        proofs.direct_intersects(right_only)
        or proofs.side_forced_by_or(right_only)
        or proofs.proves_cte_present(join.right_cte, right_all)
        or (bool(pairs) and all(proves_right_key(p.right.address) for p in pairs))
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
    proofs: _ProofState,
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
    right_base = _base_datasource(right_ds)
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
    # Partials only block promotion when they render from a complete copy off
    # the right datasource (see _blocked_partials / _downgrade).
    right_partial = _partial_addresses(right_ds)
    if right_base is not None:
        right_partial |= _partial_addresses(right_base)
    right_operand_ds = _source_datasources(right_ds)
    if right_base is not None:
        right_operand_ds |= _source_datasources(right_base)
    right_block = _blocked_partials(cte, right_partial, right_operand_ds)
    right_only = right_only - right_block
    # Structurally-non-null (CASE/raw) bindings don't force the side present.
    right_only = right_only - _opaque_binding_addresses(right_ds)
    if right_base is not None:
        right_only = right_only - _opaque_binding_addresses(right_base)

    def proves_key(address: str) -> bool:
        if address in right_block:
            return (right_ds.identifier, address) in proofs.datasource_keys
        return proofs.proves_datasource_key(right_ds, address)

    right_forced = (
        proofs.direct_intersects(right_only)
        or proofs.side_forced_by_or(right_only)
        or proofs.proves_datasource_present(right_ds, right_all)
        or (
            right_base is not None
            and proofs.proves_datasource_present(right_base, right_all)
        )
        or (bool(pairs) and all(proves_key(p.right.address) for p in pairs))
    )
    # NULL-padding lives on the right side; right_forced removes the padded
    # rows, leaving only matched rows → INNER.
    if right_forced:
        return JoinType.INNER
    return None


def _add_inner_join_key_proofs(join: Join, proofs: _ProofState) -> bool:
    """Propagate only key non-nullness proven by rendered INNER predicates."""
    changed = False
    groups: dict[tuple[str, str], list[CTEConceptPair]] = {}
    for pair in join.joinkey_pairs or []:
        groups.setdefault((pair.right.address, pair.left.address), []).append(pair)

    for pairs in groups.values():
        left_ctes = {p.cte.name for p in pairs}
        if len(left_ctes) > 1:
            # Renderer uses COALESCE(left1, left2, ...) = right here. That
            # proves the right key, but not any individual left input key.
            changed = (
                proofs.add_cte_key(join.right_cte, pairs[0].right.address) or changed
            )
            continue

        pair = pairs[0]
        if _pair_can_match_nulls(pair, join.modifiers):
            continue
        changed = proofs.add_cte_key(pair.cte, pair.left.address) or changed
        changed = proofs.add_cte_key(join.right_cte, pair.right.address) or changed
    return changed


def _add_inner_base_join_key_proofs(
    base_join: BaseJoin,
    proofs: _ProofState,
) -> bool:
    changed = False
    groups: dict[tuple[str, str], list[ConceptPair]] = {}
    for pair in base_join.concept_pairs or []:
        groups.setdefault((pair.right.address, pair.left.address), []).append(pair)

    for pairs in groups.values():
        left_sources = {p.existing_datasource.identifier for p in pairs}
        if len(left_sources) > 1:
            changed = (
                proofs.add_datasource_key(
                    base_join.right_datasource, pairs[0].right.address
                )
                or changed
            )
            continue

        pair = pairs[0]
        if _pair_can_match_nulls(pair, base_join.modifiers):
            continue
        changed = (
            proofs.add_datasource_key(pair.existing_datasource, pair.left.address)
            or changed
        )
        changed = (
            proofs.add_datasource_key(base_join.right_datasource, pair.right.address)
            or changed
        )
    return changed


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

        direct_proofs = _gather_proofs(cte.condition)
        or_groups = _gather_or_groups(cte.condition)
        if not direct_proofs and not or_groups:
            return False, None
        proofs = _ProofState(direct=direct_proofs, or_groups=or_groups)

        # Iterate to fixpoint: when an INNER join uses null-rejecting key
        # predicates, keys for those specific sources are non-null. Source-bound
        # key proofs can unlock further upgrades. A merged key proves that some
        # branch supplied a value, not every branch sharing that address.
        changed = False
        while True:
            proof_changed = False
            join_changed = False
            if not self.base_join_only:
                for join in cte.joins or []:
                    if isinstance(join, Join) and join.jointype == JoinType.INNER:
                        proof_changed = (
                            _add_inner_join_key_proofs(join, proofs) or proof_changed
                        )
            for base_join in cte.source.joins or []:
                if (
                    isinstance(base_join, BaseJoin)
                    and base_join.join_type == JoinType.INNER
                ):
                    proof_changed = (
                        _add_inner_base_join_key_proofs(base_join, proofs)
                        or proof_changed
                    )

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
                        _add_inner_join_key_proofs(join, proofs)
                    join_changed = True

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
                    _add_inner_base_join_key_proofs(base_join, proofs)
                join_changed = True

            if not proof_changed and not join_changed:
                break
            changed = changed or join_changed
        return changed, None
