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
from trilogy.core.models.execute import (
    CTE,
    Join,
    UnionCTE,
    coalesced_key_groups,
    pair_matches_nulls,
    preserved_key_pairs,
)
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import (
    SENSITIVE_DERIVATIONS,
    accumulated_left_ctes,
    cte_source_keys,
    join_padded_ctes,
    output_addresses,
    seed_ctes,
    zero_filled_reads,
)
from trilogy.core.processing.condition_utility import (
    gather_non_null_proofs,
    gather_or_groups,
    opaque_binding_addresses,
    partial_addresses,
)
from trilogy.core.processing.join_resolution import OUTER_JOIN_TYPES


@dataclass
class _ProofState:
    direct: set[str]
    cte_keys: set[tuple[str, str]] = field(default_factory=set)
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

    def proves_cte_key(self, cte: CTE | UnionCTE, address: str) -> bool:
        return address in self.direct or (cte.name, address) in self.cte_keys

    def proves_cte_present(self, cte: CTE | UnionCTE, addresses: set[str]) -> bool:
        return any((cte.name, address) in self.cte_keys for address in addresses)

    def add_cte_key(self, cte: CTE | UnionCTE, address: str) -> bool:
        key = (cte.name, address)
        if key in self.cte_keys:
            return False
        self.cte_keys.add(key)
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
) -> JoinType | None:
    """Pick the strictest join that still produces the same surviving rows."""
    current = join.join_type
    if current not in OUTER_JOIN_TYPES:
        return None

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
    right_only = right_only - right_block
    left_only = left_only - left_block

    # A non-null proof on a concept whose column binding is structurally
    # non-null (a CASE/raw expression) doesn't prove its side matched.
    right_only = right_only - opaque_binding_addresses(join.right)
    left_opaque = {a for lc in left_ctes for a in opaque_binding_addresses(lc)}
    left_only = left_only - left_opaque

    def proves_left_key(c: CTE | UnionCTE, address: str) -> bool:
        if address in left_block:
            return (c.name, address) in proofs.cte_keys
        return proofs.proves_cte_key(c, address)

    def proves_right_key(address: str) -> bool:
        if address in right_block:
            return (join.right.name, address) in proofs.cte_keys
        return proofs.proves_cte_key(join.right, address)

    # A side is forced present when the WHERE references a concept that only
    # exists on it, or when every join key is proven non-null for the specific
    # CTE that supplies it. Either rules out NULL-padded rows on that side.
    left_forced = (
        proofs.direct_intersects(left_only)
        or proofs.side_forced_by_or(left_only)
        or any(
            proofs.proves_cte_present(left_cte, output_addresses(left_cte))
            for left_cte in left_ctes
        )
        or (bool(pairs) and all(proves_left_key(p.node, p.left.address) for p in pairs))
    )
    right_forced = (
        proofs.direct_intersects(right_only)
        or proofs.side_forced_by_or(right_only)
        or proofs.proves_cte_present(join.right, right_all)
        or (bool(pairs) and all(proves_right_key(p.right.address) for p in pairs))
    )

    if current == JoinType.FULL:
        if left_forced and right_forced:
            return JoinType.INNER
        # A forced left drops left-unmatched rows, the same set LEFT_OUTER
        # drops. Mirror for the right.
        if left_forced:
            return JoinType.LEFT_OUTER
        if right_forced:
            return JoinType.RIGHT_OUTER
        return None

    if current == JoinType.LEFT_OUTER:
        if right_forced:
            return JoinType.INNER
        return None

    if current == JoinType.RIGHT_OUTER:
        if left_forced:
            return JoinType.INNER
        return None

    return None


def _add_inner_join_key_proofs(join: Join, proofs: _ProofState) -> bool:
    """Propagate only key non-nullness proven by rendered INNER predicates."""
    changed = False
    for pairs in coalesced_key_groups(join.pairs or []):
        left_ctes = {p.node.name for p in pairs}
        if len(left_ctes) > 1:
            # Renders as COALESCE(left1, left2, ...) = right, which proves the
            # right key but no individual left key.
            changed = proofs.add_cte_key(join.right, pairs[0].right.address) or changed
            continue

        pair = pairs[0]
        if pair_matches_nulls(pair, join.modifiers):
            continue
        changed = proofs.add_cte_key(pair.node, pair.left.address) or changed
        changed = proofs.add_cte_key(join.right, pair.right.address) or changed
    return changed


def _sensitive_outputs(cte: CTE) -> bool:
    return any(
        c.derivation in SENSITIVE_DERIVATIONS for c in cte.source.output_concepts
    )


def _renders_exclusively_from(
    consumer: CTE, address: str, producer_keys: set[str]
) -> bool:
    """The consumer renders ``address`` as a plain reference to one producer
    column. A multi-source entry renders as COALESCE, which masks a one-sided
    NULL, so it never carries a rejection back to either producer."""
    sources = set(consumer.source_map.get(address, ()))
    return bool(sources) and len(sources) == 1 and sources <= producer_keys


def _inner_pair_rejections(consumer: CTE, producer_keys: set[str]) -> set[str]:
    """Producer output addresses a rendered equality in the consumer forces
    non-null: a NULL key never matches plain ``=``, so the producer row
    contributes nothing to the consumer. Only a side whose unmatched rows the
    join DISCARDS is provable: both sides of an INNER, the right of a
    LEFT_OUTER, the left of a RIGHT_OUTER; a preserved side's rows survive
    unmatched, key NULL or not. Null-safe pairs match NULLs and prove
    nothing; a multi-left-source group renders COALESCE on the left, which
    still rejects a NULL right key but no individual left key."""
    out: set[str] = set()
    for join in consumer.joins or []:
        if not isinstance(join, Join):
            continue
        harvest_right = join.join_type in (JoinType.INNER, JoinType.LEFT_OUTER)
        harvest_left = join.join_type in (JoinType.INNER, JoinType.RIGHT_OUTER)
        if not harvest_right and not harvest_left:
            continue
        for pairs in coalesced_key_groups(join.pairs or []):
            if any(pair_matches_nulls(p, join.modifiers) for p in pairs):
                continue
            first = pairs[0]
            if harvest_right and cte_source_keys(join.right) & producer_keys:
                out.add(first.right.address)
            left_ctes = {p.node.name for p in pairs}
            if (
                harvest_left
                and len(left_ctes) == 1
                and cte_source_keys(first.node) & producer_keys
            ):
                out.add(first.left.address)
    return out


def _external_forced_map(
    ctes: list[CTE | UnionCTE],
    inverse_map: dict[str, list[CTE | UnionCTE]],
) -> dict[str, set[str]]:
    """Per producer CTE, output addresses every consumer forces non-null in
    any row it lets contribute to its own output: the cross-CTE extension of
    the same-CTE WHERE proofs, for a null-rejecting filter that lives several
    CTEs downstream of the outer join that pads the column.

    Sound as a join-reduction proof at the producer because a producer row
    NULL there is, at every consumption site, either dropped by the consumer's
    WHERE/INNER equality or never matched at all, so emitting it is
    output-invisible. Channels per consumer:

    * its condition's non-null proofs, when the address renders as a plain
      single-source reference to this producer (a COALESCE masks the NULL);
    * rendered INNER-join equalities on the producer's columns;
    * transitively, its own forced set for plain pass-through projections,
      gated to group keys when the consumer aggregates (a NULL group key
      isolates exactly the padded rows; an aggregate output does not).

    A consumer that reads the producer existentially, a UnionCTE, or a
    window-computing consumer is opaque and empties the producer's set: every
    consumer must reject, or none may."""
    forced: dict[str, set[str]] = {c.name: set() for c in ctes}
    static: dict[tuple[str, str], set[str] | None] = {}
    consumer_proofs: dict[str, set[str]] = {}
    for producer in ctes:
        if not isinstance(producer, CTE):
            continue
        producer_keys = cte_source_keys(producer)
        outputs = output_addresses(producer)
        for consumer in inverse_map.get(producer.name, []):
            key = (consumer.name, producer.name)
            if not isinstance(consumer, CTE):
                static[key] = None
                continue
            existential = {
                s for vals in consumer.existence_source_map.values() for s in vals
            }
            if existential & producer_keys:
                static[key] = None
                continue
            contribution: set[str] = set()
            if consumer.condition is not None and not _sensitive_outputs(consumer):
                if consumer.name not in consumer_proofs:
                    consumer_proofs[consumer.name] = gather_non_null_proofs(
                        consumer.condition
                    ) - zero_filled_reads(consumer, consumer.condition)
                contribution |= {
                    a
                    for a in consumer_proofs[consumer.name]
                    if _renders_exclusively_from(consumer, a, producer_keys)
                }
            contribution |= _inner_pair_rejections(consumer, producer_keys)
            static[key] = contribution & outputs
    while True:
        changed = False
        for producer in ctes:
            if not isinstance(producer, CTE):
                continue
            consumers = inverse_map.get(producer.name, [])
            if not consumers:
                continue
            producer_keys = cte_source_keys(producer)
            outputs = output_addresses(producer)
            new: set[str] | None = None
            for consumer in consumers:
                entry = static.get((consumer.name, producer.name))
                if entry is None or not isinstance(consumer, CTE):
                    new = set()
                    break
                transferred: set[str] = set()
                upstream = forced.get(consumer.name, set())
                if upstream and not _sensitive_outputs(consumer):
                    group_keys = (
                        set(consumer.grain.components)
                        if consumer.group_to_grain and consumer.grain
                        else None
                    )
                    transferred = {
                        a
                        for a in upstream
                        if (
                            not consumer.group_to_grain
                            or (group_keys is not None and a in group_keys)
                        )
                        and _renders_exclusively_from(consumer, a, producer_keys)
                    }
                contribution = (entry | transferred) & outputs
                new = contribution if new is None else (new & contribution)
                if not new:
                    break
            new = new or set()
            if new != forced[producer.name]:
                forced[producer.name] = new
                changed = True
        if not changed:
            return forced


class UpgradeJoinOnGuards(OptimizationRule):
    """Upgrade FULL/LEFT_OUTER/RIGHT_OUTER joins to a stricter form when the
    enclosing WHERE rejects the unmatched rows the OUTER join was preserving.

    ``left_only=True`` is the early pass before UnionDimPushdown: it only
    makes LEFT joins INNER, on the CTE's own WHERE, so the dim joins that
    rule matches are INNER.
    """

    def __init__(self, left_only: bool = False) -> None:
        super().__init__()
        self.left_only = left_only
        self._forced_key: int | None = None
        self._forced: dict[str, set[str]] = {}

    def _external_proofs(
        self, cte: CTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> set[str]:
        """Consumer-forced non-null addresses for this CTE, gated to addresses
        whose rejection maps back onto raw join columns here: a plain
        single-source projection (a COALESCE output is NULL only when every
        side is), and a group key when this CTE aggregates (the NULL-keyed
        group holds exactly the padded rows). Computed once per optimizer
        sweep; the sweep loops to fixpoint, so upgrades this pass enables feed
        the next."""
        if self.left_only:
            return set()
        # A consumer's rejection applies post-limit; using it to drop rows
        # inside a row-limited CTE changes which rows fill the limit. The
        # CTE's own pre-limit condition remains a valid proof source.
        if cte.limit is not None:
            return set()
        if self._forced_key != id(inverse_map):
            nodes: dict[str, CTE | UnionCTE] = {cte.name: cte}
            for consumers in inverse_map.values():
                for consumer in consumers:
                    nodes[consumer.name] = consumer
                    for parent in consumer.dependency_nodes():
                        nodes.setdefault(parent.name, parent)
            self._forced = _external_forced_map(list(nodes.values()), inverse_map)
            self._forced_key = id(inverse_map)
        external = self._forced.get(cte.name, set())
        if not external:
            return set()
        if cte.group_to_grain:
            group_keys = set(cte.grain.components) if cte.grain else set()
            external = external & group_keys
        return {a for a in external if len(set(cte.source_map.get(a, ()))) == 1}

    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE):
            return False, None
        upgradable = {JoinType.LEFT_OUTER} if self.left_only else set(OUTER_JOIN_TYPES)
        if not any(
            isinstance(j, Join) and j.join_type in upgradable for j in cte.joins or []
        ):
            return False, None

        direct_proofs = (
            gather_non_null_proofs(cte.condition) if cte.condition else set()
        )
        direct_proofs = direct_proofs | self._external_proofs(cte, inverse_map)
        or_groups = gather_or_groups(cte.condition) if cte.condition else []
        # `count = 0` over a COUNT this CTE coalesces to 0 on the padded rows
        # accepts those rows: it proves nothing about the side that padded them
        zero = zero_filled_reads(cte, cte.condition)
        if zero:
            direct_proofs -= zero
            or_groups = [[d - zero for d in group] for group in or_groups]
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
            for join in cte.joins or []:
                if isinstance(join, Join) and join.join_type == JoinType.INNER:
                    proof_changed = (
                        _add_inner_join_key_proofs(join, proofs) or proof_changed
                    )

            for idx, join in enumerate(cte.joins or []):
                if not isinstance(join, Join) or join.join_type not in upgradable:
                    continue
                target = _downgrade(cte, idx, join, proofs)
                if target is None or target == join.join_type:
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
                    _add_inner_join_key_proofs(join, proofs)
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
