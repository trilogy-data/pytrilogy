"""A merge narrows its joins on the NULL rejections of the filters above it.

A WHERE or HAVING the planner places on a node above a merge rejects the
merge's padded rows just as the merge's own WHERE would, wherever it reads a
column the merge supplies alone. The optimizer found these only after pushing
the filter down (`UpgradeJoinOnGuards`); here the planner hands each merge
what every one of its consumers rejects.

A proof passes a node only where rows pass it unchanged: a plain select, a
merge, or a group that is not a grouping-set pass. Through a grouping only a
key passes, never an aggregate: a rejected key's group holds only the rows
with that key NULL, while dropping a padded row changes the aggregates of the
group it joined. A window, unnest, limit, union or rowset boundary stops it.
Nodes that render as one relation and every consumer of it count as one: it
keeps what all of its consumers reject.

A consumer's own join rejects too (`join_rejections`): a NULL never matches
``=``, so a parent row NULL on a column the consumer joins on, on a side the
join does not preserve, reaches nothing above it."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from trilogy.core.enums import Derivation, JoinType
from trilogy.core.models.build import BoolExpr, BuildConcept
from trilogy.core.models.execute import (
    ConceptPair,
    Join,
    UnnestJoin,
    coalesced_key_groups,
    pair_matches_nulls,
)
from trilogy.core.processing.aggregate_rollup import pending_rollups
from trilogy.core.processing.condition_utility import (
    gather_non_null_proofs,
    gather_or_groups,
    opaque_binding_addresses,
)
from trilogy.core.processing.grain_utility import non_null_proofs
from trilogy.core.processing.join_resolution import OUTER_JOIN_TYPES
from trilogy.core.processing.nodes import (
    GroupNode,
    MergeNode,
    SelectNode,
    StrategyNode,
)

OrGroups = tuple[tuple[frozenset[str], ...], ...]


@dataclass(frozen=True)
class ConsumerProofs:
    """The harvests a merge reads its own WHERE into (`JoinProofs`), for the
    columns its consumers read off it alone; ``keys``, the columns a
    consumer's join equality reads (`join_rejections`)."""

    proofs: frozenset[str] = frozenset()
    side_proofs: frozenset[str] = frozenset()
    or_groups: OrGroups = ()
    keys: frozenset[str] = frozenset()

    @classmethod
    def of(cls, condition: BoolExpr | None) -> ConsumerProofs:
        if condition is None:
            return cls()
        groups = gather_or_groups(condition)
        return cls(
            frozenset(non_null_proofs(condition)),
            frozenset(gather_non_null_proofs(condition)),
            tuple(tuple(frozenset(d) for d in group) for group in groups),
        )

    def __bool__(self) -> bool:
        return bool(self.proofs or self.side_proofs or self.or_groups or self.keys)

    def __or__(self, other: ConsumerProofs) -> ConsumerProofs:
        extra = tuple(g for g in other.or_groups if g not in self.or_groups)
        return ConsumerProofs(
            self.proofs | other.proofs,
            self.side_proofs | other.side_proofs,
            self.or_groups + extra,
            self.keys | other.keys,
        )

    def __and__(self, other: ConsumerProofs) -> ConsumerProofs:
        return ConsumerProofs(
            self.proofs & other.proofs,
            self.side_proofs & other.side_proofs,
            tuple(g for g in self.or_groups if g in other.or_groups),
            self.keys & other.keys,
        )

    def restrict(self, addresses: set[str]) -> ConsumerProofs:
        """An OR group survives only if each disjunct still proves a column."""
        groups = tuple(tuple(d & addresses for d in g) for g in self.or_groups)
        return ConsumerProofs(
            self.proofs & addresses,
            self.side_proofs & addresses,
            tuple(g for g in groups if all(g)),
            self.keys & addresses,
        )


def _passes_rows(node: StrategyNode) -> bool:
    if node.limit is not None:
        return False
    if type(node) is MergeNode:
        return node.node_joins is None
    if type(node) is GroupNode:
        return node.grouping_pass() is None
    return type(node) is SelectNode


def _outer_joins(node: MergeNode) -> bool:
    return any(
        isinstance(join, Join) and join.join_type in OUTER_JOIN_TYPES
        for join in node.resolve().joins
    )


def row_reads(node: StrategyNode, parent: StrategyNode) -> set[str]:
    """Columns ``node`` reads off ``parent``'s rows and no other source; an
    aggregate only where ``node`` does not group, and never a partial one the
    parent still holds for a regroup to roll up.
    A count ``node`` zero-fills is 0, not NULL, on the rows a join padded."""
    resolved = node.resolve()
    source = parent.resolve()
    grouped = resolved.group_required
    rolled = {c.address for c in pending_rollups(source.rollup_concepts, source.grain)}
    return {
        c.address
        for c in source.output_concepts
        if c.address not in resolved.zero_filled | rolled
        and (sources := resolved.source_map.get(c.address))
        and all(
            not isinstance(s, UnnestJoin) and s.identifier == source.identifier
            for s in sources
        )
        and not (grouped and c.derivation == Derivation.AGGREGATE)
    }


def _pair_source(pair: ConceptPair) -> str:
    return pair.node.identifier


def join_rejections(node: StrategyNode, parent: StrategyNode) -> set[str]:
    """Columns of ``parent`` an equality in ``node``'s own joins reads on a
    side whose unmatched rows the join drops: both sides of an INNER, the
    right of a LEFT, the left of a RIGHT. A NULL never matches ``=``, so a
    parent row NULL there reaches nothing above ``node``. Null-safe pairs
    match NULLs, a coalesced left rejects no one side, and an opaque binding
    is not NULL on the parent's padded rows."""
    source = parent.resolve()
    ident = source.identifier
    opaque = opaque_binding_addresses(source)
    out: set[str] = set()
    for raw in node.resolve().joins:
        if not isinstance(raw, Join):
            continue
        join = raw.bound_keys()
        right = join.join_type in (JoinType.INNER, JoinType.LEFT_OUTER)
        left = join.join_type in (JoinType.INNER, JoinType.RIGHT_OUTER)
        for pairs in coalesced_key_groups(join.pairs, _pair_source):
            if any(pair_matches_nulls(p, join.modifiers) for p in pairs):
                continue
            if right and join.right.identifier == ident:
                out.add(pairs[0].right.address)
            if left and len(pairs) == 1 and pairs[0].node.identifier == ident:
                out.add(pairs[0].left.address)
    return (out - opaque) & {c.address for c in source.output_concepts}


def _walk(root: StrategyNode) -> list[StrategyNode]:
    """Every node once, each after all of its parents."""
    order: list[StrategyNode] = []
    seen: set[int] = set()
    stack: list[tuple[StrategyNode, bool]] = [(root, False)]
    while stack:
        node, done = stack.pop()
        if done:
            order.append(node)
        elif id(node) not in seen:
            seen.add(id(node))
            stack.append((node, True))
            stack.extend((p, False) for p in node.parents)
    return order


def _consumers_first(nodes: list[StrategyNode]) -> list[str] | None:
    """Relation identifiers, each after every consumer of it; None if two
    nodes of one relation read each other."""
    consumers: dict[str, set[str]] = defaultdict(set)
    for node in nodes:
        for parent in node.parents:
            consumers[parent.resolve().identifier].add(node.resolve().identifier)
    idents = list(dict.fromkeys(n.resolve().identifier for n in reversed(nodes)))
    order: list[str] = []
    placed: set[str] = set()
    pending = list(idents)
    while pending:
        ready = [i for i in pending if consumers[i] <= placed]
        if not ready:
            return None
        order.extend(ready)
        placed.update(ready)
        pending = [i for i in pending if i not in placed]
    return order


def narrow_for_consumers(root: StrategyNode) -> None:
    """Hand each outer-joining merge under ``root`` what its consumers reject,
    and re-resolve the plan above the merges that took it.

    A consumer's join type is not fixed: re-resolved over a narrowed parent it
    may preserve a side it dropped before (a merge whose other side lost its
    padding keeps this side's NULL-keyed rows). So the join channel is checked
    against the plan it built, and dropped wholesale if any consumer stopped
    rejecting what a merge took from it."""
    nodes = _walk(root)
    order = _consumers_first(nodes)
    if order is None:
        return
    by_ident: dict[str, list[StrategyNode]] = defaultdict(list)
    for node in nodes:
        by_ident[node.resolve().identifier].append(node)
    # a re-resolved node's identifier can change: relations stay keyed as found
    ident_of = {id(node): ident for ident, group in by_ident.items() for node in group}
    saved = [_State.of(node) for node in nodes]
    narrowed: set[int] = set()
    _propagate(order, by_ident, ident_of, True, narrowed)
    _rebuild(nodes, narrowed, saved)
    if _keys_hold(order, by_ident, ident_of):
        return
    for state in saved:
        state.restore()
    narrowed = set()
    _propagate(order, by_ident, ident_of, False, narrowed)
    _rebuild(nodes, narrowed, saved)


@dataclass
class _State:
    node: StrategyNode
    nullable: list[BuildConcept]
    resolved_nullable: set[str]
    proofs: ConsumerProofs | None
    partials: set[str] | None

    @classmethod
    def of(cls, node: StrategyNode) -> _State:
        merge = node if isinstance(node, MergeNode) else None
        return cls(
            node,
            list(node.nullable_concepts),
            _nullable(node),
            merge.consumer_proofs if merge else None,
            merge.unfiltered_partials if merge else None,
        )

    def restore(self) -> None:
        self.node.nullable_concepts = list(self.nullable)
        if isinstance(self.node, MergeNode):
            self.node.consumer_proofs = self.proofs
            self.node.unfiltered_partials = self.partials
        self.node.rebuild_cache()


def _propagate(
    order: list[str],
    by_ident: dict[str, list[StrategyNode]],
    ident_of: dict[int, str],
    keys: bool,
    narrowed: set[int] | None,
) -> dict[str, ConsumerProofs]:
    """What every consumer of each relation rejects, consumers first. With
    ``narrowed``, each outer-joining merge takes it and is re-resolved before
    its own joins are read: narrowed, they reject more of its parents' rows."""
    received: dict[str, ConsumerProofs] = {}
    for ident in order:
        here = received.get(ident, ConsumerProofs())
        for node in by_ident[ident]:
            passed = _passes_rows(node)
            if (
                narrowed is not None
                and here
                and passed
                and isinstance(node, MergeNode)
                and _outer_joins(node)
            ):
                node.consumer_proofs = here
                node.unfiltered_partials = {
                    c.address for c in node.resolve().partial_concepts
                }
                narrowed.add(id(node))
                node.rebuild_cache()
            forward = here | ConsumerProofs.of(node.conditions)
            for parent in dict.fromkeys(node.parents):
                given = (
                    forward.restrict(row_reads(node, parent))
                    if passed
                    else ConsumerProofs()
                )
                if keys:
                    given |= ConsumerProofs(
                        keys=frozenset(join_rejections(node, parent))
                    )
                pident = ident_of[id(parent)]
                prior = received.get(pident)
                received[pident] = given if prior is None else prior & given
    return received


def _keys_hold(
    order: list[str],
    by_ident: dict[str, list[StrategyNode]],
    ident_of: dict[int, str],
) -> bool:
    """Every narrowed merge's consumers, as rebuilt, still reject its keys."""
    received = _propagate(order, by_ident, ident_of, True, None)
    return all(
        node.consumer_proofs.keys <= received.get(ident, ConsumerProofs()).keys
        for ident, group in by_ident.items()
        for node in group
        if isinstance(node, MergeNode) and node.consumer_proofs is not None
    )


def _nullable(node: StrategyNode) -> set[str]:
    return {c.address for c in node.resolve().nullable_concepts}


def _rebuild(
    nodes: list[StrategyNode], narrowed: set[int], saved: list[_State]
) -> None:
    """Re-resolve each narrowed merge and everything reading it. A node keeps
    the nullability its first resolve found (`StrategyNode.resolve`), so the
    padding a narrowing removed is cleared first: on the merge, what no parent
    reports; on a reader, what its parents stopped reporting."""
    before = {id(state.node): state.resolved_nullable for state in saved}
    dropped: dict[int, set[str]] = {}
    for node in nodes:
        changed = [p for p in node.parents if id(p) in dropped]
        if id(node) not in narrowed and not changed:
            continue
        stale = before[id(node)] - set().union(*(_nullable(p) for p in node.parents))
        if id(node) not in narrowed:
            stale &= set().union(*(dropped[id(p)] for p in changed))
        node.nullable_concepts = [
            c for c in node.nullable_concepts if c.address not in stale
        ]
        node.rebuild_cache()
        dropped[id(node)] = before[id(node)] - _nullable(node)
