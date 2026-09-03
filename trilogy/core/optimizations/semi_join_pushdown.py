"""Mirror a consumer's INNER join onto the aggregate CTE it restricts.

When a CTE grouped on key ``K`` is INNER-joined by a downstream consumer to a
relation that only some ``K`` values survive, the group is still computed over
every ``K`` in the base table and the surplus groups are discarded after the
fact. Hand-written SQL expresses these as correlated ``EXISTS`` and the engine
plans the restriction into the scan; the grouped-CTE shape hides it.

Restricting the *set of groups* is sound in a way that pushing the consumer's
predicate is not. A predicate only moves below an aggregate when it is
evaluable at the aggregate's input grain; a restriction on (a subset of) the
GROUP BY key removes whole groups and leaves every surviving group's value
byte-identical, whatever grain the consumer's own predicates live at. That is
the only thing this rule pushes.

The restriction is placed at the deepest aggregate that still groups on the
key, not merely the joined one: with stacked group-bys the bottom aggregate is
the one scanning the fact table.

The consumer often joins a projection or an enrichment rather than the
aggregate itself, so the search for that aggregate starts above it and walks
down through nodes that neither filter nor truncate. Row-preserving joins are
crossed on the way: dropping a target row can leave a dimension row unmatched,
so the join synthesizes a NULL-padded row that did not exist before. That is
still exact, because such a row carries the dropped row's own key, which is by
construction absent from the feeder, and the consumer's join rejects it. Any
aggregate met on the way down has to group on the key, otherwise removing rows
would move a surviving group's measure.

Only the feeder's key may not be nullable. The probe reads
``k in (select k from feeder)``, which drops a NULL k, and the join being
mirrored rejects that same row whether it rendered as ``=`` or as IS NOT
DISTINCT FROM, because a non-nullable feeder key has no NULL to pair with. A
nullable key on the target side is therefore no reason to skip; every key of
an enrichment built over full joins is nullable there.
"""

from __future__ import annotations

from trilogy.core.enums import JoinType, SourceType
from trilogy.core.models.build import BuildConcept
from trilogy.core.models.execute import (
    CTE,
    Join,
    RecursiveCTE,
    SemiJoinFilter,
    UnionCTE,
)
from trilogy.core.optimizations.base_optimization import MergedCTEMap, OptimizationRule
from trilogy.core.optimizations.utils import is_grouped_cte, is_sole_consumer

# A restriction may only ride below these; anything that reorders, pads or
# truncates rows changes which groups exist independently of the key.
UNSAFE_SOURCE_TYPES = {
    SourceType.WINDOW,
    SourceType.UNNEST,
    SourceType.RECURSIVE,
    SourceType.UNION,
}


def groups_on(cte: CTE, keys: list[BuildConcept]) -> bool:
    """True when every key is a grain component of `cte` (a GROUP BY column,
    never an aggregate output). Restricting on one of these can only delete
    whole groups."""
    components = set(cte.grain.components)
    for key in keys:
        if key.address not in components:
            return False
        if key.is_aggregate:
            return False
    return True


def restricts_rows(cte: CTE | UnionCTE) -> bool:
    """True when `cte`'s subtree carries a WHERE somewhere, so its key set is
    genuinely narrower than the aggregate's and the subquery buys something.

    A join is deliberately not evidence. Most joins here are FK lookups that
    every fact row matches, so mirroring one produces a semi-join against an
    unrestricted relation: all cost, no rows removed."""
    seen: set[str] = set()
    stack: list[CTE | UnionCTE] = [cte]
    while stack:
        node = stack.pop()
        if node.name in seen:
            continue
        seen.add(node.name)
        if node.condition is not None:
            return True
        stack.extend(node.dependency_nodes())
    return False


def nullable_in(cte: CTE | UnionCTE, key: BuildConcept) -> bool:
    """True when `key` can be NULL in `cte`. Applied to the feeder: a NULL there
    would make the mirrored join (null-safe until SimplifyNullSafeJoins proves
    otherwise) pair NULL to NULL where ``IN (SELECT ...)`` would not."""
    if key.is_nullable:
        return True
    if not isinstance(cte, CTE):
        return True
    return any(c.address == key.address for c in cte.nullable_concepts)


def reaches(node: CTE | UnionCTE, target: str) -> bool:
    seen: set[str] = set()
    stack: list[CTE | UnionCTE] = [node]
    while stack:
        current = stack.pop()
        if current.name == target:
            return True
        if current.name in seen:
            continue
        seen.add(current.name)
        stack.extend(current.dependency_nodes())
    return False


def exposed_key(cte: CTE | UnionCTE, key: BuildConcept) -> BuildConcept | None:
    return next(
        (c for c in cte.output_columns if c.address == key.address),
        None,
    )


def placement_target(
    aggregate: CTE,
    keys: list[BuildConcept],
    inverse_map: dict[str, list[CTE | UnionCTE]],
) -> CTE:
    """Walk down from `aggregate` to the deepest ancestor that still groups on
    every key and is consumed by nothing but this chain. Each step must be a
    sole-consumer link: restricting a shared parent would silently narrow the
    sibling that shares it."""
    target = aggregate
    while True:
        parents = target.dependency_nodes()
        if len(parents) != 1:
            return target
        parent = parents[0]
        if not isinstance(parent, CTE) or isinstance(parent, RecursiveCTE):
            return target
        if (
            parent.limit is not None
            or parent.source.source_type in UNSAFE_SOURCE_TYPES
            or not is_grouped_cte(parent)
            or not groups_on(parent, keys)
            or not is_sole_consumer(target, parent, inverse_map)
        ):
            return target
        target = parent


def descend_to_aggregate(
    node: CTE | UnionCTE,
    keys: list[BuildConcept],
    inverse_map: dict[str, list[CTE | UnionCTE]],
) -> CTE | None:
    """Walk from the joined CTE down to the aggregate that scans the fact table.

    Each step is a sole-consumer link through a node that neither filters nor
    truncates; an aggregate reached on the way has to group on the keys, or
    deleting rows would move a surviving group's measure.
    """
    seen: set[str] = set()
    while isinstance(node, CTE) and not isinstance(node, RecursiveCTE):
        if is_grouped_cte(node):
            return node if groups_on(node, keys) else None
        if node.name in seen:
            return None
        seen.add(node.name)
        if node.condition is not None or node.limit is not None:
            return None
        if node.source.source_type in UNSAFE_SOURCE_TYPES:
            return None
        below = [
            parent
            for parent in node.dependency_nodes()
            if isinstance(parent, CTE)
            and all(exposed_key(parent, key) is not None for key in keys)
            and is_sole_consumer(node, parent, inverse_map)
        ]
        if len(below) != 1:
            return None
        node = below[0]
    return None


class PushSemiJoinIntoAggregate(OptimizationRule):
    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        if not isinstance(cte, CTE) or isinstance(cte, RecursiveCTE):
            return False, None
        changed = False
        inlined = {p.name for p in cte.inlined_parents}
        for join in cte.joins:
            if not isinstance(join, Join) or join.jointype != JoinType.INNER:
                continue
            if not join.joinkey_pairs or join.condition is not None:
                continue
            if join.left_is_local:
                continue
            # Each pair names the node its LEFT key reads from; a feeder has to
            # be one relation, and one this consumer actually renders as a CTE.
            sources = {pair.cte.name for pair in join.joinkey_pairs}
            if len(sources) != 1:
                continue
            restrictor = join.joinkey_pairs[0].cte
            if restrictor.name in inlined or restrictor.name == cte.name:
                # An inlined datasource has no CTE to select from; the feeder
                # would have to be synthesized as a subquery over the raw table
                # plus the consumer's own predicates.
                continue
            if not any(p.name == restrictor.name for p in cte.dependency_nodes()):
                continue
            if self._apply(cte, join.right_cte, restrictor, join, inverse_map):
                changed = True
        return changed, None

    def _apply(
        self,
        consumer: CTE,
        aggregate: CTE | UnionCTE,
        restrictor: CTE | UnionCTE,
        join: Join,
        inverse_map: dict[str, list[CTE | UnionCTE]],
    ) -> bool:
        if not isinstance(aggregate, CTE) or isinstance(aggregate, RecursiveCTE):
            return False
        if aggregate.limit is not None:
            return False
        if aggregate.source.source_type in UNSAFE_SOURCE_TYPES:
            return False
        # Only this consumer may see the narrowed group set.
        if not is_sole_consumer(consumer, aggregate, inverse_map):
            return False
        # A feeder that reads the aggregate (directly or transitively) would
        # make the CTE graph cyclic.
        if reaches(restrictor, aggregate.name):
            return False
        # An aggregate that already reads from the feeder is restricted by it
        # by construction, so the probe is a tautology (and, once the descent
        # reaches the feeder itself, a CTE probing its own name).
        if reaches(aggregate, restrictor.name):
            return False
        if not restricts_rows(restrictor):
            return False

        keys: list[BuildConcept] = []
        members: list[BuildConcept] = []
        for pair in join.joinkey_pairs or []:
            # Pair sides are (left=restrictor, right=aggregate) by construction,
            # but both must be genuinely exposed to render.
            local = exposed_key(aggregate, pair.right)
            member = exposed_key(restrictor, pair.left)
            if local is None or member is None:
                return False
            # A hidden output is pruned from the feeder's SELECT list, so the
            # subquery could not name it.
            if member.address in restrictor.hidden_concepts:
                return False
            # A nullable feeder key is dropped rather than abandoning the whole
            # mirror: probing on fewer keys is weaker but still exact.
            if nullable_in(restrictor, member):
                continue
            keys.append(local)
            members.append(member)
        if not keys:
            return False

        host = descend_to_aggregate(aggregate, keys, inverse_map)
        if host is None:
            return False
        target = placement_target(host, keys, inverse_map)
        # Fire only into an aggregate grouping its base table whole. There is
        # no cardinality model here, so the mirror is only taken where the
        # asymmetry is structural: an unfiltered target scans everything while
        # the feeder is provably narrowed. Against an already-filtered target
        # the feeder's hash build can cost more than it saves.
        if restricts_rows(target):
            return False
        # `target` may sit below `aggregate`; re-resolve the probe against it.
        probes = [exposed_key(target, key) for key in keys]
        if any(probe is None for probe in probes):
            return False
        semi = SemiJoinFilter(
            feeder=restrictor.name,
            keys=[
                (probe, member)
                for probe, member in zip(probes, members)
                if probe is not None
            ],
        )
        if any(
            existing.identity == semi.identity for existing in target.semi_join_filters
        ):
            return False
        target.semi_join_filters.append(semi)
        self.log(
            f"Mirrored {consumer.name}'s INNER join onto {target.name} as a "
            f"semi-join against {restrictor.name} on "
            f"{[k.address for k in keys]}"
        )
        return True
