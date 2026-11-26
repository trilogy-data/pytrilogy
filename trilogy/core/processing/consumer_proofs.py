"""A merge narrows its joins on the NULL rejections of the filters above it.

A WHERE or HAVING the planner places on a node above a merge rejects the
merge's padded rows just as the merge's own WHERE would, wherever it reads a
column the merge supplies alone. The optimizer found these only after pushing
the filter down (`UpgradeJoinOnGuards`); here the planner hands each merge
what every one of its consumers rejects.

A proof passes a node only where rows pass it unchanged: a plain select or
filter, a merge (a MULTISELECT align's included), or a group that is not a
grouping-set pass. Through a grouping only a key passes, never an aggregate: a
rejected key's group holds only the rows with that key NULL, while dropping a
padded row changes the aggregates of the group it joined. A node's own WHERE
reads its parent's rows before any group, so it rejects on an aggregate the
parent computed too (`where_reads`). A window, unnest, limit, union or rowset
boundary stops it.
Nodes that render as one relation and every consumer of it count as one: it
keeps what all of its consumers reject, and passes a parent what any of its
nodes reads off it. A column a node computes per row from
its parent's (``x <- y is not null``) passes what a proof drawn through its
lineage rejects on those (`computed_inputs`); a proof on a value the node
computes itself reaches its NULL-strict inputs (`lineage_proofs`).

A group whose every aggregate counts only the rows a filter passes
(``count(x ? p)``) passes that filter's proofs on as a WHERE where its
consumers reject a group that counted none (`group_filter`): the rows ``p``
fails change no group that reaches them.

A consumer's own join rejects too (`join_rejections`): a NULL never matches
``=``, so a parent row NULL on a column the consumer joins on, on a side the
join does not preserve, reaches nothing above it."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace

from trilogy.core.enums import Derivation, FunctionType, JoinType
from trilogy.core.models.build import (
    BoolExpr,
    BuildAggregateWrapper,
    BuildConcept,
    BuildDatasource,
    BuildFilterItem,
    BuildFunction,
    BuildParenthetical,
)
from trilogy.core.models.execute import (
    ConceptPair,
    Join,
    QueryDatasource,
    SourceJoin,
    UnnestJoin,
    coalesced_key_groups,
    pair_matches_nulls,
)
from trilogy.core.processing.aggregate_rollup import pending_rollups
from trilogy.core.processing.condition_utility import (
    concepts_implied_non_null,
    gather_non_null_proofs,
    gather_or_groups,
    opaque_binding_addresses,
    zero_rejections,
)
from trilogy.core.processing.grain_utility import non_null_proofs
from trilogy.core.processing.join_resolution import (
    OUTER_JOIN_TYPES,
    preserved_sources,
)
from trilogy.core.processing.nodes import (
    FilterNode,
    GroupNode,
    MergeNode,
    SelectNode,
    StrategyNode,
)
from trilogy.core.processing.utility import (
    PADS_LEFT_JOIN_TYPES,
    PADS_RIGHT_JOIN_TYPES,
    JoinSequence,
    join_sequence,
    joined_right_ids,
)

OrGroups = tuple[tuple[frozenset[str], ...], ...]

NULL_IGNORING_AGGREGATES = {
    FunctionType.AVG,
    FunctionType.COUNT,
    FunctionType.MAX,
    FunctionType.MIN,
    FunctionType.SUM,
}


@dataclass(frozen=True)
class ConsumerProofs:
    """The harvests a merge reads its own WHERE into (`JoinProofs`), for the
    columns its consumers read off it alone; ``keys``, the columns a
    consumer's join equality reads (`join_rejections`); ``nonzero``, the
    columns it rejects at 0 (`zero_rejections`)."""

    proofs: frozenset[str] = frozenset()
    side_proofs: frozenset[str] = frozenset()
    or_groups: OrGroups = ()
    keys: frozenset[str] = frozenset()
    nonzero: frozenset[str] = frozenset()

    @classmethod
    def of(cls, condition: BoolExpr | None) -> ConsumerProofs:
        if condition is None:
            return cls()
        groups = gather_or_groups(condition)
        return cls(
            frozenset(non_null_proofs(condition)),
            frozenset(gather_non_null_proofs(condition)),
            tuple(tuple(frozenset(d) for d in group) for group in groups),
            nonzero=frozenset(zero_rejections(condition)),
        )

    def __bool__(self) -> bool:
        return bool(
            self.proofs
            or self.side_proofs
            or self.or_groups
            or self.keys
            or self.nonzero
        )

    def __or__(self, other: ConsumerProofs) -> ConsumerProofs:
        extra = tuple(g for g in other.or_groups if g not in self.or_groups)
        return ConsumerProofs(
            self.proofs | other.proofs,
            self.side_proofs | other.side_proofs,
            self.or_groups + extra,
            self.keys | other.keys,
            self.nonzero | other.nonzero,
        )

    def __and__(self, other: ConsumerProofs) -> ConsumerProofs:
        return ConsumerProofs(
            self.proofs & other.proofs,
            self.side_proofs & other.side_proofs,
            tuple(g for g in self.or_groups if g in other.or_groups),
            self.keys & other.keys,
            self.nonzero & other.nonzero,
        )

    def restrict(self, addresses: set[str]) -> ConsumerProofs:
        """An OR group survives only if each disjunct still proves a column."""
        groups = tuple(tuple(d & addresses for d in g) for g in self.or_groups)
        return ConsumerProofs(
            self.proofs & addresses,
            self.side_proofs & addresses,
            tuple(g for g in groups if all(g)),
            self.keys & addresses,
            self.nonzero & addresses,
        )


def _passes_rows(node: StrategyNode) -> bool:
    if node.limit is not None:
        return False
    if type(node) is GroupNode:
        return node.grouping_pass() is None
    # a MULTISELECT align's merge too: its HAVING reads the aligned rows
    return type(node) in (MergeNode, SelectNode, FilterNode)


def _outer_joins(node: MergeNode) -> bool:
    return any(
        isinstance(join, Join) and join.join_type in OUTER_JOIN_TYPES
        for join in node.resolve().joins
    )


def row_reads(node: StrategyNode, parent: StrategyNode) -> set[str]:
    """Columns ``node`` reads off ``parent``'s rows and no other source; an
    aggregate only where ``node`` does not group or is a merge (whose group
    only dedupes, so it reads the aggregate as a key), and never a partial one
    the parent still holds for a regroup to roll up.
    A count ``node`` zero-fills is 0, not NULL, on the rows a join padded."""
    resolved = node.resolve()
    grouped = resolved.group_required and type(node) is not MergeNode
    return _reads(node, parent, grouped)


def where_reads(node: StrategyNode, parent: StrategyNode) -> set[str]:
    """Columns ``node``'s own WHERE reads off ``parent``'s rows: before any
    group, so an aggregate ``parent`` computed is a value of each row."""
    if type(node) is GroupNode or node.conditions is None:
        return set()
    return _reads(node, parent, False)


def _reads(node: StrategyNode, parent: StrategyNode, grouped: bool) -> set[str]:
    resolved = node.resolve()
    source = parent.resolve()
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


def computed_inputs(node: StrategyNode, reads: set[str]) -> set[str]:
    """Columns ``node`` reads to compute a per-row value in ``reads``, or that
    the parent it passes such a value through alone does. A consumer's proof
    drawn through that value's lineage (``x = true`` over ``x <- y is not
    null``) names them, and holds of the rows they come from."""
    resolved = node.resolve()
    known = {c.address: c for c in resolved.output_concepts}
    out: set[str] = set()
    todo = [a for a in reads if not resolved.source_map.get(a)]
    while todo:
        concept = known.get(todo.pop())
        if concept is None or concept.derivation != Derivation.BASIC:
            continue
        for arg in concept.lineage.concept_arguments if concept.lineage else []:
            if arg.address in out:
                continue
            out.add(arg.address)
            if not resolved.source_map.get(arg.address):
                known.setdefault(arg.address, arg)
                todo.append(arg.address)
    computed = {
        a for a in reads if (c := known.get(a)) and c.derivation == Derivation.BASIC
    }
    if not computed or not _passes_rows(node):
        return out
    for parent in dict.fromkeys(node.parents):
        if passed := computed & row_reads(node, parent):
            out |= computed_inputs(parent, passed)
    return out


def lineage_proofs(node: StrategyNode, addresses: frozenset[str]) -> frozenset[str]:
    """Add what a proven value ``node`` computes per row proves of its
    inputs: non-null, its NULL-strict arguments are too (``v <- c / nullif(m
    - 5, 0)`` proves ``m``). The optimizer read these off the CTE that
    renders the value (`_lineage_proofs`)."""
    resolved = node.resolve()
    known = {c.address: c for c in resolved.output_concepts}
    out = set(addresses)
    todo = [a for a in addresses if not resolved.source_map.get(a)]
    while todo:
        concept = known.get(todo.pop())
        if concept is None or concept.derivation != Derivation.BASIC:
            continue
        lineage = concept.lineage
        if _evaluated_at_a_grain(lineage):
            continue
        if isinstance(lineage, BuildFunction | BuildParenthetical):
            for arg in lineage.concept_arguments:
                known.setdefault(arg.address, arg)
        for address in concepts_implied_non_null(lineage) - out:
            out.add(address)
            if not resolved.source_map.get(address):
                todo.append(address)
    return frozenset(out)


def _evaluated_at_a_grain(value: object) -> bool:
    """A grouped value is read at its anchors' grain, not off the row
    computing it: its anchor is not this row's column. A pinned value is
    (`grain_pin`): it has no value off its anchors' rows, as a property has
    none without its key, so it is NULL-strict on them."""
    if isinstance(value, BuildParenthetical):
        return _evaluated_at_a_grain(value.content)
    if not isinstance(value, BuildFunction):
        return False
    if value.operator == FunctionType.GROUP:
        return True
    return any(_evaluated_at_a_grain(arg) for arg in value.arguments)


def _through_lineage(node: StrategyNode, rejected: ConsumerProofs) -> ConsumerProofs:
    return replace(
        rejected,
        proofs=lineage_proofs(node, rejected.proofs),
        side_proofs=lineage_proofs(node, rejected.side_proofs),
    )


def filtered_aggregate(
    concept: BuildConcept,
) -> tuple[BuildConcept, BuildFilterItem] | None:
    """The filtered argument of an aggregate over one (``count(x ? p)``)."""
    lineage = concept.lineage
    if not isinstance(lineage, BuildAggregateWrapper):
        return None
    if len(lineage.function.arguments) != 1:
        return None
    argument = lineage.function.arguments[0]
    if not isinstance(argument, BuildConcept) or not isinstance(
        argument.lineage, BuildFilterItem
    ):
        return None
    if not isinstance(argument.lineage.content, BuildConcept):
        return None
    return argument, argument.lineage


def _rejects_empty(concept: BuildConcept, rejected: ConsumerProofs) -> bool:
    assert isinstance(concept.lineage, BuildAggregateWrapper)
    if concept.lineage.function.operator == FunctionType.COUNT:
        return concept.address in rejected.nonzero
    # an aggregate is never a merged key read through a COALESCE
    return concept.address in rejected.proofs | rejected.side_proofs


def group_filter(node: StrategyNode, rejected: ConsumerProofs) -> BoolExpr | None:
    """The filter every aggregate ``node`` computes reads its rows through,
    when ``rejected`` drops a group none of whose rows passes it. A row the
    filter fails adds nothing to a NULL-ignoring aggregate, and a group of
    only such rows reaches no consumer, so the filter rejects as a WHERE."""
    resolved = node.resolve()
    if not resolved.group_required:
        return None
    computed = [
        c for c in resolved.output_concepts if not resolved.source_map.get(c.address)
    ]
    matches = [filtered_aggregate(c) for c in computed]
    if not computed or any(
        match is None
        or not isinstance(c.lineage, BuildAggregateWrapper)
        or c.lineage.function.operator not in NULL_IGNORING_AGGREGATES
        for c, match in zip(computed, matches)
    ):
        return None
    filters = [match[1].where.conditional for match in matches if match]
    if any(f != filters[0] for f in filters[1:]):
        return None
    if not any(_rejects_empty(c, rejected) for c in computed):
        return None
    return filters[0]


def single_source_reads(node: StrategyNode) -> dict[str, str]:
    """Columns ``node`` renders off one source alone, by its identifier: NULL
    on every row an outer join pads that source onto, join key or not. An
    outer key paired with another address renders coalesced across the pair
    (`CTE.outer_join_key_class`)."""
    resolved = node.resolve()
    classed = {
        address
        for join in resolved.joins
        if isinstance(join, Join) and join.join_type in OUTER_JOIN_TYPES
        for pair in join.pairs or []
        if pair.left.address != pair.right.address
        for address in (pair.left.address, pair.right.address)
    }
    out: dict[str, str] = {}
    for address, sources in resolved.source_map.items():
        source = next(iter(sources), None)
        if (
            len(sources) == 1
            and address not in classed
            and isinstance(source, (QueryDatasource, BuildDatasource))
        ):
            out[address] = source.identifier
    return out


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
    relations read each other. A node reading another of its own relation (a
    joinless merge over the regroup it passes through) renders as it, so
    both take what the relation's consumers reject."""
    consumers: dict[str, set[str]] = defaultdict(set)
    for node in nodes:
        ident = node.resolve().identifier
        for parent in node.parents:
            if (pident := parent.resolve().identifier) != ident:
                consumers[pident].add(ident)
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
    against the plan it built: where a merge's consumers stopped rejecting
    what it took from them, or a re-resolved node keeps a parent's rows it
    dropped before (`_widened`), each merge whose joins grew is held to the
    types it planned (`_pin_join_types`) and the plan narrowed again. Where
    none can be held, the merges to blame are barred from the channel; if no
    keyed merge is to blame, the channel is dropped wholesale."""
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
    kept = {id(node): _Kept.of(node) for node in nodes}
    planned = {id(node): _join_sequence(node) for node in nodes}
    barred: set[int] = set()
    while True:
        _narrow(nodes, order, by_ident, ident_of, barred, saved)
        grown = [n for n in nodes if _Kept.of(n).typed - kept[id(n)].typed]
        failing = _failing_keys(order, by_ident, ident_of) | _widened(nodes, kept)
        if not failing:
            return
        for state in saved:
            state.restore()
        if _pin_join_types(grown, planned):
            continue
        if failing <= barred:
            break
        barred |= failing
    _narrow(nodes, order, by_ident, ident_of, None, saved)


def _by_parent(node: StrategyNode, idents: set[str]) -> frozenset[int]:
    position = {p.resolve().identifier: i for i, p in enumerate(node.parents)}
    return frozenset(position[i] for i in idents if i in position)


def _typed_kept(joins: list[SourceJoin | UnnestJoin], sources: set[str]) -> set[str]:
    """The sources whose unmatched rows the join types keep, matched or not."""
    alive = sources - joined_right_ids(joins)
    for join in joins:
        if not isinstance(join, Join) or join.join_type not in PADS_RIGHT_JOIN_TYPES:
            alive = set()
        if isinstance(join, Join) and join.join_type in PADS_LEFT_JOIN_TYPES:
            alive.add(join.right.identifier)
    return alive


@dataclass(frozen=True)
class _Kept:
    """Which parents' rows ``node`` keeps: by its join types, and every row
    surviving (`preserved_sources`, a join whose rows all find a partner)."""

    typed: frozenset[int]
    rows: frozenset[int]

    @classmethod
    def of(cls, node: StrategyNode) -> _Kept:
        resolved = node.resolve()
        sources = {ds.identifier for ds in resolved.datasources}
        return cls(
            _by_parent(node, _typed_kept(resolved.joins, sources)),
            _by_parent(node, preserved_sources(resolved.datasources, resolved.joins)),
        )


def _widened(nodes: list[StrategyNode], kept: dict[int, _Kept]) -> set[int]:
    """The keyed merges under a node whose joins now keep a parent's unmatched
    rows it dropped before; ``{-1}`` if none is keyed. A narrowed merge can
    stamp a key partial that it was not, and a passthrough above re-resolves
    into a LEFT from a lookup that keeps the rows the narrowing dropped. A
    wider type over rows that all matched before keeps nothing new."""
    out: set[int] = set()
    for node in nodes:
        if _Kept.of(node).typed - kept[id(node)].typed - kept[id(node)].rows:
            out |= _keyed_under(node) or {-1}
    return out


def _join_sequence(node: StrategyNode) -> JoinSequence | None:
    if not isinstance(node, MergeNode):
        return None
    parents = [p.resolve().identifier for p in node.parents]
    return join_sequence(node.resolve().joins, parents)


def _pin_join_types(
    grown: list[StrategyNode], planned: dict[int, JoinSequence | None]
) -> bool:
    """Hold each merge whose joins now keep a side they dropped before to the
    types it planned, where it still pairs its parents as it did then. A
    parent's narrowing dropped only rows those types reject, so they return
    the rows they did, and keep rejecting what the narrowing took: a reader
    that re-typed itself off a parent's dropped padding (a key nullable on
    one side alone now, `_nullable_join`) stops rejecting it. Whether any
    merge took a pin it did not hold."""
    out = False
    for node in grown:
        sequence = planned[id(node)]
        if (
            isinstance(node, MergeNode)
            and sequence is not None
            and node.planned_join_types is None
            and _pairs(_join_sequence(node)) == _pairs(sequence)
        ):
            node.planned_join_types = sequence
            out = True
    return out


def _pairs(sequence: JoinSequence | None) -> list[tuple[frozenset[int], int]] | None:
    return None if sequence is None else [(left, right) for left, right, _ in sequence]


def _keyed_under(node: StrategyNode) -> set[int]:
    return {
        id(n)
        for n in _walk(node)
        if isinstance(n, MergeNode)
        and n.consumer_proofs is not None
        and n.consumer_proofs.keys
    }


def _narrow(
    nodes: list[StrategyNode],
    order: list[str],
    by_ident: dict[str, list[StrategyNode]],
    ident_of: dict[int, str],
    barred: set[int] | None,
    saved: list[_State],
) -> None:
    """Narrow to a fixpoint; with ``barred`` None, on WHERE proofs alone,
    else on join keys too, but for the merges it names. Consumers are read before the merges under them
    narrow, and a narrowing can make a consumer reject more: a key its other
    side no longer pads pairs on plain ``=``, which rejects the NULL a merge
    below pads it with."""
    for _ in range(len(nodes)):
        narrowed: set[int] = set()
        _propagate(order, by_ident, ident_of, barred, narrowed)
        if not narrowed:
            return
        _rebuild(nodes, narrowed, saved)


@dataclass
class _State:
    node: StrategyNode
    nullable: list[BuildConcept]
    resolved_nullable: set[str]
    proofs: ConsumerProofs | None
    partials: set[str] | None
    key_reads: dict[str, str] | None

    @classmethod
    def of(cls, node: StrategyNode) -> _State:
        merge = node if isinstance(node, MergeNode) else None
        return cls(
            node,
            list(node.nullable_concepts),
            _nullable(node),
            merge.consumer_proofs if merge else None,
            merge.unfiltered_partials if merge else None,
            merge.key_reads if merge else None,
        )

    def restore(self) -> None:
        self.node.nullable_concepts = list(self.nullable)
        if isinstance(self.node, MergeNode):
            self.node.consumer_proofs = self.proofs
            self.node.unfiltered_partials = self.partials
            self.node.key_reads = self.key_reads
        self.node.rebuild_cache()


def _propagate(
    order: list[str],
    by_ident: dict[str, list[StrategyNode]],
    ident_of: dict[int, str],
    barred: set[int] | None,
    narrowed: set[int] | None,
) -> dict[str, ConsumerProofs]:
    """What every consumer of each relation rejects, consumers first; join
    keys only with ``barred`` given. With ``narrowed``, each outer-joining
    merge takes it (keys only if not barred) and is re-resolved before its own
    joins are read: narrowed, they reject more of its parents' rows."""
    received: dict[str, ConsumerProofs] = {}
    for ident in order:
        here = received.get(ident, ConsumerProofs())
        siblings = by_ident[ident]
        # nodes of one relation render as one CTE over one row set
        # (`CTE.__add__`): a row any of them reads a rejected column of is
        # the relation's row, so their reads of a parent combine
        one_rowset = all(_passes_rows(node) for node in siblings)
        gives: dict[str, ConsumerProofs] = {}
        for node in siblings:
            passed = _passes_rows(node)
            take = (
                replace(here, keys=frozenset())
                if barred is None or id(node) in barred
                else here
            )
            if (
                narrowed is not None
                and take
                and passed
                and isinstance(node, MergeNode)
                and _outer_joins(node)
                and (
                    (taken := node.consumer_proofs or ConsumerProofs()) != take | taken
                    or _reads_changed(node, take)
                )
            ):
                # a later round only adds: what the merge took before still
                # holds, and `_failing_keys` checks the plan that results
                node.consumer_proofs = take | taken
                if node.unfiltered_partials is None:
                    node.unfiltered_partials = {
                        c.address for c in node.resolve().partial_concepts
                    }
                node.key_reads = single_source_reads(node) | (node.key_reads or {})
                narrowed.add(id(node))
                node.rebuild_cache()
            forward = _through_lineage(node, here | ConsumerProofs.of(node.conditions))
            for parent in dict.fromkeys(node.parents):
                pident = ident_of[id(parent)]
                if pident == ident:
                    continue
                reads = row_reads(node, parent) if passed else set()
                readable = reads | computed_inputs(parent, reads)
                given = forward.restrict(readable)
                if passed and (own := where_reads(node, parent)):
                    given |= ConsumerProofs.of(node.conditions).restrict(
                        own | computed_inputs(parent, own)
                    )
                if passed and (where := group_filter(node, forward)) is not None:
                    given |= ConsumerProofs.of(where).restrict(readable)
                if barred is not None:
                    given |= ConsumerProofs(
                        keys=frozenset(join_rejections(node, parent))
                    )
                prior = gives.get(pident)
                if prior is not None:
                    given = prior | given if one_rowset else prior & given
                gives[pident] = given
        for pident, given in gives.items():
            prior = received.get(pident)
            received[pident] = given if prior is None else prior & given
    return received


def _reads_changed(node: MergeNode, take: ConsumerProofs) -> bool:
    """Whether a key ``take`` rejects now renders off one source where it did
    not when the merge last took: its own narrowing un-coalesced it, and a
    single-source key proves its source present (`_keys_force`)."""
    reads = node.key_reads or {}
    return any(a not in reads for a in single_source_reads(node).keys() & take.keys)


def _failing_keys(
    order: list[str],
    by_ident: dict[str, list[StrategyNode]],
    ident_of: dict[int, str],
) -> set[int]:
    """The narrowed merges whose consumers, as rebuilt, stopped rejecting a
    key they took."""
    received = _propagate(order, by_ident, ident_of, set(), None)
    return {
        id(node)
        for ident, group in by_ident.items()
        for node in group
        if isinstance(node, MergeNode)
        and node.consumer_proofs is not None
        and not node.consumer_proofs.keys <= received.get(ident, ConsumerProofs()).keys
    }


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
