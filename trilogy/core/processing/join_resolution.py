from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field, replace
from functools import cached_property, partial
from itertools import permutations
from operator import attrgetter
from typing import Any

from trilogy.constants import DEFAULT_NAMESPACE, PRESENCE_MARKER_PREFIX, logger
from trilogy.core.domain_graph import DomainGraph
from trilogy.core.enums import (
    Derivation,
    FunctionType,
    Granularity,
    JoinType,
    Modifier,
    Purpose,
    SourceType,
)
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildFunction,
    BuildGrain,
    BuildUnionDatasource,
    get_grouped_aggregate_wrapper,
)
from trilogy.core.models.build_environment import (
    BuildEnvironment,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.execute import (
    ConceptPair,
    GuardTerm,
    Join,
    JoinGuard,
    QueryDatasource,
    SourceJoin,
    UnnestJoin,
    coalesced_key_groups,
    preserved_key_pairs,
)
from trilogy.core.models.keyspace import Keyspace
from trilogy.core.processing import plan_trace
from trilogy.core.processing.aggregate_rollup import pending_rollups
from trilogy.core.processing.condition_utility import (
    is_scalar_condition,
)
from trilogy.core.processing.join_key_groups import axis_member_keys
from trilogy.core.processing.null_provenance import (
    ProvenanceMemo,
    active_memo,
    side_nullable,
    span_padded_addresses,
)
from trilogy.core.processing.utility import (
    PADS_LEFT_JOIN_TYPES,
    PADS_RIGHT_JOIN_TYPES,
    applied_atoms,
    join_left_sources,
    joined_right_ids,
    left_deep_joins,
    padded_by,
)
from trilogy.utility import string_to_hash

DataSource = QueryDatasource | BuildDatasource


@dataclass
class JoinOrderOutput:
    right: str
    type: JoinType
    keys: dict[str, set[str]]
    left: str | None = None
    # left sources providing the same keys, left out as redundant: a FULL
    # join needs them back (`restore_full_join_providers`)
    redundant: dict[str, set[str]] = field(default_factory=dict, compare=False)
    # the type before `ensure_content_preservation` widened it
    own_type: JoinType | None = None

    @property
    def lefts(self) -> set[str]:
        return set(self.keys.keys())


OUTER_JOIN_TYPES = (JoinType.FULL, JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER)
DIRECTIONAL_OUTER_JOIN_TYPES = (JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER)


def held_region_spans(source: DataSource) -> frozenset[str]:
    """Spans a source holds a region's rows on; a leaf holds none."""
    return source.region_spans if isinstance(source, QueryDatasource) else frozenset()


def compute_outer_null_status(
    joins: list[SourceJoin | UnnestJoin],
) -> dict[str, int]:
    """Score how often each datasource is null-extended by outer joins. A
    padded right side scores past every source it is joined from: down a
    chain of LEFT joins each side can only lose rows the earlier ones carry."""
    score: dict[str, int] = defaultdict(int)
    for join, left in left_deep_joins(joins):
        padded = padded_by(join, left)
        for identifier in padded:
            score[identifier] += 1
        right = join.right.identifier
        if right in padded and join.join_type not in PADS_LEFT_JOIN_TYPES:
            score[right] = max(
                score[right],
                1
                + max(
                    (score[s.identifier] for s in join_left_sources(join)), default=0
                ),
            )
    return score


def prune_outer_join_pairs(joins: list[SourceJoin | UnnestJoin]) -> None:
    """Drop redundant duplicate-key pairs from directional outer joins: a
    left side no earlier join pads equals the key on every row, so the
    others' pairs only bloat the coalesce. With every left side padded the
    coalesce IS the key, and all pairs stay. A join keyed off other lefts
    than it was typed against keeps no type of its own to restore: the left
    walked first, now pruned, decided it (`restore_unpadded_joins`)."""
    padded: set[str] = set()
    for join, left in left_deep_joins(joins):
        if join.pairs and join.join_type in DIRECTIONAL_OUTER_JOIN_TYPES:
            kept = preserved_key_pairs(join.pairs, padded, _pair_source)
            if len(kept) < len(join.pairs):
                join.own_type = None
            join.pairs = kept
        padded |= padded_by(join, left)


def restore_unpadded_joins(joins: list[SourceJoin | UnnestJoin]) -> None:
    """A join widened to keep the rows an earlier join padded takes back its
    own type once narrowing made every such earlier join pad nothing: the
    widening only held rows that no longer arrive."""
    padded: set[str] = set()
    for join, left in left_deep_joins(joins):
        own = join.own_type
        if own is not None and own != join.join_type:
            lefts = {source.identifier for source in join_left_sources(join)}
            if not lefts & padded and own in _NARROWER.get(join.join_type, ()):
                join.join_type = own
        padded |= padded_by(join, left)


_NARROWER: dict[JoinType, frozenset[JoinType]] = {
    JoinType.FULL: frozenset(
        {JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER, JoinType.INNER}
    ),
    JoinType.LEFT_OUTER: frozenset({JoinType.INNER}),
    JoinType.RIGHT_OUTER: frozenset({JoinType.INNER}),
}


def _pair_source(pair: ConceptPair) -> str:
    return pair.node.identifier


@dataclass(frozen=True)
class PlannedPairs:
    """A merge's join plan, pair by pair of its parents."""

    # id of each planned parent -> the planned groups whose rows it holds
    sides: Mapping[int, frozenset[str]]
    # two groups (one, for a group split into projections) -> what they may
    # pair on
    keys: Mapping[frozenset[str], frozenset[str]]
    # groups whose rows are finer than what the plan knows of them (a derived
    # connector's subplan): each also pairs on its resolved grain
    grain_paired: frozenset[str] = frozenset()
    # the groups in the order the plan joins them when nothing else decides;
    # a group it leaves out comes after, in no stated order
    order: tuple[str, ...] = ()

    @classmethod
    def star(
        cls, hub: object, spokes: Sequence[tuple[object, frozenset[str]]]
    ) -> PlannedPairs:
        """`hub` joined to each spoke on that spoke's keys: two spokes pair
        only on keys both join the hub on."""
        sides = ["hub", *(f"spoke{i}" for i in range(len(spokes)))]
        keys = {side: spoke[1] for side, spoke in zip(sides[1:], spokes)}
        # the hub's rows are the merge's: each spoke hangs off them
        return cls(
            {
                id(node): frozenset({side})
                for node, side in zip([hub, *(n for n, _ in spokes)], sides)
            },
            {
                frozenset({a, b}): keys[b] if a == "hub" else keys[a] & keys[b]
                for i, a in enumerate(sides)
                for b in sides[i + 1 :]
            },
            order=("hub",),
        )

    @classmethod
    def over_scans(
        cls,
        sides: Mapping[int, frozenset[str]],
        keys: Mapping[frozenset[str], frozenset[str]],
        datasources: Mapping[str, BuildDatasource | BuildUnionDatasource],
        grain_paired: frozenset[str] = frozenset(),
    ) -> PlannedPairs:
        """A plan between datasource scans, stating `scan_precedence` over
        each scan's declared grain. A side no datasource names (a derived
        connector, whose rows are its subplan's) states none."""
        grains = {
            name: frozenset(datasources[name].grain.components)
            for name in sorted(frozenset().union(*sides.values()))
            if name in datasources
        }
        return cls(
            sides,
            keys,
            grain_paired=grain_paired,
            order=scan_precedence(keys, grains),
        )

    def rank(self, groups: frozenset[str]) -> int:
        """Where a side holding `groups` comes in the plan's order: the join
        walk breaks the ties its facts leave on it before any spelling."""
        return min(
            (i for i, gid in enumerate(self.order) if gid in groups),
            default=len(self.order),
        )

    @property
    def permitted(self) -> frozenset[str]:
        """Everything the merge may pair on, across all of its sides."""
        return frozenset().union(*self.keys.values())

    def rebound(self, ids: Mapping[int, int]) -> PlannedPairs:
        """The plan over parents replaced by others: `ids` maps a replaced
        parent's id to its replacement's."""
        return replace(
            self, sides={ids.get(key, key): side for key, side in self.sides.items()}
        )

    def joined(
        self, key: int, side: frozenset[str], keys: frozenset[str]
    ) -> PlannedPairs:
        """The plan with one more parent, holding `side`, paired with every
        planned group on `keys`."""
        others = frozenset().union(*self.sides.values()) - side
        return replace(
            self,
            sides={**self.sides, key: side},
            keys={
                **self.keys,
                **{
                    frozenset({a, b}): self.keys.get(frozenset({a, b}), frozenset())
                    | keys
                    for a in side
                    for b in others
                },
            },
        )

    def between(self, left: frozenset[str], right: frozenset[str]) -> frozenset[str]:
        return frozenset().union(
            *(
                self.keys.get(frozenset({a, b}), frozenset())
                for a in left
                for b in right
            )
        )


def scan_precedence(
    keys: Mapping[frozenset[str], frozenset[str]],
    grains: Mapping[str, frozenset[str]],
) -> tuple[str, ...]:
    """The order a merge of scans joins them in when nothing else decides,
    as `JoinPlan.precedence` states it for groups: a scan of rows before
    the lookups its pairings identify (a scan whose grain lies inside the
    keys it pairs on), finer rows first. A scan of no declared grain is
    stated with the coarsest. Where a `~` binding puts a scan is resolve's
    to state (`_binding_tiers`)."""
    looked_up = {
        side
        for pair, pairs in keys.items()
        if len(pair) == 2
        for side in pair
        if grains.get(side) and grains[side] <= pairs
    }
    return tuple(
        sorted(sorted(grains), key=lambda s: (s in looked_up, -len(grains[s])))
    )


@dataclass(frozen=True)
class SideFacts:
    """What a merge knows about ONE of its sides, in the merge's node spelling.

    Several fields classify the same keys: ``nullables`` is every key the side
    can emit NULL for, and ``value_nullables`` / ``extent_nullables`` /
    ``guest_padded`` / ``rollup_padded`` are the provenances that decide
    whether such a NULL pairs with another side's.
    """

    # every column the side emits, in this merge's spelling
    columns: frozenset[str] = frozenset()
    # keys the side binds as a declared SUBSET (`~`)
    partials: frozenset[str] = frozenset()
    nullables: frozenset[str] = frozenset()
    value_nullables: frozenset[str] = frozenset()
    extent_nullables: frozenset[str] = frozenset()
    guest_padded: frozenset[str] = frozenset()
    rollup_padded: frozenset[str] = frozenset()
    # the side's axes in this merge's spelling; the join ordering breaks ties
    # on how many there are
    grain: frozenset[str] = frozenset()
    # what that grain determines (its FD closure), in this merge's spelling
    determined: frozenset[str] = frozenset()
    # covers every `~`-licensed key the node emits, so extension rows ride it
    hosts: bool = False
    # every column is a one-row value (a `by *` total, a constant)
    one_row: bool = False
    # reads one datasource partial on its own grain (an arm of a union): its
    # rows are a subset of a whole relation's, so its partials bind no key
    arm: bool = False
    # spans the side holds a region's rows on
    held_spans: frozenset[str] = frozenset()
    # extent-free spans the side carries every value of
    complete_spans: frozenset[str] = frozenset()
    # extent-free span or `~` key -> the leaf tables whose own `~` column
    # binds it here
    span_bindings: Mapping[str, frozenset[str]] = field(default_factory=dict)
    # nullable key -> the spans whose extension rows NULL it
    span_padding: Mapping[str, frozenset[str]] = field(default_factory=dict)


_NO_FACTS = SideFacts()


@dataclass(frozen=True)
class JoinFacts:
    """What a merge knows about every side, for typing any pair of them.

    One object because these are merge-wide: only `left`, `right` and the
    connecting keys change from pair to pair, and `get_node_joins` builds this
    once per merge.
    """

    sides: Mapping[str, SideFacts] = field(default_factory=dict)
    # keys of query-scoped FULL joins (`full`/`union join`, EQUAL/∦ declared
    # edges): neither side's domain contains the other's
    full_join_keys: frozenset[str] = frozenset()
    # keys an authored relation in the environment declares: scoped partial
    # derived keys and scoped join key groups
    scoped_keys: frozenset[str] = frozenset()
    # anchor keys of query-scoped LEFT joins (declared-subset anchors)
    anchor_keys: frozenset[str] = frozenset()
    # keys of ROOT-member authored join groups
    authored_join_keys: frozenset[str] = frozenset()
    # spans the statement elected another group to own
    extent_free_keys: frozenset[str] = frozenset()
    # the plan's regions in this merge's spelling: a holder unions the spans of
    # every region it reads, so only this says how many families the merge has
    region_partition: tuple[frozenset[str], ...] = ()
    # domains the node emits: a `~` key outside this set licenses no extension
    # rows here, so empty means none of them do
    demanded_domains: frozenset[str] = frozenset()
    # an arm key of a merged attribute -> the attribute it reaches
    span_families: Mapping[str, str] = field(default_factory=dict)
    # two sides -> the keys the join plan pairs them on, each a column both
    # hold; a pair it does not name pairs on nothing
    pair_keys: Mapping[frozenset[str], frozenset[str]] = field(default_factory=dict)
    # each side's place in the one order the walk joins the merge's sides in
    # (`_stated_order`)
    side_ranks: Mapping[str, int] = field(default_factory=dict)
    # the side holding the merge's rows, when the merge states one: the walk
    # starts from it
    seed: str | None = None

    def side(self, node: str) -> SideFacts:
        return self.sides.get(node) or _NO_FACTS

    def rank(self, node: str) -> tuple[int, str]:
        """Where the merge states a side; the name decides only between two
        sides it states alike (projections of one split group)."""
        return (self.side_ranks.get(node, len(self.side_ranks)), node)

    @cached_property
    def authored_keys(self) -> frozenset[str]:
        """Keys whose typing an authored relation owns (`subset join` anchors,
        scoped coalescing members): the host/dimension direction inference
        stands down on these."""
        return self.scoped_keys | self.anchor_keys | self.authored_join_keys

    @cached_property
    def axis_keys(self) -> frozenset[str]:
        """Keys an authored FULL declares row intent for on BOTH sides, facts
        hanging off either included (``ensure_content_preservation``)."""
        return self.full_join_keys | self.anchor_keys | self.authored_join_keys

    @cached_property
    def held_spans(self) -> frozenset[str]:
        """Every span some side of the merge holds a region's rows on."""
        return frozenset().union(*(s.held_spans for s in self.sides.values()))

    def authored(self, keys: set[str]) -> bool:
        return bool(keys & self.authored_keys)

    def paired(self, a: str, b: str) -> frozenset[str]:
        """What the plan pairs two sides on."""
        return self.pair_keys.get(frozenset({a, b}), frozenset())


def _preserving(left: bool) -> JoinType:
    """The directional join keeping the left side's rows, else the right's."""
    return JoinType.LEFT_OUTER if left else JoinType.RIGHT_OUTER


def _pads_for_different_members(
    left: SideFacts,
    right: SideFacts,
    keys: set[str],
    families: Mapping[str, str] | None = None,
) -> bool:
    """Both sides NULL the connecting keys to carry extension members, and of
    different ``~`` spans: a product never sold beside a user who never
    ordered. Those NULLs name nothing in common, so pairing them invents a
    (product, user) row; each side's padding has to survive on its own.

    `families` spells the arm keys of one merged attribute as the attribute:
    each arm's undated rows are its NULL group, which the arms share."""
    if not (left.span_padding and right.span_padding):
        return False
    families = families or {}

    def spans(side: SideFacts) -> frozenset[str]:
        return frozenset(
            families.get(span, span)
            for key in keys
            for span in side.span_padding.get(key, frozenset())
        )

    left_spans, right_spans = spans(left), spans(right)
    return bool(left_spans and right_spans and left_spans.isdisjoint(right_spans))


def _is_nullable_grain_aligned_merge(
    left: SideFacts, right: SideFacts, all_connecting_keys: set[str]
) -> bool:
    """Both sides are complete group-sets at the merge grain (each side's
    grain sits within the connecting keys, so each row is a result in its own
    right rather than feeder content for the other side) and no side can pair
    completely on intact keys. Only extent nullability (a `?` binding, outer
    join padding) weakens a domain claim; when the keys free of it still cover
    one side's grain, that side's intact claim makes the pairing total (a
    value-nullable attribute riding a solid key that determines it) and the
    ordinary directional/INNER typing stands."""
    if not (
        left.grain
        and right.grain
        and left.grain <= all_connecting_keys
        and right.grain <= all_connecting_keys
    ):
        return False
    solid = all_connecting_keys - left.extent_nullables - right.extent_nullables
    return not (left.grain <= solid or right.grain <= solid)


def _unpaired_value_nulls(
    keys: set[str], held: frozenset[str], feeder: SideFacts, holder: SideFacts
) -> bool:
    """A NULL the feeder carries on a join key with nothing to pair it.

    On the region's own key (``held``) an extent NULL always vetoes: a guest
    names no member. Otherwise a value NULL vetoes only when the holder has no
    value NULL of its own, since two value NULLs pair null-safely. A key the
    feeder NULLs for a `~?` guest pairs only with a holder padded for the same
    guests."""
    for key in keys:
        feeder_value = key in feeder.value_nullables
        feeder_extent = key in feeder.extent_nullables
        if not (feeder_value or feeder_extent):
            continue
        if key in held and feeder_extent:
            return True
        if key in feeder.guest_padded:
            if key not in holder.guest_padded:
                return True
            continue
        if not (feeder_value and key in holder.value_nullables):
            return True
    return False


def _sole_host(left: str, right: str, keys: set[str], facts: JoinFacts) -> str | None:
    """The side that ALONE hosts the node's extension rows, when there is one.

    Preservation exists to keep those rows, and they ride the HOST: the side
    covering every `~`-licensed key the node emits (or the node's grain when
    none are in play). The other side is then a feeder whose unmatched rows
    carry no reachable content. Symmetric or absent hosting, and an authored
    axis, leave the typing to the rules below.
    """
    if facts.authored(keys):
        return None
    left_hosts = facts.side(left).hosts
    if left_hosts == facts.side(right).hosts:
        return None
    return left if left_hosts else right


def _holds_another_family(
    held: frozenset[str], sides: Collection[str], facts: JoinFacts
) -> bool:
    """Some side holds a region other than the one ``held`` spans. Counted
    over the plan's regions, not the spans held: one composite-key region
    contributes every span of its grain."""
    theirs: frozenset[str] = frozenset().union(
        *(facts.side(side).held_spans for side in sides)
    )
    return any(
        region & theirs and not region & held for region in facts.region_partition
    )


def _region_contract_join(
    left: str, right: str, keys: set[str], facts: JoinFacts, joined: Collection[str]
) -> tuple[JoinType | None, bool]:
    """The region contract: a side holding a region's rows, joined on that
    region's span, is preserved; the other side is too only where its key
    carries a value NULL (a guest order), else its unmatched rows are members
    nobody referenced.

    Returns the type when the contract decides the pair, and whether hosting
    is suppressed for the rules below: two holders of one region (its domain
    and a reader of it) pair on what they hold, and neither out-hosts the
    other by its bindings.
    """
    if facts.authored(keys):
        return None, False
    left_facts, right_facts = facts.side(left), facts.side(right)
    left_holds = bool(left_facts.held_spans & keys)
    right_holds = bool(right_facts.held_spans & keys)
    if left_holds and right_holds:
        return None, True
    if not (left_holds or right_holds):
        return None, False
    holder, feeder = (
        (left_facts, right_facts) if left_holds else (right_facts, left_facts)
    )
    # Another family's extension rows are NULL on this key, so only FULL
    # keeps them, and only the stream joined against the holder can carry
    # them: the side being added when the holder is already joined (whatever
    # else is joined rides the preserved side), everything joined when the
    # holder is the one being added.
    against = {right} if left_holds else {left, *joined}
    if _unpaired_value_nulls(
        keys, holder.held_spans, feeder, holder
    ) or _holds_another_family(holder.held_spans & keys, against, facts):
        return JoinType.FULL, False
    return _preserving(left_holds), False


def _bound_alike(left: SideFacts, right: SideFacts, key: str) -> bool:
    """Both sides bind `key` through the same leaf `~` columns."""
    bound = left.span_bindings.get(key)
    return bool(bound) and bound == right.span_bindings.get(key)


def _extent_free_join(
    left: str, right: str, keys: set[str], facts: JoinFacts
) -> tuple[JoinType | None, set[str] | None]:
    """A span the statement elected another group to own
    (v4_helper/extent_ownership.py). Its extension members are not this
    merge's to manufacture, so its `~` mark grants no row intent here: with a
    clean fact/dimension split anchor the fact and let equality shed the
    members it never referenced.

    Returns the type when the span decides the pair, else the keys partiality
    should be re-read over (the span excluded) when both sides bind it.
    """
    free = facts.extent_free_keys
    if not free:
        return None, None
    left_facts, right_facts = facts.side(left), facts.side(right)
    span_keys = (keys & free) & (left_facts.partials | right_facts.partials)
    if not span_keys:
        return None, None
    left_binds = bool(span_keys & left_facts.partials)
    right_binds = bool(span_keys & right_facts.partials)
    if left_binds != right_binds:
        binder, other = (
            (left_facts, right_facts) if left_binds else (right_facts, left_facts)
        )
        # A binder keyed by the span itself (a `~` notes table beside the
        # customers) is a lookup, not a fact referencing it: it has no rows of
        # its own to anchor, and the members it lacks keep the other side's.
        if binder.grain and binder.grain <= span_keys:
            return None, None
        # The other side carrying the span's whole domain and the binder no
        # NULL on it: every binder row has its partner, so anchoring preserves
        # nothing. Typed INNER here rather than left to the narrowing pass,
        # because the anchoring's stamp (the domain's columns nullable on this
        # stream) would otherwise reach every merge above as a value NULL.
        if keys <= other.complete_spans and not keys & binder.nullables:
            return JoinType.INNER, None
        return _preserving(left_binds), None
    # Both sides bind it. Two projections of ONE binding cover the same subset,
    # so the span carries no row intent between them and the remaining keys
    # decide the typing. PEER facts (sales and returns each referencing their
    # own slice of the group domain) each hold rows the other lacks, and
    # dropping either side's is a chasm, not an extension: their typing stands
    # whoever owns the extent.
    if all(_bound_alike(left_facts, right_facts, key) for key in span_keys):
        return None, keys - free
    return None, None


def _one_binding(left: SideFacts, right: SideFacts) -> frozenset[str]:
    """`~` keys both sides bind through the same leaf `~` columns: two
    projections of one binding cover the same subset, so the `~` carries no
    row intent between them and the other rules type the pair."""
    return frozenset(
        key for key in left.partials & right.partials if _bound_alike(left, right, key)
    )


def _partial_domain_join(
    left: str, right: str, keys: set[str], facts: JoinFacts, host: str | None
) -> JoinType:
    """A partial side declares a SUBSET domain. Subset speaks to VALUES and
    NULL is not a value, so partiality and nullability never interact here:
    render preserving, and the narrowing pass restores direction exactly when
    the superset side provably carries the key's full domain and the subset
    side's NULLs have a null-safe partner."""
    left_facts, right_facts = facts.side(left), facts.side(right)
    # Preserving the feeder of a sole host (``_sole_host``) manufactures padded
    # join keys the FINAL merge then null-pairs across extension families. A
    # feeder carrying VALUE nulls (`~?`) on the key stays row-preserving
    # though: its NULL-keyed rows are real fact rows equality would drop.
    # Padding NULLs on the feeder are exactly what the direction exists to
    # shed, so only value nulls veto.
    if host is not None:
        # a key null-extended off a value-null one below (a guest order's
        # address) is a value null here too
        feeder = right_facts if host == left else left_facts
        if not _unpaired_value_nulls(keys, frozenset(), feeder, facts.side(host)):
            return _preserving(host == left)
    partial_keys = keys & (left_facts.partials | right_facts.partials)
    # A `~` key the node never emits (not a visible output, no grain component
    # keyed by it) licenses no extension rows here. When the pair is
    # recognizably fact-to-dimension (one side's grain is the connecting keys
    # themselves) the dimension is a pure lookup whose unmatched rows are
    # grainless, so anchor the fact side. A demanded key, ambiguous topology
    # (which a side with no known grain is), or a value-null fact key stays
    # row-preserving.
    if (
        partial_keys
        and not partial_keys & facts.demanded_domains
        and not facts.authored(keys)
    ):
        left_is_dim = bool(left_facts.grain) and left_facts.grain <= keys
        right_is_dim = bool(right_facts.grain) and right_facts.grain <= keys
        if left_is_dim != right_is_dim:
            fact = right_facts if left_is_dim else left_facts
            if not keys & fact.value_nullables:
                return _preserving(right_is_dim)
    return JoinType.FULL


def _nullable_join(
    left: str, right: str, keys: set[str], facts: JoinFacts, host: str | None
) -> JoinType:
    """Neither side partial: each binding declares the key's full domain
    (EQUAL, mutual subset), whose narrowed form is INNER. NULL-key rows must
    still never drop: when both sides are nullable the null-safe equality
    (get_modifiers) pairs the NULL groups, and a nullable side with no
    null-safe partner keeps the join preserving toward it."""
    left_facts, right_facts = facts.side(left), facts.side(right)
    left_is_nullable = bool(keys & left_facts.nullables)
    right_is_nullable = bool(keys & right_facts.nullables)
    if left_is_nullable and right_is_nullable:
        # Null-pairing is only sound when the padded rows name the same thing.
        # The host's padding is the grain-bearing extension family; the feeder's
        # padding lacks grain columns entirely, so pairing the two invents rows
        # (extension-family cross products). Preserve the host and let plain
        # equality drop the feeder's padding.
        if host is not None:
            return _preserving(host == left)
        # A value NULL on one side (a guest order's address, grouped) names a
        # real row; the other side's padding names nothing. Keep the row. An
        # authored axis declines this like every other direction inference: the
        # relation declares the pairing, so a NULL a side happens to carry on
        # the declared key is not this rule's to read.
        left_values = bool(keys & left_facts.extent_nullables)
        if left_values != bool(
            keys & right_facts.extent_nullables
        ) and not facts.authored(keys):
            return _preserving(left_values)
        # Padding for different spans never pairs (`get_node_joins` drops the
        # null-safe equality), so INNER would shed both extension families.
        if _pads_for_different_members(
            left_facts, right_facts, keys, facts.span_families
        ):
            return JoinType.FULL
        # Grain-aligned sides both weakened their EQUAL-domain claims on the
        # merge axis itself, so INNER would drop each side's exclusive members;
        # preserve both and let the null-safe equality pair the NULL groups.
        if _is_nullable_grain_aligned_merge(left_facts, right_facts, keys):
            return JoinType.FULL
        # a value NULL group the other side has none of pairs with nothing,
        # null-safe or not: keep it
        left_value = bool(keys & left_facts.value_nullables)
        if left_value != bool(
            keys & right_facts.value_nullables
        ) and not facts.authored(keys):
            return _preserving(left_value)
        return JoinType.INNER
    if left_is_nullable != right_is_nullable:
        # A nullable key weakens that side's EQUAL-domain claim to "some
        # subset, plus a NULL group". Between a fact and its lookup that still
        # directs the join: the other side is a feeder whose unmatched rows
        # carry no content. But when both sides are complete group-sets at the
        # merge grain and the nullability rides the merge axis, a directional
        # join would silently drop the non-nullable side's exclusive members,
        # the very rows its intact domain claim promises. Preserve both, padded.
        if _is_nullable_grain_aligned_merge(left_facts, right_facts, keys):
            return JoinType.FULL
        # A nullable side whose rows the other side's rows determine is that
        # side's lookup, not its fact: a NULL key it holds without being a
        # value names no row of the other side's (a species dimension beside
        # the trees a WHERE kept, #972), so the fact's rows are the ones kept.
        lookup = left_facts if left_is_nullable else right_facts
        fact = right_facts if left_is_nullable else left_facts
        if (
            _dimension_of(lookup, fact)
            and not keys & lookup.value_nullables
            and not facts.authored(keys)
        ):
            return _preserving(not left_is_nullable)
        return _preserving(left_is_nullable)
    return JoinType.INNER


def _fd_closure(
    environment: BuildEnvironment, determinants: Collection[str]
) -> frozenset[str]:
    # local import: v4_helper's package init imports this module
    from trilogy.core.processing.v4_helper.functional_dependency import (
        strict_fd_closure,
    )

    return strict_fd_closure(environment, determinants)


def _dimension_of(lookup: SideFacts, fact: SideFacts) -> bool:
    """`fact`'s rows determine `lookup`'s grain, and not conversely."""
    return (
        bool(lookup.grain)
        and bool(fact.grain)
        and lookup.grain <= fact.grain | fact.determined
        and not fact.grain <= lookup.grain | lookup.determined
    )


def get_join_type(
    left: str,
    right: str,
    all_connecting_keys: set[str],
    facts: JoinFacts,
    joined: Collection[str] = frozenset(),
) -> JoinType:
    """Type one pair of a merge's sides, by the first rule that decides it.
    ``joined`` is every side the join tree holds when ``right`` is added.

    Rendering is row-preserving by default: a relation declares DOMAIN
    knowledge, never row intent, and no join silently drops a row
    (docs/subset_union_join_design.md). The narrowing pass
    (UpgradeOuterFromKeySetEquivalence) restores a directional/INNER form only
    when provably row-identical.
    """
    join_type, rule = _decide_join_type(left, right, all_connecting_keys, facts, joined)
    if plan_trace.active():
        _trace_join_type(
            left, right, all_connecting_keys, facts, joined, join_type, rule
        )
    return join_type


def _trace_join_type(
    left: str,
    right: str,
    keys: set[str],
    facts: JoinFacts,
    joined: Collection[str],
    join_type: JoinType,
    rule: str,
) -> None:
    plan_trace.record(
        f"{left} ~ {right}: {join_type.value}",
        plan_trace.JoinTypeStep(
            left=left,
            right=right,
            keys=sorted(keys),
            joined=sorted(joined),
            type=join_type.value,
            rule=rule,
            left_facts=plan_trace.jsonable(facts.side(left)),
            right_facts=plan_trace.jsonable(facts.side(right)),
            merge_facts=plan_trace.jsonable(replace(facts, sides={})),
        ),
    )


def _decide_join_type(
    left: str,
    right: str,
    all_connecting_keys: set[str],
    facts: JoinFacts,
    joined: Collection[str],
) -> tuple[JoinType, str]:
    """`get_join_type`'s answer and the name of the rule that decided it."""
    # UNION-declared keys (query-scoped `full join` / `union join`, non-partial
    # merges): neither domain contains the other, so FULL with the key
    # coalesced; the registry also vetoes narrowing, and keeps the key complete.
    if all_connecting_keys & facts.full_join_keys:
        return JoinType.FULL, "full_join_keys"

    region_type, region_hosts_equally = _region_contract_join(
        left, right, all_connecting_keys, facts, joined
    )
    if region_type is not None:
        return region_type, "region_contract"
    # two holders of one region (its domain and a reader of it) pair on what
    # they hold, and neither out-hosts the other by its bindings
    host = (
        None
        if region_hosts_equally
        else _sole_host(left, right, all_connecting_keys, facts)
    )

    extent_type, typed_keys = _extent_free_join(left, right, all_connecting_keys, facts)
    if extent_type is not None:
        return extent_type, "extent_free"
    left_facts, right_facts = facts.side(left), facts.side(right)
    partial_keys = all_connecting_keys if typed_keys is None else typed_keys
    partial_keys = partial_keys - _one_binding(left_facts, right_facts)
    if partial_keys & (left_facts.partials | right_facts.partials):
        return (
            _partial_domain_join(left, right, all_connecting_keys, facts, host),
            "partial_domain",
        )

    # A grouping-set NULL is padding, not a value: the subtotal/grand-total row
    # a ROLLUP/CUBE/GROUPING SETS emits has no counterpart on a side that does
    # not pad the same key, so null-safe equality has nothing to pair it with
    # and the INNER form below would silently drop it. Preserve toward the
    # padded side. Both sides padded is the ordinary case again: same grouping
    # sets, so the NULL groups do pair.
    left_pads = bool(all_connecting_keys & left_facts.rollup_padded)
    if left_pads != bool(all_connecting_keys & right_facts.rollup_padded):
        return _preserving(left_pads), "rollup_padding"
    return _nullable_join(left, right, all_connecting_keys, facts, host), "nullable"


def reduce_join_types(join_types: set[JoinType]) -> JoinType:
    if JoinType.FULL in join_types:
        return JoinType.FULL
    has_left = JoinType.LEFT_OUTER in join_types
    has_right = JoinType.RIGHT_OUTER in join_types
    if has_left and has_right:
        return JoinType.FULL
    if has_left:
        return JoinType.LEFT_OUTER
    if has_right:
        return JoinType.RIGHT_OUTER
    return JoinType.INNER


def ensure_content_preservation(
    joins: list[JoinOrderOutput],
    authored_axis_keys: frozenset[str] = frozenset(),
    demanded_domains: frozenset[str] = frozenset(),
    grains: Mapping[str, frozenset[str]] | None = None,
) -> None:
    """`grains` is each side's grain: a right its join's keys identify is a
    lookup, whose unmatched rows are its domain's, not an axis's content."""
    for idx, review_join in enumerate(joins):
        predecessors = joins[:idx]
        if review_join.type == JoinType.FULL:
            continue
        has_prior_left = False
        has_prior_right = False
        review_keys: set[str] = set().union(*review_join.keys.values())
        right_grain = (grains or {}).get(review_join.right, frozenset())
        lookup_off_axis = bool(
            right_grain
            and right_grain <= review_keys
            and not review_keys & authored_axis_keys
        )
        for pred in predecessors:
            on_pred_right = pred.right in review_join.lefts
            on_pred_left = any(x in review_join.lefts for x in pred.lefts)
            # A prior FULL padded rows into the accumulated stream, so this
            # join preserves its LEFT stream. It preserves its RIGHT relation
            # too when the FULL is an AUTHORED axis (row intent for both
            # sides' content), or when this join is keyed ON the FULL's own
            # spine (coalesced across both families) and the spine is a domain
            # this merge demands. A join keyed OFF the spine hangs off one
            # family: only get_join_type can license preserving it. Nor is a
            # lookup off the axis the axis's content: a date no sale fell on
            # is no customer's row, however the customers were merged.
            if pred.type == JoinType.FULL and (on_pred_right or on_pred_left):
                has_prior_left = True
                pred_keys: set[str] = set().union(*pred.keys.values())
                if (
                    review_keys
                    and review_keys <= pred_keys
                    and review_keys <= demanded_domains
                ) or (pred_keys & authored_axis_keys and not lookup_off_axis):
                    has_prior_right = True
                continue
            # Either way the padded relation is in the ACCUMULATED stream,
            # the left of this join, so that is the side to preserve.
            # Preserving this join's own right relation after a prior RIGHT
            # handed it unmatched rows nothing licensed: the users dimension
            # padded into a solid fact stream (`orders RIGHT JOIN items`, then
            # users keyed off orders), and a CASE over its columns took its
            # ELSE on the padding.
            if (pred.type == JoinType.LEFT_OUTER and on_pred_right) or (
                pred.type == JoinType.RIGHT_OUTER and on_pred_left
            ):
                has_prior_left = True
        # a prior right preservation only comes with a prior left one
        if has_prior_left:
            widened = (
                JoinType.FULL
                if has_prior_right or review_join.type == JoinType.RIGHT_OUTER
                else JoinType.LEFT_OUTER
            )
            if widened != review_join.type:
                review_join.own_type = review_join.type
                review_join.type = widened


def _keyless_order(facts: JoinFacts, ds: str) -> tuple[bool, tuple[int, str]]:
    """A keyless side joins in the stated order; one holding no column of
    its own joins after."""
    others = frozenset().union(
        *(side.columns for node, side in facts.sides.items() if node != ds)
    )
    return (not facts.side(ds).columns - others, facts.rank(ds))


def _join_onto(right: str, walked: list[str], facts: JoinFacts) -> JoinOrderOutput:
    """`right` joined onto every side already walked that the plan pairs it
    with, on what the plan pairs them on. Of two lefts on the same keys, the
    one walked first is kept and the other is redundant."""
    joinkeys: dict[str, set[str]] = {}
    join_types: set[JoinType] = set()
    deduped: list[tuple[str, set[str]]] = []
    for left in walked:
        keys = set(facts.paired(left, right))
        if not keys:
            continue
        # A FULL-join key must keep EVERY left source that provides it: the
        # row may exist on only one of them, so the ON clause has to coalesce
        # across all (`coalesce(l1.k, l2.k) = r.k`). Non-FULL keys dedup.
        if not keys & facts.full_join_keys and any(
            v == keys
            and not (
                keys & facts.side(left).partials and keys & facts.side(prior).partials
            )
            for prior, v in joinkeys.items()
        ):
            deduped.append((left, keys))
            continue
        join_types.add(get_join_type(left, right, keys, facts, walked))
        joinkeys[left] = keys
    return JoinOrderOutput(
        right=right,
        type=reduce_join_types(join_types),
        keys=joinkeys,
        redundant=dict(deduped),
    )


def _preserved_past_inner(joins: list[JoinOrderOutput]) -> str | None:
    """The first right a join preserves against the sides walked before it
    (RIGHT OUTER) when an earlier join between them is INNER: on the right's
    rows that INNER already dropped every walked side's values wherever its
    partner missed, which no later promotion gives back."""
    inner = False
    for join in joins:
        if join.type == JoinType.RIGHT_OUTER and inner:
            return join.right
        inner = inner or (join.type == JoinType.INNER and bool(join.keys))
    return None


def resolve_join_order_v2(facts: JoinFacts) -> list[JoinOrderOutput]:
    """Order the merge's sides (`facts.sides`) into a join tree by walking
    the plan's pairings (`facts.pair_keys`): two sides pair on what it pairs
    them on, and on nothing else. Each step joins the side stated first
    (`facts.rank`) among the unjoined ones the plan pairs with a joined one;
    a side pairing with none seeds a tree only when none does. The walk
    chooses nothing itself, so the plan is deterministic across runs.

    The stated order (`_stated_order`) is the plan's shape below what each
    key's holders' rows are (`_key_row_terms`). Two more facts about rows
    outrank it. A merge whose rows are one side's states that side (`facts.seed`), and the
    walk starts from it. And the walk repairs a side preserved past an INNER
    join among the sides walked before it (`_preserved_past_inner`): that
    side seeds the walk instead, so each of them hangs off its rows on its
    own pairing.
    ``ensure_content_preservation`` keeps every later join from dropping the
    rows an earlier one preserved.
    """
    output = _walk(facts, facts.seed)
    seed = None if facts.seed else _preserved_past_inner(output)
    if seed is not None:
        output = _walk(facts, seed)
    ensure_content_preservation(
        output,
        facts.axis_keys,
        facts.demanded_domains,
        {node: side.grain for node, side in facts.sides.items()},
    )
    restore_full_join_providers(output)
    return output


def _walk(facts: JoinFacts, seed: str | None = None) -> list[JoinOrderOutput]:
    """One walk over the plan's pairings, from `seed` when given. Each step
    joins the side the merge states first (`facts.rank`) among those the plan
    pairs with a joined one; when it pairs none, the first of the rest seeds
    another island."""
    paired = {
        side
        for pair, keys in facts.pair_keys.items()
        if len(pair) == 2 and keys
        for side in pair
    }
    output: list[JoinOrderOutput] = []
    walked: list[str] = [seed] if seed else []
    eligible_left: set[str] = set(walked)

    while unjoined := paired - eligible_left:
        touching = [
            side
            for side in unjoined
            if any(facts.paired(side, left) for left in walked)
        ]
        right = min(touching or unjoined, key=facts.rank)
        if touching:
            output.append(_join_onto(right, walked, facts))
        elif eligible_left:
            # a second island: its seed joins on nothing and the keyless-join
            # guard judges it
            output.append(JoinOrderOutput(right=right, type=JoinType.INNER, keys={}))
        eligible_left.add(right)
        walked.append(right)

    # Every side the plan pairs joined above; the rest pair with nothing, so
    # they join on nothing and the keyless-join guard judges them. A one-row
    # side (a `by *` total) is empty only when its input is, or a gate on it
    # failed: it cross-joins, never pads.
    for ds in sorted(
        (x for x in facts.sides if x not in eligible_left),
        key=partial(_keyless_order, facts),
    ):
        if eligible_left:
            one_row = facts.side(ds).one_row or all(
                facts.side(x).one_row for x in eligible_left
            )
            output.append(
                JoinOrderOutput(
                    left=min(eligible_left, key=facts.rank),
                    right=ds,
                    type=JoinType.INNER if one_row else JoinType.FULL,
                    keys={},
                )
            )
        eligible_left.add(ds)
    return output


def restore_full_join_providers(joins: list[JoinOrderOutput]) -> None:
    """A FULL join's ON clause coalesces across every left source providing
    its keys, as `is_full_key` pre-empts for registry keys: the row may exist
    on only one of them (a customer whose orders a WHERE rejected is on the
    orders stream, not on the aggregate over them). So does any join whose
    kept provider an earlier join padded, if it preserves rows itself: on the
    padded rows the key is read off the provider left out (a product a
    user's lines name, beside a total the WHERE emptied). Read after the types are final, so a join
    `ensure_content_preservation` widened gets its providers back too;
    `prune_preserved_join_keys` drops the ones no join padded."""
    padded: set[str] = set()
    joined: set[str] = set()
    for join in joins:
        joined |= join.lefts
        if join.type == JoinType.FULL or (
            join.type in OUTER_JOIN_TYPES and padded & set(join.keys)
        ):
            for left, keys in join.redundant.items():
                join.keys.setdefault(left, keys)
        if join.type in PADS_RIGHT_JOIN_TYPES:
            padded.add(join.right)
        if join.type in PADS_LEFT_JOIN_TYPES:
            padded |= joined
        joined.add(join.right)


def get_modifiers(
    left_concept: BuildConcept,
    right_concept: BuildConcept,
    left: DataSource | None,
    right: DataSource | None,
    memo: ProvenanceMemo | None = None,
    anchored: bool = True,
) -> list[Modifier]:
    """Use null-safe equality only when both exposed join keys can be NULL.

    Asymmetric padding is the exception: when one side's NULLs are outer-join
    extension (absence) and the other's are values, they name nothing in
    common and null-safe equality cross-joins them. Both sides extended is the
    ordinary case again: the padding shares provenance, so those rows pair,
    as long as a solid key of the join says which padded row is which
    (`anchored`) or both sides' rows passed the same filters."""
    if not (side_nullable(left_concept, left) and side_nullable(right_concept, right)):
        return []
    assert left is not None and right is not None
    memo = memo or ProvenanceMemo()
    values = memo.of(left).values(left_concept)
    if values != memo.of(right).values(right_concept):
        return []
    if not values and not anchored and not _same_filters(left, right):
        return []
    return [Modifier.NULLABLE]


def _pairs_solid(
    left_concept: BuildConcept,
    right_concept: BuildConcept,
    left: DataSource,
    right: DataSource,
) -> bool:
    return not side_nullable(left_concept, left) and not side_nullable(
        right_concept, right
    )


def _same_filters(left: DataSource, right: DataSource) -> bool:
    """Whether both sides' rows passed the same filters. A padded NULL names
    no member, so a pairing on it does not restrict either side: a lookup
    that skipped the WHERE pairs every padded row it has (each customer with
    no order, of any nation) with the other side's."""
    lefts, rights = applied_atoms(left), applied_atoms(right)
    return all(any(a == b for b in rights) for a in lefts) and all(
        any(a == b for b in lefts) for a in rights
    )


def preserved_sources(
    datasets: list[DataSource], joins: list[SourceJoin | UnnestJoin]
) -> set[str]:
    """Identifiers of the sources whose every row survives the join tree.
    Joins are left-deep: a join that does not preserve its left side drops
    rows of everything joined so far, and an UNNEST can drop rows too. A
    right the join does not preserve keeps every row all the same when each
    has a partner (`_partners_every_right_row`)."""
    right_ids = joined_right_ids(joins)
    alive = {ds.identifier for ds in datasets if ds.identifier not in right_ids}
    for join in joins:
        if not isinstance(join, Join):
            alive = set()
            continue
        right_kept = join.join_type in PADS_LEFT_JOIN_TYPES or (
            _partners_every_right_row(join, alive)
        )
        if join.join_type not in PADS_RIGHT_JOIN_TYPES:
            alive = set()
        if right_kept:
            alive.add(join.right.identifier)
    return alive


def _partners_every_right_row(join: Join, alive: set[str]) -> bool:
    """A join on one key from a left whose every row survives and which holds
    the key's whole domain: every right row with a key has a partner, so only
    a NULL key on the right drops one (`trees LEFT JOIN ~species`)."""
    pairs = join.pairs or []
    if len(pairs) != 1:
        return False
    pair = pairs[0]
    return (
        pair.node.identifier in alive
        and complete_key_domain(pair.node, pair.left.address)
        and all(c.address != pair.right.address for c in join.right.nullable_concepts)
    )


def merge_partial_addresses(
    datasets: list[DataSource],
    joins: list[SourceJoin | UnnestJoin],
    outputs: list[BuildConcept],
) -> set[str]:
    """Output addresses a merge binds partially, from its sides' own stamps
    and the resolved join types. A fully preserved side binding the address
    complete makes it complete (the merge coalesces every present member);
    with no such side the rows come from the matched intersection, so any
    partial binding keeps the address partial."""
    preserved = preserved_sources(datasets, joins)
    bound = [
        (
            ds.identifier in preserved,
            {c.address for c in ds.output_concepts},
            {c.address for c in ds.partial_concepts},
        )
        for ds in datasets
    ]
    out: set[str] = set()
    for concept in outputs:
        address = concept.address
        sides = [
            (kept, address in partial)
            for kept, output, partial in bound
            if address in output
        ]
        if not sides:
            continue
        kept_sides = [partial for kept, partial in sides if kept]
        if kept_sides:
            if all(kept_sides):
                out.add(address)
        elif any(partial for _, partial in sides):
            out.add(address)
    return out


def partial_binding_sources(ds: DataSource, address: str) -> frozenset[str]:
    """Identifiers of the leaf tables whose own ``~`` column on ``address``
    makes this source partial against it.

    Two sides with the SAME set are two projections of one binding: whatever
    subset of the key they cover, they cover the same one, and neither holds
    members the other lacks. Different sets are peer facts (sales and returns
    each referencing their own slice of the group domain), and a join between
    them owes both sides' rows."""
    if isinstance(ds, BuildDatasource):
        return (
            frozenset({ds.identifier})
            if address in ds.column_level_partial_addresses
            else frozenset()
        )
    out: frozenset[str] = frozenset()
    for sub in ds.datasources:
        out |= partial_binding_sources(sub, address)
    # a rowset boundary built not to extend a span (its reader holds the
    # region) is partial on the handle with no leaf `~` behind it: the
    # boundary is the binding, and two projections of it are one
    if not out and any(c.address == address for c in ds.partial_concepts):
        return frozenset({ds.identifier})
    return out


def complete_key_domain(ds: DataSource, address: str) -> bool:
    """Whether this source carries every value of ``address``: an unfiltered
    scan binding it complete, or an unfiltered, unlimited read keeping every
    row of one (a group over it, a join preserving it).

    The plan-time half of the optimizer's subset proof
    (``_pair_side_fully_matches``): a `~` binding joined to this side has a
    partner for every row."""
    if any(c.address == address for c in ds.partial_concepts):
        return False
    if isinstance(ds, BuildDatasource):
        return (
            ds.where is None
            and ds.non_partial_for is None
            and any(c.address == address for c in ds.output_concepts)
        )
    if ds.condition is not None or ds.limit is not None:
        return False
    preserved = preserved_sources(ds.datasources, ds.joins)
    return any(
        sub.identifier in preserved and complete_key_domain(sub, address)
        for sub in ds.datasources
    )


def _tree_union(
    ds: DataSource, attr: Callable[[QueryDatasource], frozenset[str]]
) -> frozenset[str]:
    if not isinstance(ds, QueryDatasource):
        return frozenset()
    out = attr(ds)
    for sub in ds.datasources:
        out |= _tree_union(sub, attr)
    return out


def deep_extent_free_spans(ds: DataSource) -> frozenset[str]:
    """Spans anything in this source's tree was built not to extend.

    Kept off the identifier (unlike a scan's own ``extent_free_spans``, which
    is identity): a wrapper does not change what it wraps, and folding the
    inherited set into wrapper names splits CTEs that should stay shared."""
    return _tree_union(ds, attrgetter("extent_free_spans"))


def deep_extent_free_carried(ds: DataSource) -> frozenset[str]:
    """What the region domains of `deep_extent_free_spans` carry: held in this
    source's tree for the members its facts bound only."""
    return _tree_union(ds, attrgetter("extent_free_carried"))


def _is_authored_pair(pair: ConceptPair, members: Collection[str]) -> bool:
    return pair.left.address in members or pair.right.address in members


class _Agreement:
    """Which left nodes earlier joins equated on a column: a pair read off one
    node is implied by another node's pairs only where the two agree. Not
    `strict`, every node agrees (a lone left side, or no join history)."""

    def __init__(self, strict: bool = False) -> None:
        self.strict = strict
        self.parent: dict[tuple[str, str], tuple[str, str]] = {}

    def _find(self, item: tuple[str, str]) -> tuple[str, str]:
        while (up := self.parent.get(item, item)) != item:
            item = up
        return item

    def union(self, a: tuple[str, str], b: tuple[str, str]) -> None:
        self.parent[self._find(a)] = self._find(b)

    def same(self, left: str, right: str, address: str) -> bool:
        return (
            not self.strict
            or left == right
            or self._find((left, address)) == self._find((right, address))
        )


def _whole_coalesce_groups(
    pairs: list[ConceptPair], kept: list[ConceptPair]
) -> list[ConceptPair]:
    """`kept` with every pair of a key group it keeps any of: an outer join
    renders the group `coalesce(a.k, b.k) = c.k`, and dropping `a.k` alone
    leaves `b.k`, which a padded `b` holds NULL on every row it missed
    (`prune_outer_join_pairs` then keeps the side nothing pads)."""
    kept_ids = {id(pair) for pair in kept}
    restore = {
        id(pair)
        for group in coalesced_key_groups(pairs, _pair_source)
        if any(id(p) in kept_ids for p in group)
        for pair in group
    }
    seen: set[tuple[str, str]] = set()
    out: list[ConceptPair] = []
    for pair in pairs:
        key = (pair.right.address, pair.node.identifier)
        if (id(pair) in kept_ids or id(pair) in restore) and key not in seen:
            seen.add(key)
            out.append(pair)
    return out


def _aggregate_beside_its_grain(pair: ConceptPair, pairs: list[ConceptPair]) -> bool:
    """An aggregate paired with itself beside every key of its grain: the
    grain's pairs decide it, NULL groups included, where its own `=` would drop
    a group whose value is NULL."""
    concept = pair.left
    if (
        concept.address != pair.right.address
        or concept.derivation != Derivation.AGGREGATE
    ):
        return False
    grain = set(concept.grain.components)
    others = [
        o
        for o in pairs
        if o.node.identifier == pair.node.identifier
        and o.left.address == o.right.address
        and o.left.address != concept.address
    ]
    return bool(others) and grain <= {o.left.address for o in others}


def reduce_concept_pairs(
    pairs: list[ConceptPair],
    right_source: DataSource,
    join_type: JoinType = JoinType.INNER,
    domain_graph: DomainGraph | None = None,
    agreement: _Agreement | None = None,
) -> list[ConceptPair]:
    left_keys = {
        pair.left.address for pair in pairs if pair.left.purpose == Purpose.KEY
    }
    right_keys = {
        pair.right.address for pair in pairs if pair.right.purpose == Purpose.KEY
    }
    grain_components = set(right_source.grain.components)
    # An authored join key member (`subset`/`equal`/`union`) pairs by its own
    # physical column as part of the join's semantics. FD/grain implication
    # holds within one entity, not across independently-authored sides, so
    # inferring such a pair away changes which rows match.
    authored_members: Collection[str] = (
        domain_graph.authored_join_members() if domain_graph else frozenset()
    )
    # FD-closure pruning (docs/domain_graph_design.md step 4): a pair both of
    # whose sides are functionally determined by the SURVIVING joined keys is
    # redundant, since equality on the determinants implies equality here.
    # A property paired beside its own keys is the simplest case (its keys ->
    # property FD is declared); the closure also sees transitive dependencies
    # and grain FDs carried through complete bindings. Greedy over a working
    # determinant set so mutually-dependent keys keep exactly one pair; grain
    # pairs are never pruned (the grain restriction below relies on them).
    # Only a key paired on plain equality vouches for its dependents. A
    # null-safe pair also matches NULL to NULL, and an FD says nothing about
    # rows with no key: two extension families both pad `item_id`, and
    # `product_id` is the one pair that still tells them apart.
    # Equality on one left node's keys says nothing about another left node's
    # columns unless an earlier join equated the two nodes on them
    # (`agreement`): `d0.species = r.species` leaves `gate.tree_form` free
    # when the gate joined on `city` alone.
    null_safe_left = {pair.left.address for pair in pairs if pair.is_nullable}
    null_safe_right = {pair.right.address for pair in pairs if pair.is_nullable}
    agree = agreement or _Agreement()
    fd_pruned: set[int] = set()
    if domain_graph is not None and domain_graph.fd_edges:
        working = [
            pair
            for pair in pairs
            if pair.left.address in left_keys - null_safe_left
            and pair.right.address in right_keys - null_safe_right
        ]
        for index, pair in enumerate(pairs):
            left_addr, right_addr = pair.left.address, pair.right.address
            if right_addr in grain_components:
                continue
            if _is_authored_pair(pair, authored_members):
                continue
            node = pair.node.identifier
            determinants = [
                other
                for other in working
                if agree.same(node, other.node.identifier, other.left.address)
            ]
            determinant_left = {o.left.address for o in determinants} - {left_addr}
            determinant_right = {o.right.address for o in determinants} - {right_addr}
            if not (determinant_left and determinant_right):
                continue
            if domain_graph.determines(
                determinant_left, left_addr
            ) and domain_graph.determines(determinant_right, right_addr):
                fd_pruned.add(index)
                working = [
                    o
                    for o in working
                    if (o.left.address, o.right.address) != (left_addr, right_addr)
                ]
    final: list[ConceptPair] = []
    seen: set[tuple[str, str]] = set()
    is_outer = join_type in OUTER_JOIN_TYPES
    right_left_seen: dict[tuple[str, str], bool] = {}
    for index, pair in enumerate(pairs):
        dedup_key = (pair.right.address, pair.node.identifier)
        if dedup_key in seen:
            continue
        rl_key = (pair.right.address, pair.left.address)
        if (
            rl_key in right_left_seen
            and not is_outer
            and not (right_left_seen[rl_key] or pair.is_partial)
        ):
            continue
        if index in fd_pruned or _aggregate_beside_its_grain(pair, pairs):
            continue

        seen.add(dedup_key)
        right_left_seen[rl_key] = right_left_seen.get(rl_key, False) or pair.is_partial
        final.append(pair)
    all_keys = {x.right.address for x in final}
    if (
        right_source.grain.components
        and right_source.grain.components.issubset(all_keys)
        and not right_source.grain.components & null_safe_right
    ):
        grain_pairs = [
            x for x in final if x.right.address in right_source.grain.components
        ]
        final = [
            x
            for x in final
            if x.right.address in right_source.grain.components
            or _is_authored_pair(x, authored_members)
            or not all(
                any(
                    y.right.address == component
                    and agree.same(x.node.identifier, y.node.identifier, y.left.address)
                    for y in grain_pairs
                )
                for component in right_source.grain.components
            )
        ]
    return _whole_coalesce_groups(pairs, final) if is_outer else final


def _sole_projected_relation(ds: DataSource) -> str | None:
    """The identifier of the one relation this source only projects (and
    possibly dedups): a single parent, and nothing it computes itself changes
    the row set. Two such sources over the same relation hold the SAME rows
    under different columns, so pairing them is never a cross product no matter
    what their axes look like. An aggregating source is excluded: `sum(x) by k1`
    beside `sum(y) by k2` over one scan is an authored fan-out, not a lost key.
    """
    if not isinstance(ds, QueryDatasource) or len(ds.datasources) != 1:
        return None
    if any(
        concept.purpose == Purpose.METRIC or concept.derivation == Derivation.AGGREGATE
        for concept in ds.output_concepts
    ):
        return None
    return ds.datasources[0].identifier


def _keyless_metric_grain(ds: DataSource) -> bool:
    """Every grain component is a metric this source emits with no keys."""
    output_by_addr = {c.address: c for c in ds.output_concepts}
    return all(
        (c := output_by_addr.get(component)) is not None
        and c.purpose == Purpose.METRIC
        and not c.keys
        for component in ds.grain.components
    )


def _row_independent(ds: DataSource) -> bool:
    """True when cross-joining this source cannot fan out row counts: no
    grain, an authored literal fan-out, or every column it exposes is
    single-row (the `utility.calculate_graph_relevance` rule: a single-row
    concept can always be crossjoined)."""
    if not ds.grain.components:
        return True
    # An UNNEST source here is the standalone literal flavor (`unnest([1,2,3])
    # as value` beside an unrelated scan): its fan-out is authored by the
    # query, not a lost join key. A row-correlated unnest rides an UnnestJoin,
    # never a keyless SourceJoin. One carrying its parent's columns beside
    # the elements (`group_id` beside `unnest(group_regions)`) is correlated.
    if isinstance(ds, QueryDatasource) and ds.source_type == SourceType.UNNEST:
        elements = {c.address for c in ds.join_derived_concepts}
        return all(c.address in elements for c in ds.output_concepts)
    outputs = ds.output_concepts
    if bool(outputs) and all(c.granularity == Granularity.SINGLE_ROW for c in outputs):
        return True
    # A global-aggregate scalar carries a SELF-grain (grain = the metric
    # itself) rather than an empty grain; with no keys there is no row axis to
    # pair on and the cross join is the plan (the `calculate_graph_relevance`
    # metric rule). A KEYED metric is a per-group aggregate with a real axis,
    # so it stays subject to the guard.
    return _keyless_metric_grain(ds)


def _closure_keys(
    concept: BuildConcept, environment: BuildEnvironment | None
) -> frozenset[str]:
    """The keys whose rows determine `concept`: its declared keys, or for a
    bare key the rows of every table binding it."""
    if environment is None:
        return frozenset(concept.keys or ())
    # Imported lazily: the v4 helper package imports join resolution.
    from trilogy.core.processing.v4_helper.functional_dependency import row_keys

    return row_keys(concept, environment)


def _raise_if_keyless_row_bearing_join(
    joins: list[JoinOrderOutput],
    ds_node_map: dict[str, DataSource],
    canonical: dict[str, str],
    rollup_padded_addresses: frozenset[str],
    environment: BuildEnvironment | None,
) -> None:
    """A keyless join between row-bearing sources is a planner bug when the
    sides SHARE a join axis the planner failed to use, or when they are two
    projections of ONE relation (``_sole_projected_relation``) and so hold the
    same rows however their axes look. The axis test is FD-aware: one side's
    outputs (hidden included, since hiding is how an axis gets lost) closed
    over row keys and pseudonyms, intersected with the
    other side's direct addresses, after canonicalization. Axis-DISJOINT
    row-bearing sides off DIFFERENT relations cross-join legitimately
    (selecting an aggregate without its grouping key is an authored fan-out),
    as does a row-independent side (constant / global-aggregate scalar).
    ROLLUP-padded keys are excluded: subtotal rows NULL them, so consumers
    deliberately avoid joining on them.

    Hard failure by design: silently shipping the cartesian is the worse
    outcome. When this fires, the fix belongs upstream in the demand/contract
    passes that let the axis go missing, not in relaxing the check."""

    # Both axis views are pure functions of the node and every keyless join
    # re-asks them for the same sources; memoize so the guard stays
    # proportional to the tree rather than to joins x tree.
    direct_cache: dict[str, frozenset[str]] = {}
    closure_cache: dict[str, frozenset[str]] = {}
    independent_cache: dict[str, bool] = {}

    def _canon(addr: str) -> str:
        return canonical.get(addr, addr)

    def row_independent(node: str) -> bool:
        if node not in independent_cache:
            independent_cache[node] = _row_independent(ds_node_map[node])
        return independent_cache[node]

    def direct_axis(node: str) -> frozenset[str]:
        """Addresses this source can actually be JOINED ON: the columns it
        projects. Hidden outputs count (rendered, just masked). A grain
        component the source never emits does NOT count: you cannot join on a
        column that isn't there."""
        if node in direct_cache:
            return direct_cache[node]
        ds = ds_node_map[node]
        addrs: set[str] = set()
        for concept in ds.output_concepts:
            addrs.add(concept.address)
            # Pseudonyms are same-value addresses (an alias output IS its
            # source column): the axis a sibling renders under the original
            # name.
            addrs.update(concept.pseudonyms)
        result = frozenset({_canon(a) for a in addrs}) - rollup_padded_addresses
        direct_cache[node] = result
        return result

    def key_closure(node: str) -> frozenset[str]:
        """Direct axis plus everything reachable through row keys
        (`_closure_keys`) and pseudonyms, to fixpoint: the addresses whose rows
        FD-determine this source's PROJECTED values. A rename chain can hide
        its key several environment hops deep. Seeded from outputs only, for
        the same reason as `direct_axis`."""
        if node in closure_cache:
            return closure_cache[node]
        ds = ds_node_map[node]
        frontier: set[str] = set()
        for concept in ds.output_concepts:
            frontier.add(concept.address)
            frontier.update(concept.pseudonyms)
            frontier.update(_closure_keys(concept, environment))
        closure: set[str] = set()
        while frontier:
            addr = frontier.pop()
            if addr in closure:
                continue
            closure.add(addr)
            if environment is None:
                continue
            concept_ref = environment.concepts.get(
                addr
            ) or environment.alias_origin_lookup.get(addr)
            if concept_ref is None:
                continue
            frontier.update(_closure_keys(concept_ref, environment))
            frontier.update(concept_ref.pseudonyms)
        result = frozenset({_canon(a) for a in closure}) - rollup_padded_addresses
        closure_cache[node] = result
        return result

    # the tree's base is no join's right side: a keyless join onto it names
    # no left
    tree = set(ds_node_map) - {j.right for j in joins}
    for j in joins:
        if j.left is not None:
            tree.add(j.left)
        tree.update(j.lefts)
        if not j.keys and not row_independent(j.right):
            right_direct = direct_axis(j.right)
            right_closure = key_closure(j.right)
            right_relation = _sole_projected_relation(ds_node_map[j.right])
            offenders = sorted(
                d
                for d in tree
                if not row_independent(d)
                and (
                    right_closure & direct_axis(d)
                    or right_direct & key_closure(d)
                    # Axis-disjoint but the same rows: two projections of one
                    # relation are correlated by construction, so the shared
                    # axis test never sees them (a union-TVF arm's key and
                    # value close over different domains).
                    or (
                        right_relation is not None
                        and right_relation == _sole_projected_relation(ds_node_map[d])
                    )
                )
            )
            if offenders:
                raise UnresolvableQueryException(
                    "Planner emitted a keyless join between row-bearing sources "
                    "that share a join axis or one source relation: "
                    f"{ds_node_map[j.right].identifier} onto "
                    f"{', '.join(ds_node_map[d].identifier for d in offenders)}. "
                    "This would render as a cross join (ON 1=1) and fan out; "
                    "the join axis was lost upstream. This is a planner bug."
                )
        tree.add(j.right)


def single_row_source(ds: DataSource) -> bool:
    """True when ``ds`` provably emits exactly one row: an ungrouped
    aggregate computing every output here (a passed-through column would be
    a group key), with no LIMIT, no ROLLUP and at most a scalar WHERE (which
    filters the aggregate's input, never its single output row), or an
    unfiltered, unjoined projection over one such source. A HAVING can
    delete that row, so a non-scalar condition disqualifies."""
    if not isinstance(ds, QueryDatasource):
        return False
    if ds.source_type in (SourceType.UNION, SourceType.RECURSIVE, SourceType.UNNEST):
        return False
    # a re-aggregate of a parent's partials is computed here, as any
    # aggregate is; one of a summary's stored column is not claimed
    reaggregated = ds.partial_sources.keys()
    if ds.limit is not None or any(
        c.address not in reaggregated for c in ds.rollup_concepts
    ):
        return False
    if not ds.group_required:
        return (
            not ds.joins
            and ds.condition is None
            and len(ds.datasources) == 1
            and single_row_source(ds.datasources[0])
        )
    outputs = ds.output_concepts
    if any(
        ds.source_map.get(c.address) and c.address not in reaggregated for c in outputs
    ):
        return False
    if not _keyless_metric_grain(ds):
        return False
    if not any(get_grouped_aggregate_wrapper(c) is not None for c in outputs):
        return False
    if ds.condition is None:
        return True
    materialized = {address for address, v in ds.source_map.items() if v}
    return is_scalar_condition(ds.condition, materialized=materialized)


def _gate_total(source: DataSource, gates: Collection[str], single: bool) -> bool:
    """A total only the WHERE reads holds a row wherever its atom can keep one:
    on no rows the atom reads NULL and keeps none, so it pairs as one row,
    however it was computed (a re-sum of stored partials included)."""
    return single or (source.identifier in gates and not source.grain.components)


def narrow_keyless_joins(
    joins: list[SourceJoin | UnnestJoin], gates: Collection[str] = frozenset()
) -> None:
    """A keyless outer join (``ON 1=1``) pairs every row with every row, so it
    only differs from INNER when a side is EMPTY. A side that provably holds
    one row pairs with every row of the other, and when the other is empty
    there is no row for it to pair with: a statement-wide gate (`count(x) by *
    > 2`, FULL narrowed to RIGHT by the WHERE's proof) beside a WHERE that
    empties the row stream must not come back as one NULL-padded row, so the
    join is INNER. Walk the joins in order carrying whether the relation built
    so far provably has rows. While only keyless outer joins precede, a join's
    explicit left is part of that relation and its rows count; a keyed or
    unnest join can empty the relation, so after one only the keyless right
    sides accumulate.

    Grand totals joined only to each other stay outer: each is the `()`
    group of its own rows, so the result has that group when any of them
    does, as a union-merged key keeps a key from either side. A `gates`
    total (one only the WHERE reads) is not the select's group: it holds a
    row whether or not the WHERE keeps any, so it never keeps the join outer."""
    left_has_rows = False
    all_totals = True
    keyed_seen = False
    for join in joins:
        if (
            not isinstance(join, Join)
            or join.join_type not in OUTER_JOIN_TYPES
            or join.pairs
            or join.concepts
        ):
            left_has_rows = False
            all_totals = False
            keyed_seen = True
            continue
        if join.left is not None and not keyed_seen:
            left_single = single_row_source(join.left)
            left_has_rows = left_has_rows or _gate_total(join.left, gates, left_single)
            all_totals = all_totals and left_single
            all_totals = all_totals and join.left.identifier not in gates
        right_single = single_row_source(join.right)
        right_has_rows = _gate_total(join.right, gates, right_single)
        all_totals = all_totals and right_single
        all_totals = all_totals and join.right.identifier not in gates
        if (left_has_rows or right_has_rows) and not all_totals:
            join.join_type = JoinType.INNER
        left_has_rows = left_has_rows or right_has_rows


def _span_spellings(
    spans: frozenset[str],
    environment: BuildEnvironment,
    witnessed: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Every address a join pair can spell one of `spans` with -> one name for
    it. A scoped join substitutes its canonical for the member bound `~`
    (`subset join pr.item.sk = ss.item.sk` keys the join on `ss.item.sk`); a
    rowset body pads under its own spelling of the handle the plan reads
    (`witnessed`), and a spelling the plan itself uses keeps its own name."""
    out = {span: span for span in spans}
    for canonical, members in environment.scoped_join_key_groups.items():
        group = {canonical, *members}
        if group & spans:
            out.update(dict.fromkeys(group, canonical))
    for below, handle in (witnessed or {}).items():
        out.setdefault(below, out.get(handle, handle))
    return out


def _span_padding_matrix(
    ds_node_map: dict[str, DataSource],
    nullables: Mapping[str, frozenset[str]],
    spellings: dict[str, str],
    canon_node: Callable[[str], str],
    memo: ProvenanceMemo | None = None,
) -> dict[str, dict[str, frozenset[str]]]:
    """Per side, per nullable key: the spans (as `held_spans` spells them)
    whose extension rows NULL it."""
    memo = memo or ProvenanceMemo()
    out: dict[str, dict[str, frozenset[str]]] = {}
    for ds_node, datasource in ds_node_map.items():
        by_key: dict[str, set[str]] = defaultdict(set)
        for spelling in sorted(spellings):
            for address in span_padded_addresses(
                datasource, frozenset({spelling}), memo
            ):
                by_key[canon_node(address)].add(canon_node(spellings[spelling]))
        out[ds_node] = {
            key: frozenset(found)
            for key, found in by_key.items()
            if key in nullables[ds_node]
        }
    return out


def _hidden_addresses(datasource: DataSource) -> set[str]:
    return {
        h.address if isinstance(h, BuildConcept) else h
        for h in datasource.hidden_concepts
    }


@dataclass(frozen=True)
class _Merge:
    """A merge's sides as its joins are built: by node name, in the merge's
    spelling (`canon_node`)."""

    facts: JoinFacts
    sources: dict[str, DataSource]
    # (side, key) -> the concept the side spells the key with
    concepts: dict[tuple[str, str], BuildConcept]
    spellings: dict[str, str]
    canon_node: Callable[[str], str]
    memo: ProvenanceMemo


def _padding_witness(
    merge: _Merge, node: str, spans: frozenset[str]
) -> list[BuildConcept]:
    """Columns of side `node` that are NULL exactly on the rows padded for
    `spans`: one per span, a KEY padded by it that carries no value NULL (the
    solid grain's, absent on a row standing in for a region's member; a
    property is NULL on a `~?` NULL member's row too) and no extent NULL (a
    key an outer join found no partner for is NULL on solid rows too). Empty
    when a span has none."""
    datasource = merge.sources[node]
    out: list[BuildConcept] = []
    hidden = _hidden_addresses(datasource)
    provenance = merge.memo.of(datasource)
    extent = provenance.extent
    for span in sorted(spans):
        padded: set[str] = set()
        for spelling, named in merge.spellings.items():
            if merge.canon_node(named) == span:
                padded |= provenance.padded_by(frozenset({spelling}))
        witnesses = sorted(
            (
                c
                for c in datasource.output_concepts
                if c.purpose == Purpose.KEY
                and c.address in padded
                and c.address not in hidden
                and c.address not in extent
                and not provenance.values(c)
            ),
            key=lambda c: c.address,
        )
        if not witnesses:
            return []
        if witnesses[0] not in out:
            out.append(witnesses[0])
    return out


def _row_witness(
    datasource: DataSource, prefer: list[BuildConcept]
) -> BuildConcept | None:
    """A column of `datasource` NULL exactly where the side has no row: a KEY
    it never emits NULL for, the join key it was paired on first."""
    hidden = _hidden_addresses(datasource)
    emitted = {c.address for c in datasource.output_concepts}
    for c in (*prefer, *sorted(datasource.output_concepts, key=lambda c: c.address)):
        if (
            c.purpose == Purpose.KEY
            and c.address in emitted
            and c.address not in hidden
            and not side_nullable(c, datasource)
        ):
            return c
    return None


def _mark_present(datasource: QueryDatasource, marker: BuildConcept) -> None:
    """A UNION's arms each project the marker, so the union has it."""
    datasource.presence_marker = marker
    if datasource.source_type == SourceType.UNION:
        for arm in datasource.datasources:
            assert isinstance(arm, QueryDatasource)
            _mark_present(arm, marker)


def _presence_marker(datasource: DataSource) -> BuildConcept | None:
    """A constant `datasource` projects so a join can tell its padded rows
    from its rows: the side had no column NULL exactly there."""
    if not isinstance(datasource, QueryDatasource):
        return None
    if datasource.presence_marker is not None:
        return datasource.presence_marker
    name = f"{PRESENCE_MARKER_PREFIX}{string_to_hash(datasource.identifier)}"
    marker = BuildConcept(
        name=name,
        canonical_name=name,
        namespace=DEFAULT_NAMESPACE,
        datatype=DataType.INTEGER,
        purpose=Purpose.CONSTANT,
        derivation=Derivation.CONSTANT,
        grain=BuildGrain(),
        build_is_aggregate=False,
        lineage=BuildFunction(
            operator=FunctionType.CONSTANT,
            arguments=[1],
            output_data_type=DataType.INTEGER,
            output_purpose=Purpose.CONSTANT,
        ),
    )
    _mark_present(datasource, marker)
    return marker


@dataclass
class _MergePadding:
    """Rows an earlier join of this merge null-extended every `padded` side
    on, to carry the regions `host` holds (`spans`). `keys`: what each side
    was paired on. `absent`: every side with no row there, the padded ones
    and each later side that could not pair with them, or paired with them
    only through its own NULL group of those rows."""

    padded: frozenset[str]
    host: str
    spans: frozenset[str]
    keys: dict[str, set[str]]
    absent: set[str]


def _merge_paddings(
    joins: list[JoinOrderOutput], facts: JoinFacts
) -> list[list[_MergePadding]]:
    """The padding each join adds for the regions of a side it pairs with: a
    LEFT pads its right for each left holding any, a RIGHT pads every side
    already joined for the right's; a FULL does both."""
    out: list[list[_MergePadding]] = []
    joined: set[str] = set()
    for join in joins:
        joined |= join.lefts | ({join.left} if join.left else set())
        added: list[_MergePadding] = []
        if join.type in PADS_RIGHT_JOIN_TYPES:
            added += [
                _MergePadding(
                    padded=frozenset({join.right}),
                    host=left,
                    spans=facts.side(left).held_spans,
                    keys={left: keys, join.right: keys},
                    absent={join.right},
                )
                for left, keys in sorted(join.keys.items())
                if facts.side(left).held_spans
            ]
        if join.type in PADS_LEFT_JOIN_TYPES and facts.side(join.right).held_spans:
            added.append(
                _MergePadding(
                    padded=frozenset(joined),
                    host=join.right,
                    spans=facts.side(join.right).held_spans,
                    keys={
                        **join.keys,
                        join.right: set().union(*join.keys.values()),
                    },
                    absent=set(joined),
                )
            )
        out.append(added)
        joined.add(join.right)
    return out


def _region_padded_sides(
    paddings: list[list[_MergePadding]],
) -> dict[str, frozenset[str]]:
    """Each side an earlier join of the merge null-extends, with the region
    spans the side preserved over it holds: the side's columns are NULL on
    those regions' rows in the joined stream, whatever the side says of
    itself."""
    padded: dict[str, frozenset[str]] = defaultdict(frozenset)
    for padding in (p for added in paddings for p in added):
        for side in padding.padded:
            padded[side] |= padding.spans
    return {side: held for side, held in padded.items() if held}


def _side_witness(
    side: str, keys: Collection[str], merge: _Merge
) -> BuildConcept | None:
    source = merge.sources[side]
    paired = [
        merge.concepts[(side, k)] for k in sorted(keys) if (side, k) in merge.concepts
    ]
    return _row_witness(source, paired) or _presence_marker(source)


def _padding_clause(
    padding: _MergePadding, presence: set[frozenset[str]], merge: _Merge
) -> tuple[GuardTerm, ...] | None:
    """False exactly on the padding's rows: some padded side has a row (the
    fewest whose rows cover every row any of them has, paired sides first),
    or the host has none (tested only when some row lacks it and every
    padded side). None when a side to test has no witness."""
    rows = [present & padding.padded for present in presence]
    covered: set[str] = set()
    terms: list[tuple[str, bool]] = []
    for side in sorted(padding.padded, key=lambda s: (s not in padding.keys, s)):
        if any(side in row and not row & covered for row in rows):
            covered.add(side)
            terms.append((side, True))
    if any(
        padding.host not in present and not row for present, row in zip(presence, rows)
    ):
        terms.append((padding.host, False))
    tested: list[GuardTerm] = []
    for side, present in terms:
        witness = _side_witness(side, padding.keys.get(side, ()), merge)
        if witness is None:
            return None
        tested.append(GuardTerm(witness, merge.sources[side], present))
    return tuple(tested) or None


def _absent_key_groups(
    pairs: list[ConceptPair], absent: set[str]
) -> list[list[ConceptPair]]:
    """The join's keys (its pairs grouped by right concept, as they render
    coalesced) NULL by absence on rows where no `absent` side has a row:
    every left side providing them is one of those."""
    groups: dict[str, list[ConceptPair]] = defaultdict(list)
    for pair in pairs:
        groups[pair.right.address].append(pair)
    return [
        group
        for group in groups.values()
        if all(f"ds~{p.node.identifier}" in absent for p in group)
    ]


def _pads_beside(other: SideFacts, key: str, spans: frozenset[str]) -> bool:
    """`other` holds those regions' rows too: its NULL group on `key` is the
    same rows, and pairs."""
    return bool(
        other.held_spans & spans or other.span_padding.get(key, frozenset()) & spans
    )


def _equality_drops_padding(
    pairs: list[ConceptPair],
    padded: SideFacts,
    spans: frozenset[str],
    canon_node: Callable[[str], str],
) -> bool:
    """The join compares with `=` a key those regions also pad: the padded
    rows are NULL on it and pair with nothing."""
    return any(
        not pair.is_nullable
        and spans
        <= padded.span_padding.get(canon_node(pair.right.address), frozenset())
        for pair in pairs
    )


def _padding_guard(
    join: JoinOrderOutput,
    merge: _Merge,
    earlier: list[_MergePadding],
    pairs: list[ConceptPair],
    presence: set[frozenset[str]],
) -> JoinGuard:
    """A key NULL by absence never pairs with a value-NULL group.

    A side whose NULLs on a key are both (`pstatus`, a no-ELSE CASE, on a
    stream a region padded) keeps the null-safe pairing for its value NULLs
    and excludes its padded rows: `order_id is not null`, a column NULL
    exactly there. Padding an earlier join of this merge added (`customers
    LEFT orders`, then `orders.bucket` paired with `targets.bucket`) is the
    rows where the host has a row and the padded side none:
    `orders.customer_id is not null or customers.customer_id is null`, the
    second test dropped when no row lacks both sides. The
    other side's NULL group is a value of rows that are not the region's,
    unless it holds the region too (`_pads_beside`)."""
    facts = merge.facts
    clauses: list[tuple[GuardTerm, ...]] = []
    for left_node, keys in join.keys.items():
        for padded_node, other_node in (
            (left_node, join.right),
            (join.right, left_node),
        ):
            padded, other = facts.side(padded_node), facts.side(other_node)
            for key in sorted(keys):
                spans = padded.span_padding.get(key, frozenset())
                if (
                    not spans
                    or key not in padded.value_nullables
                    or key not in other.value_nullables
                    or _pads_beside(other, key, spans)
                    or _equality_drops_padding(pairs, padded, spans, merge.canon_node)
                ):
                    continue
                witnesses = _padding_witness(merge, padded_node, spans)
                if not witnesses:
                    logger.info(
                        f"[JOIN_RESOLUTION] no row witness to keep {padded_node}'s"
                        f" padding on {key} from pairing with {other_node}"
                    )
                for witness in witnesses:
                    clause = (GuardTerm(witness, merge.sources[padded_node], True),)
                    if clause not in clauses:
                        clauses.append(clause)
    right = facts.side(join.right)
    for padding in earlier:
        absent = _absent_key_groups(pairs, padding.absent)
        if not absent:
            continue
        if any(
            _pads_beside(right, merge.canon_node(group[0].right.address), padding.spans)
            for group in absent
        ):
            # the right's NULL group is those rows: paired there, its key is
            # their NULL too, and a later side reading it pairs the padding
            padding.absent.add(join.right)
            continue
        if not all(any(p.is_nullable for p in group) for group in absent):
            # a key compared with `=` never pairs a NULL
            padding.absent.add(join.right)
            continue
        tested = _padding_clause(padding, presence, merge)
        if tested is None:
            logger.info(
                f"[JOIN_RESOLUTION] no row witness to keep {sorted(padding.padded)}'s"
                f" padding from pairing on {join.right}"
            )
            continue
        clauses.append(tested)
        padding.absent.add(join.right)
    return tuple(clauses)


def _presence_after(
    presence: set[frozenset[str]], join: JoinOrderOutput
) -> set[frozenset[str]]:
    """Which sides have a row, over the rows of the stream once `join` is
    applied: one set per pattern a row can show."""
    right = frozenset({join.right})
    out = {present | right for present in presence}
    if join.type in PADS_RIGHT_JOIN_TYPES:
        out |= presence
    if join.type in PADS_LEFT_JOIN_TYPES:
        out.add(right)
    return out


def _padding_guards(
    joins: list[JoinOrderOutput],
    base_joins: list[SourceJoin],
    paddings: list[list[_MergePadding]],
    merge: _Merge,
) -> list[JoinGuard]:
    """Each join's guard, in join order: a join is guarded against the
    padding the joins before it added, read off the key pairs it renders."""
    earlier: list[_MergePadding] = []
    presence = {frozenset(joins[0].lefts)} if joins else set()
    out: list[JoinGuard] = []
    for join, base, added in zip(joins, base_joins, paddings):
        out.append(_padding_guard(join, merge, earlier, base.pairs or [], presence))
        earlier += added
        presence = _presence_after(presence, join)
    return out


def _pairs_region_padding(
    padded_for: frozenset[str], right: SideFacts, key: str
) -> bool:
    """The left key is NULL on rows padded for a region `right` holds too: an
    aggregate over the region's rows grouped by a key absent there
    (`count(customer_id) by status`) puts them in its NULL group. The two
    NULLs are the same rows and pair."""
    return bool(padded_for & right.held_spans) and key in right.nullables


def _planned_pair_nodes(
    columns: Mapping[str, frozenset[str]],
    planned_pairs: Mapping[frozenset[str], frozenset[str]],
    environment: BuildEnvironment,
) -> dict[frozenset[str], frozenset[str]]:
    """`planned_pairs` (datasource identifiers -> addresses) in the merge's
    spelling: a pair's keys are the columns both sides hold of the classes the
    plan pairs them on."""
    held = frozenset().union(*columns.values())
    roots = environment.address_roots(
        {node.removeprefix("c~") for node in held}
        | frozenset().union(*planned_pairs.values())
    )
    out: dict[frozenset[str], frozenset[str]] = {}
    for pair, keys in planned_pairs.items():
        planned = {roots.get(address, address) for address in keys}
        both = frozenset.intersection(
            *(columns.get(f"ds~{identifier}", frozenset()) for identifier in pair)
        )
        out[frozenset(f"ds~{identifier}" for identifier in pair)] = frozenset(
            node
            for node in both
            if roots.get(node.removeprefix("c~"), node.removeprefix("c~")) in planned
        )
    return out


def _pair_modifiers(
    merge: _Merge,
    left: str,
    right: str,
    key: str,
    keys: set[str],
    padded_for: frozenset[str],
) -> list[Modifier]:
    """How `key` pairs `left` with `right`, one of the `keys` the join is on;
    `padded_for` is the regions earlier joins padded `left` for."""
    facts = merge.facts
    left_facts, right_facts = facts.side(left), facts.side(right)
    partial = [Modifier.PARTIAL] if key in left_facts.partials else []
    # a key both sides NULL as a value too pairs its value NULLs;
    # `_padding_guard` keeps the padding out
    if (
        key not in left_facts.value_nullables & right_facts.value_nullables
        and _pads_for_different_members(
            left_facts, right_facts, keys, facts.span_families
        )
    ):
        return partial
    if _pairs_region_padding(padded_for, right_facts, key):
        return [Modifier.NULLABLE, *partial]
    left_source, right_source = merge.sources[left], merge.sources[right]
    return (
        get_modifiers(
            merge.concepts[(left, key)],
            merge.concepts[(right, key)],
            left_source,
            right_source,
            merge.memo,
            anchored=any(
                _pairs_solid(
                    merge.concepts[(left, other)],
                    merge.concepts[(right, other)],
                    left_source,
                    right_source,
                )
                for other in keys - {key}
            ),
        )
        + partial
    )


def _side_facts(
    datasource: DataSource,
    environment: BuildEnvironment,
    canon_node: Callable[[str], str],
    memo: ProvenanceMemo,
    host_canon: set[str] | None,
    extent_free_spans: frozenset[str],
) -> tuple[SideFacts, dict[str, BuildConcept]]:
    """One side's facts (bar `span_padding`, which reads every side), and the
    concept it spells each of its columns with."""
    concepts: dict[str, BuildConcept] = {}
    own_partials = {canon_node(c.address) for c in datasource.partial_concepts}
    # A LEFT scoped join on a derived key has no datasource column binding
    # to carry Modifier.PARTIAL. The merge keeps that key as a distinct
    # output present ONLY on the partial side (the complete side outputs
    # the canonical), so intersecting outputs with scoped_partial_derived
    # marks exactly the partial side. Root/rowset partial keys carry
    # partiality through the column-partial / rowset machinery and are
    # excluded, since a rowset key also survives as a distinct output.
    partial_nodes = own_partials | {
        canon_node(c.address)
        for c in datasource.output_concepts
        if c.address in environment.scoped_partial_derived
    }
    provenance = memo.of(datasource)
    nullable_nodes = {canon_node(c.address) for c in datasource.nullable_concepts}
    if extent_free_spans:
        # a `?` binding's NULL is a value (an undated sale), not padding
        value_null = {
            c.address for c in datasource.output_concepts if provenance.values(c)
        }
        nullable_nodes -= {
            canon_node(a)
            for a in provenance.padded_by(extent_free_spans)
            if a not in value_null
        }
    padded_nodes = {canon_node(a) for a in provenance.rollup}
    partial_keys: set[str] = set()
    nullable_keys: set[str] = set()
    rollup_keys: set[str] = set()
    value_keys: set[str] = set()
    # a partial still to be re-aggregated is not a value of any key
    # (`Rollup.partial_is_not_a_key`)
    unfinished = (
        {
            c.address
            for c in pending_rollups(datasource.rollup_concepts, datasource.grain)
        }
        if isinstance(datasource, QueryDatasource)
        else set()
    )
    visible = [
        c
        for c in datasource.output_concepts
        if c.address not in datasource.hidden_concepts
    ]
    for concept in visible:
        if concept.address in unfinished:
            continue
        node = canon_node(concept.address)
        concepts.setdefault(node, concept)
        if node in partial_nodes:
            partial_keys.add(node)
        # the FIRST concept spelling the node decides the provenance
        if node in nullable_nodes and node not in nullable_keys:
            nullable_keys.add(node)
            if provenance.values(concept):
                value_keys.add(node)
        if node in padded_nodes:
            rollup_keys.add(node)
    facts = SideFacts(
        columns=frozenset(concepts),
        partials=frozenset(partial_keys),
        nullables=frozenset(nullable_keys),
        value_nullables=frozenset(value_keys),
        extent_nullables=frozenset(
            nullable_keys & {canon_node(a) for a in provenance.extent}
        ),
        guest_padded=frozenset(
            nullable_keys & {canon_node(a) for a in provenance.guest}
        ),
        rollup_padded=frozenset(rollup_keys),
        grain=frozenset(canon_node(a) for a in datasource.grain.components),
        determined=frozenset(
            canon_node(a) for a in _fd_closure(environment, datasource.grain.components)
        ),
        # Hosting a domain requires binding it COMPLETELY: a side carrying a
        # `~` key only partially (the fact's FK column) exposes the address
        # but not the domain, so it can never out-host the preserved span.
        # Own-level marks, not the deep collection: a span that completed a
        # key against its dimension clears its own mark while the raw fact
        # scan below it keeps one.
        hosts=host_canon is not None
        and host_canon
        <= {canon_node(c.address) for c in datasource.output_concepts} - own_partials,
        one_row=bool(concepts)
        and all(c.granularity == Granularity.SINGLE_ROW for c in visible),
        arm=_reads_arm(datasource),
        # A side holding a region's rows (its domain, or whatever read it)
        # hosts that region's extension rows on the join keyed by its span,
        # whatever columns it emits: the contract, not an inference from the
        # bindings. Not on a span this merge is built not to extend (a rowset
        # body whose reader holds the region): those rows are not this plan's
        # to return.
        held_spans=frozenset(
            canon_node(span)
            for span in held_region_spans(datasource)
            if span not in extent_free_spans
        ),
        complete_spans=frozenset(
            canon_node(address)
            for address in extent_free_spans
            if complete_key_domain(datasource, address)
        ),
        span_bindings={
            canon_node(address): partial_binding_sources(datasource, address)
            for address in extent_free_spans
            | {c.address for c in datasource.partial_concepts}
        },
    )
    return facts, concepts


def _declared_subset_keys(environment: BuildEnvironment) -> frozenset[str]:
    """Every key a declared `subset` relation names, member or anchor."""
    graph = environment.domain_graph
    return frozenset(
        address
        for member, anchor in graph.declared_subset_pairs()
        for address in (member, anchor, graph.canonical(anchor))
    )


def _reads_arm(datasource: DataSource) -> bool:
    if isinstance(datasource, BuildDatasource):
        grain = datasource.grain.components
        return bool(grain) and grain <= {c.address for c in datasource.partial_concepts}
    return len(datasource.datasources) == 1 and _reads_arm(datasource.datasources[0])


def _subset_bindings(side: SideFacts, declared: frozenset[str]) -> frozenset[str]:
    """The keys a side binds as `~`: its partials, or for an arm of a union
    only those a `declared` subset relation names."""
    return side.partials & declared if side.arm else side.partials


def _binding_tiers(
    sides: Mapping[str, SideFacts],
    pair_keys: Mapping[frozenset[str], frozenset[str]],
    declared: frozenset[str],
) -> dict[str, int]:
    """Where a side's `~` bindings put it: one partial on a key it pairs on
    after every side that is not, and after those one binding a key as `~`
    whose partner binds it completely (its anchor: the member's rows hang off
    the anchor's)."""
    tier: dict[str, int] = defaultdict(int)
    for pair, keys in pair_keys.items():
        if len(pair) != 2:
            continue
        for side, other in permutations(pair):
            if _subset_bindings(sides[side], declared) & keys - sides[other].partials:
                tier[side] = 2
            elif sides[side].partials & keys:
                tier[side] = max(tier[side], 1)
    return tier


def _key_holders(
    pair_keys: Mapping[frozenset[str], frozenset[str]],
) -> dict[str, set[str]]:
    """Each planned key's holders: the sides the plan pairs on it."""
    out: dict[str, set[str]] = defaultdict(set)
    for pair, keys in pair_keys.items():
        if len(pair) == 2:
            for key in keys:
                out[key] |= pair
    return out


def _solid_holders(
    facts: JoinFacts, holders: Mapping[str, set[str]]
) -> dict[str, set[str]]:
    """Each value-nullable key's holders that hold no region, in a merge
    where some side holds one: they pair on it before a region's rows unite.
    A padded NULL is no member of the NULL group an aggregate the region does
    not feed computes on the key."""
    if not facts.held_spans:
        return {}
    out: dict[str, set[str]] = {}
    for key, held_by in holders.items():
        solid = {s for s in held_by if not facts.side(s).held_spans}
        if len(solid) > 1 and any(key in facts.side(s).value_nullables for s in solid):
            out[key] = solid
    return out


RowTerm = tuple[bool, bool, bool, int, int]


def _key_row_terms(
    facts: JoinFacts, holders: Mapping[str, set[str]]
) -> list[tuple[RowTerm, set[str]]]:
    """Where each planned key comes for what its holders' rows are, with the
    holders it places. An AUTHORED key (scoped join / merge relation) comes
    first: its equality is the pairing between its holders' rows, and sides
    met on a cheaper shared key before it pair on that key alone. Then a
    value-nullable key between the sides holding no region
    (`_solid_holders`). Then a span some side holds a region on: the stitch
    on the span unites the region's rows with its feeder before a key the
    region CARRIES joins them, or a `~?` guest never reaches its NULL group.
    A key held by a side holding several regions' rows comes after the others
    are stitched: its NULL group holds their padded rows. Then the key the
    plan pairs the fewest sides on: a side that also pairs on a wider key
    meets its holders on both."""
    out: list[tuple[RowTerm, set[str]]] = []
    for key, held_by in holders.items():
        others = max(len(facts.side(s).held_spans - {key}) for s in held_by)
        term = (
            key not in facts.authored_join_keys,
            True,
            key not in facts.held_spans,
            others,
            len(held_by),
        )
        out.append((term, held_by))
    for solid in _solid_holders(facts, holders).values():
        out.append(((True, False, False, 0, len(solid)), solid))
    return out


def _shape_terms(
    ranks: Mapping[str, int],
    sides: Mapping[str, SideFacts],
    pair_keys: Mapping[frozenset[str], frozenset[str]],
    declared: frozenset[str],
) -> dict[str, tuple[int, int, bool]]:
    """Where a side comes for the plan's shape alone: its `~` bindings
    (`_binding_tiers`), then `ranks` (the plan's order), then, between sides
    the plan ranks alike, the one holding the merge's rows, whose feeders
    hang LEFT off it instead of meeting FULL before it arrives."""
    tiers = _binding_tiers(sides, pair_keys, declared)
    # a side the plan left out comes after the ones it states
    last = max(ranks.values(), default=-1) + 1
    return {
        node: (tiers[node], ranks.get(node, last), not side.hosts)
        for node, side in sides.items()
    }


def _places(terms: Mapping[str, Any]) -> dict[str, int]:
    """Each side's place under its term; sides of one term share a place."""
    place = {term: i for i, term in enumerate(sorted(set(terms.values())))}
    return {node: place[term] for node, term in terms.items()}


def _stated_order(
    ranks: Mapping[str, int], facts: JoinFacts, declared: frozenset[str]
) -> dict[str, int]:
    """The one order the walk joins a merge's sides in. A side comes where
    the best key the plan pairs it on puts it: a key is placed by what its
    holders' rows are (`_key_row_terms`), then by where the plan's shape
    puts those holders (`_shape_terms`, over `ranks`, the plan's order), so
    a lookup follows the rows it hangs off. Sides one key places alike come
    in the shape's order. All of it reads the built sides, so no plan states
    it: resolve does, once for the merge, before the walk."""
    sides, pair_keys = facts.sides, facts.pair_keys
    shape = _places(_shape_terms(ranks, sides, pair_keys, declared))
    best: dict[str, tuple[RowTerm, tuple[int, ...]]] = {}
    for rows, held_by in _key_row_terms(facts, _key_holders(pair_keys)):
        term = (rows, tuple(sorted(shape[side] for side in held_by)))
        for side in held_by:
            best[side] = min(best.get(side, term), term)
    # a side the plan pairs on nothing comes after the ones it pairs
    unpaired = ((True, True, True, len(sides), len(sides)), ())
    return _places({node: (best.get(node, unpaired), shape[node]) for node in sides})


def get_node_joins(
    datasources: list[DataSource],
    environment: BuildEnvironment,
    host_grain: set[str] | None = None,
    demanded_domains: Collection[str] = frozenset(),
    extent_free_spans: frozenset[str] = frozenset(),
    keyspace: Keyspace | None = None,
    *,
    planned_pairs: Mapping[frozenset[str], frozenset[str]],
    side_ranks: Mapping[str, int] | None = None,
    seed: str | None = None,
) -> list[SourceJoin]:
    """`keyspace` is the plan's, for the spans it can pad for, the spellings a
    rowset body pads them under and its region partition. `planned_pairs` is
    the merge's join plan pair by pair of sides (by datasource identifier):
    two sides pair only on what the plan pairs them on, and a column two sides
    merely share is no key. `side_ranks` is each side's place in the plan's
    order (by identifier), which the order the walk reads is stated over
    (`_stated_order`). `seed` is the side the walk
    starts from when the merge's rows are that side's. Resolve orders and
    types the joins; it never chooses a key."""
    keyspace = keyspace or Keyspace()

    # each pseudonym class is one graph node, named by a source's own output
    canonical = environment.address_roots(
        {
            concept.address
            for datasource in datasources
            for concept in datasource.output_concepts
            if concept.address not in datasource.hidden_concepts
        }
    )

    def canon_node(address: str) -> str:
        return f"c~{canonical.get(address, address)}"

    memo = active_memo()
    ds_node_map: dict[str, DataSource] = {}
    ds_concept_map: dict[tuple[str, str], BuildConcept] = {}
    sides: dict[str, SideFacts] = {}
    host_canon = {canon_node(a) for a in host_grain} if host_grain else None

    for datasource in datasources:
        ds_node = f"ds~{datasource.identifier}"
        ds_node_map[ds_node] = datasource
        sides[ds_node], spelled = _side_facts(
            datasource, environment, canon_node, memo, host_canon, extent_free_spans
        )
        for node, concept in spelled.items():
            ds_concept_map.setdefault((ds_node, node), concept)

    pair_nodes = _planned_pair_nodes(
        {node: side.columns for node, side in sides.items()}, planned_pairs, environment
    )

    # Beside the region contract: a FULL join between two families' padding
    # must not pair NULL with NULL null-safely.
    spellings = _span_spellings(keyspace.in_play_spans, environment, keyspace.witnessed)
    if sum(1 for side in sides.values() if side.nullables) > 1:
        matrix = _span_padding_matrix(
            ds_node_map,
            {ds_node: side.nullables for ds_node, side in sides.items()},
            spellings,
            canon_node,
            memo,
        )
        sides = {
            ds_node: replace(side, span_padding=matrix.get(ds_node, {}))
            for ds_node, side in sides.items()
        }

    # Local import: common.py imports nodes.merge_node, which imports this module.
    from trilogy.core.processing.node_generators.common import (
        authored_join_pair_candidates,
    )
    from trilogy.core.processing.node_generators.presence_probe import (
        axis_scalar_reads,
        is_coalescing_axis,
    )

    full_join_keys = {
        canon_node(a) for a in environment.domain_graph.outer_relation_keys()
    }
    if full_join_keys:
        # a scalar of a coalescing axis pairs its arms as the axis does
        full_join_keys |= {
            canon_node(c.address)
            for datasource in datasources
            for c in datasource.output_concepts
            if any(
                is_coalescing_axis(axis, environment)
                for axis in axis_scalar_reads(c, environment)
            )
        }
    span_families = {
        canon_node(key): canon_node(axis)
        for axis in environment.scoped_join_key_groups
        for key in axis_member_keys(axis, environment)
    }
    facts = JoinFacts(
        sides=sides,
        full_join_keys=frozenset(full_join_keys),
        scoped_keys=frozenset(canon_node(a) for a in environment.scoped_partial_derived)
        | frozenset(canon_node(a) for a in environment.all_scoped_join_group_members()),
        # the join tree bases on the complete source providing an anchor key so
        # co-anchored optional sources stay LEFT
        anchor_keys=frozenset(
            canon_node(a) for a in environment.domain_graph.left_anchor_keys()
        ),
        # ROOT-member authored join groups pivot the join tree first, so the
        # authored equality is the pairing between the sides regardless of
        # cheaper shared-key edges. Derived/rowset-keyed groups are excluded;
        # their ordering rides the rowset exposure machinery.
        authored_join_keys=frozenset(
            canon_node(pair.canonical.address)
            for pair in authored_join_pair_candidates(environment)
        ),
        extent_free_keys=frozenset(canon_node(a) for a in extent_free_spans),
        region_partition=tuple(
            frozenset(canon_node(span) for span in spans) for spans in keyspace.families
        ),
        demanded_domains=frozenset(canon_node(a) for a in demanded_domains),
        span_families=span_families,
        pair_keys=pair_nodes,
        seed=f"ds~{seed}" if seed else None,
    )
    facts = replace(
        facts,
        side_ranks=_stated_order(
            {
                f"ds~{identifier}": rank
                for identifier, rank in (side_ranks or {}).items()
            },
            facts,
            frozenset(canon_node(a) for a in _declared_subset_keys(environment)),
        ),
    )
    merge = _Merge(facts, ds_node_map, ds_concept_map, spellings, canon_node, memo)
    joins = resolve_join_order_v2(facts)
    paddings = _merge_paddings(joins, facts)
    # Per join, only what the joins BEFORE it padded: a join's own padding of
    # the joined sides is absence it introduces, not the region's rows.
    region_padded = [_region_padded_sides(paddings[:i]) for i in range(len(joins))]
    _raise_if_keyless_row_bearing_join(
        joins,
        ds_node_map,
        canonical,
        frozenset(
            node.removeprefix("c~")
            for side in sides.values()
            for node in side.rollup_padded
        ),
        environment,
    )
    agreement = _Agreement(strict=True)
    base_joins: list[SourceJoin] = []
    for j, padded_sides in zip(joins, region_padded):
        base = Join(
            left=ds_node_map[j.left] if j.left else None,
            right=ds_node_map[j.right],
            join_type=j.type,
            own_type=j.own_type,
            concepts=[] if not j.keys else None,
            pairs=reduce_concept_pairs(
                [
                    ConceptPair(
                        left=ds_concept_map[(k, concept)],
                        right=ds_concept_map[(j.right, concept)],
                        node=ds_node_map[k],
                        modifiers=_pair_modifiers(
                            merge,
                            k,
                            j.right,
                            concept,
                            v,
                            padded_sides.get(k, frozenset()),
                        ),
                    )
                    for k, v in j.keys.items()
                    # sorted: v is a set, and reduce_concept_pairs prunes
                    # greedily in input order, so unordered iteration would
                    # make the surviving pair set vary run to run.
                    for concept in sorted(v)
                ],
                ds_node_map[j.right],
                j.type,
                domain_graph=environment.domain_graph,
                agreement=agreement,
            ),
        )
        for pair in base.pairs or []:
            if not pair.is_nullable:
                agreement.union(
                    (pair.node.identifier, pair.left.address),
                    (base.right.identifier, pair.right.address),
                )
        base_joins.append(base)
    for base, guard in zip(
        base_joins, _padding_guards(joins, base_joins, paddings, merge)
    ):
        base.guard = guard
    return base_joins
