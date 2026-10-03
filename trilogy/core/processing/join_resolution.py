from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass, field, replace
from functools import cached_property, partial
from operator import attrgetter
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from trilogy.core import graph as nx

from trilogy.core.domain_graph import DomainGraph
from trilogy.core.enums import (
    AggregateGroupingMode,
    Derivation,
    Granularity,
    JoinType,
    Modifier,
    Purpose,
    SourceType,
)
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.functions import propagates_argument_nulls
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildRowsetItem,
    get_grouped_aggregate_wrapper,
)
from trilogy.core.models.build_environment import (
    BuildEnvironment,
)
from trilogy.core.models.execute import (
    BaseJoin,
    ConceptPair,
    QueryDatasource,
    UnnestJoin,
    preserved_key_pairs,
)
from trilogy.core.models.keyspace import Keyspace
from trilogy.core.processing import plan_trace
from trilogy.core.processing.condition_utility import is_scalar_condition
from trilogy.core.processing.utility import (
    PADS_LEFT_JOIN_TYPES,
    PADS_RIGHT_JOIN_TYPES,
    NodeType,
    left_deep_joins,
    padded_by,
)

DataSource = QueryDatasource | BuildDatasource


@dataclass
class JoinOrderOutput:
    right: str
    type: JoinType
    keys: dict[str, set[str]]
    left: str | None = None

    @property
    def lefts(self) -> set[str]:
        return set(self.keys.keys())


OUTER_JOIN_TYPES = (JoinType.FULL, JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER)
DIRECTIONAL_OUTER_JOIN_TYPES = (JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER)


def held_region_spans(source: DataSource) -> frozenset[str]:
    """Spans a source holds a region's rows on; a leaf holds none."""
    return source.region_spans if isinstance(source, QueryDatasource) else frozenset()


def compute_outer_null_status(
    joins: list[BaseJoin | UnnestJoin],
) -> dict[str, int]:
    """Score how often each datasource is null-extended by outer joins."""
    score: dict[str, int] = defaultdict(int)
    for join, left in left_deep_joins(joins):
        for identifier in padded_by(join, left):
            score[identifier] += 1
    return score


def prune_outer_join_pairs(joins: list[BaseJoin | UnnestJoin]) -> None:
    """Drop redundant duplicate-key pairs from directional outer joins: a
    left side no earlier join pads equals the key on every row, so the
    others' pairs only bloat the coalesce. With every left side padded the
    coalesce IS the key, and all pairs stay."""
    padded: set[str] = set()
    for join, left in left_deep_joins(joins):
        if join.concept_pairs and join.join_type in DIRECTIONAL_OUTER_JOIN_TYPES:
            join.concept_pairs = preserved_key_pairs(
                join.concept_pairs, padded, _pair_source
            )
        padded |= padded_by(join, left)


def _pair_source(pair: ConceptPair) -> str:
    return pair.existing_datasource.identifier


def find_all_connecting_concepts(g: nx.Graph, ds1: str, ds2: str) -> set[str]:
    return set(g.neighbors(ds1)) & set(g.neighbors(ds2))


def get_connection_keys(
    all_connections: dict[tuple[str, str], set[str]], left: str, right: str
) -> set[str]:
    key: tuple[str, str] = (min(left, right), max(left, right))
    return all_connections.get(key, set())


def rollup_padded_addresses(datasource: DataSource) -> set[str]:
    """Grouping-key addresses this source NULL-pads because it renders
    `GROUP BY ROLLUP/CUBE/GROUPING SETS` itself. A wrapper already computed
    upstream is a passthrough: it re-emits the padded rows, it does not
    create them."""
    if not isinstance(datasource, QueryDatasource):
        return set()
    upstream = {
        c.address for parent in datasource.datasources for c in parent.output_concepts
    }
    padded: set[str] = set()
    for concept in datasource.output_concepts:
        wrapper = get_grouped_aggregate_wrapper(concept)
        if (
            wrapper is None
            or wrapper.grouping == AggregateGroupingMode.STANDARD
            or concept.address in upstream
        ):
            continue
        padded.update(b.address for b in wrapper.by)
    return padded


def _leaf_null_addresses(datasource: BuildDatasource) -> set[str]:
    out: set[str] = set()
    for concept in datasource.nullable_concepts:
        out.add(concept.address)
        out.update(concept.pseudonyms)
    return out


def _no_leaf_addresses(datasource: BuildDatasource) -> set[str]:
    return set()


def _value_null_driven(join: BaseJoin, memo: dict[int, bool]) -> bool:
    driven = memo.get(id(join))
    if driven is None:
        driven = memo[id(join)] = any(
            nulls_are_values(pair.left, pair.existing_datasource)
            or nulls_are_values(pair.right, join.right_datasource)
            for pair in join.concept_pairs or []
        )
    return driven


def _span_keyed(join: BaseJoin, spans: frozenset[str]) -> bool:
    return any(
        pair.left.address in spans or pair.right.address in spans
        for pair in join.concept_pairs or []
    ) or any(concept.address in spans for concept in join.concepts or [])


def _padded_addresses(
    datasource: DataSource,
    leaf_addresses: Callable[[BuildDatasource], set[str]],
    join_extends: Callable[[BaseJoin], bool],
    memo: dict[int, frozenset[str]] | None = None,
    chain: bool = False,
) -> frozenset[str]:
    """Addresses this source emits NULL for because an outer join `join_extends`
    licenses padded them, or because a leaf declared them nullable.

    Left-deep accumulation as in find_nullable_concepts: a RIGHT/FULL join
    null-extends the whole accumulated left input, not just one operand. An
    address counts only when EVERY provider of it was extended (or was already
    padded inside that provider)."""
    memo = {} if memo is None else memo
    cached = memo.get(id(datasource))
    if cached is not None:
        return cached
    memo[id(datasource)] = frozenset()
    if isinstance(datasource, BuildDatasource):
        memo[id(datasource)] = frozenset(leaf_addresses(datasource))
        return memo[id(datasource)]
    child_padded = {
        child.identifier: _padded_addresses(
            child, leaf_addresses, join_extends, memo, chain
        )
        for child in datasource.datasources
    }
    right_ids = {
        j.right_datasource.identifier
        for j in datasource.joins
        if isinstance(j, BaseJoin)
    }
    extended: set[str] = set()
    out: set[str] = set()
    base_ids = [i for i in child_padded if i not in right_ids]
    for join, accumulated in left_deep_joins(datasource.joins, base_ids):
        right_id = join.right_datasource.identifier
        pairs = join.concept_pairs or []
        extends = join_extends(join)
        # a lookup keyed on a column already padded on its preserved side pads
        # for the same rows (a guest order's customer, then that customer's
        # address)
        left_padded = chain and any(
            pair.existing_datasource.identifier in extended
            or pair.left.address
            in child_padded.get(pair.existing_datasource.identifier, frozenset())
            for pair in pairs
        )
        right_padded = chain and any(
            pair.right.address in child_padded.get(right_id, frozenset())
            for pair in pairs
        )
        if join.join_type in PADS_RIGHT_JOIN_TYPES and (extends or left_padded):
            extended.add(right_id)
        if join.join_type in PADS_LEFT_JOIN_TYPES and (extends or right_padded):
            extended |= accumulated
    for address, providers in datasource.source_map.items():
        idents = {
            p.identifier
            for p in providers
            if isinstance(p, (BuildDatasource, QueryDatasource))
        }
        if idents and all(
            ident in extended or address in child_padded.get(ident, frozenset())
            for ident in idents
        ):
            out.add(address)
    for concept in datasource.output_concepts:
        if concept.address in out:
            out.update(concept.pseudonyms)
    memo[id(datasource)] = frozenset(out)
    return memo[id(datasource)]


def extent_null_addresses(
    datasource: DataSource,
    _memo: dict[int, frozenset[str]] | None = None,
    _driven: dict[int, bool] | None = None,
) -> frozenset[str]:
    """Addresses this source can genuinely emit NULL for or omit a member of
    BECAUSE of a `?` declaration: a `?` binding at a leaf, or a key every
    provider of which is null-extended by a VALUE-NULL-DRIVEN outer join in
    this source's own tree (or already extent-null within that provider).
    Narrower than ``nullable_concepts`` twice over: a side merely JOINED on a
    nullable condition gets no mark (an INNER join introduces no NULLs), and
    padding from partial-driven (`~`) preserving joins gets none either, since
    extension families ride the host machinery and claiming their padding here
    would re-preserve rows that machinery already keeps exactly once. ROLLUP
    padding is likewise excluded; ``rollup_padded_addresses`` owns it."""
    return _padded_addresses(
        datasource,
        _leaf_null_addresses,
        partial(_value_null_driven, memo={} if _driven is None else _driven),
        _memo,
        chain=True,
    )


def guest_padded_addresses(
    datasource: DataSource,
    _memo: dict[int, frozenset[str]] | None = None,
    _driven: dict[int, bool] | None = None,
) -> frozenset[str]:
    """Addresses this source emits NULL for because a VALUE-NULL key found no
    partner: a `~?` guest sale's item columns, and whatever is chained off
    them. ``extent_null_addresses`` without the `?` leaves: a leaf's NULL is a
    member some holder of the key has a row for, and a guest's NULL is not."""
    return _padded_addresses(
        datasource,
        _no_leaf_addresses,
        partial(_value_null_driven, memo={} if _driven is None else _driven),
        _memo,
        chain=True,
    )


def extension_padded_addresses(
    datasource: DataSource,
    spans: frozenset[str],
    _memo: dict[int, frozenset[str]] | None = None,
) -> frozenset[str]:
    """Addresses this source only emits NULL for because a ``~``-preserving
    join keyed on one of ``spans`` padded them to carry its extension members.

    A merge extent-free for those spans reads them as absence, not content:
    another branch owns those rows. An ordinary outer lookup's nullability
    stands."""
    return _padded_addresses(
        datasource,
        _no_leaf_addresses,
        partial(_span_keyed, spans=spans),
        _memo,
    )


def _span_padded_addresses(
    datasource: DataSource, span: str, memo: dict[int, frozenset[str]]
) -> frozenset[str]:
    """Addresses this source emits NULL for on the rows that carry `span`'s
    extension members.

    Wider than ``extension_padded_addresses`` by one step: a lookup chained off
    an already padded key (`users LEFT orders` on the span, then `LEFT lines`
    on `order_id`) pads for the same member, though the span does not key it."""
    return _padded_addresses(
        datasource,
        _no_leaf_addresses,
        partial(_span_keyed, spans=frozenset({span})),
        memo,
        chain=True,
    )


@dataclass(frozen=True)
class SideFacts:
    """What a merge knows about ONE of its sides, in the merge's node spelling.

    Several fields classify the same keys: ``nullables`` is every key the side
    can emit NULL for, and ``value_nullables`` / ``extent_nullables`` /
    ``guest_padded`` / ``rollup_padded`` are the provenances that decide
    whether such a NULL pairs with another side's.
    """

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
    # covers every `~`-licensed key the node emits, so extension rows ride it
    hosts: bool = False
    # spans the side holds a region's rows on
    held_spans: frozenset[str] = frozenset()
    # extent-free spans the side carries every value of
    complete_spans: frozenset[str] = frozenset()
    # extent-free span -> the leaf tables whose own `~` column binds it here
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

    def side(self, node: str) -> SideFacts:
        return self.sides.get(node) or _NO_FACTS

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


def _pads_for_different_members(
    left: SideFacts, right: SideFacts, keys: set[str]
) -> bool:
    """Both sides NULL the connecting keys to carry extension members, and of
    different ``~`` spans: a product never sold beside a user who never
    ordered. Those NULLs name nothing in common, so pairing them invents a
    (product, user) row; each side's padding has to survive on its own."""
    if not (left.span_padding and right.span_padding):
        return False
    left_spans: frozenset[str] = frozenset().union(
        *(left.span_padding.get(key, frozenset()) for key in keys)
    )
    right_spans: frozenset[str] = frozenset().union(
        *(right.span_padding.get(key, frozenset()) for key in keys)
    )
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
    return (JoinType.LEFT_OUTER if left_holds else JoinType.RIGHT_OUTER), False


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
    if not free or facts.authored(keys):
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
        # The other side carrying the span's whole domain and the binder no
        # NULL on it: every binder row has its partner, so anchoring preserves
        # nothing. Typed INNER here rather than left to the narrowing pass,
        # because the anchoring's stamp (the domain's columns nullable on this
        # stream) would otherwise reach every merge above as a value NULL.
        if keys <= other.complete_spans and not keys & binder.nullables:
            return JoinType.INNER, None
        return (JoinType.LEFT_OUTER if left_binds else JoinType.RIGHT_OUTER), None
    # Both sides bind it. Two projections of ONE binding cover the same subset,
    # so the span carries no row intent between them and the remaining keys
    # decide the typing. PEER facts (sales and returns each referencing their
    # own slice of the group domain) each hold rows the other lacks, and
    # dropping either side's is a chasm, not an extension: their typing stands
    # whoever owns the extent.
    if all(
        (bound := left_facts.span_bindings.get(key))
        and bound == right_facts.span_bindings.get(key)
        for key in span_keys
    ):
        return None, keys - free
    return None, None


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
            return JoinType.LEFT_OUTER if host == left else JoinType.RIGHT_OUTER
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
                return JoinType.LEFT_OUTER if right_is_dim else JoinType.RIGHT_OUTER
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
            return JoinType.LEFT_OUTER if host == left else JoinType.RIGHT_OUTER
        # A value NULL on one side (a guest order's address, grouped) names a
        # real row; the other side's padding names nothing. Keep the row. An
        # authored axis declines this like every other direction inference: the
        # relation declares the pairing, so a NULL a side happens to carry on
        # the declared key is not this rule's to read.
        left_values = bool(keys & left_facts.extent_nullables)
        if left_values != bool(
            keys & right_facts.extent_nullables
        ) and not facts.authored(keys):
            return JoinType.LEFT_OUTER if left_values else JoinType.RIGHT_OUTER
        # Padding for different spans never pairs (`get_node_joins` drops the
        # null-safe equality), so INNER would shed both extension families.
        if _pads_for_different_members(left_facts, right_facts, keys):
            return JoinType.FULL
        # Grain-aligned sides both weakened their EQUAL-domain claims on the
        # merge axis itself, so INNER would drop each side's exclusive members;
        # preserve both and let the null-safe equality pair the NULL groups.
        if _is_nullable_grain_aligned_merge(left_facts, right_facts, keys):
            return JoinType.FULL
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
        return JoinType.LEFT_OUTER if left_is_nullable else JoinType.RIGHT_OUTER
    return JoinType.INNER


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
        return (
            JoinType.LEFT_OUTER if left_pads else JoinType.RIGHT_OUTER
        ), "rollup_padding"
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
) -> None:
    for idx, review_join in enumerate(joins):
        predecessors = joins[:idx]
        if review_join.type == JoinType.FULL:
            continue
        has_prior_left = False
        has_prior_right = False
        review_keys: set[str] = set().union(*review_join.keys.values())
        for pred in predecessors:
            on_pred_right = pred.right in review_join.lefts
            on_pred_left = any(x in review_join.lefts for x in pred.lefts)
            # A prior FULL padded rows into the accumulated stream, so this
            # join preserves its LEFT stream. It preserves its RIGHT relation
            # too when the FULL is an AUTHORED axis (row intent for both
            # sides' content), or when this join is keyed ON the FULL's own
            # spine (coalesced across both families) and the spine is a domain
            # this merge demands. A join keyed OFF the spine hangs off one
            # family: only get_join_type can license preserving it.
            if pred.type == JoinType.FULL and (on_pred_right or on_pred_left):
                has_prior_left = True
                pred_keys: set[str] = set().union(*pred.keys.values())
                if (
                    review_keys
                    and review_keys <= pred_keys
                    and review_keys <= demanded_domains
                ) or (pred_keys & authored_axis_keys):
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
            review_join.type = (
                JoinType.FULL
                if has_prior_right or review_join.type == JoinType.RIGHT_OUTER
                else JoinType.LEFT_OUTER
            )


def _score_join_candidate(
    x: str,
    *,
    root: str,
    eligible_left: set[str],
    facts: JoinFacts,
    multi_partial: bool,
    anchor_sources: frozenset[str],
) -> tuple[int, int, str]:
    side = facts.side(x)
    base = 1
    if x in eligible_left:
        base += 3
    # A query-scoped LEFT anchor must seed the join base AND be processed first in
    # the per-right dedup loop so each co-anchored optional source dedups against
    # the anchor (LEFT_OUTER) instead of against the other optional source (FULL).
    # The boost dominates the multi_partial bump so the anchor always outranks.
    if x in anchor_sources:
        base += 10
    is_partial = root in side.partials
    if multi_partial and is_partial:
        base += 2
    elif is_partial:
        base -= 1
    if root in side.nullables:
        base += 1
    return (base, len(side.grain), x)


_SOLID = "|solid"


def _solid_value_null_pivots(
    pivot_map: dict[str, list[str]], facts: JoinFacts
) -> dict[str, list[str]]:
    """A value-nullable key pairs the sides holding no region on it before a
    region's rows unite: a padded NULL is no member of the NULL group an
    aggregate the region does not feed computes on it. A side holding the
    region joins on it after, through the key's own pivot."""
    if not facts.held_spans:
        return {}
    out: dict[str, list[str]] = {}
    for key, sides in pivot_map.items():
        solid = [s for s in sides if not facts.side(s).held_spans]
        if len(solid) > 1 and any(key in facts.side(s).value_nullables for s in solid):
            out[key + _SOLID] = solid
    return out


def resolve_join_order_v2(g: nx.Graph, facts: JoinFacts) -> list[JoinOrderOutput]:
    """Greedily order the datasources into a join tree.

    Pick a pivot (shared concept), then absorb datasources that connect to the
    growing left set, scoring candidates by eligibility / partial / nullable
    status and breaking ties on grain width (``_score_join_candidate``).
    Every choice point sorts its inputs, so the plan is deterministic across runs.

    Ordering is a heuristic for plan shape only; ``ensure_content_preservation``
    guarantees the result set regardless of the order chosen here.
    """
    datasources = sorted(x for x in g.nodes if x.startswith("ds~"))
    concepts = sorted(x for x in g.nodes if x.startswith("c~"))

    # A source is an anchor when it provides a scoped-LEFT anchor key as a
    # COMPLETE (non-partial) concept; optional sources are partial against it.
    # An anchor key is only ACTIVE when some present source is partial against
    # it: the boost exists to keep optional sources directional (LEFT, not
    # FULL), and with no optional side in the plan, seeding the tree on it
    # would just perturb unrelated joins.
    anchor_sources: frozenset[str] = frozenset()
    active_anchor_keys: set[str] = set()
    if facts.anchor_keys:
        active_anchor_keys = {
            key
            for key in facts.anchor_keys
            if any(key in facts.side(ds).partials for ds in datasources)
        }
        anchor_sources = frozenset(
            ds
            for ds in datasources
            if (set(g.neighbors(ds)) & active_anchor_keys)
            and not (active_anchor_keys & facts.side(ds).partials)
        )

    all_connections: dict[tuple[str, str], set[str]] = {}
    for i, ds1 in enumerate(datasources):
        for ds2 in datasources[i + 1 :]:
            connecting_concepts = find_all_connecting_concepts(g, ds1, ds2)
            if connecting_concepts:
                all_connections[(min(ds1, ds2), max(ds1, ds2))] = connecting_concepts

    output: list[JoinOrderOutput] = []

    pivot_map = {
        concept: [x for x in g.neighbors(concept) if x in datasources]
        for concept in concepts
    }
    # An AUTHORED join key (scoped join / merge relation) pivots FIRST: its
    # equality is a semantic pairing contract, not a heuristic tree edge. If a
    # cheaper shared key seeds the tree instead, the sides pair on that key
    # alone and the authored predicate lands on a leaf dimension, where a
    # preserving join NULLs the dimension instead of un-pairing the rows.
    # A span some side holds a region on pivots next: the stitch on the span
    # unites the region's rows with its feeder first, and a key the region
    # CARRIES (an aggregate by the description) joins the united rows after
    # it. Pivoting on the carried key instead joins the aggregate to the
    # domain alone, where a `~?` guest (a value-NULL key, so no domain row)
    # never reaches its NULL group.
    held = facts.held_spans
    solo = [x for x in pivot_map if len(pivot_map[x]) == 1]
    pivot_map.update(_solid_value_null_pivots(pivot_map, facts))
    pivots = sorted(
        [x for x in pivot_map if len(pivot_map[x]) > 1],
        key=lambda x: (
            x not in facts.authored_join_keys,
            not x.endswith(_SOLID),
            x not in held,
            len(pivot_map[x]),
            len(x),
            x,
        ),
    )
    eligible_left: set[str] = set()

    while pivots:
        next_pivots = [
            x for x in pivots if any(y in eligible_left for y in pivot_map[x])
        ]
        if next_pivots:
            root = next_pivots[0]
            pivots = [x for x in pivots if x != root]
        else:
            root = pivots.pop(0)

        key = root.removesuffix(_SOLID)
        unjoined_for_root = [x for x in pivot_map[root] if x not in eligible_left]
        multi_partial = (
            sum(1 for x in unjoined_for_root if key in facts.side(x).partials) > 1
        )

        score_key = partial(
            _score_join_candidate,
            root=key,
            eligible_left=eligible_left,
            facts=facts,
            multi_partial=multi_partial,
            anchor_sources=anchor_sources,
        )

        to_join = sorted(
            [x for x in pivot_map[root] if x not in eligible_left], key=score_key
        )
        while to_join:
            base = sorted([x for x in eligible_left], key=score_key)
            if not base:
                new = to_join.pop()
                eligible_left.add(new)
                base = [new]
            right = to_join.pop()
            if right in eligible_left:
                continue

            joinkeys: dict[str, set[str]] = {}
            join_types: set[JoinType] = set()
            deduped: list[tuple[str, set[str]]] = []

            for left_candidate in reversed(base):
                all_connecting_keys = get_connection_keys(
                    all_connections, left_candidate, right
                )

                if not all_connecting_keys:
                    continue

                # A FULL-join key must keep EVERY left source that provides it:
                # the row may exist on only one of them, so the ON clause has to
                # coalesce across all (`coalesce(l1.k, l2.k) = r.k`). Skipping a
                # redundant left here would drop that source from the coalesce and
                # split rows present only on it. Non-FULL keys still dedup.
                is_full_key = bool(all_connecting_keys & facts.full_join_keys)
                exists = False
                if not is_full_key:
                    for existing_left, v in joinkeys.items():
                        if v == all_connecting_keys:
                            left_is_partial = bool(
                                all_connecting_keys
                                & facts.side(left_candidate).partials
                            )
                            existing_is_partial = bool(
                                all_connecting_keys & facts.side(existing_left).partials
                            )
                            if not (left_is_partial and existing_is_partial):
                                exists = True
                if exists:
                    deduped.append((left_candidate, all_connecting_keys))
                    continue

                join_type = get_join_type(
                    left_candidate, right, all_connecting_keys, facts, eligible_left
                )
                join_types.add(join_type)
                joinkeys[left_candidate] = all_connecting_keys

            final_join_type = reduce_join_types(join_types)

            # A FULL from get_join_type (a nullable-driven grain-aligned merge)
            # arrives after the dedup above ran; restore the dropped providers
            # so the ON clause coalesces across every left source, as
            # is_full_key pre-empts for registry keys. A single-source ON
            # misses rows that exist only on a previously-preserved side.
            if final_join_type == JoinType.FULL:
                for left_candidate, all_connecting_keys in deduped:
                    joinkeys[left_candidate] = all_connecting_keys

            output.append(
                JoinOrderOutput(
                    right=right,
                    type=final_join_type,
                    keys=joinkeys,
                )
            )
            eligible_left.add(right)

    for concept in solo:
        for ds in pivot_map[concept]:
            if ds in eligible_left:
                continue
            if not eligible_left:
                eligible_left.add(ds)
                continue
            best_left = None
            best_keys: set[str] = set()
            for existing_left in sorted(eligible_left):
                connecting_keys = get_connection_keys(
                    all_connections, existing_left, ds
                )
                if connecting_keys and len(connecting_keys) > len(best_keys):
                    best_left = existing_left
                    best_keys = connecting_keys

            if best_left and best_keys:
                output.append(
                    JoinOrderOutput(
                        left=best_left,
                        right=ds,
                        type=JoinType.FULL,
                        keys={best_left: best_keys},
                    )
                )
            else:
                output.append(
                    JoinOrderOutput(
                        left=min(eligible_left),
                        right=ds,
                        type=JoinType.FULL,
                        keys={},
                    )
                )
            eligible_left.add(ds)

    ensure_content_preservation(output, facts.axis_keys, facts.demanded_domains)

    return output


def side_nullable(concept: BuildConcept, side: DataSource | None) -> bool:
    if side is None:
        return False
    # Intrinsic nullability: the concept's own definition can yield NULL (a
    # `?` column, a filtered value or aggregate, a no-else CASE) on any side
    # that carries it, regardless of that side's join structure.
    if concept.is_nullable:
        return True
    equivalent = concept.equivalent_addresses
    if any(equivalent & nc.equivalent_addresses for nc in side.nullable_concepts):
        return True
    # a side that COMPUTES the join key from nullable inputs yields NULL keys
    # too (`l_key + 1` is NULL wherever `l_key` is) even when the derived key
    # itself never got flagged
    if not propagates_argument_nulls(concept):
        return False
    args = {a.address for a in concept.concept_arguments}
    if not args:
        return False
    nullable_addrs: set[str] = set()
    for nc in side.nullable_concepts:
        nullable_addrs |= nc.equivalent_addresses
    return bool(args & nullable_addrs)


def _side_outputs(concept: BuildConcept, side: DataSource) -> bool:
    equivalent = concept.equivalent_addresses
    return any(equivalent & c.equivalent_addresses for c in side.output_concepts)


def nulls_are_values(
    concept: BuildConcept,
    side: DataSource,
    _seen: frozenset[tuple[str, int]] = frozenset(),
) -> bool:
    """Whether the NULLs this side carries for ``concept`` are VALUES (a `?`
    column, a nullable derivation, a nullable input to a null-propagating
    expression, a ROLLUP grouping key) rather than pure outer-join extension.

    Outer-join extension means absent: there is no row on that side, so no
    key. Pairing that against a real NULL group cross-joins the two."""
    if concept.is_nullable:
        return True
    # Argument chains can be mutually recursive; a repeat visit of the same
    # concept on the same source contributes nothing new.
    visit = (concept.address, id(side))
    if visit in _seen:
        return False
    seen = _seen | {visit}
    equivalent = concept.equivalent_addresses
    if isinstance(side, BuildDatasource):
        # Column-level `?` is the only value-NULL source on a physical table.
        return any(
            equivalent & nc.equivalent_addresses for nc in side.nullable_concepts
        )
    # A grouping-set NULL is padding too, but a twin-rollup partner pads the
    # same key, so it stays pairable here; get_join_type handles the mismatch.
    if equivalent & rollup_padded_addresses(side):
        return True
    carriers = [p for p in side.datasources if _side_outputs(concept, p)]
    if not carriers:
        # Nothing upstream to attribute the NULL to; stay conservative rather
        # than call an unexplained nullability extension.
        return True
    if any(nulls_are_values(concept, p, seen) for p in carriers):
        return True
    if not propagates_argument_nulls(concept):
        return False
    args = {a.address for a in concept.concept_arguments}
    if not args:
        return False
    return any(
        (args & nc.equivalent_addresses) and nulls_are_values(nc, side, seen)
        for nc in side.nullable_concepts
    )


def get_modifiers(
    left_concept: BuildConcept,
    right_concept: BuildConcept,
    left: DataSource | None,
    right: DataSource | None,
) -> list[Modifier]:
    """Use null-safe equality only when both exposed join keys can be NULL.

    Asymmetric padding is the exception: when one side's NULLs are outer-join
    extension (absence) and the other's are values, they name nothing in
    common and null-safe equality cross-joins them. Both sides extended is the
    ordinary case again: the padding shares provenance, so those rows pair."""
    if not (side_nullable(left_concept, left) and side_nullable(right_concept, right)):
        return []
    assert left is not None and right is not None
    if nulls_are_values(left_concept, left) != nulls_are_values(right_concept, right):
        return []
    return [Modifier.NULLABLE]


def preserved_sources(
    datasets: list[DataSource], joins: list[BaseJoin | UnnestJoin]
) -> set[str]:
    """Identifiers of the sources whose every row survives the join tree.
    Joins are left-deep: a join that does not preserve its left side drops
    rows of everything joined so far, and an UNNEST can drop rows too."""
    right_ids = {
        join.right_datasource.identifier for join in joins if isinstance(join, BaseJoin)
    }
    alive = {ds.identifier for ds in datasets if ds.identifier not in right_ids}
    for join in joins:
        if not isinstance(join, BaseJoin):
            alive = set()
            continue
        if join.join_type not in PADS_RIGHT_JOIN_TYPES:
            alive = set()
        if join.join_type in PADS_LEFT_JOIN_TYPES:
            alive.add(join.right_datasource.identifier)
    return alive


def merge_partial_addresses(
    datasets: list[DataSource],
    joins: list[BaseJoin | UnnestJoin],
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


def reduce_concept_pairs(
    pairs: list[ConceptPair],
    right_source: DataSource,
    join_type: JoinType = JoinType.INNER,
    domain_graph: DomainGraph | None = None,
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
    # The closure sees what the local property check below cannot: transitive
    # dependencies and grain FDs carried through complete bindings. Greedy
    # over a working determinant set so mutually-dependent keys keep exactly
    # one pair; grain pairs are never pruned (the grain restriction below
    # relies on them).
    # Only a key paired on plain equality vouches for its dependents. A
    # null-safe pair also matches NULL to NULL, and an FD says nothing about
    # rows with no key: two extension families both pad `item_id`, and
    # `product_id` is the one pair that still tells them apart.
    null_safe_left = {pair.left.address for pair in pairs if pair.is_nullable}
    null_safe_right = {pair.right.address for pair in pairs if pair.is_nullable}
    fd_pruned: set[int] = set()
    if domain_graph is not None and domain_graph.fd_edges:
        working_left = set(left_keys) - null_safe_left
        working_right = set(right_keys) - null_safe_right
        for index, pair in enumerate(pairs):
            left_addr, right_addr = pair.left.address, pair.right.address
            if right_addr in grain_components:
                continue
            if _is_authored_pair(pair, authored_members):
                continue
            determinant_left = working_left - {left_addr}
            determinant_right = working_right - {right_addr}
            if not (determinant_left and determinant_right):
                continue
            if domain_graph.determines(
                determinant_left, left_addr
            ) and domain_graph.determines(determinant_right, right_addr):
                fd_pruned.add(index)
                working_left.discard(left_addr)
                working_right.discard(right_addr)
    final: list[ConceptPair] = []
    seen: set[tuple[str, str]] = set()
    is_outer = join_type in OUTER_JOIN_TYPES
    right_left_seen: dict[tuple[str, str], bool] = {}
    for index, pair in enumerate(pairs):
        dedup_key = (pair.right.address, pair.existing_datasource.identifier)
        if dedup_key in seen:
            continue
        rl_key = (pair.right.address, pair.left.address)
        if (
            rl_key in right_left_seen
            and not is_outer
            and not (right_left_seen[rl_key] or pair.is_partial)
        ):
            continue
        if (
            pair.left.purpose == Purpose.PROPERTY
            and pair.left.keys
            and pair.left.keys.issubset(left_keys)
            and not _is_authored_pair(pair, authored_members)
        ):
            continue
        if (
            pair.right.purpose == Purpose.PROPERTY
            and pair.right.keys
            and pair.right.keys.issubset(right_keys)
            and not _is_authored_pair(pair, authored_members)
        ):
            continue
        if index in fd_pruned:
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
        return [
            x
            for x in final
            if x.right.address in right_source.grain.components
            or _is_authored_pair(x, authored_members)
        ]

    return final


def build_canonical_address_map(
    datasources: list[DataSource],
    environment: BuildEnvironment,
) -> dict[str, str]:
    """Collapse pseudonym-equivalent concept addresses to one canonical address.

    Join resolution treats each class as one graph node. Pseudonym addresses are
    also linked through ``alias_origin_lookup`` so merged targets and their
    pre-merge addresses share a class.
    """
    from trilogy.core import graph as nx

    pseudonym_graph = nx.Graph()
    for datasource in datasources:
        hidden = datasource.hidden_concepts
        for concept in datasource.output_concepts:
            if concept.address in hidden:
                continue
            pseudonym_graph.add_node(concept.address)
            for pseudo_addr in concept.pseudonyms:
                pseudonym_graph.add_edge(concept.address, pseudo_addr)
                origin = environment.alias_origin_lookup.get(pseudo_addr)
                if origin is not None and origin.address != pseudo_addr:
                    pseudonym_graph.add_edge(pseudo_addr, origin.address)

    canonical: dict[str, str] = {}
    for component in nx.connected_components(pseudonym_graph):
        root = min(component, key=lambda a: (a in environment.alias_origin_lookup, a))
        for address in component:
            canonical[address] = root
    return canonical


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
    # never a keyless BaseJoin.
    if isinstance(ds, QueryDatasource) and ds.source_type == SourceType.UNNEST:
        return True
    outputs = ds.output_concepts
    if bool(outputs) and all(c.granularity == Granularity.SINGLE_ROW for c in outputs):
        return True
    # A global-aggregate scalar carries a SELF-grain (grain = the metric
    # itself) rather than an empty grain; with no keys there is no row axis to
    # pair on and the cross join is the plan (the `calculate_graph_relevance`
    # metric rule). A KEYED metric is a per-group aggregate with a real axis,
    # so it stays subject to the guard.
    output_by_addr = {c.address: c for c in outputs}
    return all(
        (c := output_by_addr.get(component)) is not None
        and c.purpose == Purpose.METRIC
        and not c.keys
        for component in ds.grain.components
    )


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
    over concept ``keys`` and pseudonyms, intersected with the
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
        """Direct axis plus everything reachable through concept ``keys`` and
        pseudonyms, to fixpoint: the addresses whose rows
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
            frontier.update(concept.keys or set())
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
            frontier.update(concept_ref.keys or set())
            frontier.update(concept_ref.pseudonyms)
        result = frozenset({_canon(a) for a in closure}) - rollup_padded_addresses
        closure_cache[node] = result
        return result

    tree: set[str] = set()
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
    if ds.limit is not None or ds.rollup_concepts:
        return False
    if not ds.group_required:
        return (
            not ds.joins
            and ds.condition is None
            and len(ds.datasources) == 1
            and single_row_source(ds.datasources[0])
        )
    outputs = ds.output_concepts
    if any(ds.source_map.get(c.address) for c in outputs):
        return False
    output_by_addr = {c.address: c for c in outputs}
    if not all(
        (c := output_by_addr.get(component)) is not None
        and c.purpose == Purpose.METRIC
        and not c.keys
        for component in ds.grain.components
    ):
        return False
    if not any(get_grouped_aggregate_wrapper(c) is not None for c in outputs):
        return False
    if ds.condition is None:
        return True
    materialized = {address for address, v in ds.source_map.items() if v}
    return is_scalar_condition(ds.condition, materialized=materialized)


def _narrowed_keyless_type(left_has_rows: bool, right_has_rows: bool) -> JoinType:
    if left_has_rows and right_has_rows:
        return JoinType.INNER
    if left_has_rows:
        return JoinType.LEFT_OUTER
    if right_has_rows:
        return JoinType.RIGHT_OUTER
    return JoinType.FULL


def narrow_keyless_joins(joins: list[BaseJoin | UnnestJoin]) -> None:
    """A keyless FULL (``ON 1=1``) pairs every row with every row, so FULL
    only differs from INNER when a side is EMPTY. Walk the joins in order
    carrying whether the relation built so far provably has rows. While only
    keyless FULL joins precede, a join's explicit left is part of that
    relation and its rows count; a keyed or unnest join can empty the
    relation, so after one only the keyless right sides accumulate."""
    left_has_rows = False
    keyed_seen = False
    for join in joins:
        if (
            not isinstance(join, BaseJoin)
            or join.join_type != JoinType.FULL
            or join.concept_pairs
            or join.concepts
        ):
            left_has_rows = False
            keyed_seen = True
            continue
        if join.left_datasource is not None and not keyed_seen:
            left_has_rows = left_has_rows or single_row_source(join.left_datasource)
        right_has_rows = single_row_source(join.right_datasource)
        join.join_type = _narrowed_keyless_type(left_has_rows, right_has_rows)
        left_has_rows = left_has_rows or right_has_rows


def _padding_sources(
    side: DataSource, keys: set[str], canon: Callable[[str], str]
) -> set[str]:
    """Identifiers of the sources at or above `side` whose own rows carry the
    key as join-analysis padding. A leaf datasource never pads: a NULL in its
    column is a value, which the caller has already exempted."""
    found: set[str] = set()
    if not isinstance(side, QueryDatasource):
        return found
    if keys & {canon(c.address) for c in side.nullable_concepts}:
        found.add(side.identifier)
    for parent in side.datasources:
        found |= _padding_sources(parent, keys, canon)
    return found


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
) -> dict[str, dict[str, frozenset[str]]]:
    """Per side, per nullable key: the spans whose extension rows NULL it."""
    span_memos: dict[str, dict[int, frozenset[str]]] = {
        spelling: {} for spelling in sorted(spellings)
    }
    out: dict[str, dict[str, frozenset[str]]] = {}
    for ds_node, datasource in ds_node_map.items():
        by_key: dict[str, set[str]] = defaultdict(set)
        for spelling, span_memo in span_memos.items():
            for address in _span_padded_addresses(datasource, spelling, span_memo):
                by_key[canon_node(address)].add(spellings[spelling])
        out[ds_node] = {
            key: frozenset(found)
            for key, found in by_key.items()
            if key in nullables[ds_node]
        }
    return out


def _region_padded_sides(
    joins: list[JoinOrderOutput], facts: JoinFacts
) -> dict[str, frozenset[str]]:
    """Each side an earlier join of the merge null-extends, with the region
    spans the side preserved over it holds: the side's columns are NULL on
    those regions' rows in the joined stream, whatever the side says of
    itself."""
    padded: dict[str, frozenset[str]] = {}
    for join in joins:
        if join.type not in PADS_RIGHT_JOIN_TYPES:
            continue
        held: frozenset[str] = frozenset().union(
            *(facts.side(left).held_spans for left in join.keys)
        )
        if held:
            padded[join.right] = held
    return padded


def _pairs_region_padding(
    padded_for: frozenset[str], right: SideFacts, key: str
) -> bool:
    """The left key is NULL on rows padded for a region `right` holds too: an
    aggregate over the region's rows grouped by a key absent there
    (`count(customer_id) by status`) puts them in its NULL group. The two
    NULLs are the same rows and pair."""
    return bool(padded_for & right.held_spans) and key in right.nullables


def get_node_joins(
    datasources: list[DataSource],
    environment: BuildEnvironment,
    host_grain: set[str] | None = None,
    demanded_domains: Collection[str] = frozenset(),
    extent_free_spans: frozenset[str] = frozenset(),
    keyspace: Keyspace | None = None,
) -> list[BaseJoin]:
    """`keyspace` is the plan's, for the spans it can pad for, the spellings a
    rowset body pads them under and its region partition."""
    keyspace = keyspace or Keyspace()
    from trilogy.core import graph as nx

    canonical = build_canonical_address_map(datasources, environment)

    def canon_node(address: str) -> str:
        return f"c~{canonical.get(address, address)}"

    graph = nx.Graph()
    extent_memo: dict[int, frozenset[str]] = {}
    guest_memo: dict[int, frozenset[str]] = {}
    pad_memo: dict[int, frozenset[str]] = {}
    driven_memo: dict[int, bool] = {}
    ds_node_map: dict[str, DataSource] = {}
    ds_concept_map: dict[tuple[str, str], BuildConcept] = {}
    sides: dict[str, SideFacts] = {}
    # Hosting a domain requires binding it COMPLETELY: a side carrying a `~`
    # key only partially (the fact's FK column) exposes the address but not the
    # domain, so it can never out-host the preserved span. Own-level marks, not
    # the deep collection: a span that completed a key against its dimension
    # clears its own mark while the raw fact scan below it keeps one.
    host_canon = {canon_node(a) for a in host_grain} if host_grain else None

    for datasource in datasources:
        ds_node = f"ds~{datasource.identifier}"
        ds_node_map[ds_node] = datasource
        graph.add_node(ds_node, type=NodeType.NODE)
        partial_nodes = {canon_node(c.address) for c in datasource.partial_concepts}
        # A LEFT scoped join on a derived key has no datasource column binding
        # to carry Modifier.PARTIAL. The merge keeps that key as a distinct
        # output present ONLY on the partial side (the complete side outputs
        # the canonical), so intersecting outputs with scoped_partial_derived
        # marks exactly the partial side. Root/rowset partial keys carry
        # partiality through the column-partial / rowset machinery and are
        # excluded, since a rowset key also survives as a distinct output.
        if environment.scoped_partial_derived:
            partial_nodes |= {
                canon_node(c.address)
                for c in datasource.output_concepts
                if c.address in environment.scoped_partial_derived
            }
        nullable_nodes = {canon_node(c.address) for c in datasource.nullable_concepts}
        if extent_free_spans:
            nullable_nodes -= {
                canon_node(a)
                for a in extension_padded_addresses(
                    datasource, extent_free_spans, pad_memo
                )
            }
        padded_nodes = {canon_node(a) for a in rollup_padded_addresses(datasource)}
        partial_keys: set[str] = set()
        nullable_keys: set[str] = set()
        rollup_keys: set[str] = set()
        value_keys: set[str] = set()
        for concept in datasource.output_concepts:
            if concept.address in datasource.hidden_concepts:
                continue
            node = canon_node(concept.address)
            graph.add_node(node, type=NodeType.CONCEPT)
            graph.add_edge(ds_node, node)
            ds_concept_map.setdefault((ds_node, node), concept)
            if node in partial_nodes:
                partial_keys.add(node)
            # the FIRST concept spelling the node decides the provenance
            if node in nullable_nodes and node not in nullable_keys:
                nullable_keys.add(node)
                if nulls_are_values(concept, datasource):
                    value_keys.add(node)
            if node in padded_nodes:
                rollup_keys.add(node)
        extent_addrs = {
            canon_node(a)
            for a in extent_null_addresses(datasource, extent_memo, driven_memo)
        }
        guest_addrs = {
            canon_node(a)
            for a in guest_padded_addresses(datasource, guest_memo, driven_memo)
        }
        # A side holding a region's rows (its domain, or whatever read it) hosts
        # that region's extension rows on the join keyed by its span, whatever
        # columns it emits: the contract, not an inference from the bindings.
        # Not on a span this merge is built not to extend (a rowset body whose
        # reader holds the region): those rows are not this plan's to return.
        held_spans = {
            canon_node(span)
            for span in held_region_spans(datasource)
            if span not in extent_free_spans
        }
        # A rowset handle is bound as its content is: a boundary partial on
        # `user_id` is partial on `even.user_id` too, or a filtered body
        # out-hosts the complete dimension scan and the FINAL sheds the rows
        # the statement's bare key demands.
        partial_addresses = {c.address for c in datasource.partial_concepts}
        hosts = host_canon is not None and host_canon <= (
            {canon_node(c.address) for c in datasource.output_concepts}
            - {canon_node(a) for a in partial_addresses}
            - {
                canon_node(c.address)
                for c in datasource.output_concepts
                if isinstance(c.lineage, BuildRowsetItem)
                and c.lineage.content.address in partial_addresses
            }
        )
        sides[ds_node] = SideFacts(
            partials=frozenset(partial_keys),
            nullables=frozenset(nullable_keys),
            value_nullables=frozenset(value_keys),
            extent_nullables=frozenset(nullable_keys & extent_addrs),
            guest_padded=frozenset(nullable_keys & guest_addrs),
            rollup_padded=frozenset(rollup_keys),
            grain=frozenset(canon_node(a) for a in datasource.grain.components),
            hosts=hosts,
            held_spans=frozenset(held_spans),
            complete_spans=frozenset(
                canon_node(address)
                for address in extent_free_spans
                if complete_key_domain(datasource, address)
            ),
            span_bindings={
                canon_node(address): partial_binding_sources(datasource, address)
                for address in extent_free_spans
            },
        )

    # Beside the region contract: a FULL join between two families' padding
    # must not pair NULL with NULL null-safely.
    if sum(1 for side in sides.values() if side.nullables) > 1:
        matrix = _span_padding_matrix(
            ds_node_map,
            {ds_node: side.nullables for ds_node, side in sides.items()},
            _span_spellings(keyspace.in_play_spans, environment, keyspace.witnessed),
            canon_node,
        )
        sides = {
            ds_node: replace(side, span_padding=matrix.get(ds_node, {}))
            for ds_node, side in sides.items()
        }

    # Local import: common.py imports nodes.merge_node, which imports this module.
    from trilogy.core.processing.node_generators.common import (
        authored_join_pair_candidates,
    )

    facts = JoinFacts(
        sides=sides,
        full_join_keys=frozenset(
            canon_node(a) for a in environment.domain_graph.outer_relation_keys()
        ),
        scoped_keys=frozenset(canon_node(a) for a in environment.scoped_partial_derived)
        | frozenset(
            canon_node(a)
            for group_canonical, members in environment.scoped_join_key_groups.items()
            for a in (group_canonical, *members)
        ),
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
    )
    joins = resolve_join_order_v2(graph, facts)
    region_padded = _region_padded_sides(joins, facts)
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
    return [
        BaseJoin(
            left_datasource=ds_node_map[j.left] if j.left else None,
            right_datasource=ds_node_map[j.right],
            join_type=j.type,
            concepts=[] if not j.keys else None,
            concept_pairs=reduce_concept_pairs(
                [
                    ConceptPair(
                        left=ds_concept_map[(k, concept)],
                        right=ds_concept_map[(j.right, concept)],
                        existing_datasource=ds_node_map[k],
                        modifiers=(
                            []
                            if _pads_for_different_members(
                                facts.side(k), facts.side(j.right), v
                            )
                            else (
                                [Modifier.NULLABLE]
                                if _pairs_region_padding(
                                    region_padded.get(k, frozenset()),
                                    facts.side(j.right),
                                    concept,
                                )
                                else get_modifiers(
                                    ds_concept_map[(k, concept)],
                                    ds_concept_map[(j.right, concept)],
                                    ds_node_map[k],
                                    ds_node_map[j.right],
                                )
                            )
                        )
                        + (
                            [Modifier.PARTIAL]
                            if concept in facts.side(k).partials
                            else []
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
            ),
        )
        for j in joins
    ]
